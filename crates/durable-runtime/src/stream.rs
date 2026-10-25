//! Writer side of transient user streams.
//!
//! A step obtains a [`StreamWriter`] from [`StepInfo::stream`]. Writes are
//! buffered per (run, stream) and flushed in batches by a small background
//! task, so a step never waits on the stream store. Streams are transient by
//! design: a failed flush is logged and the step carries on. The engine
//! drains a step's streams before it commits the step's result, so readers
//! see every chunk before the step's event shows up on `watch`.

use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use durable_core::{Backend, Error, Metric, NewChunk, Observer, PubSub, RunId, StreamStore};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::{runtime::Handle, sync::Notify};

use crate::{StepError, channels, step::StepInfo};

/// Chunks that trigger a flush before the interval elapses.
const FLUSH_BATCH: usize = 64;
/// Upper bound on how long the engine waits to drain a step's streams.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct StreamLimits {
    pub flush_interval: Duration,
    /// Largest serialized chunk.
    pub max_chunk_bytes: usize,
    /// Bytes a stream may hold unflushed before writes are refused.
    pub max_buffered_bytes: usize,
}

#[derive(Default)]
struct Buf {
    chunks: Vec<NewChunk>,
    bytes: usize,
    closed: bool,
}

struct Channel {
    run_id: RunId,
    name: String,
    store: Arc<dyn StreamStore>,
    pubsub: Arc<dyn PubSub>,
    observer: Arc<dyn Observer>,
    buf: Mutex<Buf>,
    /// Serializes flushes so batches reach the store in buffer order.
    flush_lock: tokio::sync::Mutex<()>,
    /// The buffer went from empty to non-empty.
    wake: Notify,
    /// The buffer reached [`FLUSH_BATCH`].
    full: Notify,
    stop: AtomicBool,
}

impl Channel {
    fn push(&self, chunk: NewChunk, bytes: usize, cap: Option<usize>) -> Result<(), StepError> {
        let mut b = self.buf.lock();
        if b.closed {
            return Err(StepError::fatal(format!(
                "stream `{}` is closed",
                self.name
            )));
        }
        if let Some(cap) = cap
            && b.bytes + bytes > cap
        {
            return Err(StepError::retryable(format!(
                "stream `{}` buffer is full ({cap} bytes unflushed); the stream store is not keeping up",
                self.name
            )));
        }
        let was_empty = b.chunks.is_empty();
        b.chunks.push(chunk);
        b.bytes += bytes;
        let len = b.chunks.len();
        drop(b);
        if was_empty {
            self.wake.notify_one();
        }
        if len >= FLUSH_BATCH {
            self.full.notify_one();
        }
        Ok(())
    }

    /// Append everything buffered. Returns false when the append failed.
    async fn flush(&self) -> bool {
        self.flush_with(None).await
    }

    /// Append everything buffered, followed by `tail` when given. The buffer
    /// is only given up once the append returns: a flush cancelled midway (a
    /// drain timeout, say) puts its chunks back, so a later flush retries
    /// them. That may duplicate a batch the store did take; readers handle
    /// a repeated chunk better than a lost `AttemptFailed`.
    async fn flush_with(&self, tail: Option<NewChunk>) -> bool {
        let _serial = self.flush_lock.lock().await;
        let mut pending = Pending::take(self);
        if let Some(tail) = tail {
            pending.chunks.push(tail);
        }
        if pending.chunks.is_empty() {
            return true;
        }
        let n = pending.chunks.len();
        let bytes = pending.bytes;
        let started = Instant::now();
        let result = self
            .store
            .append(self.run_id, &self.name, pending.chunks.clone())
            .await;
        // The store answered: whatever happened, these chunks are handled.
        pending.chunks.clear();
        let ok = result.is_ok();
        self.observer.record(&Metric::StreamFlush {
            chunks: n,
            bytes,
            duration: started.elapsed(),
            ok,
        });
        match result {
            Ok(_) => {
                channels::notify(
                    &*self.pubsub,
                    &channels::stream(self.run_id, &self.name),
                    vec![],
                )
                .await;
            }
            Err(Error::Closed) => {
                self.buf.lock().closed = true;
                tracing::warn!(
                    run_id = %self.run_id, stream = %self.name,
                    "stream was closed; dropping {n} chunks"
                );
            }
            Err(e) => tracing::warn!(
                run_id = %self.run_id, stream = %self.name,
                "stream flush failed, dropping {n} chunks: {e}"
            ),
        }
        ok
    }

    /// Mark the stream closed and append what is buffered plus `Closed` in
    /// one batch, so no write can slip in between the two.
    async fn close(&self) {
        {
            let mut b = self.buf.lock();
            if b.closed {
                return;
            }
            b.closed = true;
        }
        self.flush_with(Some(NewChunk::closed())).await;
    }
}

/// Chunks taken out of a channel's buffer for one flush. Dropped before the
/// store answered (the flush was cancelled), it returns them to the front
/// of the buffer, ahead of anything written since.
struct Pending<'a> {
    ch: &'a Channel,
    chunks: Vec<NewChunk>,
    bytes: usize,
}

impl<'a> Pending<'a> {
    fn take(ch: &'a Channel) -> Self {
        let mut b = ch.buf.lock();
        Self {
            ch,
            chunks: std::mem::take(&mut b.chunks),
            bytes: std::mem::take(&mut b.bytes),
        }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.chunks.is_empty() {
            return;
        }
        let mut b = self.ch.buf.lock();
        let newer = std::mem::replace(&mut b.chunks, std::mem::take(&mut self.chunks));
        b.chunks.extend(newer);
        b.bytes += self.bytes;
    }
}

/// The background task of one channel: batch writes, then flush.
async fn flusher(ch: Arc<Channel>, interval: Duration) {
    loop {
        // The timeout lets an orphaned flusher notice nobody uses it anymore.
        let woke = tokio::time::timeout(Duration::from_secs(5), ch.wake.notified()).await;
        if ch.stop.load(Ordering::SeqCst) || Arc::strong_count(&ch) == 1 {
            ch.flush().await;
            return;
        }
        if woke.is_err() {
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = ch.full.notified() => {}
        }
        ch.flush().await;
    }
}

/// Stream state of one execution of one run.
pub(crate) struct StreamHub {
    run_id: RunId,
    store: Option<Arc<dyn StreamStore>>,
    pubsub: Arc<dyn PubSub>,
    observer: Arc<dyn Observer>,
    /// `None` outside a tokio runtime; streams then stay unconfigured.
    handle: Option<Handle>,
    limits: StreamLimits,
    /// Set once per worker, so a missing stream store is reported once.
    warned: Arc<AtomicBool>,
    channels: Mutex<HashMap<String, Arc<Channel>>>,
    /// Streams each step attempt wrote to.
    touched: Mutex<HashMap<(String, u32), BTreeSet<String>>>,
}

impl StreamHub {
    pub fn new(
        backend: &Backend,
        run_id: RunId,
        limits: StreamLimits,
        warned: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            run_id,
            store: backend.streams.clone(),
            pubsub: backend.pubsub.clone(),
            observer: backend.observer.clone(),
            handle: Handle::try_current().ok(),
            limits,
            warned,
            channels: Mutex::default(),
            touched: Mutex::default(),
        })
    }

    /// A hub with no stream store, for unit tests of `StepInfo`.
    #[cfg(test)]
    pub fn detached() -> Arc<Self> {
        Arc::new(Self {
            run_id: RunId::nil(),
            store: None,
            pubsub: Arc::new(NullPubSub),
            observer: Arc::new(durable_core::NoopObserver),
            handle: None,
            limits: StreamLimits {
                flush_interval: Duration::from_millis(25),
                max_chunk_bytes: usize::MAX,
                max_buffered_bytes: usize::MAX,
            },
            warned: Arc::default(),
            channels: Mutex::default(),
            touched: Mutex::default(),
        })
    }

    fn channel(&self, name: &str) -> Option<Arc<Channel>> {
        let (store, handle) = (self.store.clone()?, self.handle.as_ref()?);
        let mut map = self.channels.lock();
        if let Some(ch) = map.get(name) {
            return Some(ch.clone());
        }
        let ch = Arc::new(Channel {
            run_id: self.run_id,
            name: name.to_string(),
            store,
            pubsub: self.pubsub.clone(),
            observer: self.observer.clone(),
            buf: Mutex::default(),
            flush_lock: tokio::sync::Mutex::new(()),
            wake: Notify::new(),
            full: Notify::new(),
            stop: AtomicBool::new(false),
        });
        // `Handle::spawn` works from blocking steps on the compute pool too.
        handle.spawn(flusher(ch.clone(), self.limits.flush_interval));
        map.insert(name.to_string(), ch.clone());
        Some(ch)
    }

    fn warn_unconfigured(&self) {
        if !self.warned.swap(true, Ordering::SeqCst) {
            tracing::warn!(
                "step wrote to a stream but the backend has no stream store; \
                 chunks are dropped (the step's durable output is unaffected)"
            );
        }
    }

    /// First write of a step attempt to `name`: tell readers it started, so
    /// they drop what an earlier, crashed execution of the same attempt wrote.
    /// Holds the lock while pushing, so no other writer's chunk can overtake
    /// the marker.
    fn begin(&self, key: &str, attempt: u32, channel: &Channel) {
        let mut touched = self.touched.lock();
        if touched
            .entry((key.to_string(), attempt))
            .or_default()
            .insert(channel.name.clone())
        {
            let _ = channel.push(NewChunk::attempt_started(key, attempt), 0, None);
        }
    }

    /// Flush everything a step attempt wrote before its result is committed.
    /// On failure the attempt's streams also get an `AttemptFailed` marker,
    /// so readers discard its partial output.
    pub async fn finish_step(&self, key: &str, attempt: u32, failed: bool) {
        let Some(names) = self.touched.lock().remove(&(key.to_string(), attempt)) else {
            return;
        };
        let channels: Vec<Arc<Channel>> = {
            let map = self.channels.lock();
            names.iter().filter_map(|n| map.get(n).cloned()).collect()
        };
        if failed {
            for ch in &channels {
                let _ = ch.push(NewChunk::attempt_failed(key, attempt), 0, None);
            }
        }
        let flushes = futures::future::join_all(channels.iter().map(|c| c.flush()));
        if tokio::time::timeout(DRAIN_TIMEOUT, flushes).await.is_err() {
            tracing::warn!(run_id = %self.run_id, step = key, "draining stream chunks timed out");
        }
    }

    /// Final flush; stops the background tasks.
    pub async fn shutdown(&self) {
        let channels: Vec<Arc<Channel>> = self.channels.lock().drain().map(|(_, c)| c).collect();
        for ch in &channels {
            ch.stop.store(true, Ordering::SeqCst);
            ch.wake.notify_one();
        }
        let flushes = futures::future::join_all(channels.iter().map(|c| c.flush()));
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, flushes).await;
    }
}

impl Drop for StreamHub {
    /// An execution that ends without [`shutdown`](Self::shutdown) (an
    /// error returned early) still stops its flushers right away; each one
    /// flushes what is left before it exits.
    fn drop(&mut self) {
        for ch in self.channels.get_mut().values() {
            ch.stop.store(true, Ordering::SeqCst);
            ch.wake.notify_one();
            // Cut short a flusher waiting out its interval.
            ch.full.notify_one();
        }
    }
}

/// Close every stream of a finished run so readers stop waiting. Best effort.
pub(crate) async fn close_run_streams(backend: &Backend, run_id: RunId) {
    let Some(store) = &backend.streams else {
        return;
    };
    let names: Vec<String> = match store.list(run_id).await {
        Ok(infos) => infos
            .into_iter()
            .filter(|i| !i.closed)
            .map(|i| i.stream)
            .collect(),
        Err(e) => {
            tracing::warn!(%run_id, "listing streams to close failed: {e}");
            return;
        }
    };
    if let Err(e) = store.close_all(run_id).await {
        tracing::warn!(%run_id, "closing streams failed: {e}");
        return;
    }
    for name in names {
        channels::notify(&*backend.pubsub, &channels::stream(run_id, &name), vec![]).await;
    }
}

/// A named stream a step writes to. Cheap to create; get it from
/// [`StepInfo::stream`].
pub struct StreamWriter<T> {
    hub: Arc<StreamHub>,
    channel: Option<Arc<Channel>>,
    step_key: String,
    attempt: u32,
    touched: AtomicBool,
    _item: PhantomData<fn(&T)>,
}

impl StepInfo {
    /// A writer for the stream `name` of the current run. Works in async and
    /// blocking steps. Chunks are transient: they are kept for a TTL, tagged
    /// with this step and attempt, and are *not* part of the run's durable
    /// state. Return the final answer from the step as usual.
    pub fn stream<T: Serialize>(&self, name: &str) -> StreamWriter<T> {
        StreamWriter {
            channel: self.streams.channel(name),
            hub: self.streams.clone(),
            step_key: self.step_key.clone(),
            attempt: self.attempt,
            touched: AtomicBool::new(false),
            _item: PhantomData,
        }
    }
}

impl<T: Serialize> StreamWriter<T> {
    /// Buffer one chunk. Never blocks and never waits on the stream store.
    /// Fails only for a chunk over `max_payload_bytes` or when the unflushed
    /// buffer is full. With no stream store configured it does nothing.
    pub fn write(&self, item: &T) -> Result<(), StepError> {
        let Some(ch) = &self.channel else {
            self.hub.warn_unconfigured();
            return Ok(());
        };
        let not_serializable = |e: serde_json::Error| {
            StepError::fatal(format!("stream chunk is not serializable: {e}"))
        };
        let value = serde_json::to_value(item).map_err(not_serializable)?;
        let bytes = json_len(&value);
        if bytes > self.hub.limits.max_chunk_bytes {
            return Err(StepError::fatal(format!(
                "stream chunk of {bytes} bytes exceeds max_payload_bytes ({})",
                self.hub.limits.max_chunk_bytes
            )));
        }
        if !self.touched.swap(true, Ordering::SeqCst) {
            self.hub.begin(&self.step_key, self.attempt, ch);
        }
        ch.push(
            NewChunk::data(self.step_key.clone(), self.attempt, value),
            bytes,
            Some(self.hub.limits.max_buffered_bytes),
        )
    }

    /// Write every item of `stream` (an LLM SDK stream, say) and return them,
    /// so the step can build its durable result, for example the concatenated
    /// text.
    pub async fn forward<S>(&self, stream: S) -> Result<Vec<T>, StepError>
    where
        S: Stream<Item = T>,
    {
        let mut stream = std::pin::pin!(stream);
        let mut out = vec![];
        while let Some(item) = stream.next().await {
            self.write(&item)?;
            out.push(item);
        }
        Ok(out)
    }

    /// Finish the stream now: flush and close it, so readers end without
    /// waiting for the run to finish. Optional. Later writes to it fail.
    pub async fn close(&self) {
        if let Some(ch) = &self.channel {
            ch.close().await;
        }
    }
}

/// The length of `value` serialized as JSON, without building the string.
fn json_len(value: &serde_json::Value) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    // Writing a `Value` to an infallible writer cannot fail.
    let _ = serde_json::to_writer(&mut count, value);
    count.0
}

#[cfg(test)]
struct NullPubSub;

#[cfg(test)]
#[async_trait::async_trait]
impl PubSub for NullPubSub {
    async fn publish(&self, _: &str, _: Vec<u8>) -> durable_core::Result<()> {
        Ok(())
    }
    async fn subscribe(
        &self,
        _: &str,
    ) -> durable_core::Result<futures::stream::BoxStream<'static, Vec<u8>>> {
        Ok(futures::stream::empty().boxed())
    }
}

#[cfg(test)]
mod tests {
    use durable_core::{ChunkBody, StreamChunk, StreamCursor, StreamInfo};
    use tokio::sync::watch;

    use serde_json::Value;

    use super::*;

    /// Records every append. While the gate is shut, appends wait for it.
    struct GatedStore {
        batches: Mutex<Vec<Vec<NewChunk>>>,
        open: watch::Sender<bool>,
    }

    impl GatedStore {
        fn new(open: bool) -> Arc<Self> {
            Arc::new(Self {
                batches: Mutex::default(),
                open: watch::channel(open).0,
            })
        }
        fn bodies(&self) -> Vec<ChunkBody> {
            self.batches
                .lock()
                .iter()
                .flatten()
                .map(|c| c.body.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl StreamStore for GatedStore {
        async fn append(
            &self,
            _: RunId,
            _: &str,
            chunks: Vec<NewChunk>,
        ) -> durable_core::Result<StreamCursor> {
            let mut open = self.open.subscribe();
            let _ = open.wait_for(|o| *o).await;
            self.batches.lock().push(chunks);
            Ok(StreamCursor(String::new()))
        }
        async fn read(
            &self,
            _: RunId,
            _: &str,
            _: Option<&StreamCursor>,
            _: usize,
        ) -> durable_core::Result<Vec<StreamChunk>> {
            Ok(vec![])
        }
        async fn info(&self, _: RunId, _: &str) -> durable_core::Result<Option<StreamInfo>> {
            Ok(None)
        }
        async fn list(&self, _: RunId) -> durable_core::Result<Vec<StreamInfo>> {
            Ok(vec![])
        }
        async fn close_all(&self, _: RunId) -> durable_core::Result<()> {
            Ok(())
        }
    }

    fn hub(store: Arc<GatedStore>, flush_interval: Duration) -> Arc<StreamHub> {
        Arc::new(StreamHub {
            store: Some(store),
            handle: Some(Handle::current()),
            limits: StreamLimits {
                flush_interval,
                max_chunk_bytes: usize::MAX,
                max_buffered_bytes: usize::MAX,
            },
            run_id: RunId::nil(),
            pubsub: Arc::new(NullPubSub),
            observer: Arc::new(durable_core::NoopObserver),
            warned: Arc::default(),
            channels: Mutex::default(),
            touched: Mutex::default(),
        })
    }

    fn info(hub: &Arc<StreamHub>) -> StepInfo {
        StepInfo {
            streams: hub.clone(),
            ..crate::step::test_info("s")
        }
    }

    fn data(s: &str) -> ChunkBody {
        ChunkBody::Data(Value::from(s))
    }

    #[test]
    fn json_len_matches_the_serialized_length() {
        for v in [
            serde_json::json!(null),
            serde_json::json!("tiếng việt \"quoted\" \n"),
            serde_json::json!({"a": [1, 2.5, {"b": true}], "c": ""}),
        ] {
            assert_eq!(json_len(&v), v.to_string().len(), "{v}");
        }
    }

    #[tokio::test]
    async fn close_appends_buffered_chunks_and_closed_in_one_batch() {
        let store = GatedStore::new(true);
        let hub = hub(store.clone(), Duration::from_secs(60));
        let info = info(&hub);
        let w = info.stream::<&str>("out");
        w.write(&"a").unwrap();
        w.close().await;
        let last = store.batches.lock().last().cloned().unwrap();
        let bodies: Vec<_> = last.into_iter().map(|c| c.body).collect();
        assert_eq!(
            bodies,
            [ChunkBody::AttemptStarted, data("a"), ChunkBody::Closed]
        );
        assert!(w.write(&"late").is_err(), "writes after close fail");
    }

    #[tokio::test]
    async fn write_racing_close_is_either_kept_or_refused() {
        // A flush is in flight, so close() has to wait for the flush lock.
        let store = GatedStore::new(false);
        let hub = hub(store.clone(), Duration::from_secs(60));
        let info = info(&hub);
        let w = Arc::new(info.stream::<&str>("out"));
        w.write(&"a").unwrap();
        let ch = w.channel.clone().unwrap();
        let in_flight = tokio::spawn({
            let ch = ch.clone();
            async move { ch.flush().await }
        });
        tokio::task::yield_now().await;
        w.write(&"b").unwrap();
        let closing = tokio::spawn({
            let w = w.clone();
            async move { w.close().await }
        });
        tokio::task::yield_now().await;
        assert!(
            w.write(&"c").is_err(),
            "a write once close() began is refused"
        );
        store.open.send_replace(true);
        in_flight.await.unwrap();
        closing.await.unwrap();
        assert_eq!(
            store.bodies(),
            [
                ChunkBody::AttemptStarted,
                data("a"),
                data("b"),
                ChunkBody::Closed
            ]
        );
    }

    #[tokio::test]
    async fn cancelled_flush_keeps_its_chunks() {
        let store = GatedStore::new(false);
        let hub = hub(store.clone(), Duration::from_secs(60));
        let info = info(&hub);
        let w = info.stream::<&str>("out");
        w.write(&"a").unwrap();
        w.write(&"b").unwrap();
        // The step failed, and draining its streams hits DRAIN_TIMEOUT.
        hub.finish_step("s", 1, true).await;
        let ch = w.channel.clone().unwrap();
        assert!(store.bodies().is_empty());
        assert_eq!(
            ch.buf.lock().chunks.len(),
            4,
            "chunks are back in the buffer"
        );
        w.write(&"c").unwrap();
        store.open.send_replace(true);
        assert!(ch.flush().await);
        assert_eq!(
            store.bodies(),
            [
                ChunkBody::AttemptStarted,
                data("a"),
                data("b"),
                ChunkBody::AttemptFailed,
                data("c"),
            ]
        );
    }

    #[tokio::test]
    async fn dropping_the_hub_flushes_without_waiting_out_the_interval() {
        let store = GatedStore::new(true);
        let hub = hub(store.clone(), Duration::from_secs(60));
        let info = info(&hub);
        info.stream::<&str>("out").write(&"a").unwrap();
        // Let the flusher pick up the write and start waiting out the interval.
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(info);
        drop(hub);
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.bodies().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the flusher flushed once the hub was gone");
        assert_eq!(store.bodies(), [ChunkBody::AttemptStarted, data("a")]);
    }
}
