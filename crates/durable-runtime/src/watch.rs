//! Live feeds for clients: a run's events (`watch`, durable, built on the
//! event log) and a user stream's chunks (`stream`, transient).

use std::{
    collections::{HashSet, VecDeque},
    future::IntoFuture,
    marker::PhantomData,
    time::Duration,
};

use durable_spi::{Backend, ChunkBody, Event, RunId, StreamCursor};
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{RunError, channels};

/// Fallback when pub/sub misses a notice.
const POLL: Duration = Duration::from_millis(200);
const PAGE: usize = 256;

/// One item of a run's live feed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunUpdate {
    /// A committed event, delivered in order exactly once.
    Event(Event),
    /// A step attempt just started. Best effort: it may be skipped, and is
    /// never replayed when resuming with `after`.
    StepStarted { key: String, attempt: u32 },
}

async fn require_run(backend: &Backend, id: RunId) -> Result<durable_spi::Run, RunError> {
    backend
        .store
        .get_run(id)
        .await?
        .filter(|r| r.namespace == backend.namespace)
        .ok_or(RunError::NotFound)
}

/// Builder for [`Client::watch`](crate::Client::watch). Await it to get the feed.
pub struct WatchBuilder {
    pub(crate) backend: Backend,
    pub(crate) id: RunId,
    pub(crate) after: u64,
}

impl WatchBuilder {
    /// Resume after this event seq (events with a higher seq are delivered).
    pub fn after(mut self, seq: u64) -> Self {
        self.after = seq;
        self
    }
}

type UpdateStream = BoxStream<'static, Result<RunUpdate, RunError>>;

impl IntoFuture for WatchBuilder {
    /// The feed ends after the run's terminal event.
    type Output = Result<UpdateStream, RunError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            require_run(&self.backend, self.id).await?;
            // Subscribe before loading, so nothing between the two is missed.
            let sub = self
                .backend
                .pubsub
                .subscribe(&channels::run(self.id))
                .await
                .ok();
            let state = WatchState {
                backend: self.backend,
                id: self.id,
                last_seq: self.after,
                sub,
                queue: VecDeque::new(),
                done: false,
                succeeded: HashSet::new(),
                failed: HashSet::new(),
                final_pass: false,
                checked_run: false,
            };
            Ok(stream::unfold(state, WatchState::next).boxed())
        })
    }
}

struct WatchState {
    backend: Backend,
    id: RunId,
    last_seq: u64,
    sub: Option<BoxStream<'static, Vec<u8>>>,
    queue: VecDeque<RunUpdate>,
    done: bool,
    succeeded: HashSet<String>,
    failed: HashSet<(String, u32)>,
    /// The run was terminal on the previous empty read.
    final_pass: bool,
    /// The run's status was looked up. Only needed once: a watch resumed
    /// past the run's end finds it terminal right away, and any later end
    /// arrives as a terminal event.
    checked_run: bool,
}

impl WatchState {
    async fn next(mut self) -> Option<(Result<RunUpdate, RunError>, Self)> {
        loop {
            if let Some(u) = self.queue.pop_front() {
                return Some((Ok(u), self));
            }
            if self.done {
                return None;
            }
            match self.backend.store.load_events(self.id, self.last_seq).await {
                Err(e) => {
                    self.done = true;
                    return Some((Err(e.into()), self));
                }
                Ok(events) if !events.is_empty() => {
                    self.final_pass = false;
                    for ev in events {
                        self.last_seq = ev.seq;
                        match &ev.kind {
                            durable_spi::EventKind::StepSucceeded { key, .. } => {
                                self.succeeded.insert(key.clone());
                            }
                            durable_spi::EventKind::StepFailed { key, attempt, .. } => {
                                self.failed.insert((key.clone(), *attempt));
                            }
                            _ => {}
                        }
                        let terminal = ev.kind.is_terminal();
                        self.queue.push_back(RunUpdate::Event(ev));
                        if terminal {
                            self.done = true;
                            break;
                        }
                    }
                    continue;
                }
                Ok(_) if self.checked_run && !self.final_pass => {}
                Ok(_) => {
                    // Nothing new. A run that is already over has no more
                    // events to wait for (a watch resumed past its end), but
                    // look once more in case the end landed in between.
                    self.checked_run = true;
                    match self.backend.store.get_run(self.id).await {
                        Ok(Some(run)) if run.status.is_terminal() => {
                            if self.final_pass {
                                self.done = true;
                            }
                            self.final_pass = true;
                            continue;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            self.done = true;
                            return Some((Err(e.into()), self));
                        }
                    }
                }
            }
            let poll = tokio::time::sleep(POLL);
            let Some(sub) = self.sub.as_mut() else {
                poll.await;
                continue;
            };
            tokio::select! {
                msg = sub.next() => match msg {
                    Some(bytes) => {
                        if let Some(u) = self.step_started(&bytes) {
                            return Some((Ok(u), self));
                        }
                    }
                    None => self.sub = None,
                },
                _ = poll => {}
            }
        }
    }

    /// A `step_started` notice for an attempt that is still unresolved.
    fn step_started(&self, bytes: &[u8]) -> Option<RunUpdate> {
        let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        let s = v.get("step_started")?;
        let key = s.get("key")?.as_str()?.to_string();
        let attempt = u32::try_from(s.get("attempt")?.as_u64()?).ok()?;
        if self.succeeded.contains(&key) || self.failed.contains(&(key.clone(), attempt)) {
            return None;
        }
        Some(RunUpdate::StepStarted { key, attempt })
    }
}

/// What a user stream chunk carried.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum StreamBody<T> {
    Data(T),
    /// A step attempt started writing. Discard what you hold for the same
    /// [`StreamItem::step_key`]: it came from an earlier try, for example an
    /// execution that crashed and was re-run (which keeps its attempt number).
    AttemptStarted,
    /// The attempt failed: discard the chunks it wrote.
    AttemptFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamItem<T> {
    pub cursor: StreamCursor,
    pub step_key: Option<String>,
    pub attempt: u32,
    pub body: StreamBody<T>,
}

/// Builder for [`Client::stream`](crate::Client::stream). Await it to get the feed.
pub struct StreamReadBuilder<T> {
    pub(crate) backend: Backend,
    pub(crate) id: RunId,
    pub(crate) name: String,
    pub(crate) after: Option<StreamCursor>,
    pub(crate) _item: PhantomData<fn() -> T>,
}

impl<T> StreamReadBuilder<T> {
    /// Resume after this cursor (the `cursor` of the last item you handled).
    pub fn after(mut self, cursor: StreamCursor) -> Self {
        self.after = Some(cursor);
        self
    }
}

type ItemStream<T> = BoxStream<'static, Result<StreamItem<T>, RunError>>;

impl<T: DeserializeOwned + Send + 'static> IntoFuture for StreamReadBuilder<T> {
    /// The feed ends when the stream is closed. It also ends once the run is
    /// terminal and there is nothing left to read (the stream expired, was
    /// never written, or the store has none), so it never hangs.
    type Output = Result<ItemStream<T>, RunError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            require_run(&self.backend, self.id).await?;
            let sub = self
                .backend
                .pubsub
                .subscribe(&channels::stream(self.id, &self.name))
                .await
                .ok();
            let state = ReadState {
                backend: self.backend,
                id: self.id,
                name: self.name,
                after: self.after,
                sub,
                queue: VecDeque::new(),
                done: false,
                final_pass: false,
                idle: 0,
            };
            Ok(stream::unfold(state, ReadState::next).boxed())
        })
    }
}

struct ReadState<T> {
    backend: Backend,
    id: RunId,
    name: String,
    after: Option<StreamCursor>,
    sub: Option<BoxStream<'static, Vec<u8>>>,
    queue: VecDeque<Result<StreamItem<T>, RunError>>,
    done: bool,
    /// The run was terminal on the previous empty read.
    final_pass: bool,
    /// Empty reads since the run's status was last looked up.
    idle: u32,
}

/// Empty reads between run status lookups while a stream is idle. The
/// stream normally ends on its `Closed` marker; the lookup only covers a
/// stream that expired or a store that lost it.
const RUN_CHECK_EVERY: u32 = 10;

impl<T: DeserializeOwned + Send + 'static> ReadState<T> {
    async fn next(mut self) -> Option<(Result<StreamItem<T>, RunError>, Self)> {
        loop {
            if let Some(item) = self.queue.pop_front() {
                return Some((item, self));
            }
            if self.done {
                return None;
            }
            let chunks = match &self.backend.streams {
                Some(store) => {
                    store
                        .read(self.id, &self.name, self.after.as_ref(), PAGE)
                        .await
                }
                None => Ok(vec![]),
            };
            let chunks = match chunks {
                Ok(c) => c,
                Err(e) => {
                    self.done = true;
                    return Some((Err(e.into()), self));
                }
            };
            if !chunks.is_empty() {
                self.final_pass = false;
                for c in chunks {
                    self.after = Some(c.cursor.clone());
                    let body = match c.body {
                        ChunkBody::Data(v) => match serde_json::from_value(v) {
                            Ok(t) => StreamBody::Data(t),
                            Err(e) => {
                                self.done = true;
                                self.queue.push_back(Err(RunError::Decode(e.to_string())));
                                break;
                            }
                        },
                        ChunkBody::AttemptStarted => StreamBody::AttemptStarted,
                        ChunkBody::AttemptFailed => StreamBody::AttemptFailed,
                        ChunkBody::Closed => {
                            self.done = true;
                            break;
                        }
                    };
                    self.queue.push_back(Ok(StreamItem {
                        cursor: c.cursor,
                        step_key: c.step_key,
                        attempt: c.attempt,
                        body,
                    }));
                }
                continue;
            }
            // Nothing new. If the run is over, read once more (the last
            // chunks were flushed before the terminal commit) and then stop.
            let check = self.final_pass || self.idle.is_multiple_of(RUN_CHECK_EVERY);
            self.idle = self.idle.wrapping_add(1);
            let status = if check {
                self.backend.store.get_run(self.id).await
            } else {
                Ok(None)
            };
            match status {
                Ok(Some(run)) if run.status.is_terminal() => {
                    if self.final_pass {
                        self.done = true;
                    }
                    self.final_pass = true;
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    self.done = true;
                    return Some((Err(e.into()), self));
                }
            }
            let poll = tokio::time::sleep(POLL);
            let Some(sub) = self.sub.as_mut() else {
                poll.await;
                continue;
            };
            tokio::select! {
                msg = sub.next() => if msg.is_none() { self.sub = None },
                _ = poll => {}
            }
        }
    }
}
