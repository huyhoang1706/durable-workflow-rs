use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::{Cache, PubSub, RateDecision, RateLimiter, Result};
use futures::{StreamExt, stream::BoxStream};
use parking_lot::Mutex;
use redis::{
    Client, Script,
    aio::{ConnectionManager, PubSubSink},
};
use tokio::{
    sync::broadcast::{self, error::RecvError},
    task::AbortHandle,
};

use crate::be;

fn prefixed(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}:{key}")
    }
}

/// Pub/sub over one shared subscriber connection.
///
/// Every `subscribe` shares a single Redis connection and messages are fanned
/// out locally. A channel stays `SUBSCRIBE`d while at least one of its streams
/// is alive and is `UNSUBSCRIBE`d when the last one is dropped. If the
/// connection drops, every open stream ends (callers fall back to polling) and
/// the next `subscribe` reconnects.
pub struct RedisPubSub {
    conn: ConnectionManager,
    prefix: String,
    hub: Arc<Hub>,
}

impl RedisPubSub {
    pub fn new(conn: ConnectionManager, client: Client) -> Self {
        Self {
            conn,
            prefix: String::new(),
            hub: Arc::new(Hub {
                client,
                conn: tokio::sync::Mutex::new(None),
                channels: Mutex::new(HashMap::new()),
                generation: AtomicU64::new(0),
            }),
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

#[async_trait]
impl PubSub for RedisPubSub {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<()> {
        let _: i64 = redis::cmd("PUBLISH")
            .arg(prefixed(&self.prefix, channel))
            .arg(payload)
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok(())
    }

    async fn subscribe(&self, channel: &str) -> Result<BoxStream<'static, Vec<u8>>> {
        self.hub.subscribe(prefixed(&self.prefix, channel)).await
    }
}

/// Messages buffered per channel for a slow stream before it skips ahead.
const CHANNEL_CAPACITY: usize = 64;

struct Hub {
    client: Client,
    /// The live subscriber connection. Held across `SUBSCRIBE`/`UNSUBSCRIBE`
    /// so the server's subscriptions and `channels` change together.
    conn: tokio::sync::Mutex<Option<SubConn>>,
    channels: Mutex<HashMap<String, Channel>>,
    generation: AtomicU64,
}

struct SubConn {
    generation: u64,
    sink: PubSubSink,
    reader: AbortHandle,
}

impl Drop for SubConn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

struct Channel {
    /// The connection the channel is subscribed on.
    generation: u64,
    tx: broadcast::Sender<Vec<u8>>,
    streams: usize,
}

impl Hub {
    async fn subscribe(self: &Arc<Self>, channel: String) -> Result<BoxStream<'static, Vec<u8>>> {
        let mut conn = self.conn.lock().await;
        if conn.is_none() {
            *conn = Some(self.connect().await?);
        }
        let live = conn.as_mut().expect("connected above");
        let generation = live.generation;
        let (rx, fresh) = {
            let mut channels = self.channels.lock();
            match channels.get_mut(&channel) {
                Some(c) if c.generation == generation => {
                    c.streams += 1;
                    (c.tx.subscribe(), false)
                }
                _ => {
                    let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
                    let c = Channel {
                        generation,
                        tx,
                        streams: 1,
                    };
                    channels.insert(channel.clone(), c);
                    (rx, true)
                }
            }
        };
        if fresh && let Err(e) = live.sink.subscribe(&channel).await {
            // The connection is most likely gone: start over on the next call.
            *conn = None;
            self.forget(generation);
            return Err(be(e));
        }
        let guard = StreamGuard {
            hub: self.clone(),
            channel,
            generation,
        };
        Ok(
            futures::stream::unfold((rx, guard), |(mut rx, guard)| async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => return Some((msg, (rx, guard))),
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => return None,
                    }
                }
            })
            .boxed(),
        )
    }

    async fn connect(self: &Arc<Self>) -> Result<SubConn> {
        let (sink, mut stream) = self.client.get_async_pubsub().await.map_err(be)?.split();
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        // Weak, so dropping the last `RedisPubSub` and stream frees the hub.
        let hub = Arc::downgrade(self);
        let reader = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let Some(hub) = hub.upgrade() else { return };
                if let Some(c) = hub.channels.lock().get(msg.get_channel_name()) {
                    let _ = c.tx.send(msg.get_payload_bytes().to_vec());
                }
            }
            if let Some(hub) = hub.upgrade() {
                hub.forget(generation);
                let mut conn = hub.conn.lock().await;
                if conn.as_ref().is_some_and(|c| c.generation == generation) {
                    *conn = None;
                }
            }
        });
        Ok(SubConn {
            generation,
            sink,
            reader: reader.abort_handle(),
        })
    }

    /// End every stream subscribed on connection `generation`.
    fn forget(&self, generation: u64) {
        self.channels
            .lock()
            .retain(|_, c| c.generation != generation);
    }

    async fn unsubscribe(&self, channel: String, generation: u64) {
        let mut conn = self.conn.lock().await;
        // A new stream may have re-subscribed since the last one was dropped.
        if self.channels.lock().contains_key(&channel) {
            return;
        }
        if let Some(live) = conn.as_mut().filter(|c| c.generation == generation) {
            let _ = live.sink.unsubscribe(&channel).await;
        }
    }
}

/// Unsubscribes the channel once its last stream is dropped.
struct StreamGuard {
    hub: Arc<Hub>,
    channel: String,
    generation: u64,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let last = {
            let mut channels = self.hub.channels.lock();
            match channels.get_mut(&self.channel) {
                Some(c) if c.generation == self.generation => {
                    c.streams -= 1;
                    let last = c.streams == 0;
                    if last {
                        channels.remove(&self.channel);
                    }
                    last
                }
                _ => false,
            }
        };
        if last && let Ok(rt) = tokio::runtime::Handle::try_current() {
            let (hub, channel) = (self.hub.clone(), std::mem::take(&mut self.channel));
            let generation = self.generation;
            rt.spawn(async move { hub.unsubscribe(channel, generation).await });
        }
    }
}

/// KV with TTL, counters and `SET NX PX` locks.
pub struct RedisCache {
    conn: ConnectionManager,
    prefix: String,
    incr: Script,
}

impl RedisCache {
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: String::new(),
            incr: Script::new(include_str!("lua/incr.lua")),
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn key(&self, key: &str) -> String {
        prefixed(&self.prefix, key)
    }
}

#[async_trait]
impl Cache for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        redis::cmd("GET")
            .arg(self.key(key))
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let mut cmd = redis::cmd("SET");
        cmd.arg(self.key(key)).arg(value);
        if let Some(ttl) = ttl {
            cmd.arg("PX").arg(ttl.as_millis().max(1) as u64);
        }
        cmd.query_async::<()>(&mut self.conn.clone())
            .await
            .map_err(be)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(self.key(key))
            .query_async::<()>(&mut self.conn.clone())
            .await
            .map_err(be)
    }

    async fn incr(&self, key: &str, ttl: Option<Duration>) -> Result<i64> {
        self.incr
            .key(self.key(key))
            .arg(ttl.map_or(0, |t| t.as_millis().max(1) as u64))
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)
    }

    async fn try_lock(&self, key: &str, ttl: Duration) -> Result<bool> {
        let reply: Option<String> = redis::cmd("SET")
            .arg(self.key(key))
            .arg(1)
            .arg("NX")
            .arg("PX")
            .arg(ttl.as_millis().max(1) as u64)
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok(reply.is_some())
    }
}

/// GCRA rate limiter for start-time limits.
pub struct RedisRateLimiter {
    conn: ConnectionManager,
    prefix: String,
    gcra: Script,
}

impl RedisRateLimiter {
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: String::new(),
            gcra: Script::new(include_str!("lua/gcra.lua")),
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    async fn check(
        &self,
        key: &str,
        limit: u32,
        period: Duration,
        now: DateTime<Utc>,
    ) -> Result<RateDecision> {
        let (allowed, retry_at): (i64, i64) = self
            .gcra
            .key(prefixed(&self.prefix, key))
            .arg(now.timestamp_millis())
            .arg(limit)
            .arg(period.as_millis() as u64)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok(if allowed == 1 {
            RateDecision::Allowed
        } else {
            RateDecision::Denied {
                retry_at: DateTime::from_timestamp_millis(retry_at).expect("timestamp in range"),
            }
        })
    }
}
