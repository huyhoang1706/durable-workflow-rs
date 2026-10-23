use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::{Cache, PubSub, RateDecision, RateLimiter, Result};
use futures::{StreamExt, stream::BoxStream};
use parking_lot::Mutex;
use tokio::{sync::broadcast, time::Instant};

use crate::gcra::gcra;

#[derive(Default)]
pub struct MemoryPubSub {
    channels: Mutex<HashMap<String, broadcast::Sender<Vec<u8>>>>,
}

impl MemoryPubSub {
    pub fn new() -> Self {
        Self::default()
    }

    fn sender(&self, channel: &str) -> broadcast::Sender<Vec<u8>> {
        self.channels
            .lock()
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }
}

#[async_trait]
impl PubSub for MemoryPubSub {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<()> {
        // No subscribers is fine: pub/sub is best-effort.
        let _ = self.sender(channel).send(payload);
        Ok(())
    }

    async fn subscribe(&self, channel: &str) -> Result<BoxStream<'static, Vec<u8>>> {
        let rx = self.sender(channel).subscribe();
        Ok(broadcast_stream(rx).boxed())
    }
}

#[derive(Default)]
pub struct MemoryCache {
    entries: Mutex<HashMap<String, Entry>>,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self::default()
    }
}

type Entry = (Vec<u8>, Option<Instant>);

fn live(entry: &Entry) -> bool {
    entry.1.is_none_or(|t| t > Instant::now())
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .entries
            .lock()
            .get(key)
            .filter(|e| live(e))
            .map(|e| e.0.clone()))
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let expiry = ttl.map(|d| Instant::now() + d);
        self.entries.lock().insert(key.to_string(), (value, expiry));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.entries.lock().remove(key);
        Ok(())
    }

    async fn incr(&self, key: &str, ttl: Option<Duration>) -> Result<i64> {
        let mut map = self.entries.lock();
        let (current, expiry) = match map.get(key).filter(|e| live(e)) {
            Some((v, exp)) => (String::from_utf8_lossy(v).parse::<i64>().unwrap_or(0), *exp),
            None => (0, ttl.map(|d| Instant::now() + d)),
        };
        let next = current + 1;
        map.insert(key.to_string(), (next.to_string().into_bytes(), expiry));
        Ok(next)
    }

    async fn try_lock(&self, key: &str, ttl: Duration) -> Result<bool> {
        let mut map = self.entries.lock();
        if map.get(key).is_some_and(live) {
            return Ok(false);
        }
        map.insert(key.to_string(), (vec![], Some(Instant::now() + ttl)));
        Ok(true)
    }
}

#[derive(Default)]
pub struct MemoryRateLimiter {
    tats: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl MemoryRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RateLimiter for MemoryRateLimiter {
    async fn check(
        &self,
        key: &str,
        limit: u32,
        period: Duration,
        now: DateTime<Utc>,
    ) -> Result<RateDecision> {
        let mut tats = self.tats.lock();
        match gcra(tats.get(key).copied(), now, limit, period, limit) {
            Ok(tat) => {
                tats.insert(key.to_string(), tat);
                Ok(RateDecision::Allowed)
            }
            Err(retry_at) => Ok(RateDecision::Denied { retry_at }),
        }
    }
}

/// Adapts a broadcast receiver to a stream, without pulling in `tokio-stream`.
fn broadcast_stream<T: Clone + Send + 'static>(
    rx: broadcast::Receiver<T>,
) -> impl futures::Stream<Item = T> + Send + 'static {
    futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(v) => return Some((v, rx)),
                // A slow subscriber skips; wake-ups are idempotent.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pubsub_delivers_to_subscribers() {
        let ps = MemoryPubSub::new();
        let mut sub = ps.subscribe("c").await.unwrap();
        ps.publish("c", b"hi".to_vec()).await.unwrap();
        assert_eq!(sub.next().await.unwrap(), b"hi");
    }

    #[tokio::test]
    async fn cache_incr_and_lock() {
        let c = MemoryCache::new();
        assert_eq!(c.incr("n", None).await.unwrap(), 1);
        assert_eq!(c.incr("n", None).await.unwrap(), 2);
        assert!(c.try_lock("l", Duration::from_secs(5)).await.unwrap());
        assert!(!c.try_lock("l", Duration::from_secs(5)).await.unwrap());
        c.delete("l").await.unwrap();
        assert!(c.try_lock("l", Duration::from_secs(5)).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn cache_entries_expire() {
        let c = MemoryCache::new();
        c.set("k", b"v".to_vec(), Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(c.get("k").await.unwrap().is_some());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(c.get("k").await.unwrap().is_none());
    }
}
