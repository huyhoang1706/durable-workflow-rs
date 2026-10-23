use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::{Cache, PubSub, RateDecision, RateLimiter, Result};
use futures::{StreamExt, stream::BoxStream};
use redis::{Client, Script, aio::ConnectionManager};

use crate::be;

fn prefixed(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}:{key}")
    }
}

/// Pub/sub on a dedicated connection per subscription.
pub struct RedisPubSub {
    conn: ConnectionManager,
    client: Client,
    prefix: String,
}

impl RedisPubSub {
    pub fn new(conn: ConnectionManager, client: Client) -> Self {
        Self {
            conn,
            client,
            prefix: String::new(),
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
        let mut sub = self.client.get_async_pubsub().await.map_err(be)?;
        sub.subscribe(prefixed(&self.prefix, channel))
            .await
            .map_err(be)?;
        Ok(sub
            .into_on_message()
            .map(|m| m.get_payload_bytes().to_vec())
            .boxed())
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
