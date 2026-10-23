use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde_json::Value;

use crate::Result;

#[async_trait]
pub trait PubSub: Send + Sync + 'static {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<()>;
    async fn subscribe(&self, channel: &str) -> Result<BoxStream<'static, Vec<u8>>>;
}

#[async_trait]
pub trait Cache: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Atomic increment; `ttl` is applied when the key is created.
    async fn incr(&self, key: &str, ttl: Option<Duration>) -> Result<i64>;
    /// `SET NX PX`: true when this caller now holds the lock.
    async fn try_lock(&self, key: &str, ttl: Duration) -> Result<bool>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    Allowed,
    Denied { retry_at: DateTime<Utc> },
}

#[async_trait]
pub trait RateLimiter: Send + Sync + 'static {
    /// GCRA: at most `limit` per `period`.
    async fn check(
        &self,
        key: &str,
        limit: u32,
        period: Duration,
        now: DateTime<Utc>,
    ) -> Result<RateDecision>;
}

/// Wraps every stored payload (encryption, compression, ...).
pub trait PayloadCodec: Send + Sync + 'static {
    fn encode(&self, value: Value) -> Result<Value>;
    fn decode(&self, value: Value) -> Result<Value>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IdentityCodec;

impl PayloadCodec for IdentityCodec {
    fn encode(&self, value: Value) -> Result<Value> {
        Ok(value)
    }
    fn decode(&self, value: Value) -> Result<Value> {
        Ok(value)
    }
}

pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Runs blocking jobs off the tokio worker threads.
pub trait ComputePool: Send + Sync + 'static {
    fn execute(&self, job: Job);
    /// Jobs queued or running.
    fn backlog(&self) -> usize;
}
