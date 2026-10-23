//! Redis adapters: the dispatch queue (Lua), pub/sub, cache and rate limiter.
//!
//! Redis only holds an index and in-flight state. Postgres stays the source
//! of truth, so a flushed Redis loses no runs: the reconciler rebuilds the
//! queue from `wake_at`.

mod queue;
mod services;

pub use queue::RedisQueue;
pub use services::{RedisCache, RedisPubSub, RedisRateLimiter};

use durable_core::Error;

pub(crate) fn be(e: redis::RedisError) -> Error {
    Error::Backend(e.to_string())
}

/// All Redis-backed slots, connected from a URL.
pub struct RedisParts {
    pub queue: RedisQueue,
    pub pubsub: RedisPubSub,
    pub cache: RedisCache,
    pub rate_limiter: RedisRateLimiter,
}

/// Connect every Redis slot with a shared key `prefix` (e.g. `"dw"`).
pub async fn connect(url: &str, prefix: &str) -> durable_core::Result<RedisParts> {
    let client = redis::Client::open(url).map_err(be)?;
    let conn = redis::aio::ConnectionManager::new(client.clone())
        .await
        .map_err(be)?;
    Ok(RedisParts {
        queue: RedisQueue::new(conn.clone()).prefix(prefix),
        pubsub: RedisPubSub::new(conn.clone(), client).prefix(prefix),
        cache: RedisCache::new(conn.clone()).prefix(prefix),
        rate_limiter: RedisRateLimiter::new(conn).prefix(prefix),
    })
}
