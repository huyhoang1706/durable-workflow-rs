//! In-memory implementations of the `durable-spi` plugin traits around the
//! store: queue, pub/sub, cache, rate limiter and streams. Like
//! `durable-redis`, it holds no runs; pair it with a store (for example
//! `durable-sqlite`'s `SqliteStore::in_memory()`, or Postgres for a durable
//! single process). For tests and local development; nothing here survives a
//! restart.

mod gcra;
mod queue;
mod services;
mod streams;

use std::sync::Arc;

use durable_spi::{Backend, BackendBuilder, Clock, SystemClock};

pub use queue::MemoryQueue;
pub use services::{MemoryCache, MemoryPubSub, MemoryRateLimiter};
pub use streams::MemoryStreams;

/// All in-memory slots.
pub struct MemoryParts {
    pub queue: MemoryQueue,
    pub pubsub: MemoryPubSub,
    pub cache: MemoryCache,
    pub rate_limiter: MemoryRateLimiter,
    pub streams: MemoryStreams,
}

/// Every in-memory slot; `clock` times the streams.
pub fn parts(clock: Arc<dyn Clock>) -> MemoryParts {
    MemoryParts {
        queue: MemoryQueue::new(),
        pubsub: MemoryPubSub::new(),
        cache: MemoryCache::new(),
        rate_limiter: MemoryRateLimiter::new(),
        streams: MemoryStreams::new(clock),
    }
}

/// A backend builder with every slot but the store filled in memory:
/// `durable_memory::builder().store(store).build()`.
pub fn builder() -> BackendBuilder {
    builder_with_clock(Arc::new(SystemClock))
}

/// As [`builder`], with a custom clock (e.g. a manual one for tests).
pub fn builder_with_clock(clock: Arc<dyn Clock>) -> BackendBuilder {
    let p = parts(clock.clone());
    Backend::builder()
        .clock(clock)
        .queue(p.queue)
        .pubsub(p.pubsub)
        .cache(p.cache)
        .rate_limiter(p.rate_limiter)
        .streams(p.streams)
}
