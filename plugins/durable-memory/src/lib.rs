//! In-memory implementations of the `durable-spi` plugin traits: queue,
//! pub/sub, cache, rate limiter and streams. The store is SQLite in
//! `:memory:` mode ([`SqliteStore::in_memory`]), so there is one embedded
//! store implementation. For tests and local development; nothing survives a
//! restart.

mod gcra;
mod queue;
mod services;
mod streams;

use std::sync::Arc;

use durable_spi::{Backend, Clock, SystemClock};
use durable_sqlite::SqliteStore;

pub use queue::MemoryQueue;
pub use services::{MemoryCache, MemoryPubSub, MemoryRateLimiter};
pub use streams::MemoryStreams;

/// A complete in-memory backend.
pub fn backend() -> Backend {
    backend_with_clock(Arc::new(SystemClock))
}

/// As [`backend`], with a custom clock (e.g. a manual one for tests).
pub fn backend_with_clock(clock: Arc<dyn Clock>) -> Backend {
    builder(clock).build().expect("memory backend is complete")
}

fn builder(clock: Arc<dyn Clock>) -> durable_spi::BackendBuilder {
    Backend::builder()
        .streams(MemoryStreams::new(clock.clone()))
        .clock(clock)
        .store(SqliteStore::in_memory())
        .queue(MemoryQueue::new())
        .pubsub(MemoryPubSub::new())
        .cache(MemoryCache::new())
        .rate_limiter(MemoryRateLimiter::new())
}
