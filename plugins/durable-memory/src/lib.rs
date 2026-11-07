//! In-memory reference implementation of every `durable-spi` plugin trait.
//! For tests and local development; nothing survives a restart.

mod gcra;
mod queue;
mod services;
mod store;
mod streams;

use std::sync::Arc;

use durable_spi::{Backend, Clock, SystemClock};

pub use queue::MemoryQueue;
pub use services::{MemoryCache, MemoryPubSub, MemoryRateLimiter};
pub use store::MemoryStore;
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
        .store(MemoryStore::new())
        .queue(MemoryQueue::new())
        .pubsub(MemoryPubSub::new())
        .cache(MemoryCache::new())
        .rate_limiter(MemoryRateLimiter::new())
}
