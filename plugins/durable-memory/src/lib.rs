//! In-memory reference implementation of every `durable-core` plugin trait.
//! For tests and local development; nothing survives a restart.

mod gcra;
mod queue;
mod services;
mod store;

use std::sync::Arc;

use durable_core::{Backend, Clock};

pub use queue::MemoryQueue;
pub use services::{MemoryCache, MemoryPubSub, MemoryRateLimiter};
pub use store::MemoryStore;

/// A complete in-memory backend.
pub fn backend() -> Backend {
    builder().build().expect("memory backend is complete")
}

/// As [`backend`], with a custom clock (e.g. a manual one for tests).
pub fn backend_with_clock(clock: Arc<dyn Clock>) -> Backend {
    builder()
        .clock(clock)
        .build()
        .expect("memory backend is complete")
}

fn builder() -> durable_core::BackendBuilder {
    Backend::builder()
        .store(MemoryStore::new())
        .queue(MemoryQueue::new())
        .pubsub(MemoryPubSub::new())
        .cache(MemoryCache::new())
        .rate_limiter(MemoryRateLimiter::new())
}
