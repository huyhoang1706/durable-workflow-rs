//! The in-memory backend: `durable-memory`'s queue, pub/sub, cache, rate
//! limiter and streams around an in-memory SQLite store. For tests and local
//! development; nothing survives a restart.

use std::sync::Arc;

pub use durable_memory::*;
use durable_spi::{Backend, Clock, SystemClock};
use durable_sqlite::SqliteStore;

/// A complete in-memory backend.
pub fn backend() -> Backend {
    backend_with_clock(Arc::new(SystemClock))
}

/// As [`backend`], with a custom clock (e.g. a manual one for tests).
pub fn backend_with_clock(clock: Arc<dyn Clock>) -> Backend {
    builder_with_clock(clock)
        .store(SqliteStore::in_memory())
        .build()
        .expect("memory backend is complete")
}
