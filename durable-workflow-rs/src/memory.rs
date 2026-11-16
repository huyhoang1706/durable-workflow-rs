//! In-process queue, pub/sub, cache, rate limiter and streams
//! (`durable-memory`). They hold no runs: pair them with a store.
//!
//! With the `sqlite` feature (on by default), [`backend`] is a whole backend
//! in this process, on an in-memory SQLite store. For tests and local
//! development; nothing survives a restart.

pub use durable_memory::*;

#[cfg(feature = "sqlite")]
mod whole {
    use std::sync::Arc;

    use durable_spi::{Backend, Clock, SystemClock};
    use durable_sqlite::SqliteStore;

    /// A complete in-memory backend. Needs the `sqlite` feature.
    pub fn backend() -> Backend {
        backend_with_clock(Arc::new(SystemClock))
    }

    /// As [`backend`], with a custom clock (e.g. a manual one for tests).
    pub fn backend_with_clock(clock: Arc<dyn Clock>) -> Backend {
        durable_memory::builder_with_clock(clock)
            .store(SqliteStore::in_memory())
            .build()
            .expect("memory backend is complete")
    }
}

#[cfg(feature = "sqlite")]
pub use whole::{backend, backend_with_clock};
