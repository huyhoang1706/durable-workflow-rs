//! Durable workflows for Rust that adapt to your infrastructure.
//!
//! This facade re-exports the runtime and, behind features, the backends:
//! `memory` (default; dev and tests), `postgres`, `redis`.

pub use durable_core as core;
pub use durable_core::{Backend, Clock, Error, Run, RunFilter, RunId, RunStatus};
pub use durable_runtime::*;

#[cfg(feature = "memory")]
pub use durable_memory as memory;
#[cfg(feature = "postgres")]
pub use durable_postgres as postgres;
#[cfg(feature = "redis")]
pub use durable_redis as redis;

#[cfg(feature = "macros")]
pub use durable_macros::{step, workflow};

#[cfg(feature = "postgres")]
pub mod connect;

#[cfg(feature = "standalone")]
pub mod standalone;
