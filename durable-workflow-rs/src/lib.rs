//! Durable workflows for Rust that adapt to your infrastructure.
//!
//! This facade re-exports the runtime and, behind features, the backends:
//! `memory` (default; dev and tests), `postgres`, `redis`. Observability is
//! opt-in: `observable` (local JSON logs and spans, no extra services) and
//! `otel` (OpenTelemetry over OTLP).

pub use durable_runtime::*;
pub use durable_spi as core;
pub use durable_spi::{Backend, Clock, Error, Run, RunFilter, RunId, RunStatus};

#[cfg(feature = "memory")]
pub use durable_memory as memory;
#[cfg(feature = "otel")]
pub use durable_otel as otel;
#[cfg(feature = "postgres")]
pub use durable_postgres as postgres;
#[cfg(feature = "redis")]
pub use durable_redis as redis;

#[cfg(feature = "macros")]
pub use durable_macros::{step, workflow};

#[cfg(feature = "postgres")]
pub mod connect;

#[cfg(feature = "observable")]
pub mod observable;

#[cfg(feature = "standalone")]
pub mod standalone;
