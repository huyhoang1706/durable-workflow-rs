//! OpenTelemetry traces, metrics and logs for durable-workflow.
//!
//! The runtime emits live `tracing` spans and reports [`durable_core::Metric`]s
//! to an [`Observer`](durable_core::Observer). This crate bridges both to
//! OpenTelemetry:
//!
//! - [`layers`] builds OTLP exporters for the signals that have OTLP configured
//!   (`OTEL_EXPORTER_OTLP_*`, `OTEL_{TRACES,METRICS,LOGS}_EXPORTER=otlp`) and
//!   returns `tracing` layers for spans and logs, an [`OtelObserver`] for
//!   metrics and trace propagation, and a guard that flushes on drop.
//! - [`OtelObserver`] carries W3C trace context across durable boundaries: the
//!   `workflow.start` span's context is stored on the run, and every later
//!   execution (on any worker, after any sleep) continues that trace.
//!
//! Exporters are swappable: [`OtelConfig::span_exporter`],
//! [`log_exporter`](OtelConfig::log_exporter) and
//! [`metric_exporter`](OtelConfig::metric_exporter) accept any SDK exporter, so a
//! DuckDB or ClickHouse sink needs no runtime change.
//!
//! ```ignore
//! let mut parts = durable_otel::layers(OtelConfig::from_env())?;
//! tracing_subscriber::registry().with(parts.layers()).init();
//! backend.observer = parts.observer();
//! let _guard = parts.into_guard(); // flushes on drop
//! ```
//! Call [`layers`] inside a Tokio runtime when using gRPC.

mod config;
mod observer;
mod setup;

pub use config::{OtelConfig, Signals};
pub use observer::OtelObserver;
pub use setup::{OtelError, OtelGuard, OtelParts, layers};
