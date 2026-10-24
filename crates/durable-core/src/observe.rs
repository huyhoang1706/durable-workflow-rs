//! Observability hook. The runtime emits live `tracing` spans and reports
//! [`Metric`]s; a plugin (see `durable-otel`) bridges both to OpenTelemetry.
//! The default observer does nothing.

use std::{collections::BTreeMap, time::Duration};

use crate::{RunStatus, WorkKind};

/// W3C `traceparent` / `tracestate` / `baggage` headers.
pub type TraceCarrier = BTreeMap<String, String>;

pub trait Observer: Send + Sync + 'static {
    /// Capture the trace context of `span` so a later execution can continue it.
    fn inject(&self, _span: &tracing::Span) -> TraceCarrier {
        TraceCarrier::new()
    }
    /// Make `span` a child of the context in `carrier`.
    fn set_parent(&self, _span: &tracing::Span, _carrier: &TraceCarrier) {}
    fn record(&self, _metric: &Metric<'_>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {}

#[derive(Debug, Clone, Copy)]
pub enum Metric<'a> {
    RunStarted {
        namespace: &'a str,
        workflow: &'a str,
    },
    RunFinished {
        namespace: &'a str,
        workflow: &'a str,
        status: RunStatus,
        e2e: Duration,
    },
    Execution {
        namespace: &'a str,
        workflow: &'a str,
        /// `finished`, `suspended`, `fault` or `abandoned`.
        outcome: &'static str,
        duration: Duration,
        replayed_events: usize,
    },
    ScheduleToStart {
        namespace: &'a str,
        workflow: &'a str,
        kind: WorkKind,
        lag: Duration,
    },
    StepAttempt {
        namespace: &'a str,
        workflow: &'a str,
        /// Step name without the `#n` / `[i]` suffix.
        step: &'a str,
        outcome: &'static str,
        duration: Duration,
    },
    Commit {
        duration: Duration,
        conflict: bool,
    },
    EngineFault {
        namespace: &'a str,
        workflow: &'a str,
    },
    StreamFlush {
        chunks: usize,
        bytes: usize,
        duration: Duration,
        ok: bool,
    },
    WorkerGauges {
        active_runs: usize,
        concurrency: usize,
        pool_backlog: usize,
    },
}
