use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use durable_core::{Metric, Observer, TraceCarrier, WorkKind};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider, ObservableGauge, noop::NoopMeterProvider},
    propagation::{Extractor, Injector, TextMapPropagator},
};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    propagation::{BaggagePropagator, TraceContextPropagator},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Seconds, from a few milliseconds to hours.
const SECONDS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
    3600.0, 21_600.0, 86_400.0,
];

struct CarrierInjector<'a>(&'a mut TraceCarrier);

impl Injector for CarrierInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

struct CarrierExtractor<'a>(&'a TraceCarrier);

impl Extractor for CarrierExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

#[derive(Default)]
struct Gauges {
    active_runs: AtomicUsize,
    concurrency: AtomicUsize,
    pool_backlog: AtomicUsize,
}

struct Instruments {
    runs_started: Counter<u64>,
    runs_finished: Counter<u64>,
    run_duration: Histogram<f64>,
    execution_duration: Histogram<f64>,
    replayed_events: Histogram<u64>,
    schedule_to_start: Histogram<f64>,
    step_duration: Histogram<f64>,
    step_attempts: Counter<u64>,
    commit_duration: Histogram<f64>,
    engine_faults: Counter<u64>,
    stream_chunks: Counter<u64>,
    stream_bytes: Counter<u64>,
    stream_flush_duration: Histogram<f64>,
    // Kept alive: dropping an observable instrument may unregister it.
    _gauges: Vec<ObservableGauge<u64>>,
}

fn seconds(meter: &Meter, name: &'static str, description: &'static str) -> Histogram<f64> {
    meter
        .f64_histogram(name)
        .with_unit("s")
        .with_description(description)
        .with_boundaries(SECONDS.to_vec())
        .build()
}

impl Instruments {
    fn new(meter: &Meter, gauges: &Arc<Gauges>) -> Self {
        let gauge = |name: &'static str, description: &'static str, read: fn(&Gauges) -> usize| {
            let g = gauges.clone();
            meter
                .u64_observable_gauge(name)
                .with_description(description)
                .with_callback(move |o| o.observe(read(&g) as u64, &[]))
                .build()
        };
        Self {
            runs_started: meter
                .u64_counter("durable.runs.started")
                .with_description("Workflow runs started")
                .build(),
            runs_finished: meter
                .u64_counter("durable.runs.finished")
                .with_description("Workflow runs that reached a terminal status")
                .build(),
            run_duration: seconds(
                meter,
                "durable.run.duration",
                "Time from run creation to its terminal event",
            ),
            execution_duration: seconds(
                meter,
                "durable.execution.duration",
                "Time one worker spent executing a run before it finished or suspended",
            ),
            replayed_events: meter
                .u64_histogram("durable.execution.replayed_events")
                .with_description("Events in the history an execution loaded")
                .build(),
            schedule_to_start: seconds(
                meter,
                "durable.schedule_to_start",
                "Time between a run being due and a worker claiming it",
            ),
            step_duration: seconds(
                meter,
                "durable.step.duration",
                "Duration of one step attempt",
            ),
            step_attempts: meter
                .u64_counter("durable.step.attempts")
                .with_description("Step attempts, by outcome")
                .build(),
            commit_duration: seconds(
                meter,
                "durable.commit.duration",
                "Duration of a store commit",
            ),
            engine_faults: meter
                .u64_counter("durable.engine.faults")
                .with_description("Executions that ended in an engine fault")
                .build(),
            stream_chunks: meter
                .u64_counter("durable.stream.chunks")
                .with_description("Stream chunks flushed")
                .build(),
            stream_bytes: meter
                .u64_counter("durable.stream.bytes")
                .with_unit("By")
                .with_description("Serialized bytes of flushed stream chunks")
                .build(),
            stream_flush_duration: seconds(
                meter,
                "durable.stream.flush.duration",
                "Duration of one stream flush",
            ),
            _gauges: vec![
                gauge(
                    "durable.worker.active_runs",
                    "Runs executing on the worker",
                    |g| g.active_runs.load(Ordering::Relaxed),
                ),
                gauge(
                    "durable.worker.concurrency",
                    "Configured run concurrency of the worker",
                    |g| g.concurrency.load(Ordering::Relaxed),
                ),
                gauge(
                    "durable.worker.pool_backlog",
                    "Blocking steps queued or running on the compute pool",
                    |g| g.pool_backlog.load(Ordering::Relaxed),
                ),
            ],
        }
    }
}

fn kv(key: &'static str, value: &str) -> KeyValue {
    KeyValue::new(key, value.to_string())
}

fn base(namespace: &str, workflow: &str) -> [KeyValue; 2] {
    [
        kv("durable.namespace", namespace),
        kv("durable.workflow", workflow),
    ]
}

/// Bridges the runtime's [`Observer`] hooks to OpenTelemetry: W3C trace
/// context in and out of a run's stored carrier, and the `durable.*` metrics.
pub struct OtelObserver {
    propagator: opentelemetry::propagation::TextMapCompositePropagator,
    m: Instruments,
    gauges: Arc<Gauges>,
}

impl OtelObserver {
    /// Instruments are created on `provider`'s meter. Pass `None` to record
    /// nothing (spans are still propagated).
    pub fn new(provider: Option<&SdkMeterProvider>) -> Self {
        let gauges = Arc::new(Gauges::default());
        let meter = match provider {
            Some(p) => p.meter("durable-workflow"),
            None => NoopMeterProvider::new().meter("durable-workflow"),
        };
        Self {
            propagator: opentelemetry::propagation::TextMapCompositePropagator::new(vec![
                Box::new(TraceContextPropagator::new()),
                Box::new(BaggagePropagator::new()),
            ]),
            m: Instruments::new(&meter, &gauges),
            gauges,
        }
    }

    /// The propagator used for the stored carrier (W3C TraceContext + Baggage).
    pub fn propagator(&self) -> &impl TextMapPropagator {
        &self.propagator
    }
}

impl Observer for OtelObserver {
    fn inject(&self, span: &tracing::Span) -> TraceCarrier {
        let mut carrier = TraceCarrier::new();
        self.propagator
            .inject_context(&span.context(), &mut CarrierInjector(&mut carrier));
        carrier
    }

    fn set_parent(&self, span: &tracing::Span, carrier: &TraceCarrier) {
        if carrier.is_empty() {
            return;
        }
        let cx = self.propagator.extract(&CarrierExtractor(carrier));
        // Fails only when no OpenTelemetry layer is installed: nothing to link.
        let _ = span.set_parent(cx);
    }

    fn record(&self, metric: &Metric<'_>) {
        let m = &self.m;
        match *metric {
            Metric::RunStarted {
                namespace,
                workflow,
            } => m.runs_started.add(1, &base(namespace, workflow)),
            Metric::RunFinished {
                namespace,
                workflow,
                status,
                e2e,
            } => {
                let [ns, wf] = base(namespace, workflow);
                let attrs = [ns, wf, kv("status", status.as_str())];
                m.runs_finished.add(1, &attrs);
                m.run_duration.record(e2e.as_secs_f64(), &attrs);
            }
            Metric::Execution {
                namespace,
                workflow,
                outcome,
                duration,
                replayed_events,
            } => {
                let [ns, wf] = base(namespace, workflow);
                m.execution_duration.record(
                    duration.as_secs_f64(),
                    &[ns.clone(), wf.clone(), kv("outcome", outcome)],
                );
                m.replayed_events.record(replayed_events as u64, &[ns, wf]);
            }
            Metric::ScheduleToStart {
                namespace,
                workflow,
                kind,
                lag,
            } => {
                let [ns, wf] = base(namespace, workflow);
                let kind = match kind {
                    WorkKind::Start => "start",
                    WorkKind::Resume => "resume",
                };
                m.schedule_to_start
                    .record(lag.as_secs_f64(), &[ns, wf, kv("kind", kind)]);
            }
            Metric::StepAttempt {
                namespace,
                workflow,
                step,
                outcome,
                duration,
            } => {
                let [ns, wf] = base(namespace, workflow);
                let attrs = [ns, wf, kv("durable.step", step), kv("outcome", outcome)];
                m.step_attempts.add(1, &attrs);
                m.step_duration.record(duration.as_secs_f64(), &attrs);
            }
            Metric::Commit { duration, conflict } => m.commit_duration.record(
                duration.as_secs_f64(),
                &[KeyValue::new("conflict", conflict)],
            ),
            Metric::EngineFault {
                namespace,
                workflow,
            } => m.engine_faults.add(1, &base(namespace, workflow)),
            Metric::StreamFlush {
                chunks,
                bytes,
                duration,
                ok,
            } => {
                let attrs = [KeyValue::new("ok", ok)];
                m.stream_chunks.add(chunks as u64, &attrs);
                m.stream_bytes.add(bytes as u64, &attrs);
                m.stream_flush_duration
                    .record(duration.as_secs_f64(), &attrs);
            }
            Metric::WorkerGauges {
                active_runs,
                concurrency,
                pool_backlog,
            } => {
                let g = &self.gauges;
                g.active_runs.store(active_runs, Ordering::Relaxed);
                g.concurrency.store(concurrency, Ordering::Relaxed);
                g.pool_backlog.store(pool_backlog, Ordering::Relaxed);
            }
        }
    }
}
