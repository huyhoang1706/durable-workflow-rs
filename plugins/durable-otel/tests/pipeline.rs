//! End to end: a real worker and client over the memory backend, exporting
//! into the SDK's in-memory exporters. One global subscriber serves the whole
//! binary, so every test selects its own spans by workflow name.

use std::{sync::OnceLock, time::Duration};

use durable_otel::{OtelConfig, OtelGuard};
use durable_runtime::{Client, StepError, WfAsyncCtx, Worker};
use durable_spi::Observer;
use durable_testkit::{ManualClock, t0};
use opentelemetry::{Key, Value, trace::SpanId};
use opentelemetry_sdk::{
    logs::InMemoryLogExporter,
    metrics::{
        InMemoryMetricExporter,
        data::{AggregatedMetrics, MetricData},
    },
    trace::{InMemorySpanExporter, SpanData},
};
use serde_json::{Value as Json, json};
use tracing_subscriber::layer::SubscriberExt;

struct Telemetry {
    spans: InMemorySpanExporter,
    logs: InMemoryLogExporter,
    metrics: InMemoryMetricExporter,
    observer: std::sync::Arc<dyn Observer>,
    guard: OtelGuard,
}

fn telemetry() -> &'static Telemetry {
    static T: OnceLock<Telemetry> = OnceLock::new();
    T.get_or_init(|| {
        let spans = InMemorySpanExporter::default();
        let logs = InMemoryLogExporter::default();
        let metrics = InMemoryMetricExporter::default();
        let parts = durable_otel::layers(
            OtelConfig::from_vars([])
                .span_exporter_simple(spans.clone())
                .log_exporter_simple(logs.clone())
                .metric_exporter(metrics.clone())
                .without_globals(),
        )
        .unwrap();
        assert!(parts.signals.traces && parts.signals.logs && parts.signals.metrics);
        let subscriber = tracing_subscriber::registry().with(parts.layers());
        tracing::subscriber::set_global_default(subscriber).unwrap();
        Telemetry {
            spans,
            logs,
            metrics,
            observer: parts.observer(),
            guard: parts.into_guard(),
        }
    })
}

fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key == Key::new(key.to_string()))
        .map(|kv| &kv.value)
}

fn attr_str(span: &SpanData, key: &str) -> Option<String> {
    attr(span, key).map(|v| v.as_str().to_string())
}

/// Spans of the run(s) of `workflow`, oldest first.
fn spans_of(workflow: &str) -> Vec<SpanData> {
    let mut v: Vec<_> = telemetry()
        .spans
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|s| attr_str(s, "durable.workflow").as_deref() == Some(workflow))
        .collect();
    v.sort_by_key(|s| s.start_time);
    v
}

struct Env {
    clock: std::sync::Arc<ManualClock>,
    client: Client,
    worker: durable_runtime::WorkerHandle,
}

async fn env(
    register: impl FnOnce(durable_runtime::WorkerBuilder) -> durable_runtime::WorkerBuilder,
) -> Env {
    let t = telemetry();
    let clock = ManualClock::new(t0());
    let mut backend = durable_memory::backend_with_clock(clock.clone());
    backend.observer = t.observer.clone();
    let worker = register(
        Worker::builder(backend.clone())
            .lease(Duration::from_secs(100_000_000))
            .poll_interval(Duration::from_millis(20))
            .drain_timeout(Duration::from_millis(300)),
    )
    .build()
    .unwrap()
    .spawn();
    worker.ready().await;
    Env {
        clock,
        client: Client::new(backend),
        worker,
    }
}

async fn finish(env: &Env, id: uuid::Uuid) -> Json {
    for _ in 0..2000 {
        let run = env.client.get_run(id).await.unwrap().unwrap();
        if run.status.is_terminal() {
            return run.output.unwrap_or(Json::Null);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run did not finish");
}

async fn suspended(env: &Env, id: uuid::Uuid) {
    for _ in 0..2000 {
        let run = env.client.get_run(id).await.unwrap().unwrap();
        if run.status == durable_spi::RunStatus::Waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run did not suspend");
}

fn flush() {
    telemetry().guard.force_flush();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_with_a_sleep_is_one_trace_across_two_executions() {
    let env = env(|w| {
        w.register_fn("otel_sleepy", |ctx: WfAsyncCtx, _: Json| async move {
            let a: i64 = ctx.step("a", async { Ok::<_, StepError>(1) }).await?;
            ctx.sleep("nap", Duration::from_secs(3600)).await?;
            let b: i64 = ctx
                .step("b", async move { Ok::<_, StepError>(a + 1) })
                .await?;
            Ok(b)
        })
    })
    .await;
    let id = env
        .client
        .start_raw("otel_sleepy", json!(null), Default::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .id();
    suspended(&env, id).await;
    env.clock.advance(chrono::Duration::seconds(3601));
    assert_eq!(finish(&env, id).await, json!(2));
    env.worker.shutdown().await;
    flush();

    let spans = spans_of("otel_sleepy");
    let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
    let starts: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "workflow.start")
        .collect();
    let execs: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "workflow.execute")
        .collect();
    let steps: Vec<_> = spans
        .iter()
        .filter(|s| s.name.starts_with("step "))
        .collect();
    assert_eq!(starts.len(), 1, "{names:?}");
    assert_eq!(execs.len(), 2, "{names:?}");
    assert_eq!(steps.len(), 2, "replayed step `a` emits no span: {names:?}");

    let start = starts[0];
    let trace = start.span_context.trace_id();
    for s in &spans {
        assert_eq!(
            s.span_context.trace_id(),
            trace,
            "{} is in the run's trace",
            s.name
        );
    }
    for e in &execs {
        assert_eq!(e.parent_span_id, start.span_context.span_id());
        assert_eq!(attr_str(e, "durable.run_id"), Some(id.to_string()));
    }
    // Each step hangs under the execution that ran it: a in the first, b in the second.
    let step = |name: &str| {
        *steps
            .iter()
            .find(|s| s.name == format!("step {name}"))
            .unwrap()
    };
    assert_eq!(step("a").parent_span_id, execs[0].span_context.span_id());
    assert_eq!(step("b").parent_span_id, execs[1].span_context.span_id());
    assert_eq!(attr_str(step("a"), "durable.step.key"), Some("a".into()));
    assert_eq!(
        attr_str(step("a"), "durable.step.outcome"),
        Some("succeeded".into())
    );
    assert_eq!(
        attr_str(execs[0], "durable.outcome"),
        Some("suspended".into())
    );
    assert_eq!(
        attr_str(execs[1], "durable.outcome"),
        Some("finished".into())
    );
    assert_eq!(
        start.parent_span_id,
        SpanId::INVALID,
        "the start span is the root"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_spans_and_logs_inside_a_step_nest_under_the_step() {
    let env = env(|w| {
        w.register_fn("otel_nested", |ctx: WfAsyncCtx, _: Json| async move {
            ctx.step("work", async {
                let inner = tracing::info_span!("user_op", op = "fetch");
                let _g = inner.enter();
                tracing::info!(target: "user", "inside the step");
                Ok::<_, StepError>(())
            })
            .await
        })
    })
    .await;
    let id = env
        .client
        .start_raw("otel_nested", json!(null), Default::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .id();
    finish(&env, id).await;
    env.worker.shutdown().await;
    flush();

    // `user_op` carries no durable.workflow attribute; find it by parent.
    let all = telemetry().spans.get_finished_spans().unwrap();
    let step = spans_of("otel_nested")
        .into_iter()
        .find(|s| s.name.starts_with("step "))
        .unwrap();
    let user = all
        .iter()
        .find(|s| s.name == "user_op" && s.parent_span_id == step.span_context.span_id())
        .expect("user span nests under the step span");
    assert_eq!(user.span_context.trace_id(), step.span_context.trace_id());

    let logs = telemetry().logs.get_emitted_logs().unwrap();
    let record = logs
        .iter()
        .map(|l| &l.record)
        .find(|r| {
            r.body()
                .is_some_and(|b| format!("{b:?}").contains("inside the step"))
        })
        .expect("the step's log record was exported");
    let ctx = record.trace_context().expect("log carries trace context");
    assert_eq!(ctx.trace_id, step.span_context.trace_id());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_expected_metrics_are_recorded() {
    let env = env(|w| {
        w.register_fn("otel_metrics", |ctx: WfAsyncCtx, _: Json| async move {
            ctx.step("only", async { Ok::<_, StepError>(()) }).await
        })
    })
    .await;
    let id = env
        .client
        .start_raw("otel_metrics", json!(null), Default::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .id();
    finish(&env, id).await;
    env.worker.shutdown().await;
    flush();

    let finished = telemetry().metrics.get_finished_metrics().unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut runs_started = 0;
    for rm in &finished {
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                seen.insert(m.name().to_string());
                if m.name() == "durable.runs.started"
                    && let AggregatedMetrics::U64(MetricData::Sum(sum)) = m.data()
                {
                    runs_started += sum
                        .data_points()
                        .filter(|dp| {
                            dp.attributes().any(|kv| {
                                kv.key.as_str() == "durable.workflow"
                                    && kv.value.as_str() == "otel_metrics"
                            })
                        })
                        .map(|dp| dp.value())
                        .sum::<u64>();
                }
            }
        }
    }
    for want in [
        "durable.runs.started",
        "durable.runs.finished",
        "durable.run.duration",
        "durable.execution.duration",
        "durable.step.duration",
        "durable.step.attempts",
        "durable.schedule_to_start",
        "durable.commit.duration",
        "durable.worker.active_runs",
    ] {
        assert!(seen.contains(want), "missing {want}; saw {seen:?}");
    }
    assert!(runs_started >= 1, "runs.started counted for this workflow");
}

#[tokio::test]
async fn a_carrier_round_trips_through_the_observer() {
    let t = telemetry();
    let root = tracing::info_span!("producer");
    let carrier = {
        let _g = root.enter();
        t.observer.inject(&tracing::Span::current())
    };
    assert!(
        carrier
            .get("traceparent")
            .is_some_and(|v| v.starts_with("00-")),
        "{carrier:?}"
    );
    let child = tracing::info_span!("consumer");
    t.observer.set_parent(&child, &carrier);
    drop(child);
    drop(root);
    flush();
    let spans = t.spans.get_finished_spans().unwrap();
    let producer = spans.iter().find(|s| s.name == "producer").unwrap();
    let consumer = spans.iter().find(|s| s.name == "consumer").unwrap();
    assert_eq!(
        consumer.span_context.trace_id(),
        producer.span_context.trace_id()
    );
    assert_eq!(consumer.parent_span_id, producer.span_context.span_id());
}

#[test]
fn an_empty_carrier_changes_nothing() {
    let _ = telemetry();
    let span = tracing::info_span!("lonely");
    telemetry().observer.set_parent(&span, &Default::default());
}
