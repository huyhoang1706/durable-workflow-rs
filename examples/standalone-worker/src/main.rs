//! A dedicated worker process with a CPU-bound sync workflow.
//!
//! Blocking steps run on a bounded compute pool, never on tokio's worker
//! threads, so heavy work cannot starve I/O or lease heartbeats. Run it on
//! compute nodes and keep light workflows embedded in your app.
//!
//! ```text
//! docker compose up -d
//! DURABLE_DATABASE_URL=postgres://postgres:postgres@localhost/durable \
//! DURABLE_REDIS_URL=redis://localhost DURABLE_ENABLE_API=1 cargo run -p standalone-worker
//! curl -XPOST localhost:17080/durable/v1/workflows/images.resize/runs \
//!      -H 'content-type: application/json' -d '{"input":{"images":[1,2,3,4,5,6,7,8]}}'
//! ```
//!
//! # Observability
//!
//! With no configuration the worker prints one JSON line per closed span
//! (`durable.run_id`, `durable.workflow`, `durable.step.key`, outcome and
//! `time.busy`) to stdout and writes rotating JSON logs to `./logs`. Each log
//! line carries the same span fields, so logs and spans join on the run id.
//!
//! | Variable | Default | |
//! |---|---|---|
//! | `DURABLE_LOG_DIR` | `./logs` | created if missing; an unwritable path fails startup |
//! | `DURABLE_LOG_FILE_PREFIX` | `durable` | |
//! | `DURABLE_LOG_ROTATION` | `daily` | `minutely`, `hourly`, `daily`, `never` |
//! | `DURABLE_LOG_MAX_FILES` | `7` | older files are pruned |
//! | `DURABLE_TRACES` / `DURABLE_LOGS` / `DURABLE_METRICS` | `stdout` / `file` / `none` | pick a local sink per signal |
//!
//! To export traces, metrics and logs over OTLP, build with the opt-in `otel`
//! feature (`features = ["standalone", "otel"]`, as this example does) and
//! point it at a collector:
//!
//! ```text
//! docker compose up -d otel-lgtm
//! OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 cargo run -p standalone-worker
//! # Grafana: http://localhost:3000 (traces in Tempo, metrics in Mimir, logs in Loki)
//! ```
//!
//! A signal with OTLP configured goes there; the others keep their local sink
//! (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` alone exports spans only, and logs
//! stay in files). `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` apply.

use durable_workflow::{
    StepError, WfSyncCtx, WorkflowError,
    standalone::{Config, run},
    step, workflow,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Batch {
    images: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
struct Report {
    resized: usize,
    checksum: u64,
}

/// Plain blocking code on a driver thread: no async anywhere.
#[workflow(name = "images.resize", concurrency(limit = 4))]
fn resize(ctx: &WfSyncCtx, batch: Batch) -> Result<Report, WorkflowError> {
    // Fan out on the compute pool; each item is memoized, so a crash mid-batch
    // only redoes the images that did not finish.
    let resized: Vec<u64> = ctx.map("resize", batch.images, |id: u64| {
        let mut acc = id;
        for i in 0..20_000_000u64 {
            // Long CPU steps should poll `is_cancelled()` so cancellation takes effect.
            if i % 1_000_000 == 0 && step::current().is_cancelled() {
                return Err(StepError::fatal("cancelled"));
            }
            acc = acc
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
        }
        Ok(acc)
    })?;
    let checksum = checksum(ctx, resized.clone())?;
    Ok(Report {
        resized: resized.len(),
        checksum,
    })
}

#[step(retry = 3)]
fn checksum(values: Vec<u64>) -> Result<u64, StepError> {
    Ok(values.iter().fold(0u64, |a, v| a ^ v))
}

#[tokio::main]
async fn main() -> Result<(), durable_workflow::Error> {
    run(Config::from_env()?, |worker| {
        worker.register_sync_default::<Resize>()
    })
    .await
}
