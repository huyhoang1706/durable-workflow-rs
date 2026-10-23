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
//! curl -XPOST localhost:8080/durable/v1/workflows/images.resize/runs \
//!      -H 'content-type: application/json' -d '{"input":{"images":[1,2,3,4,5,6,7,8]}}'
//! ```

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
