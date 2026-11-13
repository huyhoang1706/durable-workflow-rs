//! The tiny-step workloads of `durable-workflow-ts/bench/tiny-steps.ts`, with
//! Rust workflows on the memory backend: the baseline the TypeScript numbers
//! are compared with, plus a fan-out whose steps finish one by one.
//!
//! ```text
//! cargo run --release -p durable-workflow --example tiny_steps
//! ```

use std::time::{Duration, Instant};

use durable_workflow::{Client, StepError, WfAsyncCtx, Worker, WorkflowError};
use futures::future::try_join_all;
use serde_json::{Value, json};

fn num(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

async fn sequential(ctx: WfAsyncCtx, n: u64) -> Result<u64, WorkflowError> {
    let mut sum = 0;
    for i in 0..n {
        sum += ctx.step("s", async move { Ok::<_, StepError>(i) }).await?;
    }
    Ok(sum)
}

async fn fan_out(ctx: WfAsyncCtx, n: u64) -> Result<u64, WorkflowError> {
    let calls = (0..n).map(|i| {
        ctx.step("m", async move { Ok::<_, StepError>(i) })
            .into_future()
    });
    Ok(try_join_all(calls).await?.into_iter().sum())
}

/// Fan-out whose steps finish one by one: step `i` sleeps `i * spread_us` µs.
async fn fan_out_spread(ctx: WfAsyncCtx, (n, spread_us): (u64, u64)) -> Result<u64, WorkflowError> {
    let calls = (0..n).map(|i| {
        ctx.step("m", async move {
            tokio::time::sleep(Duration::from_micros(i * spread_us)).await;
            Ok::<_, StepError>(i)
        })
        .into_future()
    });
    Ok(try_join_all(calls).await?.into_iter().sum())
}

async fn measure(
    scenario: &str,
    steps: u64,
    workflow: &str,
    inputs: Vec<Value>,
    concurrency: usize,
) {
    let backend = durable_workflow::memory::backend();
    let worker = Worker::builder(backend.clone())
        .concurrency(concurrency)
        .poll_interval(Duration::from_millis(5))
        .register_fn("bench.sequential", sequential)
        .register_fn("bench.fan_out", fan_out)
        .register_fn("bench.fan_out_spread", fan_out_spread)
        .build()
        .unwrap()
        .spawn();
    worker.ready().await;
    let client = Client::new(backend);
    let start = Instant::now();
    let mut handles = vec![];
    for n in inputs {
        let out = client
            .start_raw(workflow, n, Default::default())
            .await
            .unwrap();
        handles.push(out.handle().unwrap().clone());
    }
    for h in handles {
        h.result::<Value>().await.unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{scenario:<36} {:>8.0} steps/s {:>8.0} µs/step",
        steps as f64 / secs,
        secs * 1e6 / steps as f64
    );
    worker.shutdown().await;
}

#[tokio::main]
async fn main() {
    let seq_steps = num("BENCH_SEQ_STEPS", 2_000);
    let runs = num("BENCH_RUNS", 500);
    let per_run = num("BENCH_STEPS_PER_RUN", 20);
    let fan = num("BENCH_FAN_OUT", 2_000);
    let concurrency = num("BENCH_CONCURRENCY", 64) as usize;
    let spread_us = num("BENCH_SPREAD_US", 250);

    println!("backend: memory, Rust workflows");
    measure(
        &format!("1 run x {seq_steps} sequential steps"),
        seq_steps,
        "bench.sequential",
        vec![json!(seq_steps)],
        concurrency,
    )
    .await;
    measure(
        &format!("{runs} runs x {per_run} steps, {concurrency} at once"),
        runs * per_run,
        "bench.sequential",
        vec![json!(per_run); runs as usize],
        concurrency,
    )
    .await;
    measure(
        &format!("1 run, {fan} parallel steps"),
        fan,
        "bench.fan_out",
        vec![json!(fan)],
        concurrency,
    )
    .await;
    // Steps finish one by one, so results reach the engine in small batches.
    // The floor is `fan * spread_us`: the time the last step sleeps.
    measure(
        &format!("1 run, {fan} parallel steps {spread_us} µs apart"),
        fan,
        "bench.fan_out_spread",
        vec![json!([fan, spread_us])],
        concurrency,
    )
    .await;
}
