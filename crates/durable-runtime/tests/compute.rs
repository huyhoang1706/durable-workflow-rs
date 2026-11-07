mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use common::*;
use durable_runtime::*;
use serde_json::{Value, json};

fn spin(d: Duration) {
    let end = Instant::now() + d;
    while Instant::now() < end {
        std::hint::black_box(0u64.wrapping_add(1));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpu_bound_steps_never_delay_tokio_work() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .compute_threads(2)
            .concurrency(8)
            .register_sync_fn("cpu", |ctx: &WfSyncCtx, _: Value| {
                ctx.step_blocking("burn", || {
                    spin(Duration::from_millis(600));
                    Ok::<_, StepError>(())
                })?;
                Ok(())
            })
            .register_fn("io", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.step("quick", async {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok::<_, StepError>(())
                })
                .await?;
                Ok(())
            }),
    );

    // A ticker measures how late tokio wakes us while the pool is saturated.
    let ticker = tokio::spawn(async {
        let mut worst = Duration::ZERO;
        let end = Instant::now() + Duration::from_millis(500);
        while Instant::now() < end {
            let t = Instant::now();
            tokio::time::sleep(Duration::from_millis(10)).await;
            worst = worst.max(t.elapsed().saturating_sub(Duration::from_millis(10)));
        }
        worst
    });

    let cpu: Vec<_> = futures::future::join_all((0..2).map(|_| h.start("cpu", json!(null)))).await;
    tokio::time::sleep(Duration::from_millis(100)).await; // pool is now full
    let started = Instant::now();
    let io = h.start("io", json!(null)).await;
    h.succeeded(io).await;
    let io_latency = started.elapsed();

    let worst_lag = ticker.await.unwrap();
    assert!(
        worst_lag < Duration::from_millis(120),
        "tokio stalled for {worst_lag:?}"
    );
    assert!(
        io_latency < Duration::from_millis(400),
        "async step waited {io_latency:?}"
    );
    for id in cpu {
        h.succeeded(id).await;
    }
}

#[tokio::test]
async fn pool_size_bounds_parallel_map_work() {
    let mut h = Harness::new();
    let (running, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (r, p) = (running.clone(), peak.clone());
    h.spawn(h.worker().compute_threads(2).register_sync_fn(
        "bounded",
        move |ctx: &WfSyncCtx, _: Value| {
            let (r, p) = (r.clone(), p.clone());
            let out: Vec<i64> = ctx.map("w", (0..8).collect(), move |i: i64| {
                let now = r.fetch_add(1, SeqCst) + 1;
                p.fetch_max(now, SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                r.fetch_sub(1, SeqCst);
                Ok::<_, StepError>(i)
            })?;
            Ok(out.len())
        },
    ));
    let id = h.start("bounded", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(8));
    assert_eq!(peak.load(SeqCst), 2);
}

#[tokio::test]
async fn async_workflows_offload_cpu_work_with_map_blocking() {
    let mut h = Harness::new();
    h.spawn(h.worker().compute_threads(3).register_fn(
        "amap",
        |ctx: WfAsyncCtx, _: Value| async move {
            let out: Vec<i64> = ctx
                .map_blocking("sq", vec![1, 2, 3], |x: i64| Ok::<_, StepError>(x * x))
                .await?;
            Ok(out)
        },
    ));
    let id = h.start("amap", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!([1, 4, 9]));
}

#[tokio::test]
async fn blocking_step_timeout_fails_the_attempt_and_sets_the_cancel_flag() {
    let mut h = Harness::new();
    let observed = Arc::new(AtomicBool::new(false));
    let o = observed.clone();
    h.spawn(
        h.worker()
            .register_fn("bt", move |ctx: WfAsyncCtx, _: Value| {
                let o = o.clone();
                async move {
                    let r = ctx
                        .step_blocking("loop", move || {
                            while !step::current().is_cancelled() {
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            o.store(true, SeqCst);
                            Ok::<_, StepError>(1)
                        })
                        .timeout(Duration::from_millis(60))
                        .retry(RetryPolicy::none())
                        .await;
                    Ok(matches!(r, Err(WorkflowError::Step(_))))
                }
            }),
    );
    let id = h.start("bt", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(true));
    h.eventually("closure to observe cancellation", || async {
        observed.load(SeqCst)
    })
    .await;
}
