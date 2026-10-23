//! End-to-end tests on real Postgres + Redis, with several workers in one
//! process: crashes, Redis loss, concurrency limits, throttling.

mod common;

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use common::*;
use durable_workflow::{
    Backend, Client, RunStatus, StepError, WfAsyncCtx, WfSyncCtx, Worker, WorkerBuilder,
    WorkflowOptions, core::EventKind,
};
use parking_lot::Mutex;
use serde_json::{Value, json};

fn worker(b: &Backend) -> WorkerBuilder {
    Worker::builder(b.clone())
        .lease(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .reconcile_interval(Duration::from_millis(200))
        .drain_timeout(Duration::from_millis(500))
        .concurrency(16)
        .compute_threads(2)
}

async fn wait_all_finished(client: &Client, ids: &[uuid::Uuid], secs: u64) {
    let start = Instant::now();
    loop {
        let mut pending = vec![];
        for id in ids {
            let r = client.get_run(*id).await.unwrap().unwrap();
            if !r.status.is_terminal() {
                pending.push((r.id, r.status, r.last_error, r.wake_at));
            }
        }
        if pending.is_empty() {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "{} runs unfinished after {secs}s; e.g. {:?}",
            pending.len(),
            &pending[..pending.len().min(3)]
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Everything the mixed workload needs, shared by every worker.
#[derive(Clone, Default)]
struct Probe {
    /// (run, step key) -> times the body executed.
    executions: Arc<Mutex<HashMap<(String, String), usize>>>,
    /// Steps that ran although their result was already committed.
    violations: Arc<Mutex<Vec<String>>>,
}

fn register_mixed(b: WorkerBuilder, client: Client, probe: Probe) -> WorkerBuilder {
    let (c1, p1) = (client.clone(), probe.clone());
    let (c2, p2) = (client, probe);
    // Blocking steps run on compute threads: reach async code through a handle.
    let rt = tokio::runtime::Handle::current();
    b.register_fn("mixed-async", move |ctx: WfAsyncCtx, n: i64| {
        let (client, probe) = (c1.clone(), p1.clone());
        async move {
            let mut total = 0i64;
            for i in 0..3 {
                let (client, probe) = (client.clone(), probe.clone());
                total += ctx
                    .step(&format!("s{i}"), move || async move {
                        let info = durable_workflow::step::current();
                        guarded(&client, &probe, info.idempotency_key()).await;
                        tokio::time::sleep(Duration::from_millis(40)).await;
                        Ok::<_, StepError>(n + i)
                    })
                    .await?;
            }
            ctx.sleep("nap", Duration::from_millis(600)).await?;
            let got: Value = ctx.wait_for_signal("go").await?;
            Ok(total + got.as_i64().unwrap_or(0))
        }
    })
    .register_sync_fn("mixed-sync", move |ctx: &WfSyncCtx, n: i64| {
        let (client, probe, rt) = (c2.clone(), p2.clone(), rt.clone());
        let parts: Vec<i64> = ctx.map("work", vec![n, n + 1, n + 2], move |x: i64| {
            let info = durable_workflow::step::current();
            rt.block_on(guarded(&client, &probe, info.idempotency_key()));
            std::thread::sleep(Duration::from_millis(30));
            Ok::<_, StepError>(x)
        })?;
        ctx.sleep("nap", Duration::from_millis(600))?;
        let got: Value = ctx.wait_for_signal("go")?;
        Ok(parts.iter().sum::<i64>() + got.as_i64().unwrap_or(0))
    })
}

/// Record an execution and flag it if the step's result is already committed.
async fn guarded(client: &Client, probe: &Probe, idempotency_key: &str) {
    let (run, key) = idempotency_key.split_once(':').unwrap();
    *probe
        .executions
        .lock()
        .entry((run.into(), key.into()))
        .or_default() += 1;
    let run_id: uuid::Uuid = run.parse().unwrap();
    let committed = client
        .events(run_id)
        .await
        .unwrap()
        .into_iter()
        .any(|e| matches!(&e.kind, EventKind::StepSucceeded { key: k, .. } if k == key));
    if committed {
        probe.violations.lock().push(idempotency_key.to_string());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_async_and_sync_runs_complete_on_postgres_and_redis() {
    let (pg, rd) = (shared_pg().await, shared_redis().await);
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let probe = Probe::default();
    let w = register_mixed(worker(&backend), client.clone(), probe.clone())
        .build()
        .unwrap()
        .spawn();
    w.ready().await;

    let mut ids = vec![];
    for i in 0..10 {
        let wf = if i % 2 == 0 {
            "mixed-async"
        } else {
            "mixed-sync"
        };
        ids.push(
            client
                .start_raw(wf, json!(i), Default::default())
                .await
                .unwrap()
                .handle()
                .unwrap()
                .id(),
        );
    }
    for id in &ids {
        // Wait until suspended, then signal.
        let start = Instant::now();
        loop {
            let r = client.get_run(*id).await.unwrap().unwrap();
            if r.awaiting == ["go"] {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "run never reached the signal wait: {r:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        client.signal(*id, "go", json!(100)).await.unwrap();
    }
    wait_all_finished(&client, &ids, 30).await;
    for id in &ids {
        assert_eq!(
            client.get_run(*id).await.unwrap().unwrap().status,
            RunStatus::Succeeded
        );
    }
    assert!(
        probe.violations.lock().is_empty(),
        "{:?}",
        probe.violations.lock()
    );
    w.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_workers_mid_step_loses_no_run_and_reruns_no_committed_step() {
    let (pg, rd) = (shared_pg().await, shared_redis().await);
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let probe = Probe::default();
    let build = || {
        register_mixed(worker(&backend), client.clone(), probe.clone())
            .build()
            .unwrap()
            .spawn()
    };
    let (w1, w2, w3) = (build(), build(), build());
    for w in [&w1, &w2, &w3] {
        w.ready().await;
    }

    let mut ids = vec![];
    for i in 0..120 {
        let wf = if i % 3 == 0 {
            "mixed-sync"
        } else {
            "mixed-async"
        };
        ids.push(
            client
                .start_raw(wf, json!(i), Default::default())
                .await
                .unwrap()
                .handle()
                .unwrap()
                .id(),
        );
    }
    // Crash one worker while steps are in flight, then another a bit later.
    tokio::time::sleep(Duration::from_millis(150)).await;
    w1.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    w2.abort();

    // Signal everything as it reaches the wait (the surviving worker finishes the rest).
    let start = Instant::now();
    let mut signalled = std::collections::HashSet::new();
    while signalled.len() < ids.len() {
        assert!(
            start.elapsed() < Duration::from_secs(90),
            "only {} runs reached the wait",
            signalled.len()
        );
        for id in &ids {
            if signalled.contains(id) {
                continue;
            }
            let r = client.get_run(*id).await.unwrap().unwrap();
            if r.awaiting == ["go"] {
                client.signal(*id, "go", json!(1)).await.unwrap();
                signalled.insert(*id);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_all_finished(&client, &ids, 90).await;
    for id in &ids {
        let r = client.get_run(*id).await.unwrap().unwrap();
        assert_eq!(r.status, RunStatus::Succeeded, "{r:?}");
    }
    assert!(
        probe.violations.lock().is_empty(),
        "committed steps were re-executed: {:?}",
        probe.violations.lock()
    );
    w3.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushing_redis_mid_flight_is_recovered_by_the_reconciler() {
    let pg = shared_pg().await;
    let rd = start_redis().await; // dedicated: FLUSHALL must not touch other tests
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let w = worker(&backend)
        .register_fn("sleepy", |ctx: WfAsyncCtx, n: i64| async move {
            ctx.step("a", move || async move { Ok::<_, StepError>(n) })
                .await?;
            ctx.sleep("nap", Duration::from_secs(2)).await?;
            let v: i64 = ctx
                .step("b", move || async move { Ok::<_, StepError>(n * 2) })
                .await?;
            Ok(v)
        })
        .build()
        .unwrap()
        .spawn();
    w.ready().await;

    let mut ids = vec![];
    for i in 0..30 {
        ids.push(
            client
                .start_raw("sleepy", json!(i), Default::default())
                .await
                .unwrap()
                .handle()
                .unwrap()
                .id(),
        );
    }
    // Let the runs suspend on their timers, then wipe all of Redis (queue, leases, cache).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let r = redis::Client::open(rd.url.as_str()).unwrap();
    let mut conn = r.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    wait_all_finished(&client, &ids, 40).await;
    for (i, id) in ids.iter().enumerate() {
        let run = client.get_run(*id).await.unwrap().unwrap();
        assert_eq!(
            (run.status, run.output),
            (RunStatus::Succeeded, Some(json!(i as i64 * 2)))
        );
    }
    w.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_holds_across_workers() {
    let (pg, rd) = (shared_pg().await, shared_redis().await);
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let (now, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let build = || {
        let (now, peak) = (now.clone(), peak.clone());
        worker(&backend)
            .register_fn_with(
                WorkflowOptions::default().concurrency(3),
                "limited",
                move |ctx: WfAsyncCtx, _: Value| {
                    let (now, peak) = (now.clone(), peak.clone());
                    async move {
                        ctx.step("work", move || async move {
                            let cur = now.fetch_add(1, SeqCst) + 1;
                            peak.fetch_max(cur, SeqCst);
                            tokio::time::sleep(Duration::from_millis(120)).await;
                            now.fetch_sub(1, SeqCst);
                            Ok::<_, StepError>(())
                        })
                        .await?;
                        Ok(())
                    }
                },
            )
            .build()
            .unwrap()
            .spawn()
    };
    let workers = [build(), build(), build()];
    for w in &workers {
        w.ready().await;
    }
    let mut ids = vec![];
    for _ in 0..30 {
        ids.push(
            client
                .start_raw("limited", json!(null), Default::default())
                .await
                .unwrap()
                .handle()
                .unwrap()
                .id(),
        );
    }
    wait_all_finished(&client, &ids, 60).await;
    assert!(
        peak.load(SeqCst) <= 3,
        "peak concurrency {} exceeded the limit of 3",
        peak.load(SeqCst)
    );
    assert!(peak.load(SeqCst) >= 2, "limit never exercised");
    for w in workers {
        w.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_spaces_out_run_starts() {
    let (pg, rd) = (shared_pg().await, shared_redis().await);
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let starts = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let s = starts.clone();
    let w = worker(&backend)
        .register_fn_with(
            WorkflowOptions::default().throttle(2, Duration::from_secs(1)),
            "throttled",
            move |ctx: WfAsyncCtx, _: Value| {
                let s = s.clone();
                async move {
                    ctx.step("mark", move || async move {
                        s.lock().push(Instant::now());
                        Ok::<_, StepError>(())
                    })
                    .await?;
                    Ok(())
                }
            },
        )
        .build()
        .unwrap()
        .spawn();
    w.ready().await;
    let mut ids = vec![];
    for _ in 0..6 {
        ids.push(
            client
                .start_raw("throttled", json!(null), Default::default())
                .await
                .unwrap()
                .handle()
                .unwrap()
                .id(),
        );
    }
    wait_all_finished(&client, &ids, 30).await;
    let mut t = starts.lock().clone();
    t.sort();
    assert_eq!(t.len(), 6);
    // No window of 1s (minus slack) may contain more than 2 starts.
    for window in t.windows(3) {
        let span = window[2].duration_since(window[0]);
        assert!(
            span >= Duration::from_millis(900),
            "3 starts within {span:?}"
        );
    }
    w.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_time_rate_limit_rejects_the_excess() {
    let (pg, rd) = (shared_pg().await, shared_redis().await);
    let backend = backend(&pg.url, &rd.url).await;
    let client = client(&backend);
    let w = worker(&backend)
        .register_fn_with(
            WorkflowOptions::default().rate_limit(3, Duration::from_secs(60)),
            "limited-start",
            |_ctx: WfAsyncCtx, _: Value| async move { Ok(()) },
        )
        .build()
        .unwrap()
        .spawn();
    w.ready().await;
    let mut outcomes = vec![];
    for _ in 0..5 {
        outcomes.push(
            client
                .start_raw("limited-start", json!(null), Default::default())
                .await
                .unwrap(),
        );
    }
    let started = outcomes.iter().filter(|o| o.handle().is_some()).count();
    assert_eq!(started, 3, "burst of 3, then rejected");
    assert!(matches!(
        outcomes[4],
        durable_workflow::StartOutcome::RateLimited { .. }
    ));
    w.shutdown().await;
}
