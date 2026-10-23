mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use common::*;
use durable_core::{Clock, RunStatus};
use durable_runtime::*;
use serde_json::{Value, json};

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[tokio::test]
async fn sync_workflow_runs_blocking_steps_to_success() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("resize", |ctx: &WfSyncCtx, n: i64| {
                let a: i64 = ctx.step("split", move || Ok::<_, StepError>(n * 2))?;
                let b: i64 = ctx.step("merge", move || Ok::<_, StepError>(a + 1))?;
                Ok(b)
            }),
    );
    let id = h.start("resize", json!(20)).await;
    assert_eq!(h.succeeded(id).await, json!(41));
}

#[tokio::test]
async fn sync_step_bodies_run_once_across_a_suspend_and_resume() {
    let mut h = Harness::new();
    let runs = counter();
    let r = runs.clone();
    h.spawn(
        h.worker()
            .register_sync_fn("memo", move |ctx: &WfSyncCtx, _: Value| {
                let r = r.clone();
                let a: i64 = ctx.step("a", move || {
                    r.fetch_add(1, SeqCst);
                    Ok::<_, StepError>(1)
                })?;
                ctx.sleep("nap", Duration::from_secs(3600))?;
                Ok(a + 1)
            }),
    );
    let id = h.start("memo", json!(null)).await;
    h.suspended(id).await;
    h.advance(3600);
    assert_eq!(h.succeeded(id).await, json!(2));
    assert_eq!(runs.load(SeqCst), 1);
}

#[tokio::test]
async fn map_fans_out_on_the_compute_pool_and_is_memoized_per_item() {
    let mut h = Harness::new();
    let runs = counter();
    let r = runs.clone();
    h.spawn(h.worker().compute_threads(4).register_sync_fn(
        "fanout",
        move |ctx: &WfSyncCtx, _: Value| {
            let r = r.clone();
            let out: Vec<i64> = ctx.map("sq", vec![1, 2, 3, 4], move |x: i64| {
                r.fetch_add(1, SeqCst);
                Ok::<_, StepError>(x * x)
            })?;
            ctx.sleep("nap", Duration::from_secs(60))?;
            Ok(out)
        },
    ));
    let id = h.start("fanout", json!(null)).await;
    h.suspended(id).await;
    h.advance(60);
    assert_eq!(h.succeeded(id).await, json!([1, 4, 9, 16]));
    assert_eq!(runs.load(SeqCst), 4, "items must not re-run after replay");
}

#[tokio::test]
async fn join2_runs_two_steps_in_parallel() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .compute_threads(2)
            .register_sync_fn("j2", |ctx: &WfSyncCtx, _: Value| {
                let (a, b): (i64, String) = ctx.join2(
                    ("a", || {
                        std::thread::sleep(Duration::from_millis(300));
                        Ok::<_, StepError>(1)
                    }),
                    ("b", || {
                        std::thread::sleep(Duration::from_millis(300));
                        Ok::<_, StepError>("x".to_string())
                    }),
                )?;
                Ok(format!("{a}{b}"))
            }),
    );
    let t = std::time::Instant::now();
    let id = h.start("j2", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!("1x"));
    assert!(t.elapsed() < Duration::from_millis(550), "ran serially");
}

#[tokio::test]
async fn join_runs_a_heterogeneous_tuple_of_steps_in_parallel() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .compute_threads(4)
            .register_sync_fn("jn", |ctx: &WfSyncCtx, _: Value| {
                let nap = || std::thread::sleep(Duration::from_millis(300));
                let (a, b, c, d): (i64, String, Vec<u8>, bool) = ctx.join((
                    ("a", move || {
                        nap();
                        Ok::<_, StepError>(1)
                    }),
                    ("b", move || {
                        nap();
                        Ok::<_, StepError>("x".to_string())
                    }),
                    ("c", move || {
                        nap();
                        Ok::<_, StepError>(vec![3u8])
                    }),
                    ("d", move || {
                        nap();
                        Ok::<_, StepError>(true)
                    }),
                ))?;
                Ok(format!("{a}{b}{c:?}{d}"))
            })
            .register_sync_fn("jn_fail", |ctx: &WfSyncCtx, _: Value| {
                let r: Result<(i64, i64, i64), _> = ctx.join((
                    ("ok", || Ok::<_, StepError>(1)),
                    ("bad", || Err::<i64, _>(StepError::fatal("boom"))),
                    ("ok2", || Ok::<_, StepError>(3)),
                ));
                Ok(r.is_err())
            }),
    );
    let t = std::time::Instant::now();
    let id = h.start("jn", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!("1x[3]true"));
    assert!(t.elapsed() < Duration::from_millis(700), "ran serially");

    let id = h.start("jn_fail", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(true));
}

#[tokio::test]
async fn sync_step_retries_with_the_given_policy() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .inline_retry_threshold(Duration::from_secs(60))
            .register_sync_fn("retry", |ctx: &WfSyncCtx, _: Value| {
                let v: u32 =
                    ctx.step_with("flaky", RetryPolicy::fixed(4, Duration::ZERO), None, || {
                        let attempt = step::current().attempt();
                        if attempt < 3 {
                            return Err(StepError::retryable("not yet"));
                        }
                        Ok(attempt)
                    })?;
                Ok(v)
            }),
    );
    let id = h.start("retry", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(3));
}

#[tokio::test]
async fn step_async_runs_io_on_tokio_while_the_driver_thread_blocks() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("io", |ctx: &WfSyncCtx, _: Value| {
                let v: i64 = ctx.step_async("fetch", || async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<_, StepError>(5)
                })?;
                Ok(v)
            }),
    );
    let id = h.start("io", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(5));
}

#[tokio::test]
async fn sync_signal_and_timeout_waits() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("approve", |ctx: &WfSyncCtx, _: Value| {
                let v: i64 = ctx.wait_for_signal("go")?;
                Ok(v)
            })
            .register_sync_fn("timeout", |ctx: &WfSyncCtx, _: Value| {
                let v: Option<i64> = ctx.wait_for_signal_timeout("go", Duration::from_secs(30))?;
                Ok(v)
            }),
    );
    let a = h.start("approve", json!(null)).await;
    h.suspended(a).await;
    h.client.signal(a, "go", json!(7)).await.unwrap();
    assert_eq!(h.succeeded(a).await, json!(7));

    let b = h.start("timeout", json!(null)).await;
    h.suspended(b).await;
    h.advance(30);
    assert_eq!(h.succeeded(b).await, json!(null));
}

#[tokio::test]
async fn sync_hook_dispose() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("disposer", |ctx: &WfSyncCtx, _: Value| {
                let hook = ctx.create_hook_with_token::<i64>("cb", "sync-tok")?;
                let v = ctx.recv(&hook)?;
                ctx.dispose_hook(hook)?;
                ctx.sleep("linger", Duration::from_secs(3600))?;
                Ok(v)
            }),
    );
    let a = h.start("disposer", json!(null)).await;
    h.suspended(a).await;
    h.client.resume_hook("sync-tok", json!(5)).await.unwrap();
    h.until(a, "sleep after dispose", |r| {
        r.wake_at.is_some_and(|t| t > h.clock.now())
    })
    .await;
    assert!(h.client.resume_hook("sync-tok", json!(6)).await.is_err());
    h.advance(3601);
    assert_eq!(h.succeeded(a).await, json!(5));
}

#[tokio::test]
async fn sync_hooks_and_events() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("hooked", |ctx: &WfSyncCtx, _: Value| {
                let hook = ctx.create_hook::<i64>("cb")?;
                let first = ctx.recv(&hook)?;
                let second = ctx.recv(&hook)?;
                Ok(first + second)
            })
            .register_sync_fn("evented", |ctx: &WfSyncCtx, _: Value| {
                let v: Value = ctx.wait_for_event("thing.happened", Some(json!({"k": 1})))?;
                Ok(v)
            }),
    );
    let a = h.start("hooked", json!(null)).await;
    let run = h.suspended(a).await;
    let token = run.awaiting[0].clone();
    h.client.resume_hook(&token, json!(1)).await.unwrap();
    h.client.resume_hook(&token, json!(2)).await.unwrap();
    assert_eq!(h.succeeded(a).await, json!(3));

    let b = h.start("evented", json!(null)).await;
    h.suspended(b).await;
    assert_eq!(
        h.client
            .send_event("thing.happened", json!({"k": 2}))
            .await
            .unwrap(),
        0
    );
    h.client
        .send_event("thing.happened", json!({"k": 1, "x": 9}))
        .await
        .unwrap();
    assert_eq!(h.succeeded(b).await, json!({"k": 1, "x": 9}));
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

#[tokio::test]
async fn interrupted_sync_body_unwinds_cleanly_when_the_run_suspends() {
    let mut h = Harness::new();
    let unwound = Arc::new(AtomicBool::new(false));
    let u = unwound.clone();
    h.spawn(
        h.worker()
            .register_sync_fn("unwind", move |ctx: &WfSyncCtx, _: Value| {
                let _guard = Dropped(u.clone());
                ctx.wait_for_signal::<i64>("never-this-run")?;
                Ok(1)
            }),
    );
    let id = h.start("unwind", json!(null)).await;
    h.suspended(id).await;
    // Suspending interrupts the body; `?` propagates and the thread exits.
    h.eventually("driver thread to unwind", || async { unwound.load(SeqCst) })
        .await;
}

#[tokio::test]
async fn cancelling_a_waiting_sync_run_and_a_running_blocking_step() {
    let mut h = Harness::new();
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let s = saw_cancel.clone();
    h.spawn(
        h.worker()
            .register_sync_fn("waiting", |ctx: &WfSyncCtx, _: Value| {
                ctx.sleep("long", Duration::from_secs(86_400))?;
                Ok(())
            })
            .register_sync_fn("cpu", move |ctx: &WfSyncCtx, _: Value| {
                let s = s.clone();
                ctx.step("spin", move || {
                    while !step::current().is_cancelled() {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    s.store(true, SeqCst);
                    Ok::<_, StepError>(())
                })?;
                Ok(())
            }),
    );
    let a = h.start("waiting", json!(null)).await;
    h.suspended(a).await;
    h.client.cancel(a, None).await.unwrap();
    h.status(a, RunStatus::Cancelled).await;

    let b = h.start("cpu", json!(null)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.client.cancel(b, None).await.unwrap();
    h.status(b, RunStatus::Cancelled).await;
    h.eventually("blocking step to observe cancel", || async {
        saw_cancel.load(SeqCst)
    })
    .await;
}

#[tokio::test]
async fn panic_in_a_sync_body_is_an_engine_fault() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("boom", |_ctx: &WfSyncCtx, _: Value| -> Result<i64, _> {
                panic!("sync body exploded")
            }),
    );
    let id = h.start("boom", json!(null)).await;
    let run = h.until(id, "fault", |r| r.last_error.is_some()).await;
    assert!(run.last_error.unwrap().contains("sync body exploded"));
    assert!(!run.status.is_terminal());
}

#[tokio::test]
async fn sync_workflow_fails_the_run_with_its_error() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_sync_fn("bad", |_ctx: &WfSyncCtx, _: Value| -> Result<i64, _> {
                Err(WorkflowError::failed("nope"))
            }),
    );
    let id = h.start("bad", json!(null)).await;
    assert!(
        h.status(id, RunStatus::Failed)
            .await
            .error
            .unwrap()
            .contains("nope")
    );
}
