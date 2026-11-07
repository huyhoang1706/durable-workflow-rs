mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use common::*;
use durable_runtime::*;
use durable_spi::{Clock, EventKind, RunStatus};
use serde_json::{Value, json};

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[tokio::test]
async fn runs_a_workflow_with_steps_to_success() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("add", |ctx: WfAsyncCtx, n: i64| async move {
                let a: i64 = ctx
                    .step("double", async move { Ok::<_, StepError>(n * 2) })
                    .await?;
                let b: i64 = ctx
                    .step("inc", async move { Ok::<_, StepError>(a + 1) })
                    .await?;
                Ok(b)
            }),
    );
    let id = h.start("add", json!(20)).await;
    assert_eq!(h.succeeded(id).await, json!(41));

    let events = h.client.events(id).await.unwrap();
    let kinds: Vec<_> = events
        .iter()
        .map(|e| std::mem::discriminant(&e.kind))
        .collect();
    assert_eq!(kinds.len(), 4); // started, 2 steps, succeeded
    assert!(matches!(
        events.last().unwrap().kind,
        EventKind::RunSucceeded { .. }
    ));
}

#[tokio::test]
async fn step_bodies_run_once_across_a_suspend_and_resume() {
    let mut h = Harness::new();
    let (a_runs, b_runs) = (counter(), counter());
    let (ac, bc) = (a_runs.clone(), b_runs.clone());
    h.spawn(
        h.worker()
            .register_fn("memo", move |ctx: WfAsyncCtx, _: Value| {
                let (ac, bc) = (ac.clone(), bc.clone());
                async move {
                    let a: i64 = ctx
                        .step("a", async move {
                            ac.fetch_add(1, SeqCst);
                            Ok::<_, StepError>(1)
                        })
                        .await?;
                    ctx.sleep("nap", Duration::from_secs(3600)).await?;
                    let b: i64 = ctx
                        .step("b", async move {
                            bc.fetch_add(1, SeqCst);
                            Ok::<_, StepError>(a + 1)
                        })
                        .await?;
                    Ok(b)
                }
            }),
    );
    let id = h.start("memo", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(
        run.wake_at,
        Some(durable_testkit::t0() + chrono::Duration::seconds(3600))
    );
    h.advance(3600);
    assert_eq!(h.succeeded(id).await, json!(2));
    assert_eq!((a_runs.load(SeqCst), b_runs.load(SeqCst)), (1, 1));
}

#[tokio::test]
async fn parallel_steps_run_concurrently() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("par", |ctx: WfAsyncCtx, _: Value| async move {
                let slow = |name: &'static str, v: i64| {
                    ctx.step(name, async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok::<_, StepError>(v)
                    })
                };
                let (a, b) = futures::try_join!(slow("a", 1), slow("b", 2))?;
                Ok(a + b)
            }),
    );
    let started = std::time::Instant::now();
    let id = h.start("par", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(3));
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "steps ran serially"
    );
}

#[tokio::test]
async fn failing_step_is_retried_and_attempt_is_visible() {
    let mut h = Harness::new();
    let attempts = counter();
    let a = attempts.clone();
    h.spawn(
        h.worker()
            .inline_retry_threshold(Duration::from_secs(60))
            .register_fn("retry", move |ctx: WfAsyncCtx, _: Value| {
                let a = a.clone();
                async move {
                    let seen: u32 = ctx
                        .step("flaky", async move {
                            a.fetch_add(1, SeqCst);
                            let attempt = step::current().attempt();
                            if attempt < 3 {
                                return Err(StepError::retryable("not yet"));
                            }
                            Ok(attempt)
                        })
                        .retry(RetryPolicy::fixed(5, Duration::from_secs(2)))
                        .await?;
                    Ok(seen)
                }
            }),
    );
    let id = h.start("retry", json!(null)).await;
    // Short backoff: retried in-process once the clock moves.
    for _ in 0..2 {
        h.eventually("a retry to be pending", || async { true })
            .await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        h.advance(2);
    }
    assert_eq!(h.succeeded(id).await, json!(3));
    assert_eq!(attempts.load(SeqCst), 3);
}

#[tokio::test]
async fn long_backoff_suspends_the_run_instead_of_holding_a_worker() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .inline_retry_threshold(Duration::from_secs(5))
            .register_fn("slow-retry", |ctx: WfAsyncCtx, _: Value| async move {
                let v: u32 = ctx
                    .step("flaky", async {
                        if step::current().attempt() < 2 {
                            return Err(StepError::retryable("later"));
                        }
                        Ok(7)
                    })
                    .retry(RetryPolicy::fixed(3, Duration::from_secs(600)))
                    .await?;
                Ok(v)
            }),
    );
    let id = h.start("slow-retry", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(
        run.wake_at,
        Some(durable_testkit::t0() + chrono::Duration::seconds(600))
    );
    h.advance(600);
    assert_eq!(h.succeeded(id).await, json!(7));
}

#[tokio::test]
async fn fatal_error_stops_retries_and_fails_the_run() {
    let mut h = Harness::new();
    let attempts = counter();
    let a = attempts.clone();
    h.spawn(
        h.worker()
            .register_fn("fatal", move |ctx: WfAsyncCtx, _: Value| {
                let a = a.clone();
                async move {
                    ctx.step("bad", async move {
                        a.fetch_add(1, SeqCst);
                        Err::<(), _>(StepError::fatal("nope"))
                    })
                    .retry(RetryPolicy::exponential(10))
                    .await?;
                    Ok(())
                }
            }),
    );
    let id = h.start("fatal", json!(null)).await;
    let run = h.status(id, RunStatus::Failed).await;
    assert!(run.error.unwrap().contains("nope"));
    assert_eq!(attempts.load(SeqCst), 1);
}

#[tokio::test]
async fn exhausted_retries_surface_to_workflow_code_which_can_recover() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("recover", |ctx: WfAsyncCtx, _: Value| async move {
                let r = ctx
                    .step("bad", async {
                        Err::<i64, _>(StepError::retryable("always"))
                    })
                    .retry(RetryPolicy::none())
                    .await;
                match r {
                    Err(WorkflowError::Step(e)) => Ok(format!("recovered from {}", e.message)),
                    other => Err(WorkflowError::failed(format!("unexpected {other:?}"))),
                }
            }),
    );
    let id = h.start("recover", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!("recovered from always"));
}

#[tokio::test]
async fn a_panicking_step_is_a_retryable_failure() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .inline_retry_threshold(Duration::from_secs(60))
            .register_fn("panic", |ctx: WfAsyncCtx, _: Value| async move {
                let v: u32 = ctx
                    .step("p", async {
                        if step::current().attempt() == 1 {
                            panic!("kaboom");
                        }
                        Ok::<_, StepError>(5)
                    })
                    .retry(RetryPolicy::fixed(3, Duration::ZERO))
                    .await?;
                Ok(v)
            }),
    );
    let id = h.start("panic", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(5));
    let failed =
        h.client.events(id).await.unwrap().into_iter().any(
            |e| matches!(e.kind, EventKind::StepFailed { error, .. } if error.contains("kaboom")),
        );
    assert!(failed);
}

#[tokio::test]
async fn step_timeout_fails_the_attempt() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("timeout", |ctx: WfAsyncCtx, _: Value| async move {
                let r = ctx
                    .step("hang", async {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        Ok::<_, StepError>(1)
                    })
                    .timeout(Duration::from_millis(50))
                    .retry(RetryPolicy::none())
                    .await;
                Ok(r.is_err())
            }),
    );
    let id = h.start("timeout", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!(true));
}

#[tokio::test]
async fn sleep_suspends_and_resumes_when_the_clock_reaches_it() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("sleepy", |ctx: WfAsyncCtx, _: Value| async move {
                let before = ctx.now();
                ctx.sleep("nap", Duration::from_secs(90)).await?;
                Ok((ctx.now() - before).num_seconds())
            }),
    );
    let id = h.start("sleepy", json!(null)).await;
    h.suspended(id).await;
    h.advance(89);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(h.run(id).await.status, RunStatus::Waiting);
    h.advance(1);
    assert_eq!(h.succeeded(id).await, json!(90));
}

#[tokio::test]
async fn signal_wakes_a_waiting_run_with_its_payload() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("approve", |ctx: WfAsyncCtx, _: Value| async move {
                let v: Value = ctx.wait_for_signal("approval").await?;
                Ok(v)
            }),
    );
    let id = h.start("approve", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(run.awaiting, ["approval"]);
    h.client
        .signal(id, "approval", json!({"by": "ann"}))
        .await
        .unwrap();
    assert_eq!(h.succeeded(id).await, json!({"by": "ann"}));
}

#[tokio::test]
async fn signal_sent_before_the_workflow_waits_is_not_lost() {
    let mut h = Harness::new();
    let w = h
        .worker()
        .register_fn("early", |ctx: WfAsyncCtx, _: Value| async move {
            ctx.sleep("first", Duration::from_secs(10)).await?;
            let v: i64 = ctx.wait_for_signal("go").await?;
            Ok(v)
        });
    h.spawn(w);
    let id = h.start("early", json!(null)).await;
    h.suspended(id).await;
    h.client.signal(id, "go", json!(42)).await.unwrap();
    h.advance(10);
    assert_eq!(h.succeeded(id).await, json!(42));
}

#[tokio::test]
async fn signal_wait_times_out_to_none() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("timeout-wait", |ctx: WfAsyncCtx, _: Value| async move {
                let v: Option<i64> = ctx
                    .wait_for_signal("approval")
                    .timeout(Duration::from_secs(300))
                    .await?;
                Ok(v)
            }),
    );
    let id = h.start("timeout-wait", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(
        run.wake_at,
        Some(durable_testkit::t0() + chrono::Duration::seconds(300))
    );
    h.advance(300);
    assert_eq!(h.succeeded(id).await, json!(null));
}

#[tokio::test]
async fn hooks_deliver_webhook_payloads_by_token() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("hooked", |ctx: WfAsyncCtx, _: Value| async move {
                let hook = ctx.create_hook::<Value>("callback").await?;
                let token = hook.token().to_string();
                ctx.step("announce", async move { Ok::<_, StepError>(token) })
                    .await?;
                let first = hook.recv().await?;
                let second = hook.recv().await?;
                Ok(json!([first, second]))
            }),
    );
    let id = h.start("hooked", json!(null)).await;
    let run = h.suspended(id).await;
    let token = run.awaiting[0].clone();
    h.client.resume_hook(&token, json!(1)).await.unwrap();
    h.client.resume_hook(&token, json!(2)).await.unwrap();
    assert_eq!(h.succeeded(id).await, json!([1, 2]));
    assert!(h.client.resume_hook("unknown", json!(1)).await.is_err());
}

#[tokio::test]
async fn disposing_a_hook_releases_its_token_while_the_run_lives_on() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("disposer", |ctx: WfAsyncCtx, _: Value| async move {
                let hook = ctx.create_hook_with_token::<i64>("cb", "reusable").await?;
                let v = hook.recv().await?;
                hook.dispose().await?;
                ctx.sleep("linger", Duration::from_secs(3600)).await?;
                Ok(v)
            })
            .register_fn("claimer", |ctx: WfAsyncCtx, _: Value| async move {
                match ctx.create_hook_with_token::<i64>("cb", "reusable").await {
                    Ok(hook) => Ok(json!(hook.recv().await?)),
                    Err(WorkflowError::HookConflict(_)) => Ok(json!("conflict")),
                    Err(e) => Err(e),
                }
            }),
    );
    let a = h.start("disposer", json!(null)).await;
    h.suspended(a).await;
    h.client.resume_hook("reusable", json!(7)).await.unwrap();
    h.until(a, "sleep after dispose", |r| {
        r.wake_at.is_some_and(|t| t > h.clock.now())
    })
    .await;

    // `a` is still alive, but the token is gone.
    assert!(h.client.resume_hook("reusable", json!(8)).await.is_err());
    let b = h.start("claimer", json!(null)).await;
    h.suspended(b).await;
    h.client.resume_hook("reusable", json!(9)).await.unwrap();
    assert_eq!(h.succeeded(b).await, json!(9));

    let events = h.client.events(a).await.unwrap();
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        EventKind::HookDisposed { token, .. } if token == "reusable"
    )));
    h.advance(3601);
    assert_eq!(h.succeeded(a).await, json!(7));
}

#[tokio::test]
async fn hook_token_conflict_is_reported_to_the_workflow() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("tok", |ctx: WfAsyncCtx, _: Value| async move {
                match ctx
                    .create_hook_with_token::<Value>("cb", "fixed-token")
                    .await
                {
                    Ok(hook) => {
                        hook.recv().await?;
                        Ok("owner".to_string())
                    }
                    Err(WorkflowError::HookConflict(_)) => Ok("conflict".to_string()),
                    Err(e) => Err(e),
                }
            }),
    );
    let a = h.start("tok", json!(null)).await;
    h.suspended(a).await;
    let b = h.start("tok", json!(null)).await;
    assert_eq!(h.succeeded(b).await, json!("conflict"));
    h.client.resume_hook("fixed-token", json!(1)).await.unwrap();
    assert_eq!(h.succeeded(a).await, json!("owner"));
}

#[tokio::test]
async fn events_reach_only_waits_whose_filter_matches() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("order", |ctx: WfAsyncCtx, id: i64| async move {
                let paid: Value = ctx
                    .wait_for_event("order.paid")
                    .filter(json!({"order_id": id}))
                    .await?;
                Ok(paid["amount"].clone())
            }),
    );
    let a = h.start("order", json!(1)).await;
    let b = h.start("order", json!(2)).await;
    h.suspended(a).await;
    h.suspended(b).await;
    h.client
        .send_event("order.paid", json!({"order_id": 2, "amount": 50}))
        .await
        .unwrap();
    assert_eq!(h.succeeded(b).await, json!(50));
    assert_eq!(h.run(a).await.status, RunStatus::Waiting);
}

#[tokio::test]
async fn cancelling_a_waiting_run_finishes_it_as_cancelled() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("cw", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.sleep("long", Duration::from_secs(86_400)).await?;
                Ok(())
            }),
    );
    let id = h.start("cw", json!(null)).await;
    h.suspended(id).await;
    h.client
        .cancel(id, Some("changed my mind".into()))
        .await
        .unwrap();
    h.status(id, RunStatus::Cancelled).await;
    let last = h.client.events(id).await.unwrap().pop().unwrap();
    assert!(
        matches!(last.kind, EventKind::RunCancelled { reason: Some(r) } if r == "changed my mind")
    );
}

#[tokio::test]
async fn cancelling_mid_step_aborts_the_step() {
    let mut h = Harness::new();
    let started = counter();
    let s = started.clone();
    h.spawn(
        h.worker()
            .register_fn("cm", move |ctx: WfAsyncCtx, _: Value| {
                let s = s.clone();
                async move {
                    ctx.step("hang", async move {
                        s.fetch_add(1, SeqCst);
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        Ok::<_, StepError>(())
                    })
                    .await?;
                    Ok(())
                }
            }),
    );
    let id = h.start("cm", json!(null)).await;
    h.eventually("step to start", || async { started.load(SeqCst) == 1 })
        .await;
    h.client.cancel(id, None).await.unwrap();
    h.status(id, RunStatus::Cancelled).await;
}

#[tokio::test]
async fn run_deadline_fails_the_run() {
    let mut h = Harness::new();
    h.spawn(h.worker().register_fn_with(
        WorkflowOptions::default().timeout(Duration::from_secs(120)),
        "deadline",
        |ctx: WfAsyncCtx, _: Value| async move {
            ctx.sleep("long", Duration::from_secs(3600)).await?;
            Ok(())
        },
    ));
    let id = h.start("deadline", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(
        run.wake_at,
        Some(durable_testkit::t0() + chrono::Duration::seconds(120))
    );
    h.advance(120);
    let run = h.status(id, RunStatus::Failed).await;
    assert!(run.error.unwrap().contains("deadline"));
}

#[tokio::test]
async fn start_is_idempotent_on_the_key() {
    let mut h = Harness::new();
    let runs = counter();
    let r = runs.clone();
    h.spawn(
        h.worker()
            .register_fn("once", move |ctx: WfAsyncCtx, _: Value| {
                let r = r.clone();
                async move {
                    ctx.step("s", async move {
                        r.fetch_add(1, SeqCst);
                        Ok::<_, StepError>(())
                    })
                    .await?;
                    Ok(())
                }
            }),
    );
    let opts = || StartOptions::default().idempotency_key("order-17");
    let first = h.client.start_raw("once", json!(1), opts()).await.unwrap();
    let second = h.client.start_raw("once", json!(1), opts()).await.unwrap();
    assert!(matches!(first, StartOutcome::Started(_)));
    assert!(matches!(second, StartOutcome::Existing(_)));
    assert_eq!(first.handle().unwrap().id(), second.handle().unwrap().id());
    h.succeeded(first.handle().unwrap().id()).await;
    assert_eq!(runs.load(SeqCst), 1);
}

#[tokio::test]
async fn handle_result_resolves_typed_output_and_failure() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("ok", |_ctx: WfAsyncCtx, n: i64| async move { Ok(n + 1) })
            .register_fn("bad", |_ctx: WfAsyncCtx, _: Value| async move {
                Err::<i64, _>(WorkflowError::failed("denied"))
            }),
    );
    let ok = h
        .client
        .start_raw("ok", json!(1), Default::default())
        .await
        .unwrap();
    assert_eq!(ok.handle().unwrap().result::<i64>().await.unwrap(), 2);
    let bad = h
        .client
        .start_raw("bad", json!(1), Default::default())
        .await
        .unwrap();
    match bad.handle().unwrap().result::<i64>().await {
        Err(RunError::Failed(m)) => assert!(m.contains("denied")),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn failed_run_can_be_retried_from_its_failure_point() {
    let mut h = Harness::new();
    let (ok_runs, bad_runs) = (counter(), counter());
    let (okc, badc) = (ok_runs.clone(), bad_runs.clone());
    h.spawn(
        h.worker()
            .register_fn("retryable-run", move |ctx: WfAsyncCtx, _: Value| {
                let (okc, badc) = (okc.clone(), badc.clone());
                async move {
                    ctx.step("ok", async move {
                        okc.fetch_add(1, SeqCst);
                        Ok::<_, StepError>(1)
                    })
                    .await?;
                    let v: i64 = ctx
                        .step("flaky", async move {
                            // Fails until the operator "fixes" it (second run of the body).
                            if badc.fetch_add(1, SeqCst) < 1 {
                                return Err(StepError::fatal("broken"));
                            }
                            Ok(9)
                        })
                        .await?;
                    Ok(v)
                }
            }),
    );
    let id = h.start("retryable-run", json!(null)).await;
    h.status(id, RunStatus::Failed).await;
    h.client.retry(id).await.unwrap();
    assert_eq!(h.succeeded(id).await, json!(9));
    assert_eq!(ok_runs.load(SeqCst), 1, "memoized step must not re-run");
    assert_eq!(bad_runs.load(SeqCst), 2);
}

#[tokio::test]
async fn deterministic_helpers_are_stable_across_replays() {
    let mut h = Harness::new();
    let seen = Arc::new(parking_lot::Mutex::new(vec![]));
    let s = seen.clone();
    h.spawn(
        h.worker()
            .register_fn("det", move |ctx: WfAsyncCtx, _: Value| {
                let s = s.clone();
                async move {
                    let (r, u) = (ctx.random_u64(), ctx.uuid());
                    s.lock().push((r, u));
                    ctx.sleep("nap", Duration::from_secs(10)).await?;
                    Ok((r, u.to_string()))
                }
            }),
    );
    let id = h.start("det", json!(null)).await;
    h.suspended(id).await;
    h.advance(10);
    h.succeeded(id).await;
    let seen = seen.lock();
    assert_eq!(
        seen.len(),
        2,
        "body should have executed twice (initial + replay)"
    );
    assert_eq!(seen[0], seen[1]);
}

#[tokio::test]
async fn changed_workflow_code_is_reported_as_non_determinism_and_run_stays_open() {
    let mut h = Harness::new();
    let v1 = h.spawn(
        h.worker()
            .register_fn("evolve", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.sleep("wait", Duration::from_secs(60)).await?;
                Ok(1)
            }),
    );
    let id = h.start("evolve", json!(null)).await;
    h.suspended(id).await;
    h.stop(v1).await;

    // v2 replaces the sleep with a step under the same key.
    h.spawn(
        h.worker()
            .register_fn("evolve", |ctx: WfAsyncCtx, _: Value| async move {
                let v: i64 = ctx.step("wait", async { Ok::<_, StepError>(2) }).await?;
                Ok(v)
            }),
    );
    h.advance(60);
    let run = h
        .until(id, "a recorded engine fault", |r| r.last_error.is_some())
        .await;
    assert!(run.last_error.unwrap().contains("non-determinism"));
    assert!(!run.status.is_terminal());
}

#[tokio::test]
async fn awaiting_a_non_durable_future_is_an_engine_fault() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("nondurable", |_ctx: WfAsyncCtx, _: Value| async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok(1)
            }),
    );
    let id = h.start("nondurable", json!(null)).await;
    let run = h.until(id, "fault", |r| r.last_error.is_some()).await;
    assert!(run.last_error.unwrap().contains("non-durable"));
    assert!(!run.status.is_terminal());
}

#[tokio::test]
async fn select_is_deterministic_between_a_signal_and_a_timeout() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("race", |ctx: WfAsyncCtx, _: Value| async move {
                let signal = ctx.wait_for_signal::<i64>("go");
                let timer = ctx.sleep("deadline", Duration::from_secs(60));
                tokio::select! {
                    v = signal => Ok(format!("signal {}", v?)),
                    t = timer => { t?; Ok("timeout".to_string()) }
                }
            }),
    );
    let a = h.start("race", json!(null)).await;
    h.suspended(a).await;
    h.client.signal(a, "go", json!(1)).await.unwrap();
    assert_eq!(h.succeeded(a).await, json!("signal 1"));

    let b = h.start("race", json!(null)).await;
    h.suspended(b).await;
    h.advance(60);
    assert_eq!(h.succeeded(b).await, json!("timeout"));
}

#[tokio::test]
async fn workflow_concurrency_limit_is_never_exceeded() {
    let mut h = Harness::new();
    let (now, peak) = (counter(), counter());
    let (nc, pc) = (now.clone(), peak.clone());
    h.spawn(h.worker().concurrency(16).register_fn_with(
        WorkflowOptions::default().concurrency(2),
        "limited",
        move |ctx: WfAsyncCtx, _: Value| {
            let (nc, pc) = (nc.clone(), pc.clone());
            async move {
                ctx.step("work", async move {
                    let cur = nc.fetch_add(1, SeqCst) + 1;
                    pc.fetch_max(cur, SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    nc.fetch_sub(1, SeqCst);
                    Ok::<_, StepError>(())
                })
                .await?;
                Ok(())
            }
        },
    ));
    let mut ids = vec![];
    for _ in 0..8 {
        ids.push(h.start("limited", json!(null)).await);
    }
    for id in ids {
        h.succeeded(id).await;
    }
    assert!(
        peak.load(SeqCst) <= 2,
        "peak concurrency {}",
        peak.load(SeqCst)
    );
    assert!(peak.load(SeqCst) >= 1);
}

#[tokio::test]
async fn ready_resolves_even_when_asked_long_after_startup() {
    let h = Harness::new();
    let worker = h
        .worker()
        .register_fn("noop", |_ctx: WfAsyncCtx, _: Value| async { Ok(()) })
        .build()
        .unwrap()
        .spawn();
    // The definition sync finishes while nobody is waiting on `ready()`.
    tokio::time::sleep(Duration::from_millis(200)).await;
    tokio::time::timeout(Duration::from_secs(2), worker.ready())
        .await
        .expect("ready() must not hang when called late");
    worker.shutdown().await;
}
