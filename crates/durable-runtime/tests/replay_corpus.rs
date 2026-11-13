//! Writes and checks the replay conformance corpus (`durable_testkit::corpus`).
//!
//! Each case runs a workflow on the memory backend until the run reaches the
//! state it describes, then replays the stored history with recording on.
//! The history and what the replay did are the case.
//!
//! - `cargo test -p durable-runtime --test replay_corpus` replays every
//!   committed history through the workflows below and fails on any change.
//! - `DURABLE_UPDATE_CORPUS=1 cargo test -p durable-runtime --test replay_corpus`
//!   runs the workflows again and rewrites the corpus. Run ids, timestamps and
//!   generated tokens change with it; review the diff of the traces.

mod common;

use std::{collections::BTreeSet, time::Duration};

use common::Harness;
use durable_runtime::*;
use durable_spi::{Clock, EventKind, RunId, RunStatus};
use durable_testkit::corpus::{self, Case, Finish};
use futures::future::BoxFuture;
use serde_json::{Value, json};

type Register = fn(WorkerBuilder) -> WorkerBuilder;
type Drive = for<'a> fn(&'a Harness, RunId) -> BoxFuture<'a, ()>;

struct Spec {
    name: &'static str,
    description: &'static str,
    workflow: &'static str,
    input: Value,
    /// The code that writes the history.
    record: Register,
    /// The code that replays it, when that is a changed version (non-determinism cases).
    replay: Option<Register>,
    /// Takes the run from its start to the state the history is captured in.
    drive: Drive,
}

impl Spec {
    fn replay_code(&self) -> Register {
        self.replay.unwrap_or(self.record)
    }
}

fn succeeds(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.succeeded(id).await;
    })
}

fn fails(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.status(id, RunStatus::Failed).await;
    })
}

fn suspends(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.suspended(id).await;
    })
}

fn faults(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.until(id, "an engine fault", |r| r.last_error.is_some())
            .await;
    })
}

/// Suspend, move the clock past every timer, finish.
fn sleeps_then_succeeds(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        let run = h.suspended(id).await;
        let wake = run.wake_at.expect("suspended on a timer");
        let secs = (wake - h.clock.now()).num_seconds();
        h.advance(secs);
        h.succeeded(id).await;
    })
}

fn signalled(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.suspended(id).await;
        h.client
            .signal(id, "approval", json!({"by": "ann"}))
            .await
            .unwrap();
        h.succeeded(id).await;
    })
}

fn hook_resumed_twice(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.suspended(id).await;
        h.client.resume_hook("corpus-hook", json!(1)).await.unwrap();
        h.client.resume_hook("corpus-hook", json!(2)).await.unwrap();
        h.succeeded(id).await;
    })
}

fn hook_resumed_by_minted_token(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        let token = h.suspended(id).await.awaiting[0].clone();
        h.client.resume_hook(&token, json!("hi")).await.unwrap();
        h.succeeded(id).await;
    })
}

fn order_paid(h: &Harness, id: RunId) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        h.suspended(id).await;
        for (order_id, amount) in [(1, 10), (2, 50)] {
            h.client
                .send_event(
                    "order.paid",
                    json!({"order_id": order_id, "amount": amount}),
                )
                .await
                .unwrap();
        }
        h.succeeded(id).await;
    })
}

fn race(ctx: WfAsyncCtx) -> BoxFuture<'static, Result<String, WorkflowError>> {
    Box::pin(async move {
        let signal = ctx.wait_for_signal::<Value>("approval");
        let timer = ctx.sleep("deadline", Duration::from_secs(60));
        // `biased`: an unbiased select polls its branches in random order, so
        // the two commands would be issued in a different order on every replay.
        tokio::select! {
            biased;
            v = signal => Ok(format!("signal {}", v?)),
            t = timer => { t?; Ok("timeout".to_string()) }
        }
    })
}

fn specs() -> Vec<Spec> {
    vec![
        Spec {
            name: "sequential_steps",
            description: "Doubles the input in step `double`, adds one in step `inc`, returns the result.",
            workflow: "add",
            input: json!(20),
            record: |b| {
                b.register_fn("add", |ctx: WfAsyncCtx, n: i64| async move {
                    let a: i64 = ctx
                        .step("double", async move { Ok::<_, StepError>(n * 2) })
                        .await?;
                    let b: i64 = ctx
                        .step("inc", async move { Ok::<_, StepError>(a + 1) })
                        .await?;
                    Ok(b)
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "parallel_steps",
            description: "Awaits steps `a` (1), `b` (2) and `c` (3) together, issued in that order; returns their sum.",
            workflow: "fan-out",
            input: json!(null),
            record: |b| {
                b.register_fn("fan-out", |ctx: WfAsyncCtx, _: Value| async move {
                    let (a, b, c) = futures::try_join!(
                        ctx.step("a", async { Ok::<i64, StepError>(1) }),
                        ctx.step("b", async { Ok::<i64, StepError>(2) }),
                        ctx.step("c", async { Ok::<i64, StepError>(3) }),
                    )?;
                    Ok(a + b + c)
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "repeated_names",
            description: "Calls step `item` three times in a loop, returning i * 10; the keys are `item`, `item#1`, `item#2`.",
            workflow: "loop",
            input: json!(null),
            record: |b| {
                b.register_fn("loop", |ctx: WfAsyncCtx, _: Value| async move {
                    let mut out = vec![];
                    for i in 0..3i64 {
                        out.push(
                            ctx.step("item", async move { Ok::<i64, StepError>(i * 10) })
                                .await?,
                        );
                    }
                    Ok(out)
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "step_retry",
            description: "Step `flaky` (retry: 5 attempts, no delay) fails attempts 1 and 2 and returns its attempt number on attempt 3.",
            workflow: "retry",
            input: json!(null),
            record: |b| {
                b.register_fn("retry", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: u32 = ctx
                        .step("flaky", async {
                            let attempt = step::current().attempt();
                            if attempt < 3 {
                                return Err(StepError::retryable(format!(
                                    "attempt {attempt} failed"
                                )));
                            }
                            Ok(attempt)
                        })
                        .retry(RetryPolicy::fixed(5, Duration::ZERO))
                        .await?;
                    Ok(v)
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "step_failure_recovered",
            description: "Step `bad` (no retries) fails with `always`; the workflow catches the error and returns `recovered from always`.",
            workflow: "recover",
            input: json!(null),
            record: |b| {
                b.register_fn("recover", |ctx: WfAsyncCtx, _: Value| async move {
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
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "step_failure_fails_run",
            description: "Step `bad` fails with the fatal error `nope`; the workflow propagates it, so the run fails.",
            workflow: "fatal",
            input: json!(null),
            record: |b| {
                b.register_fn("fatal", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.step("bad", async { Err::<(), _>(StepError::fatal("nope")) })
                        .await?;
                    Ok(())
                })
            },
            replay: None,
            drive: fails,
        },
        Spec {
            name: "sleep",
            description: "Sleeps 90 s as `nap`, then returns how many seconds `ctx.now()` moved.",
            workflow: "sleepy",
            input: json!(null),
            record: |b| {
                b.register_fn("sleepy", |ctx: WfAsyncCtx, _: Value| async move {
                    let before = ctx.now();
                    ctx.sleep("nap", Duration::from_secs(90)).await?;
                    Ok((ctx.now() - before).num_seconds())
                })
            },
            replay: None,
            drive: sleeps_then_succeeds,
        },
        Spec {
            name: "signal",
            description: "Waits for signal `approval` and returns its payload.",
            workflow: "approve",
            input: json!(null),
            record: |b| {
                b.register_fn("approve", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: Value = ctx.wait_for_signal("approval").await?;
                    Ok(v)
                })
            },
            replay: None,
            drive: signalled,
        },
        Spec {
            name: "signal_timeout",
            description: "Waits up to 300 s for signal `approval`; none arrives, so it returns null.",
            workflow: "timeout-wait",
            input: json!(null),
            record: |b| {
                b.register_fn("timeout-wait", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: Option<i64> = ctx
                        .wait_for_signal("approval")
                        .timeout(Duration::from_secs(300))
                        .await?;
                    Ok(v)
                })
            },
            replay: None,
            drive: sleeps_then_succeeds,
        },
        Spec {
            name: "race_timer_wins",
            description: "Races signal `approval` against a 60 s sleep `deadline`. The timer fires first, the signal wait is released, and it returns `timeout`.",
            workflow: "race",
            input: json!(null),
            record: |b| b.register_fn("race", |ctx: WfAsyncCtx, _: Value| race(ctx)),
            replay: None,
            drive: sleeps_then_succeeds,
        },
        Spec {
            name: "race_signal_wins",
            description: "Races signal `approval` against a 60 s sleep `deadline`. The signal arrives first, the sleep is released, and it returns `signal <payload>`.",
            workflow: "race",
            input: json!(null),
            record: |b| b.register_fn("race", |ctx: WfAsyncCtx, _: Value| race(ctx)),
            replay: None,
            drive: signalled,
        },
        Spec {
            name: "hook",
            description: "Creates hook `callback` with token `corpus-hook`, receives two payloads (keys `callback[recv]`, `callback[recv]#1`), disposes it (`callback[dispose]`), returns their sum.",
            workflow: "hooked",
            input: json!(null),
            record: |b| {
                b.register_fn("hooked", |ctx: WfAsyncCtx, _: Value| async move {
                    let hook = ctx
                        .create_hook_with_token::<i64>("callback", "corpus-hook")
                        .await?;
                    let first = hook.recv().await?;
                    let second = hook.recv().await?;
                    hook.dispose().await?;
                    Ok(first + second)
                })
            },
            replay: None,
            drive: hook_resumed_twice,
        },
        Spec {
            name: "hook_minted_token",
            description: "Creates hook `inbox` without a token (the engine mints one) and returns the first payload.",
            workflow: "inbox",
            input: json!(null),
            record: |b| {
                b.register_fn("inbox", |ctx: WfAsyncCtx, _: Value| async move {
                    let hook = ctx.create_hook::<Value>("inbox").await?;
                    hook.recv().await
                })
            },
            replay: None,
            drive: hook_resumed_by_minted_token,
        },
        Spec {
            name: "event_filter",
            description: "Waits for event `order.paid` filtered on `{\"order_id\": <input>}` and returns its `amount`. An event for another order arrives first and is ignored.",
            workflow: "order",
            input: json!(2),
            record: |b| {
                b.register_fn("order", |ctx: WfAsyncCtx, id: i64| async move {
                    let paid: Value = ctx
                        .wait_for_event("order.paid")
                        .filter(json!({"order_id": id}))
                        .await?;
                    Ok(paid["amount"].clone())
                })
            },
            replay: None,
            drive: order_paid,
        },
        Spec {
            name: "map",
            description: "Squares [1, 2, 3] with a parallel map named `square`; the keys are `square[0]`, `square[1]`, `square[2]`.",
            workflow: "squares",
            input: json!(null),
            record: |b| {
                b.register_fn("squares", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.map_blocking("square", vec![1i64, 2, 3], |x| Ok::<_, StepError>(x * x))
                        .await
                })
            },
            replay: None,
            drive: succeeds,
        },
        Spec {
            name: "deterministic_helpers",
            description: "Draws `random_u64` and `uuid`, sleeps 10 s as `nap`, draws another `uuid`, and returns all three (the number as a string) with `now`.",
            workflow: "det",
            input: json!(null),
            record: |b| {
                b.register_fn("det", |ctx: WfAsyncCtx, _: Value| async move {
                    let (random, first) = (ctx.random_u64(), ctx.uuid());
                    ctx.sleep("nap", Duration::from_secs(10)).await?;
                    Ok(json!({
                        "random": random.to_string(),
                        "first": first,
                        "second": ctx.uuid(),
                        "now": ctx.now(),
                    }))
                })
            },
            replay: None,
            drive: sleeps_then_succeeds,
        },
        Spec {
            name: "sync_workflow",
            description: "A blocking workflow: steps `a` (n + 1) and `b` (n * 2) in parallel, a 60 s sleep `cool-down`, then step `sum` adds them.",
            workflow: "sync-sum",
            input: json!(5),
            record: |b| {
                b.register_sync_fn("sync-sum", |ctx: &WfSyncCtx, n: i64| {
                    let (a, b) = ctx.join2(
                        ("a", move || Ok::<i64, StepError>(n + 1)),
                        ("b", move || Ok::<i64, StepError>(n * 2)),
                    )?;
                    ctx.sleep("cool-down", Duration::from_secs(60))?;
                    ctx.step_blocking("sum", move || Ok::<i64, StepError>(a + b))
                })
            },
            replay: None,
            drive: sleeps_then_succeeds,
        },
        Spec {
            name: "waiting_on_signal",
            description: "Runs step `prepare`, then waits for signal `approval`. Captured while waiting: the wait is the frontier and the body has not finished.",
            workflow: "pending",
            input: json!(null),
            record: |b| {
                b.register_fn("pending", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.step("prepare", async { Ok::<_, StepError>("ready".to_string()) })
                        .await?;
                    let v: Value = ctx.wait_for_signal("approval").await?;
                    Ok(v)
                })
            },
            replay: None,
            drive: suspends,
        },
        Spec {
            name: "invalid_name",
            description: "Calls a step named `bad#name`. `#` is reserved for generated keys, so the run faults.",
            workflow: "bad-name",
            input: json!(null),
            record: |b| {
                b.register_fn("bad-name", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: i64 = ctx
                        .step("bad#name", async { Ok::<_, StepError>(1) })
                        .await?;
                    Ok(v)
                })
            },
            replay: None,
            drive: faults,
        },
        Spec {
            name: "nondeterminism_kind_mismatch",
            description: "History from a version that slept 60 s as `wait`; the replaying version runs a step `wait` instead, which faults.",
            workflow: "evolve",
            input: json!(null),
            record: |b| {
                b.register_fn("evolve", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.sleep("wait", Duration::from_secs(60)).await?;
                    Ok(1)
                })
            },
            replay: Some(|b| {
                b.register_fn("evolve", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: i64 = ctx.step("wait", async { Ok::<_, StepError>(2) }).await?;
                    Ok(v)
                })
            }),
            drive: suspends,
        },
        Spec {
            name: "nondeterminism_missing_command",
            description: "History from a version that ran steps `a` then `b`; the replaying version runs `a` then `c`, so the event for `b` is never claimed and the run faults.",
            workflow: "renamed",
            input: json!(null),
            record: |b| {
                b.register_fn("renamed", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.step("a", async { Ok::<_, StepError>(1) }).await?;
                    ctx.step("b", async { Ok::<_, StepError>(2) }).await?;
                    Ok(())
                })
            },
            replay: Some(|b| {
                b.register_fn("renamed", |ctx: WfAsyncCtx, _: Value| async move {
                    ctx.step("a", async { Ok::<_, StepError>(1) }).await?;
                    ctx.step("c", async { Ok::<_, StepError>(2) }).await?;
                    Ok(())
                })
            }),
            drive: succeeds,
        },
        Spec {
            name: "nondeterminism_output_type",
            description: "History from a version whose step `name` returned a string; the replaying version expects a number, so the host faults the run when the output does not decode.",
            workflow: "typed",
            input: json!(null),
            record: |b| {
                b.register_fn("typed", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: String = ctx
                        .step("name", async { Ok::<_, StepError>("ann".to_string()) })
                        .await?;
                    Ok(json!(v))
                })
            },
            replay: Some(|b| {
                b.register_fn("typed", |ctx: WfAsyncCtx, _: Value| async move {
                    let v: i64 = ctx.step("name", async { Ok::<_, StepError>(1) }).await?;
                    Ok(json!(v))
                })
            }),
            drive: succeeds,
        },
    ]
}

fn finish(r: Option<Result<Value, durable_sm::WorkflowFailure>>) -> Option<Finish> {
    r.map(|r| match r {
        Ok(v) => Finish::Ok(v),
        Err(e) => Finish::Err(e),
    })
}

async fn replay(spec: &Spec, run_id: RunId, history: Vec<durable_spi::Event>) -> Replay {
    let h = Harness::new();
    let worker = (spec.replay_code())(h.worker()).build().unwrap();
    worker
        .replay(spec.workflow, run_id, spec.input.clone(), history)
        .await
        .expect("the workflow is registered")
}

/// Run the workflow live, then replay what it stored.
async fn record(spec: &Spec) -> Case {
    let mut h = Harness::new();
    let w = h.spawn((spec.record)(h.worker()));
    let id = h.start(spec.workflow, spec.input.clone()).await;
    (spec.drive)(&h, id).await;
    let run = h.run(id).await;
    let events = h.client.events(id).await.unwrap();
    h.stop(w).await;

    let history: Vec<_> = durable_runtime::history::normalize(events)
        .into_iter()
        .filter(|e| !e.kind.is_terminal())
        .collect();
    let r = replay(spec, id, history.clone()).await;

    if spec.replay.is_none() && run.last_error.is_none() {
        // The replay of unchanged code must agree with the live run.
        assert_eq!(r.fault, None, "{}: replay faulted", spec.name);
        match run.status {
            RunStatus::Succeeded => {
                assert_eq!(
                    r.result,
                    Some(Ok(run.output.clone().unwrap())),
                    "{}",
                    spec.name
                )
            }
            RunStatus::Failed => assert!(matches!(r.result, Some(Err(_))), "{}", spec.name),
            _ => assert_eq!(r.result, None, "{}: the body finished early", spec.name),
        }
    } else {
        assert!(r.fault.is_some(), "{}: expected a fault", spec.name);
    }
    Case {
        name: spec.name.into(),
        description: spec.description.into(),
        workflow: spec.workflow.into(),
        input: spec.input.clone(),
        run_id: id,
        history,
        trace: r.trace,
        frontier: r.frontier,
        fault: r.fault,
        result: finish(r.result),
    }
}

/// Replay the committed history; everything must come out as recorded.
async fn check(spec: &Spec, case: &Case) -> Result<(), String> {
    if case.workflow != spec.workflow || case.input != spec.input {
        return Err("workflow or input differs from the spec; regenerate the corpus".into());
    }
    let r = replay(spec, case.run_id, case.history.clone()).await;
    if let Some(diff) = corpus::first_difference(&case.trace, &r.trace) {
        return Err(diff);
    }
    if r.frontier != case.frontier {
        return Err(format!(
            "frontier: expected {:?}, got {:?}",
            case.frontier, r.frontier
        ));
    }
    if r.fault != case.fault {
        return Err(format!(
            "fault: expected {:?}, got {:?}",
            case.fault, r.fault
        ));
    }
    let result = finish(r.result);
    if result != case.result {
        return Err(format!(
            "result: expected {:?}, got {result:?}",
            case.result
        ));
    }
    Ok(())
}

#[tokio::test]
async fn the_rust_sdk_replays_the_corpus_as_recorded() {
    let update = std::env::var_os("DURABLE_UPDATE_CORPUS").is_some();
    let specs = specs();
    let mut failures = vec![];
    for spec in &specs {
        let path = corpus::path(spec.name);
        if update {
            let case = record(spec).await;
            std::fs::create_dir_all(corpus::dir()).unwrap();
            std::fs::write(&path, case.to_json()).unwrap();
            continue;
        }
        let outcome = match Case::load(&path) {
            Ok(case) => check(spec, &case).await,
            Err(e) => Err(format!(
                "{e}\nrun `DURABLE_UPDATE_CORPUS=1 cargo test -p durable-runtime --test replay_corpus`"
            )),
        };
        if let Err(e) = outcome {
            failures.push(format!("{}: {e}", spec.name));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn every_corpus_file_has_a_spec() {
    let names: BTreeSet<_> = specs().iter().map(|s| s.name).collect();
    assert_eq!(names.len(), specs().len(), "two specs share a name");
    let stale: Vec<String> = std::fs::read_dir(corpus::dir())
        .map(|d| {
            d.filter_map(|e| {
                let name = e.ok()?.path().file_stem()?.to_str()?.to_string();
                (!names.contains(name.as_str())).then_some(name)
            })
            .collect()
        })
        .unwrap_or_default();
    assert!(stale.is_empty(), "corpus files without a spec: {stale:?}");
}

#[test]
fn the_corpus_covers_every_event_kind_a_replay_consumes() {
    let Ok(cases) = corpus::load_all() else {
        return;
    };
    let seen: BTreeSet<String> = cases
        .iter()
        .flat_map(|c| &c.history)
        .filter_map(|e| e.kind.key().map(|_| kind_name(&e.kind)))
        .collect();
    let want = [
        "hook_created",
        "hook_disposed",
        "signal_received",
        "step_failed",
        "step_succeeded",
        "timer_fired",
        "timer_scheduled",
        "wait_started",
        "wait_timed_out",
    ];
    let missing: Vec<_> = want.iter().filter(|k| !seen.contains(**k)).collect();
    assert!(missing.is_empty(), "no corpus history has {missing:?}");
}

fn kind_name(k: &EventKind) -> String {
    serde_json::to_value(k).unwrap()["type"]
        .as_str()
        .unwrap()
        .to_string()
}

/// A host with no Rust futures and no `Driver`: it replays a corpus trace,
/// making the recorded replayer calls at each activation, and answers step
/// launches with the outcomes in the case's history. The engine cannot tell
/// it from a real SDK, so this checks that the host abstraction carries
/// everything an SDK needs, live phase included.
mod scripted {
    use std::{collections::HashMap, sync::Arc};

    use durable_runtime::{
        ErrorClass, StepError, WorkflowOptions,
        deps::DepKey,
        host::{ExecEnv, ExecStatus, HostExecution, HostWorkflow, ReplayHandle, StepAttempt},
        step::StepInfo,
    };
    use durable_sm::{Handle, Resolution, Resolved, TraceOp};
    use durable_spi::{EventKind, WorkflowDefinition};
    use durable_testkit::corpus::{Case, Finish};
    use futures::FutureExt;

    pub struct ScriptedHost {
        workflow: String,
        case: Arc<Case>,
    }

    impl ScriptedHost {
        pub fn new(case: Case) -> Self {
            Self {
                workflow: case.workflow.clone(),
                case: Arc::new(case),
            }
        }
    }

    impl HostWorkflow for ScriptedHost {
        fn name(&self) -> &str {
            &self.workflow
        }
        fn definition(&self) -> WorkflowDefinition {
            WorkflowOptions::default().definition(&self.workflow)
        }
        fn required_deps(&self) -> Vec<DepKey> {
            vec![]
        }
        fn start(&self, env: ExecEnv) -> Box<dyn HostExecution> {
            // One segment per `advance`: what it resolved, then the calls the
            // code made in response.
            let mut segments: Vec<(Vec<Resolved>, Vec<TraceOp>)> = vec![];
            for op in &self.case.trace {
                match op {
                    TraceOp::Advance { resolved, .. } => segments.push((resolved.clone(), vec![])),
                    op => segments
                        .last_mut()
                        .expect("a trace starts with an advance")
                        .1
                        .push(op.clone()),
                }
            }
            Box::new(Execution {
                case: self.case.clone(),
                replay: env.replay,
                segments,
                next: 0,
                handles: HashMap::new(),
            })
        }
    }

    struct Execution {
        case: Arc<Case>,
        replay: ReplayHandle,
        segments: Vec<(Vec<Resolved>, Vec<TraceOp>)>,
        next: usize,
        handles: HashMap<String, Handle>,
    }

    impl Execution {
        fn mismatch(&self, what: String) -> ExecStatus {
            self.replay
                .with(|r| r.set_fault(format!("scripted host: {what}")));
            ExecStatus::Pending
        }

        /// Make the recorded calls of segment `self.next`.
        fn play_segment(&mut self) -> Result<(), String> {
            let ops = self.segments[self.next].1.clone();
            self.next += 1;
            let same_seed = self.replay.with(|r| r.run_id()) == self.case.run_id;
            for op in ops {
                self.replay.with(|r| -> Result<(), String> {
                    match op {
                        TraceOp::UserKey { name, key } => expect_key(r.user_key(&name), key),
                        TraceOp::KeyFor { name, key } => expect_key(r.key_for(&name), key),
                        TraceOp::CheckName { name } => {
                            r.check_name(&name);
                            Ok(())
                        }
                        TraceOp::Issue { key, command } => {
                            let h = r.issue(key.clone(), command);
                            self.handles.insert(key, h);
                            Ok(())
                        }
                        TraceOp::Release { key } => {
                            r.release(self.handles[&key]);
                            Ok(())
                        }
                        // Seeded by the run id, so only a replay of the
                        // recorded run reproduces the values.
                        TraceOp::Random { value } => {
                            let got = r.random_u64();
                            match !same_seed || got == value {
                                true => Ok(()),
                                false => Err(format!("random {got}, recorded {value}")),
                            }
                        }
                        TraceOp::Uuid { value } => {
                            let got = r.uuid();
                            match !same_seed || got == value {
                                true => Ok(()),
                                false => Err(format!("uuid {got}, recorded {value}")),
                            }
                        }
                        TraceOp::SetFault { message } => {
                            r.set_fault(message);
                            Ok(())
                        }
                        // Engine-side calls, not the code's.
                        TraceOp::Interrupt | TraceOp::Cancel | TraceOp::Advance { .. } => Ok(()),
                    }
                })?;
            }
            Ok(())
        }

        /// Segments with no resolutions: the code reran with nothing new.
        fn play_unresolved(&mut self) -> Result<(), String> {
            while self.next < self.segments.len() && self.segments[self.next].0.is_empty() {
                self.play_segment()?;
            }
            Ok(())
        }

        fn activate_sync(&mut self, resolved: Vec<(Handle, Resolution)>) -> Result<(), String> {
            if self.next == 0 {
                return self.play_unresolved();
            }
            if resolved.is_empty() {
                // Live events that resolve nothing (`timer_scheduled`, ...).
                return Ok(());
            }
            let got: Vec<Resolved> = resolved
                .into_iter()
                .map(|(h, resolution)| Resolved {
                    key: self.replay.with(|r| r.key_of(h).unwrap_or("?").to_string()),
                    resolution,
                })
                .collect();
            match self.segments.get(self.next) {
                Some((want, _)) if *want == got => {}
                want => return Err(format!("resolved {got:?}, recorded {want:?}")),
            }
            self.play_segment()?;
            self.play_unresolved()
        }
    }

    fn expect_key(got: String, recorded: String) -> Result<(), String> {
        match got == recorded {
            true => Ok(()),
            false => Err(format!("key `{got}`, recorded `{recorded}`")),
        }
    }

    #[async_trait::async_trait]
    impl HostExecution for Execution {
        async fn activate(&mut self, resolved: Vec<(Handle, Resolution)>) -> ExecStatus {
            if let Err(e) = self.activate_sync(resolved) {
                return self.mismatch(e);
            }
            match (&self.case.result, self.next == self.segments.len()) {
                (Some(Finish::Ok(v)), true) => ExecStatus::Finished(Ok(v.clone())),
                (Some(Finish::Err(e)), true) => ExecStatus::Finished(Err(e.clone())),
                _ => ExecStatus::Pending,
            }
        }

        fn start_step(
            &mut self,
            key: &str,
            attempt: u32,
            _info: StepInfo,
            _span: &tracing::Span,
        ) -> Option<StepAttempt> {
            // The attempt's outcome as history recorded it; never finishes if
            // history has none.
            let outcome = self.case.history.iter().find_map(|e| match &e.kind {
                EventKind::StepFailed {
                    key: k,
                    attempt: a,
                    error,
                    retryable,
                    ..
                } if k == key && *a == attempt => Some(Err(StepError {
                    message: error.clone(),
                    class: match retryable {
                        true => ErrorClass::Retryable,
                        false => ErrorClass::Fatal,
                    },
                })),
                EventKind::StepSucceeded { key: k, output } if k == key => Some(Ok(output.clone())),
                _ => None,
            });
            Some(StepAttempt {
                kind: "scripted",
                fut: async move {
                    match outcome {
                        Some(r) => r,
                        None => std::future::pending().await,
                    }
                }
                .boxed(),
            })
        }

        fn interrupt(&mut self) {}
    }
}

#[tokio::test]
async fn a_scripted_host_runs_corpus_workflows_on_the_real_engine() {
    for name in [
        "sequential_steps",
        "step_retry",
        "signal",
        "race_timer_wins",
    ] {
        let case = Case::load(&corpus::path(name)).unwrap();
        let spec = specs().into_iter().find(|s| s.name == name).unwrap();
        let mut h = Harness::new();
        let host = std::sync::Arc::new(scripted::ScriptedHost::new(case.clone()));
        let w = h.spawn(h.worker().register_host(host));
        let id = h.start(&case.workflow, case.input.clone()).await;
        (spec.drive)(&h, id).await;
        let run = h.run(id).await;
        let events = h.client.events(id).await.unwrap();
        h.stop(w).await;

        assert_eq!(run.last_error, None, "{name}");
        match &case.result {
            Some(Finish::Ok(v)) => assert_eq!(run.output.as_ref(), Some(v), "{name}"),
            other => panic!("{name}: expected a recorded output, got {other:?}"),
        }
        // The engine wrote the same history (ids and generated values aside).
        let shape = |events: &[durable_spi::Event]| -> Vec<(String, Option<String>)> {
            events
                .iter()
                .filter(|e| !e.kind.is_terminal())
                .map(|e| (kind_name(&e.kind), e.kind.key().map(str::to_string)))
                .collect()
        };
        let written = durable_runtime::history::normalize(events);
        assert_eq!(shape(&written), shape(&case.history), "{name}");
    }
}
