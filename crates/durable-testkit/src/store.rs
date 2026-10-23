//! Store conformance cases. Each takes a fresh, empty store.

use std::sync::Arc;

use durable_core::*;
use serde_json::{Value, json};

use crate::{secs, t0};

pub fn new_run(workflow: &str, key: Option<&str>) -> NewRun {
    new_run_in("ns", workflow, key)
}

pub fn new_run_in(namespace: &str, workflow: &str, key: Option<&str>) -> NewRun {
    NewRun {
        namespace: namespace.into(),
        workflow: workflow.into(),
        input: json!({"n": 1}),
        idempotency_key: key.map(Into::into),
        concurrency_key: None,
        throttle_key: None,
        deadline_at: None,
        at: t0(),
    }
}

async fn start(store: &Arc<dyn Store>) -> Run {
    store
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone()
}

fn wait(key: &str, kind: WaitKind, name: &str) -> WaitReg {
    WaitReg {
        key: key.into(),
        kind,
        name: name.into(),
        filter: None,
        timeout_at: None,
    }
}

fn waiting(run: &Run, seq: u64, waits: Vec<WaitReg>) -> Commit {
    let mut c = Commit::new(run.id, seq, t0());
    c.status = RunStatus::Waiting;
    c.awaiting = waits.iter().map(|w| w.name.clone()).collect();
    c.waits = waits;
    c
}

pub async fn create_run_is_idempotent_on_key(store: Arc<dyn Store>) {
    let a = store.create_run(new_run("wf", Some("k"))).await.unwrap();
    assert!(matches!(a, CreateOutcome::Created(_)));
    let b = store.create_run(new_run("wf", Some("k"))).await.unwrap();
    assert!(matches!(b, CreateOutcome::Existing(_)));
    assert_eq!(a.run().id, b.run().id);
    // Same key under another workflow, or no key at all, is a distinct run.
    let c = store.create_run(new_run("other", Some("k"))).await.unwrap();
    assert!(matches!(c, CreateOutcome::Created(_)));
    let d = store.create_run(new_run("wf", None)).await.unwrap();
    let e = store.create_run(new_run("wf", None)).await.unwrap();
    assert_ne!(d.run().id, e.run().id);
}

pub async fn new_run_has_started_event(store: Arc<dyn Store>) {
    let run = start(&store).await;
    assert_eq!(run.status, RunStatus::Pending);
    assert_eq!(run.last_seq, 1);
    let events = store.load_events(run.id, 0).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, 1);
    assert_eq!(
        events[0].kind,
        EventKind::RunStarted {
            input: json!({"n": 1})
        }
    );
}

pub async fn commit_appends_events_in_order(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut c = Commit::new(run.id, 1, t0());
    c.events = vec![
        EventKind::StepSucceeded {
            key: "a".into(),
            output: json!(1),
        },
        EventKind::StepSucceeded {
            key: "b".into(),
            output: json!(2),
        },
    ];
    let out = store.commit(c).await.unwrap();
    assert_eq!(out.last_seq, 3);
    assert!(!out.cancel_requested);
    let events = store.load_events(run.id, 1).await.unwrap();
    assert_eq!(events.iter().map(|e| e.seq).collect::<Vec<_>>(), [2, 3]);
    let run = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(run.last_seq, 3);
    assert_eq!(run.status, RunStatus::Running);
}

pub async fn commit_with_stale_seq_conflicts(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut first = Commit::new(run.id, 1, t0());
    first.events = vec![EventKind::StepSucceeded {
        key: "a".into(),
        output: json!(1),
    }];
    store.commit(first).await.unwrap();
    let mut stale = Commit::new(run.id, 1, t0());
    stale.events = vec![EventKind::TimerFired { key: "x".into() }];
    let err = store.commit(stale).await.unwrap_err();
    assert!(matches!(err, Error::Conflict), "{err:?}");
    // Nothing was appended.
    assert_eq!(store.load_events(run.id, 0).await.unwrap().len(), 2);
}

pub async fn terminal_commit_sets_status_and_output(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut c = Commit::new(run.id, 1, t0());
    c.events = vec![EventKind::RunSucceeded {
        output: json!("done"),
    }];
    store.commit(c).await.unwrap();
    let run = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(run.output, Some(json!("done")));
    assert!(run.wake_at.is_none());

    let run2 = start(&store).await;
    let mut c = Commit::new(run2.id, 1, t0());
    c.events = vec![EventKind::RunFailed {
        error: "boom".into(),
    }];
    store.commit(c).await.unwrap();
    let run2 = store.get_run(run2.id).await.unwrap().unwrap();
    assert_eq!(
        (run2.status, run2.error.as_deref()),
        (RunStatus::Failed, Some("boom"))
    );
}

pub async fn signal_before_wait_is_consumed_at_commit(store: Arc<dyn Store>) {
    let run = start(&store).await;
    store
        .deliver_signal(run.id, "go", json!({"ok": true}), t0())
        .await
        .unwrap();

    let mut c = Commit::new(run.id, 1, t0());
    c.waits = vec![wait("w", WaitKind::Signal, "go")];
    c.events = vec![EventKind::WaitStarted {
        key: "w".into(),
        kind: WaitKind::Signal,
        name: "go".into(),
        filter: None,
        timeout_at: None,
    }];
    let out = store.commit(c).await.unwrap();
    assert_eq!(out.generated.len(), 1);
    match &out.generated[0].kind {
        EventKind::SignalReceived { key, payload, .. } => {
            assert_eq!(key, "w");
            assert_eq!(payload, &json!({"ok": true}));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(out.last_seq, 3); // RunStarted, WaitStarted, SignalReceived
}

pub async fn each_signal_is_consumed_once(store: Arc<dyn Store>) {
    let run = start(&store).await;
    store
        .deliver_signal(run.id, "go", json!(1), t0())
        .await
        .unwrap();
    let mut c = Commit::new(run.id, 1, t0());
    c.waits = vec![wait("w", WaitKind::Signal, "go")];
    let first = store.commit(c).await.unwrap();
    assert_eq!(first.generated.len(), 1);

    // The satisfied wait is closed by its SignalReceived; re-registering it
    // must not consume anything else or resurrect it.
    let mut c = Commit::new(run.id, first.last_seq, t0());
    c.waits = vec![wait("w2", WaitKind::Signal, "go")];
    let second = store.commit(c).await.unwrap();
    assert!(second.generated.is_empty());
}

pub async fn signal_wakes_waiting_run(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut c = waiting(&run, 1, vec![wait("w", WaitKind::Signal, "go")]);
    c.wake_at = Some(t0() + secs(3600));
    store.commit(c).await.unwrap();

    let item = store
        .deliver_signal(run.id, "go", json!(1), t0() + secs(5))
        .await
        .unwrap();
    let item = item.expect("a waiting run must be woken");
    assert_eq!(item.run_id, run.id);
    assert_eq!(item.due_at, t0() + secs(5));
    assert_eq!(item.kind, WorkKind::Resume);
    let run = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(run.wake_at, Some(t0() + secs(5)));
}

pub async fn unrelated_signal_does_not_wake(store: Arc<dyn Store>) {
    let run = start(&store).await;
    store
        .commit(waiting(&run, 1, vec![wait("w", WaitKind::Signal, "go")]))
        .await
        .unwrap();
    let item = store
        .deliver_signal(run.id, "other", json!(1), t0())
        .await
        .unwrap();
    assert!(item.is_none());
}

pub async fn signal_to_finished_run_is_rejected(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut c = Commit::new(run.id, 1, t0());
    c.events = vec![EventKind::RunSucceeded {
        output: Value::Null,
    }];
    store.commit(c).await.unwrap();
    let err = store
        .deliver_signal(run.id, "go", json!(1), t0())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidState(_)), "{err:?}");
}

/// A signal racing a suspend must never be lost: either the commit consumes
/// it, or the delivery reports a wake-up.
pub async fn signal_racing_commit_is_never_lost(store: Arc<dyn Store>) {
    for i in 0..30 {
        let run = start(&store).await;
        let id = run.id;
        let s1 = store.clone();
        let s2 = store.clone();
        let commit = tokio::spawn(async move {
            let c = waiting(&run, 1, vec![wait("w", WaitKind::Signal, "go")]);
            s1.commit(c).await.unwrap()
        });
        let deliver =
            tokio::spawn(async move { s2.deliver_signal(id, "go", json!(i), t0()).await.unwrap() });
        let out = commit.await.unwrap();
        let woke = deliver.await.unwrap();
        let consumed = !out.generated.is_empty();
        assert!(consumed || woke.is_some(), "iteration {i}: lost wake-up");
    }
}

pub async fn hook_lifecycle(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let mut c = Commit::new(run.id, 1, t0());
    c.hooks = vec![HookReg {
        key: "h".into(),
        token: "tok".into(),
    }];
    let out = store.commit(c).await.unwrap();
    assert_eq!(
        out.generated[0].kind,
        EventKind::HookCreated {
            key: "h".into(),
            token: "tok".into(),
            conflict: false
        }
    );

    let mut c = waiting(
        &run,
        out.last_seq,
        vec![wait("h:recv", WaitKind::Hook, "tok")],
    );
    c.wake_at = None;
    store.commit(c).await.unwrap();

    let item = store
        .resolve_hook("ns", "tok", json!({"paid": true}), t0())
        .await
        .unwrap();
    assert!(item.is_some());
    let err = store
        .resolve_hook("ns", "nope", json!(1), t0())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
}

pub async fn hook_token_held_by_live_run_conflicts(store: Arc<dyn Store>) {
    let a = start(&store).await;
    let b = start(&store).await;
    let reg = || {
        vec![HookReg {
            key: "h".into(),
            token: "shared".into(),
        }]
    };
    let mut c = Commit::new(a.id, 1, t0());
    c.hooks = reg();
    store.commit(c).await.unwrap();
    let mut c = Commit::new(b.id, 1, t0());
    c.hooks = reg();
    let out = store.commit(c).await.unwrap();
    assert_eq!(
        out.generated[0].kind,
        EventKind::HookCreated {
            key: "h".into(),
            token: "shared".into(),
            conflict: true
        }
    );

    // Once the holder finishes, the token is free again.
    let mut done = Commit::new(a.id, 2, t0());
    done.events = vec![EventKind::RunSucceeded {
        output: Value::Null,
    }];
    store.commit(done).await.unwrap();
    let c3 = start(&store).await;
    let mut c = Commit::new(c3.id, 1, t0());
    c.hooks = reg();
    let out = store.commit(c).await.unwrap();
    assert!(matches!(
        out.generated[0].kind,
        EventKind::HookCreated {
            conflict: false,
            ..
        }
    ));
}

pub async fn hook_dispose_releases_the_token(store: Arc<dyn Store>) {
    let a = start(&store).await;
    let mut c = Commit::new(a.id, 1, t0());
    c.hooks = vec![HookReg {
        key: "h".into(),
        token: "once".into(),
    }];
    let out = store.commit(c).await.unwrap();

    // Disposing frees the token even though the holder is still live.
    let mut c = Commit::new(a.id, out.last_seq, t0());
    c.events = vec![EventKind::HookDisposed {
        key: "h:dispose".into(),
        token: "once".into(),
    }];
    let out = store.commit(c).await.unwrap();
    let err = store
        .resolve_hook("ns", "once", json!(1), t0())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");

    // Another live run can now claim it without a conflict.
    let b = start(&store).await;
    let mut c = Commit::new(b.id, 1, t0());
    c.hooks = vec![HookReg {
        key: "h".into(),
        token: "once".into(),
    }];
    let created = store.commit(c).await.unwrap();
    assert!(matches!(
        created.generated[0].kind,
        EventKind::HookCreated {
            conflict: false,
            ..
        }
    ));
    assert!(
        store
            .resolve_hook("ns", "once", json!(1), t0())
            .await
            .is_ok()
    );

    // Disposing a token the run no longer holds must not release b's claim.
    let mut c = Commit::new(a.id, out.last_seq, t0());
    c.events = vec![EventKind::HookDisposed {
        key: "h:dispose#1".into(),
        token: "once".into(),
    }];
    store.commit(c).await.unwrap();
    assert!(
        store
            .resolve_hook("ns", "once", json!(2), t0())
            .await
            .is_ok()
    );
}

pub async fn event_reaches_only_matching_waits(store: Arc<dyn Store>) {
    let matching = start(&store).await;
    let other = start(&store).await;
    let mk = |filter: Value| {
        let mut w = wait("e", WaitKind::Event, "order.paid");
        w.filter = Some(filter);
        w
    };
    store
        .commit(waiting(&matching, 1, vec![mk(json!({"order_id": 7}))]))
        .await
        .unwrap();
    store
        .commit(waiting(&other, 1, vec![mk(json!({"order_id": 8}))]))
        .await
        .unwrap();

    let items = store
        .publish_event(
            "ns",
            "order.paid",
            json!({"order_id": 7, "amount": 5}),
            t0(),
        )
        .await
        .unwrap();
    assert_eq!(
        items.iter().map(|i| i.run_id).collect::<Vec<_>>(),
        [matching.id]
    );

    // Consumed by the woken run's next commit with the same open wait.
    let mut c = Commit::new(matching.id, 1, t0());
    c.waits = vec![mk(json!({"order_id": 7}))];
    let out = store.commit(c).await.unwrap();
    match &out.generated[0].kind {
        EventKind::SignalReceived { key, payload, .. } => {
            assert_eq!(key, "e");
            assert_eq!(payload["amount"], 5);
        }
        other => panic!("unexpected {other:?}"),
    }
}

pub async fn event_before_wait_is_not_seen(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let items = store
        .publish_event("ns", "e", json!({}), t0())
        .await
        .unwrap();
    assert!(items.is_empty());
    let mut c = Commit::new(run.id, 1, t0());
    c.waits = vec![wait("w", WaitKind::Event, "e")];
    let out = store.commit(c).await.unwrap();
    assert!(out.generated.is_empty());
}

pub async fn cancel_flags_run_and_wakes_it(store: Arc<dyn Store>) {
    let run = start(&store).await;
    store
        .commit(waiting(&run, 1, vec![wait("w", WaitKind::Signal, "go")]))
        .await
        .unwrap();
    let item = store
        .request_cancel(run.id, Some("stop".into()), t0())
        .await
        .unwrap();
    assert!(item.is_some());
    let r = store.get_run(run.id).await.unwrap().unwrap();
    assert!(r.cancel_requested_at.is_some());
    let out = store.commit(Commit::new(run.id, 1, t0())).await.unwrap();
    assert!(out.cancel_requested);
}

pub async fn retry_reopens_failed_run_only(store: Arc<dyn Store>) {
    let run = start(&store).await;
    let err = store.retry_run(run.id, t0()).await.unwrap_err();
    assert!(matches!(err, Error::InvalidState(_)), "{err:?}");

    let mut c = Commit::new(run.id, 1, t0());
    c.events = vec![EventKind::RunFailed { error: "x".into() }];
    store.commit(c).await.unwrap();
    let item = store.retry_run(run.id, t0() + secs(1)).await.unwrap();
    assert_eq!(item.run_id, run.id);
    let r = store.get_run(run.id).await.unwrap().unwrap();
    assert_eq!(r.status, RunStatus::Pending);
    assert!(r.error.is_none());
    let events = store.load_events(run.id, 0).await.unwrap();
    assert_eq!(events.last().unwrap().kind, EventKind::RunRetried);
}

pub async fn due_runs_returns_overdue_non_terminal(store: Arc<dyn Store>) {
    let due = start(&store).await;
    let later = start(&store).await;
    let done = start(&store).await;
    let mut c = waiting(&due, 1, vec![]);
    c.wake_at = Some(t0() + secs(10));
    store.commit(c).await.unwrap();
    let mut c = waiting(&later, 1, vec![]);
    c.wake_at = Some(t0() + secs(1000));
    store.commit(c).await.unwrap();
    let mut c = Commit::new(done.id, 1, t0());
    c.wake_at = Some(t0() + secs(10));
    c.events = vec![EventKind::RunSucceeded {
        output: Value::Null,
    }];
    store.commit(c).await.unwrap();

    let items = store.due_runs("ns", t0() + secs(60), 10).await.unwrap();
    assert_eq!(items.iter().map(|i| i.run_id).collect::<Vec<_>>(), [due.id]);
    assert!(
        store
            .due_runs("other-ns", t0() + secs(60), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

pub async fn list_runs_filters_and_limits(store: Arc<dyn Store>) {
    for _ in 0..3 {
        start(&store).await;
    }
    store.create_run(new_run("other", None)).await.unwrap();
    let all = store.list_runs(RunFilter::default()).await.unwrap();
    assert_eq!(all.len(), 4);
    let wf = store
        .list_runs(RunFilter {
            workflow: Some("wf".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(wf.len(), 3);
    let page = store
        .list_runs(RunFilter {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    let rest = store
        .list_runs(RunFilter {
            after: Some(page[1].id),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rest.len(), 2);
    assert!(rest.iter().all(|r| page.iter().all(|p| p.id != r.id)));
}

pub async fn definitions_round_trip(store: Arc<dyn Store>) {
    assert!(store.get_definition("ns", "d").await.unwrap().is_none());
    let mut d = WorkflowDefinition::new("d");
    d.timeout_secs = Some(5);
    store
        .upsert_definitions("ns", vec![d.clone()])
        .await
        .unwrap();
    assert_eq!(store.get_definition("ns", "d").await.unwrap(), Some(d));
}

pub async fn definitions_are_scoped_to_their_namespace(store: Arc<dyn Store>) {
    let mut a = WorkflowDefinition::new("d");
    a.timeout_secs = Some(5);
    let mut b = WorkflowDefinition::new("d");
    b.timeout_secs = Some(99);
    store
        .upsert_definitions("a", vec![a.clone()])
        .await
        .unwrap();
    store
        .upsert_definitions("b", vec![b.clone()])
        .await
        .unwrap();
    assert_eq!(store.get_definition("a", "d").await.unwrap(), Some(a));
    assert_eq!(store.get_definition("b", "d").await.unwrap(), Some(b));
    assert!(store.get_definition("c", "d").await.unwrap().is_none());
}

pub async fn hooks_are_scoped_to_their_namespace(store: Arc<dyn Store>) {
    let mut runs = vec![];
    for ns in ["a", "b"] {
        let run = store
            .create_run(new_run_in(ns, "wf", None))
            .await
            .unwrap()
            .run()
            .clone();
        let mut c = Commit::new(run.id, 1, t0());
        c.hooks = vec![HookReg {
            key: "h".into(),
            token: "tok".into(),
        }];
        let out = store.commit(c).await.unwrap();
        // The same token in another namespace is not a conflict.
        assert_eq!(
            out.generated[0].kind,
            EventKind::HookCreated {
                key: "h".into(),
                token: "tok".into(),
                conflict: false
            }
        );
        let mut c = waiting(
            &run,
            out.last_seq,
            vec![wait("h[recv]", WaitKind::Hook, "tok")],
        );
        c.wake_at = None;
        store.commit(c).await.unwrap();
        runs.push(run);
    }
    let item = store
        .resolve_hook("b", "tok", json!(1), t0())
        .await
        .unwrap()
        .expect("b's run wakes");
    assert_eq!(item.run_id, runs[1].id);
    let err = store
        .resolve_hook("c", "tok", json!(1), t0())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
}

/// Generates one `#[tokio::test]` per Store conformance case.
/// `$make` must be an async expression yielding `Arc<dyn Store>`.
#[macro_export]
macro_rules! store_conformance {
    ($make:expr) => {
        mod store_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__cases!(
                $make,
                store,
                [
                    create_run_is_idempotent_on_key,
                    new_run_has_started_event,
                    commit_appends_events_in_order,
                    commit_with_stale_seq_conflicts,
                    terminal_commit_sets_status_and_output,
                    signal_before_wait_is_consumed_at_commit,
                    each_signal_is_consumed_once,
                    signal_wakes_waiting_run,
                    unrelated_signal_does_not_wake,
                    signal_to_finished_run_is_rejected,
                    signal_racing_commit_is_never_lost,
                    hook_lifecycle,
                    hook_token_held_by_live_run_conflicts,
                    hook_dispose_releases_the_token,
                    event_reaches_only_matching_waits,
                    event_before_wait_is_not_seen,
                    cancel_flags_run_and_wakes_it,
                    retry_reopens_failed_run_only,
                    due_runs_returns_overdue_non_terminal,
                    list_runs_filters_and_limits,
                    definitions_round_trip,
                    definitions_are_scoped_to_their_namespace,
                    hooks_are_scoped_to_their_namespace,
                ]
            );
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __cases {
    ($make:expr, $module:ident, [$($case:ident),* $(,)?]) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $case() {
                $crate::$module::$case($make.await).await
            }
        )*
    };
}
