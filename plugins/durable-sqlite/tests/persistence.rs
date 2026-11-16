mod common;

use durable_spi::{Commit, EventKind, RunStatus, Store, WaitKind, WaitReg};
use durable_sqlite::SqliteStore;
use durable_testkit::{store::new_run, t0};
use serde_json::json;

async fn open(path: &std::path::Path) -> SqliteStore {
    let store = SqliteStore::open(path).await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[tokio::test]
async fn runs_and_events_survive_reopening_the_file() {
    let dir = common::TempDir::new();
    let path = dir.file("durable.db");

    let store = open(&path).await;
    let run = store
        .create_run(new_run("wf", Some("k")))
        .await
        .unwrap()
        .run()
        .clone();
    let mut c = Commit::new(run.id, 1, t0());
    c.events = vec![EventKind::RunSucceeded {
        output: json!({"ok": true}),
    }];
    store.commit(c).await.unwrap();
    store.pool().close().await;
    drop(store);

    let store = open(&path).await;
    let back = store.get_run(run.id).await.unwrap().expect("run persisted");
    assert_eq!(back.status, RunStatus::Succeeded);
    assert_eq!(back.output, Some(json!({"ok": true})));
    assert_eq!(back.created_at, run.created_at);
    let events = store.load_events(run.id, 0).await.unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].kind, EventKind::RunStarted { .. }));
    assert!(matches!(events[1].kind, EventKind::RunSucceeded { .. }));
    // The idempotency key still holds after the reopen.
    let again = store.create_run(new_run("wf", Some("k"))).await.unwrap();
    assert_eq!(again.run().id, run.id);
}

/// The CLI next to a worker: two pools on one file. Signals delivered by one
/// while the other commits are never lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_pools_on_one_file_never_lose_a_signal() {
    let dir = common::TempDir::new();
    let path = dir.file("shared.db");
    let worker = open(&path).await;
    let cli = open(&path).await;

    const SIGNALS: u64 = 20;
    let run = worker
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone();

    let sender = tokio::spawn({
        let cli = cli.clone();
        async move {
            for i in 0..SIGNALS {
                cli.deliver_signal(run.id, "go", json!(i), t0())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        }
    });

    // The worker keeps waiting on `go` until it has received every signal.
    let mut seq = 1;
    let mut received = vec![];
    while received.len() < SIGNALS as usize {
        let mut c = Commit::new(run.id, seq, t0());
        c.status = RunStatus::Waiting;
        c.waits = vec![WaitReg {
            key: format!("go#{}", received.len()),
            kind: WaitKind::Signal,
            name: "go".into(),
            filter: None,
            timeout_at: None,
        }];
        let out = worker.commit(c).await.unwrap();
        seq = out.last_seq;
        for e in out.generated {
            if let EventKind::SignalReceived { payload, .. } = e.kind {
                received.push(payload);
            }
        }
        tokio::task::yield_now().await;
    }
    sender.await.unwrap();
    let expected: Vec<_> = (0..SIGNALS).map(|i| json!(i)).collect();
    assert_eq!(received, expected);
    let stored = cli.load_events(run.id, 0).await.unwrap();
    assert_eq!(stored.last().unwrap().seq, seq);
}
