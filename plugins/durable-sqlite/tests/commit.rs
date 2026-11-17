//! The plain-commit path (no waits, hooks or terminal event) skips reading
//! the run's waits; these cases pin down what it must still do.

use durable_spi::{Commit, Error, EventKind, RunStatus, Store, WaitKind, WaitReg};
use durable_sqlite::SqliteStore;
use durable_testkit::{store::new_run, t0};
use serde_json::json;

fn wait(key: &str, name: &str) -> WaitReg {
    WaitReg {
        key: key.into(),
        kind: WaitKind::Signal,
        name: name.into(),
        filter: None,
        timeout_at: None,
    }
}

#[tokio::test]
async fn a_plain_commit_drops_the_waits_it_no_longer_lists() {
    let store = SqliteStore::in_memory();
    let run = store
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone();
    let mut c = Commit::new(run.id, 1, t0());
    c.status = RunStatus::Waiting;
    c.waits = vec![wait("go[recv]", "go")];
    let seq = store.commit(c).await.unwrap().last_seq;

    // Still waiting, but on a timer now: the signal wait is gone.
    let mut c = Commit::new(run.id, seq, t0());
    c.status = RunStatus::Waiting;
    c.wake_at = Some(t0() + chrono::Duration::hours(1));
    c.events = vec![EventKind::RunRetried];
    let seq = store.commit(c).await.unwrap().last_seq;
    assert_eq!(seq, 2);

    let woke = store
        .deliver_signal(run.id, "go", json!(1), t0())
        .await
        .unwrap();
    assert!(woke.is_none(), "a dropped wait must not wake the run");
    let waits: i64 = sqlx::query_scalar("SELECT count(*) FROM waits")
        .fetch_one(&mut *store.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(waits, 0);
}

#[tokio::test]
async fn a_plain_commit_reports_conflicts_and_unknown_runs() {
    let store = SqliteStore::in_memory();
    let run = store
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone();
    let err = store
        .commit(Commit::new(run.id, 7, t0()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict), "{err:?}");
    let err = store
        .commit(Commit::new(uuid::Uuid::now_v7(), 1, t0()))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    // A conflict writes nothing.
    assert_eq!(store.load_events(run.id, 0).await.unwrap().len(), 1);
    assert_eq!(store.get_run(run.id).await.unwrap().unwrap().last_seq, 1);
}
