//! `commit` and `publish_event` write in batches: unchanged waits are not
//! rewritten, and an event fan-out is one transaction however many runs it
//! reaches.

mod common;

use chrono::{DateTime, Utc};
use durable_core::*;
use durable_postgres::PostgresStore;
use durable_testkit::{store::new_run, t0};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool, Row};

async fn store() -> (PostgresStore, PgPool, String) {
    let pool = common::pool().await;
    let schema = common::unique_schema();
    let store = PostgresStore::from_pool(pool.clone()).schema(&schema);
    store.migrate().await.unwrap();
    (store, pool, schema)
}

fn event_wait(key: &str, order: i64, timeout_at: Option<DateTime<Utc>>) -> WaitReg {
    WaitReg {
        key: key.into(),
        kind: WaitKind::Event,
        name: "order.paid".into(),
        filter: Some(json!({"order_id": order})),
        timeout_at,
    }
}

/// Transaction ids that last wrote the run's wait rows, by key.
async fn wait_versions(pool: &PgPool, schema: &str, run: RunId) -> Vec<(String, String)> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT key, xmin::text FROM {schema}.waits WHERE run_id = $1 ORDER BY key"
    )))
    .bind(run)
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

#[tokio::test]
async fn unchanged_waits_are_not_rewritten_by_later_commits() {
    let (store, pool, schema) = store().await;
    let run = store
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone();
    // Nanosecond precision, which Postgres truncates to microseconds.
    let timeout = Some(t0() + chrono::Duration::nanoseconds(1_234_567_891));
    let waits = vec![event_wait("a", 1, timeout), event_wait("b", 2, None)];

    let mut c = Commit::new(run.id, 1, t0());
    c.waits = waits.clone();
    let out = store.commit(c).await.unwrap();
    let before = wait_versions(&pool, &schema, run.id).await;
    assert_eq!(before.len(), 2);

    // A step result with the same open waits (in another order).
    let mut c = Commit::new(run.id, out.last_seq, t0());
    c.events = vec![EventKind::StepSucceeded {
        key: "s".into(),
        output: json!(1),
    }];
    c.waits = vec![waits[1].clone(), waits[0].clone()];
    let out = store.commit(c).await.unwrap();
    assert_eq!(wait_versions(&pool, &schema, run.id).await, before);

    // A changed set is written.
    let mut c = Commit::new(run.id, out.last_seq, t0());
    c.waits = vec![event_wait("a", 1, timeout), event_wait("c", 3, None)];
    store.commit(c).await.unwrap();
    let after = wait_versions(&pool, &schema, run.id).await;
    assert_eq!(
        after.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        ["a", "c"]
    );
    assert_ne!(after[0].1, before[0].1);
}

#[tokio::test]
async fn a_large_commit_round_trips() {
    let (store, _pool, _schema) = store().await;
    let run = store
        .create_run(new_run("wf", None))
        .await
        .unwrap()
        .run()
        .clone();
    for i in 0..10 {
        store
            .deliver_signal(run.id, &format!("sig{i}"), json!(i), t0())
            .await
            .unwrap();
    }
    let mut c = Commit::new(run.id, 1, t0());
    c.events = (0..50)
        .map(|i| EventKind::StepSucceeded {
            key: format!("s{i}"),
            output: json!(i),
        })
        .collect();
    c.waits = (0..30)
        .map(|i| WaitReg {
            key: format!("w{i}"),
            kind: WaitKind::Signal,
            name: format!("sig{i}"),
            filter: None,
            timeout_at: None,
        })
        .collect();
    c.status = RunStatus::Waiting;
    let out = store.commit(c).await.unwrap();

    // RunStarted, the 50 events, then the 10 inbox signals that matched open waits.
    assert_eq!(out.last_seq, 61);
    assert_eq!(out.generated.len(), 10);
    let events = store.load_events(run.id, 0).await.unwrap();
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=61).collect::<Vec<_>>());
    assert!(matches!(
        &events[51].kind,
        EventKind::SignalReceived { key, payload, .. } if key == "w0" && *payload == json!(0)
    ));
    let run = store.get_run(run.id).await.unwrap().unwrap();
    // Something was delivered, so the engine keeps going.
    assert_eq!(run.status, RunStatus::Running);

    // The consumed waits are closed; the other 20 stay open.
    let mut c = Commit::new(run.id, 61, t0());
    c.waits = (10..30)
        .map(|i| WaitReg {
            key: format!("w{i}"),
            kind: WaitKind::Signal,
            name: format!("sig{i}"),
            filter: None,
            timeout_at: None,
        })
        .collect();
    c.status = RunStatus::Waiting;
    let out = store.commit(c).await.unwrap();
    assert!(out.generated.is_empty(), "each signal is consumed once");
}

#[tokio::test]
async fn an_event_fan_out_is_one_transaction() {
    let (store, pool, schema) = store().await;
    let mut matching = vec![];
    for i in 0..25 {
        let run = store
            .create_run(new_run("wf", None))
            .await
            .unwrap()
            .run()
            .clone();
        let mut c = Commit::new(run.id, 1, t0());
        c.status = RunStatus::Waiting;
        c.waits = vec![event_wait("e", if i < 20 { 7 } else { 8 }, None)];
        store.commit(c).await.unwrap();
        if i < 20 {
            matching.push(run.id);
        }
    }

    let items = store
        .publish_event("ns", "order.paid", json!({"order_id": 7, "total": 3}), t0())
        .await
        .unwrap();
    let mut woken: Vec<RunId> = items.iter().map(|i| i.run_id).collect();
    woken.sort();
    matching.sort();
    assert_eq!(woken, matching);

    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT count(*), count(DISTINCT xmin::text) FROM {schema}.signals"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    let (signals, transactions): (i64, i64) = (row.get(0), row.get(1));
    assert_eq!((signals, transactions), (20, 1));

    // Every woken run consumes its event on the next commit.
    let run = store.get_run(matching[0]).await.unwrap().unwrap();
    assert_eq!(run.wake_at, Some(t0()));
    let mut c = Commit::new(run.id, run.last_seq, t0());
    c.waits = vec![event_wait("e", 7, None)];
    let out = store.commit(c).await.unwrap();
    assert!(matches!(
        &out.generated[0].kind,
        EventKind::SignalReceived { payload, .. } if payload["total"] == 3
    ));
}
