//! Queue conformance cases. Each takes a fresh, empty queue.

use std::{sync::Arc, time::Duration};

use durable_spi::*;
use uuid::Uuid;

use crate::{secs, t0};

const LEASE: Duration = Duration::from_secs(30);

fn scope() -> QueueScope {
    QueueScope {
        namespace: "ns".into(),
        workflow: "wf".into(),
    }
}

fn item(due: chrono::DateTime<chrono::Utc>, kind: WorkKind, ckey: Option<&str>) -> QueueItem {
    QueueItem {
        namespace: "ns".into(),
        workflow: "wf".into(),
        run_id: Uuid::now_v7(),
        due_at: due,
        kind,
        concurrency_key: ckey.map(Into::into),
        throttle_key: Some("-".into()),
    }
}

fn start_item(due: chrono::DateTime<chrono::Utc>) -> QueueItem {
    item(due, WorkKind::Start, None)
}

async fn claim(
    q: &Arc<dyn Queue>,
    limits: &ClaimLimits,
    max: usize,
    now: chrono::DateTime<chrono::Utc>,
) -> Claimed {
    q.claim(&scope(), limits, max, LEASE, now).await.unwrap()
}

pub async fn claims_only_due_items(q: Arc<dyn Queue>) {
    let soon = start_item(t0() + secs(10));
    q.enqueue(vec![soon.clone()]).await.unwrap();
    let got = claim(&q, &ClaimLimits::default(), 10, t0()).await;
    assert!(got.leases.is_empty());
    assert_eq!(got.next_due, Some(t0() + secs(10)));
    let got = claim(&q, &ClaimLimits::default(), 10, t0() + secs(10)).await;
    assert_eq!(got.leases.len(), 1);
    assert_eq!(got.leases[0].item.run_id, soon.run_id);
    assert_eq!(got.leases[0].expires_at, t0() + secs(10) + secs(30));
}

pub async fn claims_are_scoped_to_workflow(q: Arc<dyn Queue>) {
    let mut other = start_item(t0());
    other.workflow = "other".into();
    q.enqueue(vec![other]).await.unwrap();
    assert!(
        claim(&q, &ClaimLimits::default(), 10, t0())
            .await
            .leases
            .is_empty()
    );
}

pub async fn enqueue_dedupes_per_run_keeping_earliest(q: Arc<dyn Queue>) {
    let mut a = start_item(t0() + secs(100));
    q.enqueue(vec![a.clone()]).await.unwrap();
    a.due_at = t0() + secs(5);
    q.enqueue(vec![a.clone()]).await.unwrap();
    let got = claim(&q, &ClaimLimits::default(), 10, t0() + secs(5)).await;
    assert_eq!(got.leases.len(), 1);
    let again = claim(&q, &ClaimLimits::default(), 10, t0() + secs(200)).await;
    assert!(again.leases.is_empty(), "duplicate item leaked");
}

pub async fn resume_is_claimed_before_start(q: Arc<dyn Queue>) {
    let s = start_item(t0());
    let r = item(t0(), WorkKind::Resume, None);
    q.enqueue(vec![s, r.clone()]).await.unwrap();
    let got = claim(&q, &ClaimLimits::default(), 1, t0()).await;
    assert_eq!(got.leases[0].item.run_id, r.run_id);
}

pub async fn enqueue_while_leased_reruns_after_ack(q: Arc<dyn Queue>) {
    let i = start_item(t0());
    q.enqueue(vec![i.clone()]).await.unwrap();
    let lease = claim(&q, &ClaimLimits::default(), 1, t0())
        .await
        .leases
        .remove(0);
    // A signal arrives while the run is executing.
    q.enqueue(vec![item_for(&i, WorkKind::Resume, t0())])
        .await
        .unwrap();
    assert!(
        claim(&q, &ClaimLimits::default(), 1, t0())
            .await
            .leases
            .is_empty()
    );
    q.ack(&lease, None, t0()).await.unwrap();
    let again = claim(&q, &ClaimLimits::default(), 1, t0()).await;
    assert_eq!(again.leases.len(), 1);
    assert_eq!(again.leases[0].item.run_id, i.run_id);
}

fn item_for(base: &QueueItem, kind: WorkKind, due: chrono::DateTime<chrono::Utc>) -> QueueItem {
    QueueItem {
        kind,
        due_at: due,
        ..base.clone()
    }
}

pub async fn ack_reschedules_at_next_wake(q: Arc<dyn Queue>) {
    let i = start_item(t0());
    q.enqueue(vec![i.clone()]).await.unwrap();
    let lease = claim(&q, &ClaimLimits::default(), 1, t0())
        .await
        .leases
        .remove(0);
    q.ack(&lease, Some(t0() + secs(60)), t0()).await.unwrap();
    assert!(
        claim(&q, &ClaimLimits::default(), 1, t0() + secs(59))
            .await
            .leases
            .is_empty()
    );
    assert_eq!(
        claim(&q, &ClaimLimits::default(), 1, t0() + secs(60))
            .await
            .leases
            .len(),
        1
    );
}

pub async fn workflow_concurrency_is_enforced(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        workflow_concurrency: Some(2),
        ..Default::default()
    };
    q.enqueue((0..3).map(|_| start_item(t0())).collect())
        .await
        .unwrap();
    let got = claim(&q, &limits, 10, t0()).await;
    assert_eq!(got.leases.len(), 2);
    assert!(claim(&q, &limits, 10, t0()).await.leases.is_empty());
    q.ack(&got.leases[0], None, t0()).await.unwrap();
    assert_eq!(claim(&q, &limits, 10, t0()).await.leases.len(), 1);
}

pub async fn key_concurrency_is_enforced_per_key(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        key_concurrency: Some(1),
        ..Default::default()
    };
    q.enqueue(vec![
        item(t0(), WorkKind::Start, Some("a")),
        item(t0(), WorkKind::Start, Some("a")),
        item(t0(), WorkKind::Start, Some("b")),
    ])
    .await
    .unwrap();
    let got = claim(&q, &limits, 10, t0()).await;
    let mut keys: Vec<_> = got
        .leases
        .iter()
        .map(|l| l.item.concurrency_key.clone().unwrap())
        .collect();
    keys.sort();
    assert_eq!(keys, ["a", "b"]);
}

pub async fn concurrent_claims_never_exceed_limit(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        workflow_concurrency: Some(3),
        ..Default::default()
    };
    q.enqueue((0..20).map(|_| start_item(t0())).collect())
        .await
        .unwrap();
    let mut tasks = vec![];
    for _ in 0..8 {
        let q = q.clone();
        let limits = limits.clone();
        tasks.push(tokio::spawn(async move {
            q.claim(&scope(), &limits, 5, LEASE, t0())
                .await
                .unwrap()
                .leases
                .len()
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert_eq!(total, 3);
}

pub async fn throttle_delays_starts_but_not_resumes(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        throttle: Some(ThrottleLimit {
            limit: 1,
            period: Duration::from_secs(10),
            burst: 1,
        }),
        ..Default::default()
    };
    q.enqueue(vec![start_item(t0()), start_item(t0())])
        .await
        .unwrap();
    let first = claim(&q, &limits, 10, t0()).await;
    assert_eq!(first.leases.len(), 1);
    assert!(first.next_due.is_some());
    let next = first.next_due.unwrap();
    assert!(next > t0() && next <= t0() + secs(10), "{next}");
    // Delayed, not dropped.
    assert_eq!(
        claim(&q, &limits, 10, t0() + secs(10)).await.leases.len(),
        1
    );
    // Resumes ignore the throttle.
    q.enqueue(vec![item(t0() + secs(11), WorkKind::Resume, None)])
        .await
        .unwrap();
    assert_eq!(
        claim(&q, &limits, 10, t0() + secs(11)).await.leases.len(),
        1
    );
}

pub async fn throttle_burst_1_spaces_starts_evenly(q: Arc<dyn Queue>) {
    // 2 per 10s with burst 1: one start now, the next only 5s later.
    let limits = ClaimLimits {
        throttle: Some(ThrottleLimit {
            limit: 2,
            period: Duration::from_secs(10),
            burst: 1,
        }),
        ..Default::default()
    };
    q.enqueue((0..3).map(|_| start_item(t0())).collect())
        .await
        .unwrap();
    assert_eq!(claim(&q, &limits, 10, t0()).await.leases.len(), 1);
    assert_eq!(claim(&q, &limits, 10, t0() + secs(4)).await.leases.len(), 0);
    assert_eq!(claim(&q, &limits, 10, t0() + secs(5)).await.leases.len(), 1);
    assert_eq!(claim(&q, &limits, 10, t0() + secs(9)).await.leases.len(), 0);
    assert_eq!(
        claim(&q, &limits, 10, t0() + secs(10)).await.leases.len(),
        1
    );
}

pub async fn throttle_burst_allows_back_to_back_starts(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        throttle: Some(ThrottleLimit {
            limit: 2,
            period: Duration::from_secs(10),
            burst: 2,
        }),
        ..Default::default()
    };
    q.enqueue((0..3).map(|_| start_item(t0())).collect())
        .await
        .unwrap();
    let first = claim(&q, &limits, 10, t0()).await;
    assert_eq!(first.leases.len(), 2, "burst of 2 starts at once");
    assert_eq!(first.next_due, Some(t0() + secs(5)));
    assert_eq!(claim(&q, &limits, 10, t0() + secs(5)).await.leases.len(), 1);
}

pub async fn extend_keeps_lease_and_reports_lost(q: Arc<dyn Queue>) {
    q.enqueue(vec![start_item(t0())]).await.unwrap();
    let lease = claim(&q, &ClaimLimits::default(), 1, t0())
        .await
        .leases
        .remove(0);
    let lost = q
        .extend(std::slice::from_ref(&lease), LEASE, t0() + secs(10))
        .await
        .unwrap();
    assert!(lost.is_empty());
    // Not expired at +35 because it was extended at +10 (valid to +40).
    assert_eq!(
        q.requeue_expired(&scope(), t0() + secs(35)).await.unwrap(),
        0
    );
    // After it expires and is requeued, the old token is stale.
    assert_eq!(
        q.requeue_expired(&scope(), t0() + secs(41)).await.unwrap(),
        1
    );
    let lost = q
        .extend(std::slice::from_ref(&lease), LEASE, t0() + secs(42))
        .await
        .unwrap();
    assert_eq!(lost.len(), 1);
    assert!(
        q.ack(&lease, None, t0() + secs(42)).await.is_err(),
        "stale ack must fail"
    );
}

pub async fn expired_lease_is_requeued_and_frees_capacity(q: Arc<dyn Queue>) {
    let limits = ClaimLimits {
        workflow_concurrency: Some(1),
        ..Default::default()
    };
    let i = start_item(t0());
    q.enqueue(vec![i.clone()]).await.unwrap();
    claim(&q, &limits, 1, t0()).await;
    assert!(
        claim(&q, &limits, 1, t0() + secs(10))
            .await
            .leases
            .is_empty()
    );
    assert_eq!(
        q.requeue_expired(&scope(), t0() + secs(31)).await.unwrap(),
        1
    );
    let again = claim(&q, &limits, 1, t0() + secs(31)).await;
    assert_eq!(again.leases[0].item.run_id, i.run_id);
}

pub async fn nack_retries_later(q: Arc<dyn Queue>) {
    q.enqueue(vec![start_item(t0())]).await.unwrap();
    let lease = claim(&q, &ClaimLimits::default(), 1, t0())
        .await
        .leases
        .remove(0);
    q.nack(&lease, t0() + secs(30), t0()).await.unwrap();
    assert!(
        claim(&q, &ClaimLimits::default(), 1, t0() + secs(29))
            .await
            .leases
            .is_empty()
    );
    assert_eq!(
        claim(&q, &ClaimLimits::default(), 1, t0() + secs(30))
            .await
            .leases
            .len(),
        1
    );
}

pub async fn nack_with_a_pending_rerun_does_not_wait_for_the_retry_time(q: Arc<dyn Queue>) {
    let i = start_item(t0());
    q.enqueue(vec![i.clone()]).await.unwrap();
    let lease = claim(&q, &ClaimLimits::default(), 1, t0())
        .await
        .leases
        .remove(0);
    // A signal arrives while the run executes, then the worker gives up.
    q.enqueue(vec![item_for(&i, WorkKind::Resume, t0())])
        .await
        .unwrap();
    q.nack(&lease, t0() + secs(60), t0()).await.unwrap();
    assert_eq!(
        claim(&q, &ClaimLimits::default(), 1, t0())
            .await
            .leases
            .len(),
        1
    );
}

/// Generates one `#[tokio::test]` per Queue conformance case.
/// `$make` must be an async expression yielding `Arc<dyn Queue>`.
#[macro_export]
macro_rules! queue_conformance {
    ($make:expr) => {
        mod queue_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__cases!(
                $make,
                queue,
                [
                    claims_only_due_items,
                    claims_are_scoped_to_workflow,
                    enqueue_dedupes_per_run_keeping_earliest,
                    resume_is_claimed_before_start,
                    enqueue_while_leased_reruns_after_ack,
                    ack_reschedules_at_next_wake,
                    workflow_concurrency_is_enforced,
                    key_concurrency_is_enforced_per_key,
                    concurrent_claims_never_exceed_limit,
                    throttle_delays_starts_but_not_resumes,
                    throttle_burst_1_spaces_starts_evenly,
                    throttle_burst_allows_back_to_back_starts,
                    extend_keeps_lease_and_reports_lost,
                    expired_lease_is_requeued_and_frees_capacity,
                    nack_retries_later,
                    nack_with_a_pending_rerun_does_not_wait_for_the_retry_time,
                ]
            );
        }
    };
}
