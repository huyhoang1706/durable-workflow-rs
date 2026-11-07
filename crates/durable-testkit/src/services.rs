//! Conformance cases for `Cache`, `PubSub` and `RateLimiter`.

use std::{sync::Arc, time::Duration};

use durable_spi::*;
use futures::StreamExt;

use crate::{secs, t0};

const MS: fn(u64) -> Duration = Duration::from_millis;

pub async fn cache_get_set_delete(c: Arc<dyn Cache>) {
    assert_eq!(c.get("k").await.unwrap(), None);
    c.set("k", b"v".to_vec(), None).await.unwrap();
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some(&b"v"[..]));
    c.set("k", b"w".to_vec(), None).await.unwrap();
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some(&b"w"[..]));
    c.delete("k").await.unwrap();
    assert_eq!(c.get("k").await.unwrap(), None);
    c.delete("k").await.unwrap(); // deleting a missing key is fine
}

pub async fn cache_entries_expire(c: Arc<dyn Cache>) {
    c.set("k", b"v".to_vec(), Some(MS(150))).await.unwrap();
    assert!(c.get("k").await.unwrap().is_some());
    tokio::time::sleep(MS(400)).await;
    assert_eq!(c.get("k").await.unwrap(), None);
}

pub async fn cache_incr_counts_and_applies_ttl_on_creation(c: Arc<dyn Cache>) {
    assert_eq!(c.incr("n", Some(MS(300))).await.unwrap(), 1);
    tokio::time::sleep(MS(150)).await;
    // The ttl is set when the key is created, not refreshed by later incrs.
    assert_eq!(c.incr("n", Some(MS(300))).await.unwrap(), 2);
    tokio::time::sleep(MS(250)).await;
    assert_eq!(
        c.incr("n", Some(MS(300))).await.unwrap(),
        1,
        "counter should have expired"
    );
    assert_eq!(c.incr("plain", None).await.unwrap(), 1);
    assert_eq!(c.incr("plain", None).await.unwrap(), 2);
}

pub async fn cache_try_lock_is_exclusive_until_ttl(c: Arc<dyn Cache>) {
    assert!(c.try_lock("lock", MS(300)).await.unwrap());
    assert!(!c.try_lock("lock", MS(300)).await.unwrap());
    assert!(c.try_lock("other", MS(300)).await.unwrap());
    tokio::time::sleep(MS(450)).await;
    assert!(c.try_lock("lock", MS(300)).await.unwrap());
}

pub async fn pubsub_delivers_to_every_subscriber(p: Arc<dyn PubSub>) {
    let mut a = p.subscribe("chan").await.unwrap();
    let mut b = p.subscribe("chan").await.unwrap();
    p.publish("chan", b"hello".to_vec()).await.unwrap();
    for sub in [&mut a, &mut b] {
        let got = tokio::time::timeout(Duration::from_secs(3), sub.next())
            .await
            .unwrap();
        assert_eq!(got.unwrap(), b"hello");
    }
}

pub async fn pubsub_channels_are_isolated_and_not_replayed(p: Arc<dyn PubSub>) {
    p.publish("early", b"missed".to_vec()).await.unwrap();
    let mut early = p.subscribe("early").await.unwrap();
    let mut other = p.subscribe("other").await.unwrap();
    p.publish("early", b"seen".to_vec()).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), early.next())
        .await
        .unwrap();
    assert_eq!(got.unwrap(), b"seen", "history must not be replayed");
    assert!(tokio::time::timeout(MS(150), other.next()).await.is_err());
}

pub async fn pubsub_publish_without_subscribers_is_ok(p: Arc<dyn PubSub>) {
    p.publish("nobody", b"x".to_vec()).await.unwrap();
}

pub async fn rate_limiter_allows_a_burst_then_denies(r: Arc<dyn RateLimiter>) {
    let period = Duration::from_secs(10);
    for _ in 0..2 {
        assert_eq!(
            r.check("k", 2, period, t0()).await.unwrap(),
            RateDecision::Allowed
        );
    }
    match r.check("k", 2, period, t0()).await.unwrap() {
        RateDecision::Denied { retry_at } => {
            assert!(retry_at > t0() && retry_at <= t0() + secs(10), "{retry_at}");
            // Allowed again once the retry time arrives.
            assert_eq!(
                r.check("k", 2, period, retry_at).await.unwrap(),
                RateDecision::Allowed
            );
        }
        other => panic!("expected denial, got {other:?}"),
    }
}

pub async fn rate_limiter_keys_are_independent(r: Arc<dyn RateLimiter>) {
    let period = Duration::from_secs(10);
    assert_eq!(
        r.check("a", 1, period, t0()).await.unwrap(),
        RateDecision::Allowed
    );
    assert!(matches!(
        r.check("a", 1, period, t0()).await.unwrap(),
        RateDecision::Denied { .. }
    ));
    assert_eq!(
        r.check("b", 1, period, t0()).await.unwrap(),
        RateDecision::Allowed
    );
}

pub async fn rate_limiter_recovers_over_time(r: Arc<dyn RateLimiter>) {
    let period = Duration::from_secs(10);
    assert_eq!(
        r.check("k", 1, period, t0()).await.unwrap(),
        RateDecision::Allowed
    );
    assert!(matches!(
        r.check("k", 1, period, t0() + secs(5)).await.unwrap(),
        RateDecision::Denied { .. }
    ));
    assert_eq!(
        r.check("k", 1, period, t0() + secs(10)).await.unwrap(),
        RateDecision::Allowed
    );
}

/// One `#[tokio::test]` per Cache case. `$make`: async expr yielding `Arc<dyn Cache>`.
#[macro_export]
macro_rules! cache_conformance {
    ($make:expr) => {
        mod cache_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__cases!(
                $make,
                services,
                [
                    cache_get_set_delete,
                    cache_entries_expire,
                    cache_incr_counts_and_applies_ttl_on_creation,
                    cache_try_lock_is_exclusive_until_ttl,
                ]
            );
        }
    };
}

/// One `#[tokio::test]` per PubSub case. `$make`: async expr yielding `Arc<dyn PubSub>`.
#[macro_export]
macro_rules! pubsub_conformance {
    ($make:expr) => {
        mod pubsub_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__cases!(
                $make,
                services,
                [
                    pubsub_delivers_to_every_subscriber,
                    pubsub_channels_are_isolated_and_not_replayed,
                    pubsub_publish_without_subscribers_is_ok,
                ]
            );
        }
    };
}

/// One `#[tokio::test]` per RateLimiter case.
#[macro_export]
macro_rules! rate_limiter_conformance {
    ($make:expr) => {
        mod rate_limiter_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__cases!(
                $make,
                services,
                [
                    rate_limiter_allows_a_burst_then_denies,
                    rate_limiter_keys_are_independent,
                    rate_limiter_recovers_over_time,
                ]
            );
        }
    };
}
