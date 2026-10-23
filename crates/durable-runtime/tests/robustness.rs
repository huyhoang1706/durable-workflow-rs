//! Inputs that used to wedge or corrupt a run: overflowing delays, panicking
//! plugins and step names that collide with generated keys.

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Duration,
};

use async_trait::async_trait;
use common::*;
use durable_core::{Cache, EventKind, Result, RunStatus, far_future};
use durable_runtime::*;
use serde_json::{Value, json};

#[tokio::test]
async fn huge_retry_after_suspends_instead_of_panicking() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("rate_limited", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.step("call", || async {
                    // What a hostile `Retry-After: 10000000000000` header becomes.
                    Err::<(), _>(StepError::retry_after(
                        Duration::from_secs(10_000_000_000_000),
                        "429",
                    ))
                })
                .await?;
                Ok(())
            }),
    );
    let id = h.start("rate_limited", json!(null)).await;
    let run = h.suspended(id).await;
    assert_eq!(run.wake_at, Some(far_future()));
    assert_eq!(run.last_error, None);
    let events = h.client.events(id).await.unwrap();
    assert!(events.iter().any(|e| matches!(
        e.kind,
        EventKind::StepFailed { retry_at: Some(t), .. } if t == far_future()
    )));
}

/// Delegates to a real cache, but the first history lookup panics.
struct PanicOnceCache {
    inner: Arc<dyn Cache>,
    armed: AtomicBool,
}

#[async_trait]
impl Cache for PanicOnceCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.starts_with("dw:hist:") && self.armed.swap(false, SeqCst) {
            panic!("cache plugin bug");
        }
        self.inner.get(key).await
    }
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(key, value, ttl).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
    async fn incr(&self, key: &str, ttl: Option<Duration>) -> Result<i64> {
        self.inner.incr(key, ttl).await
    }
    async fn try_lock(&self, key: &str, ttl: Duration) -> Result<bool> {
        self.inner.try_lock(key, ttl).await
    }
}

#[tokio::test]
async fn a_panic_during_execution_releases_the_lease() {
    let mut h = Harness::new();
    h.backend.cache = Some(Arc::new(PanicOnceCache {
        inner: h.backend.cache.clone().unwrap(),
        armed: AtomicBool::new(true),
    }));
    h.client = Client::new(h.backend.clone());
    h.spawn(
        h.worker()
            .register_fn("ok", |_: WfAsyncCtx, _: Value| async move { Ok(1) }),
    );
    let id = h.start("ok", json!(null)).await;
    // The panicked execution is handed back with a short backoff instead of
    // keeping its lease (and the run) forever.
    h.eventually("panic consumed", || async {
        h.client.get_run(id).await.unwrap().unwrap().status == RunStatus::Pending
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.advance(10);
    assert_eq!(h.succeeded(id).await, json!(1));
}

#[tokio::test]
async fn a_step_name_that_collides_with_a_generated_key_faults_the_run() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("collide", |ctx: WfAsyncCtx, _: Value| async move {
                // The second `a` is keyed `a#1`: before the fix it took over the
                // explicit `a#1` step and both resolved to "second a".
                let explicit = ctx.step("a#1", || async {
                    Ok::<_, StepError>("explicit".to_string())
                });
                let first = ctx.step("a", || async { Ok::<_, StepError>("first a".to_string()) });
                let second = ctx.step("a", || async { Ok::<_, StepError>("second a".to_string()) });
                let (x, y, z): (String, String, String) =
                    futures::try_join!(explicit, first, second)?;
                Ok(json!([x, y, z]))
            }),
    );
    let id = h.start("collide", json!(null)).await;
    let run = h
        .until(id, "a recorded fault", |r| r.last_error.is_some())
        .await;
    assert!(
        run.last_error
            .as_deref()
            .unwrap()
            .contains("invalid name `a#1`"),
        "{run:#?}"
    );
    assert!(!run.status.is_terminal());
    assert!(
        h.client
            .events(id)
            .await
            .unwrap()
            .iter()
            .all(|e| !matches!(e.kind, EventKind::StepSucceeded { .. }))
    );
}
