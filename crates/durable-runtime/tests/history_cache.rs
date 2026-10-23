mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::*;
use durable_core::*;
use durable_runtime::*;
use serde_json::{Value, json};

/// Delegates to a real store and counts `load_events` calls.
struct CountingStore {
    inner: Arc<dyn Store>,
    loads: Arc<AtomicUsize>,
}

#[async_trait]
impl Store for CountingStore {
    async fn create_run(&self, new: NewRun) -> Result<CreateOutcome> {
        self.inner.create_run(new).await
    }
    async fn get_run(&self, id: RunId) -> Result<Option<Run>> {
        self.inner.get_run(id).await
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<Run>> {
        self.inner.list_runs(filter).await
    }
    async fn load_events(&self, id: RunId, after_seq: u64) -> Result<Vec<Event>> {
        self.loads.fetch_add(1, SeqCst);
        self.inner.load_events(id, after_seq).await
    }
    async fn commit(&self, commit: Commit) -> Result<CommitOutcome> {
        self.inner.commit(commit).await
    }
    async fn deliver_signal(
        &self,
        run_id: RunId,
        name: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        self.inner.deliver_signal(run_id, name, payload, at).await
    }
    async fn resolve_hook(
        &self,
        namespace: &str,
        token: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        self.inner.resolve_hook(namespace, token, payload, at).await
    }
    async fn publish_event(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        at: DateTime<Utc>,
    ) -> Result<Vec<QueueItem>> {
        self.inner.publish_event(namespace, name, data, at).await
    }
    async fn request_cancel(
        &self,
        run_id: RunId,
        reason: Option<String>,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        self.inner.request_cancel(run_id, reason, at).await
    }
    async fn retry_run(&self, run_id: RunId, at: DateTime<Utc>) -> Result<QueueItem> {
        self.inner.retry_run(run_id, at).await
    }
    async fn due_runs(
        &self,
        namespace: &str,
        before: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<QueueItem>> {
        self.inner.due_runs(namespace, before, limit).await
    }
    async fn upsert_definitions(
        &self,
        namespace: &str,
        defs: Vec<WorkflowDefinition>,
    ) -> Result<()> {
        self.inner.upsert_definitions(namespace, defs).await
    }
    async fn get_definition(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<WorkflowDefinition>> {
        self.inner.get_definition(namespace, name).await
    }
}

fn counting(h: &mut Harness, with_cache: bool) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let loads = Arc::new(AtomicUsize::new(0));
    h.backend.store = Arc::new(CountingStore {
        inner: h.backend.store.clone(),
        loads: loads.clone(),
    });
    if !with_cache {
        h.backend.cache = None;
    }
    h.client = Client::new(h.backend.clone());
    (loads, Arc::new(AtomicUsize::new(0)))
}

fn sleeper(h: &Harness, runs: Arc<AtomicUsize>) -> WorkerBuilder {
    h.worker()
        .register_fn("nap", move |ctx: WfAsyncCtx, _: Value| {
            let runs = runs.clone();
            async move {
                let a: i64 = ctx
                    .step("a", move || async move {
                        runs.fetch_add(1, SeqCst);
                        Ok::<_, StepError>(1)
                    })
                    .await?;
                ctx.sleep("nap", Duration::from_secs(3600)).await?;
                ctx.sleep("nap2", Duration::from_secs(3600)).await?;
                Ok(a + 1)
            }
        })
}

#[tokio::test]
async fn resuming_a_suspended_run_skips_the_store_load_when_the_cache_is_current() {
    let mut h = Harness::new();
    let (loads, runs) = counting(&mut h, true);
    h.spawn(sleeper(&h, runs.clone()));
    let id = h.start("nap", json!(null)).await;
    h.suspended(id).await;
    assert_eq!(
        loads.load(SeqCst),
        1,
        "first execution loads from the store"
    );

    h.advance(3601);
    h.until(id, "second suspension", |r| {
        r.wake_at.is_some() && r.last_seq >= 5
    })
    .await;
    h.advance(3601);
    assert_eq!(h.succeeded(id).await, json!(2));
    assert_eq!(runs.load(SeqCst), 1, "memoized step does not re-run");
    assert_eq!(
        loads.load(SeqCst),
        1,
        "resumptions are served from the cache"
    );
}

#[tokio::test]
async fn a_stale_or_corrupt_cache_falls_back_to_the_store() {
    let mut h = Harness::new();
    let (loads, runs) = counting(&mut h, true);
    h.spawn(sleeper(&h, runs.clone()));
    let id = h.start("nap", json!(null)).await;
    h.suspended(id).await;
    let cache = h.backend.cache.clone().unwrap();
    let key = format!("dw:hist:{id}");

    // Stale: the cached log lags the run's last_seq.
    let stale = json!({"last_seq": 1, "events": []});
    cache
        .set(&key, serde_json::to_vec(&stale).unwrap(), None)
        .await
        .unwrap();
    h.advance(3601);
    h.until(id, "second suspension", |r| r.last_seq >= 5).await;
    assert_eq!(loads.load(SeqCst), 2, "stale entry ignored");

    // Corrupt bytes are ignored too.
    cache.set(&key, b"not json".to_vec(), None).await.unwrap();
    h.advance(3601);
    assert_eq!(h.succeeded(id).await, json!(2));
    assert_eq!(loads.load(SeqCst), 3, "corrupt entry ignored");
    assert_eq!(runs.load(SeqCst), 1);
}

#[tokio::test]
async fn without_a_cache_every_execution_loads() {
    let mut h = Harness::new();
    let (loads, runs) = counting(&mut h, false);
    h.spawn(sleeper(&h, runs.clone()));
    let id = h.start("nap", json!(null)).await;
    h.suspended(id).await;
    h.advance(3601);
    h.until(id, "second suspension", |r| r.last_seq >= 5).await;
    assert!(loads.load(SeqCst) >= 2);
}
