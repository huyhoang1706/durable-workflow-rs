use std::{
    collections::HashMap, future::Future, marker::PhantomData, panic::AssertUnwindSafe, sync::Arc,
    time::Duration,
};

use durable_core::{
    Backend, ClaimLimits, Commit, ComputePool, EventKind, Lease, QueueScope, RunId, ThrottleLimit,
    WorkflowDefinition,
};
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, watch},
    task::{AbortHandle, JoinHandle},
};

use crate::{
    Deps, DepsBuilder, ThreadPool, WfAsyncCtx, WfSyncCtx, WorkflowError, WorkflowOptions, channels,
    engine::{self, EngineEnv, Outcome},
    workflow::{
        AsyncAdapter, AsyncFn, AsyncWorkflow, ErasedWorkflow, SyncAdapter, SyncFn, SyncWorkflow,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("workflow `{workflow}` requires dependencies that are not registered: {missing:?}")]
    MissingDeps {
        workflow: String,
        missing: Vec<&'static str>,
    },
    #[error("workflow `{0}` is registered twice")]
    Duplicate(String),
}

#[derive(Clone)]
struct Config {
    concurrency: usize,
    compute_threads: usize,
    lease: Duration,
    poll_interval: Duration,
    reconcile_interval: Duration,
    inline_retry_threshold: Duration,
    max_deliveries: i64,
    max_payload_bytes: usize,
    drain_timeout: Duration,
    worker_id: Option<String>,
}

pub struct WorkerBuilder {
    backend: Backend,
    deps: DepsBuilder,
    workflows: Vec<Arc<dyn ErasedWorkflow>>,
    cfg: Config,
}

impl WorkerBuilder {
    fn new(backend: Backend) -> Self {
        Self {
            backend,
            deps: Deps::builder(),
            workflows: vec![],
            cfg: Config {
                concurrency: 16,
                compute_threads: ThreadPool::default_size(),
                lease: Duration::from_secs(30),
                poll_interval: Duration::from_secs(1),
                reconcile_interval: Duration::from_secs(5),
                inline_retry_threshold: Duration::from_secs(5),
                max_deliveries: 48,
                max_payload_bytes: 1 << 20,
                drain_timeout: Duration::from_secs(30),
                worker_id: None,
            },
        }
    }

    /// Register an effectful dependency (usable only inside steps).
    pub fn dep<T: ?Sized + Send + Sync + 'static>(mut self, v: Arc<T>) -> Self {
        self.deps.insert(v);
        self
    }

    /// Register a deterministic, read-only dependency (usable in the body).
    pub fn dep_pure<T: ?Sized + Send + Sync + 'static>(mut self, v: Arc<T>) -> Self {
        self.deps.insert_pure(v);
        self
    }

    pub fn register<W: AsyncWorkflow>(mut self, w: W) -> Self {
        self.workflows.push(Arc::new(AsyncAdapter(Arc::new(w))));
        self
    }

    pub fn register_default<W: AsyncWorkflow + Default>(self) -> Self {
        self.register(W::default())
    }

    pub fn register_sync<W: SyncWorkflow>(mut self, w: W) -> Self {
        self.workflows.push(Arc::new(SyncAdapter(Arc::new(w))));
        self
    }

    pub fn register_sync_default<W: SyncWorkflow + Default>(self) -> Self {
        self.register_sync(W::default())
    }

    pub fn register_fn<I, O, F, Fut>(self, name: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(WfAsyncCtx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send + 'static,
    {
        self.register_fn_with(WorkflowOptions::default(), name, f)
    }

    pub fn register_fn_with<I, O, F, Fut>(
        mut self,
        options: WorkflowOptions,
        name: &str,
        f: F,
    ) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(WfAsyncCtx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, WorkflowError>> + Send + 'static,
    {
        self.workflows.push(Arc::new(AsyncFn {
            name: name.into(),
            options,
            f: Arc::new(f),
            _t: PhantomData,
        }));
        self
    }

    pub fn register_sync_fn<I, O, F>(self, name: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(&WfSyncCtx, I) -> Result<O, WorkflowError> + Send + Sync + 'static,
    {
        self.register_sync_fn_with(WorkflowOptions::default(), name, f)
    }

    pub fn register_sync_fn_with<I, O, F>(
        mut self,
        options: WorkflowOptions,
        name: &str,
        f: F,
    ) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(&WfSyncCtx, I) -> Result<O, WorkflowError> + Send + Sync + 'static,
    {
        self.workflows.push(Arc::new(SyncFn {
            name: name.into(),
            options,
            f: Arc::new(f),
            _t: PhantomData,
        }));
        self
    }

    /// Runs executing at once on this worker.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.cfg.concurrency = n.max(1);
        self
    }

    /// Threads for blocking steps. Defaults to the available parallelism.
    pub fn compute_threads(mut self, n: usize) -> Self {
        self.cfg.compute_threads = n.max(1);
        self
    }

    pub fn lease(mut self, d: Duration) -> Self {
        self.cfg.lease = d;
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.cfg.poll_interval = d;
        self
    }

    pub fn reconcile_interval(mut self, d: Duration) -> Self {
        self.cfg.reconcile_interval = d;
        self
    }

    /// Timers and retry backoffs up to this long are waited out in-process
    /// instead of suspending the run.
    pub fn inline_retry_threshold(mut self, d: Duration) -> Self {
        self.cfg.inline_retry_threshold = d;
        self
    }

    /// Fail a run after this many crashed executions.
    pub fn max_deliveries(mut self, n: i64) -> Self {
        self.cfg.max_deliveries = n;
        self
    }

    pub fn max_payload_bytes(mut self, n: usize) -> Self {
        self.cfg.max_payload_bytes = n;
        self
    }

    /// Name shown as the executing worker in the ops API (default: random).
    pub fn worker_id(mut self, id: impl Into<String>) -> Self {
        self.cfg.worker_id = Some(id.into());
        self
    }

    /// How long `shutdown()` waits for in-flight runs before handing them back.
    pub fn drain_timeout(mut self, d: Duration) -> Self {
        self.cfg.drain_timeout = d;
        self
    }

    /// Fails fast if a registered workflow requires a missing dependency.
    pub fn build(self) -> Result<Worker, BuildError> {
        let deps = self.deps.build();
        let mut seen = HashMap::new();
        for wf in &self.workflows {
            if seen.insert(wf.name().to_string(), ()).is_some() {
                return Err(BuildError::Duplicate(wf.name().to_string()));
            }
            let missing: Vec<_> = wf
                .required_deps()
                .into_iter()
                .filter(|k| !deps.has(k.id))
                .map(|k| k.name)
                .collect();
            if !missing.is_empty() {
                return Err(BuildError::MissingDeps {
                    workflow: wf.name().to_string(),
                    missing,
                });
            }
        }
        let pool: Arc<dyn ComputePool> = Arc::new(ThreadPool::new(self.cfg.compute_threads));
        let grace = self.cfg.lease.saturating_mul(3);
        let heartbeat = Arc::new(Notify::new());
        let env = EngineEnv {
            backend: self.backend.clone(),
            deps,
            pool,
            lease_grace: grace,
            inline_threshold: self.cfg.inline_retry_threshold,
            max_payload_bytes: self.cfg.max_payload_bytes,
            heartbeat: heartbeat.clone(),
        };
        let (shutdown_tx, _) = watch::channel(false);
        let (ready_tx, _) = watch::channel(false);
        Ok(Worker {
            shared: Arc::new(Shared {
                worker_id: self
                    .cfg
                    .worker_id
                    .clone()
                    .unwrap_or_else(|| format!("worker-{}", uuid::Uuid::now_v7().simple())),
                sem: Arc::new(Semaphore::new(self.cfg.concurrency)),
                backend: self.backend,
                env,
                workflows: self.workflows,
                cfg: self.cfg,
                active: Mutex::new(HashMap::new()),
                slot_free: Notify::new(),
                idle: Notify::new(),
                shutdown_tx,
                ready_tx,
            }),
        })
    }
}

struct Active {
    lease: Lease,
    lost: Arc<Notify>,
    abort: Option<AbortHandle>,
}

struct Shared {
    backend: Backend,
    env: EngineEnv,
    workflows: Vec<Arc<dyn ErasedWorkflow>>,
    cfg: Config,
    worker_id: String,
    sem: Arc<Semaphore>,
    active: Mutex<HashMap<RunId, Active>>,
    slot_free: Notify,
    idle: Notify,
    shutdown_tx: watch::Sender<bool>,
    ready_tx: watch::Sender<bool>,
}

pub struct Worker {
    shared: Arc<Shared>,
}

impl Worker {
    pub fn builder(backend: Backend) -> WorkerBuilder {
        WorkerBuilder::new(backend)
    }

    /// Run inside the host's tokio runtime (embedded mode).
    pub fn spawn(self) -> WorkerHandle {
        let sh = self.shared;
        let mut tasks = vec![];
        let rx = sh.shutdown_tx.subscribe();

        let boot = sh.clone();
        tasks.push(tokio::spawn(async move {
            let defs: Vec<WorkflowDefinition> =
                boot.workflows.iter().map(|w| w.definition()).collect();
            if let Err(e) = boot
                .backend
                .store
                .upsert_definitions(&boot.backend.namespace, defs)
                .await
            {
                tracing::error!("definition sync failed: {e}");
            }
            // `send_replace`: `send` drops the value when nobody is subscribed yet.
            boot.ready_tx.send_replace(true);
        }));
        for wf in sh.workflows.clone() {
            tasks.push(tokio::spawn(claim_loop(sh.clone(), wf, rx.clone())));
        }
        tasks.push(tokio::spawn(lease_keeper(sh.clone(), rx.clone())));
        tasks.push(tokio::spawn(reconciler(sh.clone(), rx)));
        WorkerHandle { shared: sh, tasks }
    }
}

pub struct WorkerHandle {
    shared: Arc<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl WorkerHandle {
    /// Resolves once this worker's workflow definitions are synced to the
    /// store, so by-name starts see their flow-control settings.
    pub async fn ready(&self) {
        let mut rx = self.shared.ready_tx.subscribe();
        let _ = rx.wait_for(|ready| *ready).await;
    }

    /// Simulate a process crash: stop everything at once without acking or
    /// handing back leases. Other workers pick the runs up once the leases
    /// expire. Meant for tests; use [`shutdown`](Self::shutdown) in production.
    pub fn abort(self) {
        for t in &self.tasks {
            t.abort();
        }
        for (_, a) in self.shared.active.lock().drain() {
            if let Some(h) = a.abort {
                h.abort();
            }
        }
    }

    /// Stop claiming, drain in-flight runs until the drain timeout, then hand
    /// back anything still running so other workers resume it promptly.
    pub async fn shutdown(self) {
        let sh = &self.shared;
        sh.shutdown_tx.send_replace(true);
        for t in self.tasks {
            let _ = t.await;
        }
        let drain = async {
            loop {
                let idle = sh.idle.notified();
                tokio::pin!(idle);
                idle.as_mut().enable();
                if sh.active.lock().is_empty() {
                    return;
                }
                idle.await;
            }
        };
        let _ = tokio::time::timeout(sh.cfg.drain_timeout, drain).await;
        let leftovers: Vec<Active> = sh.active.lock().drain().map(|(_, a)| a).collect();
        let now = sh.backend.clock.now();
        for a in leftovers {
            if let Some(h) = &a.abort {
                h.abort();
            }
            let _ = sh.backend.queue.nack(&a.lease, now, now).await;
        }
    }
}

fn limits_for(def: &WorkflowDefinition) -> ClaimLimits {
    ClaimLimits {
        workflow_concurrency: def
            .concurrency
            .as_ref()
            .filter(|c| c.key.is_none())
            .map(|c| c.limit),
        key_concurrency: def
            .concurrency
            .as_ref()
            .filter(|c| c.key.is_some())
            .map(|c| c.limit),
        throttle: def.throttle.as_ref().map(|t| ThrottleLimit {
            limit: t.limit,
            period: Duration::from_secs(t.period_secs),
            burst: t.burst,
        }),
    }
}

async fn claim_loop(
    sh: Arc<Shared>,
    wf: Arc<dyn ErasedWorkflow>,
    mut shutdown: watch::Receiver<bool>,
) {
    let ns = sh.backend.namespace.clone();
    let scope = QueueScope {
        namespace: ns.clone(),
        workflow: wf.name().to_string(),
    };
    let limits = limits_for(&wf.definition());
    let mut nudges = sh
        .backend
        .pubsub
        .subscribe(&channels::nudge(&ns, wf.name()))
        .await
        .ok();
    let backlog_limit = sh.cfg.compute_threads * 4;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut next_due = None;
        let mut permits: Vec<OwnedSemaphorePermit> = vec![];
        if sh.env.pool.backlog() < backlog_limit {
            while permits.len() < 16 {
                match sh.sem.clone().try_acquire_owned() {
                    Ok(p) => permits.push(p),
                    Err(_) => break,
                }
            }
        }
        if !permits.is_empty() {
            let now = sh.backend.clock.now();
            match sh
                .backend
                .queue
                .claim(&scope, &limits, permits.len(), sh.cfg.lease, now)
                .await
            {
                Ok(claimed) => {
                    next_due = claimed.next_due;
                    let got = claimed.leases.len();
                    for (lease, permit) in claimed.leases.into_iter().zip(permits.drain(..)) {
                        let run_id = lease.item.run_id;
                        let lost = Arc::new(Notify::new());
                        sh.active.lock().insert(
                            run_id,
                            Active {
                                lease: lease.clone(),
                                lost: lost.clone(),
                                abort: None,
                            },
                        );
                        let task =
                            tokio::spawn(run_one(sh.clone(), wf.clone(), lease, lost, permit));
                        if let Some(a) = sh.active.lock().get_mut(&run_id) {
                            a.abort = Some(task.abort_handle());
                        }
                    }
                    drop(permits);
                    if got > 0 {
                        continue;
                    }
                }
                Err(e) => tracing::warn!("claim failed: {e}"),
            }
        }
        let jitter = rand::rng().random_range(0.8..1.2);
        let poll = sh.cfg.poll_interval.mul_f64(jitter);
        let clock = sh.backend.clock.clone();
        tokio::select! {
            _ = shutdown.changed() => {}
            _ = async {
                match next_due {
                    Some(t) => clock.sleep_until(t).await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = tokio::time::sleep(poll) => {}
            _ = async {
                match nudges.as_mut() {
                    Some(s) => { if s.next().await.is_none() { std::future::pending::<()>().await } }
                    None => std::future::pending().await,
                }
            } => {}
            _ = sh.slot_free.notified() => {}
        }
    }
}

async fn run_one(
    sh: Arc<Shared>,
    wf: Arc<dyn ErasedWorkflow>,
    lease: Lease,
    lost: Arc<Notify>,
    _permit: OwnedSemaphorePermit,
) {
    let run_id = lease.item.run_id;
    let cache = sh.backend.cache.clone();
    let deliveries_key = format!("dw:deliveries:{run_id}");
    let mut deliveries = 0;
    if let Some(c) = &cache {
        deliveries = c
            .incr(&deliveries_key, Some(Duration::from_secs(3600)))
            .await
            .unwrap_or(0);
        let state = crate::InFlight {
            worker: sh.worker_id.clone(),
            since: sh.backend.clock.now(),
            deliveries,
            steps: Default::default(),
        };
        crate::inflight::write(&**c, run_id, &state).await;
    }
    let exceeded = deliveries > sh.cfg.max_deliveries;
    // A panic (engine bug, misbehaving plugin) must still release the lease
    // below; otherwise the lease keeper would renew it forever.
    let execution = async {
        if exceeded {
            poison(&sh, run_id).await
        } else {
            engine::execute(&sh.env, &*wf, run_id, lost).await
        }
    };
    let outcome = AssertUnwindSafe(execution)
        .catch_unwind()
        .await
        .unwrap_or_else(|p| {
            Err(durable_core::Error::Backend(format!(
                "execution panicked: {}",
                crate::driver::panic_message(p)
            )))
        });
    let now = sh.backend.clock.now();
    let queue = &sh.backend.queue;
    let acked = match outcome {
        Ok(Outcome::Finished) => queue.ack(&lease, None, now).await,
        Ok(Outcome::Suspended { wake_at }) => queue.ack(&lease, wake_at, now).await,
        Ok(Outcome::Fault { retry_at }) => queue.nack(&lease, retry_at, now).await,
        Ok(Outcome::Abandoned) => Ok(()),
        Err(durable_core::Error::Conflict) => Ok(()),
        Err(e) => {
            tracing::error!(run = %run_id, "execution failed: {e}");
            queue
                .nack(&lease, now + chrono::Duration::seconds(5), now)
                .await
        }
    };
    if let Err(e) = acked {
        tracing::debug!(run = %run_id, "lease already gone: {e}");
    }
    if let Some(c) = &cache {
        let _ = c.delete(&deliveries_key).await;
        crate::inflight::clear(&**c, run_id).await;
    }
    sh.active.lock().remove(&run_id);
    sh.idle.notify_waiters();
    sh.slot_free.notify_waiters();
}

/// A run that keeps crashing workers is failed instead of crash-looping.
async fn poison(sh: &Shared, run_id: RunId) -> durable_core::Result<Outcome> {
    let store = &sh.backend.store;
    let Some(run) = store.get_run(run_id).await? else {
        return Ok(Outcome::Finished);
    };
    if run.status.is_terminal() {
        return Ok(Outcome::Finished);
    }
    let mut c = Commit::new(run_id, run.last_seq, sh.backend.clock.now());
    c.events = vec![EventKind::RunFailed {
        error: format!(
            "MaxDeliveriesExceeded: execution crashed {} times",
            sh.cfg.max_deliveries
        ),
    }];
    store.commit(c).await?;
    Ok(Outcome::Finished)
}

async fn lease_keeper(sh: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let every = (sh.cfg.lease / 3).max(Duration::from_millis(10));
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(every) => {}
            _ = sh.env.heartbeat.notified() => {}
        }
        let leases: Vec<Lease> = sh.active.lock().values().map(|a| a.lease.clone()).collect();
        if leases.is_empty() {
            continue;
        }
        let now = sh.backend.clock.now();
        match sh.backend.queue.extend(&leases, sh.cfg.lease, now).await {
            Ok(lost) => {
                let active = sh.active.lock();
                for l in lost {
                    if let Some(a) = active.get(&l.item.run_id) {
                        a.lost.notify_one();
                    }
                }
            }
            Err(e) => tracing::warn!("lease extension failed: {e}"),
        }
    }
}

/// Re-enqueues overdue runs (idempotent) and requeues expired leases. When a
/// cache is available only one worker at a time acts as leader.
async fn reconciler(sh: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let ns = sh.backend.namespace.clone();
    let every = sh.cfg.reconcile_interval;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(every) => {}
        }
        if let Some(cache) = &sh.backend.cache
            && !cache
                .try_lock(&format!("dw:reconciler:{ns}"), every)
                .await
                .unwrap_or(false)
        {
            continue;
        }
        let now = sh.backend.clock.now();
        match sh.backend.store.due_runs(&ns, now, 1000).await {
            Ok(items) if !items.is_empty() => {
                let workflows: std::collections::HashSet<_> =
                    items.iter().map(|i| i.workflow.clone()).collect();
                if sh.backend.queue.enqueue(items).await.is_ok() {
                    for wf in workflows {
                        let _ = sh
                            .backend
                            .pubsub
                            .publish(&channels::nudge(&ns, &wf), vec![])
                            .await;
                    }
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("reconcile failed: {e}"),
        }
        for wf in &sh.workflows {
            let scope = QueueScope {
                namespace: ns.clone(),
                workflow: wf.name().to_string(),
            };
            if let Err(e) = sh.backend.queue.requeue_expired(&scope, now).await {
                tracing::warn!("requeue_expired failed: {e}");
            }
        }
    }
}
