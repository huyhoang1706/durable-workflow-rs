//! The execution loop for one claimed run.
//!
//! Load history, replay it through a driver, then act on the frontier of
//! unresolved commands until the run finishes or can only wait, in which case
//! it suspends and holds no worker. Every state change is committed to the
//! store first.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use durable_sm::{
    DecideInput, Decision, FailureKind, Pending, Replayer, RetryPolicy, StepLaunch, WorkflowFailure,
};
use durable_spi::{
    Backend, Commit, CommitOutcome, ComputePool, Error, Event, EventKind, HookReg, Metric, Result,
    Run, RunId, RunStatus, saturating_add,
};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::{sync::Notify, task::AbortHandle};
use tracing::Instrument;

use crate::{
    Deps, StepError, channels, history,
    host::{self, ExecEnv, ExecStatus, HostExecution, HostWorkflow, ReplayHandle},
    step::StepInfo,
    stream::{StreamHub, StreamLimits, close_run_streams},
};

pub(crate) struct EngineEnv {
    pub backend: Backend,
    pub deps: Deps,
    pub pool: Arc<dyn ComputePool>,
    /// A `Running` run that stops committing becomes due again after this.
    pub lease_grace: Duration,
    /// Timers and retries shorter than this are waited out in-process.
    pub inline_threshold: Duration,
    pub max_payload_bytes: usize,
    /// Poked by `step::current().heartbeat()` so the lease keeper extends now.
    pub heartbeat: Arc<Notify>,
    pub stream_limits: StreamLimits,
    /// Set once the "no stream store" warning has been logged.
    pub stream_warned: Arc<AtomicBool>,
}

pub(crate) enum Outcome {
    Finished,
    Suspended {
        wake_at: Option<DateTime<Utc>>,
    },
    /// Engine fault recorded on the run; retry after backoff.
    Fault {
        retry_at: DateTime<Utc>,
    },
    /// Another worker owns the run, or our lease is gone. Do nothing.
    Abandoned,
}

struct StepDone {
    key: String,
    attempt: u32,
    result: Result<Value, StepError>,
}

struct Running {
    attempt: u32,
    /// The step's retry policy, for the attempt's outcome.
    policy: RetryPolicy,
    cancelled: Arc<AtomicBool>,
    aborts: Vec<AbortHandle>,
    started: Instant,
    /// Closed (and exported) when the attempt's result is committed.
    span: tracing::Span,
}

impl Running {
    /// Tell the attempt it is cancelled, and stop its tasks.
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.aborts.iter().for_each(AbortHandle::abort);
    }
}

/// The engine's copy of the frontier, minus the steps that have a running
/// attempt (those are in `Exec::running`). Kept current from the replayer's
/// change feed, so a turn costs what changed instead of everything that is
/// open: with thousands of parallel steps in flight, the idle part is small.
#[derive(Default)]
struct Idle {
    /// By handle, which is issue order: the order `decide` expects.
    by_handle: BTreeMap<u32, Pending>,
    by_key: HashMap<String, u32>,
}

impl Idle {
    fn insert(&mut self, handle: u32, p: Pending) {
        self.by_key.insert(p.key().to_string(), handle);
        self.by_handle.insert(handle, p);
    }

    fn remove(&mut self, key: &str) -> Option<Pending> {
        let handle = self.by_key.remove(key)?;
        self.by_handle.remove(&handle)
    }

    fn get(&self, key: &str) -> Option<&Pending> {
        self.by_handle.get(self.by_key.get(key)?)
    }

    fn to_vec(&self) -> Vec<Pending> {
        self.by_handle.values().cloned().collect()
    }
}

/// The step's name without the `#n` / `[i]` suffix that makes its key unique.
fn step_name(key: &str) -> &str {
    // `split` always yields at least one piece.
    key.split(['#', '[']).next().unwrap_or_default()
}

/// Tells watchers of the run that something happened.
pub(crate) async fn notify_run(backend: &Backend, run_id: RunId, notice: Value) {
    channels::notify(
        &*backend.pubsub,
        &channels::run(run_id),
        notice.to_string().into_bytes(),
    )
    .await;
}

/// Most step results handled in one turn of the run loop, and so in one
/// commit. Bounds the size of a commit when thousands of steps finish at once.
const MAX_DONE_PER_TURN: usize = 1024;

/// The in-flight overlay is rewritten at most this often while steps start and
/// finish, since each write serializes every running attempt.
const PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

enum Wake {
    /// Every step result that was ready, oldest first.
    Done(Vec<StepDone>),
    /// A deferred write of the in-flight overlay is due.
    Publish,
    Timer,
    Cancel,
    CancelStreamClosed,
    Lost,
}

pub(crate) async fn execute(
    env: &EngineEnv,
    wf: &dyn HostWorkflow,
    run_id: RunId,
    lost: Arc<Notify>,
    delivery: i64,
) -> Result<Outcome> {
    let run = env
        .backend
        .store
        .get_run(run_id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
    if run.status.is_terminal() {
        return Ok(Outcome::Finished);
    }
    // Every execution of a run continues the trace its start opened.
    let span = tracing::info_span!(
        "workflow.execute",
        otel.kind = "consumer",
        durable.run_id = %run_id,
        durable.workflow = %run.workflow,
        durable.namespace = %run.namespace,
        durable.delivery = delivery,
        durable.outcome = tracing::field::Empty,
    );
    if let Some(carrier) = &run.trace_context {
        env.backend.observer.set_parent(&span, carrier);
    }
    let (namespace, workflow) = (run.namespace.clone(), run.workflow.clone());
    let started = Instant::now();
    let mut replayed = 0;
    let result = execute_run(env, wf, run, lost, &mut replayed)
        .instrument(span.clone())
        .await;
    let outcome = match &result {
        Ok(Outcome::Finished) => "finished",
        Ok(Outcome::Suspended { .. }) => "suspended",
        Ok(Outcome::Fault { .. }) => "fault",
        Ok(Outcome::Abandoned) | Err(Error::Conflict) => "abandoned",
        Err(_) => "error",
    };
    span.record("durable.outcome", outcome);
    env.backend.observer.record(&Metric::Execution {
        namespace: &namespace,
        workflow: &workflow,
        outcome,
        duration: started.elapsed(),
        replayed_events: replayed,
    });
    result
}

async fn execute_run(
    env: &EngineEnv,
    wf: &dyn HostWorkflow,
    run: Run,
    lost: Arc<Notify>,
    replayed: &mut usize,
) -> Result<Outcome> {
    let run_id = run.id;
    let events = load_history(env, &run).await?;
    *replayed = events.len();
    let cancel_sub = env
        .backend
        .pubsub
        .subscribe(&channels::cancel(run_id))
        .await
        .ok();
    let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut exec = Exec {
        env,
        wf,
        cancel_requested: run.cancel_requested_at.is_some(),
        run,
        replay: ReplayHandle::new(run_id, vec![]),
        host: None,
        status: ExecStatus::Pending,
        history: history::normalize(events.clone()),
        last_seq: events.last().map_or(0, |e| e.seq),
        raw: events,
        running: HashMap::new(),
        idle: Idle::default(),
        done_tx,
        done_rx,
        cancel_sub,
        lost,
        inbox_checked: false,
        published_steps: Default::default(),
        steps_dirty: false,
        last_publish: None,
        publish_at: None,
        streams: StreamHub::new(
            &env.backend,
            run_id,
            env.stream_limits.clone(),
            env.stream_warned.clone(),
        ),
    };
    let result = exec.run_loop().await;
    exec.abort_all();
    if let Some(h) = exec.host.as_mut() {
        h.interrupt();
    }
    exec.streams.shutdown().await;
    if matches!(
        result,
        Ok(Outcome::Suspended { .. } | Outcome::Fault { .. })
    ) {
        store_history(env, exec.run.id, exec.last_seq, &exec.raw).await;
    }
    match result {
        Err(Error::Conflict) => Ok(Outcome::Abandoned),
        other => other,
    }
}

/// How long a run's cached log lives after its last execution.
const HISTORY_TTL: Duration = Duration::from_secs(24 * 3600);

#[derive(serde::Serialize, serde::Deserialize)]
struct CachedHistory {
    last_seq: u64,
    events: Vec<Event>,
}

pub(crate) fn history_key(run_id: RunId) -> String {
    format!("dw:hist:{run_id}")
}

/// The run's full log: from the cache when its `last_seq` matches the run's,
/// otherwise from the store. The cache is only ever a shortcut.
async fn load_history(env: &EngineEnv, run: &Run) -> Result<Vec<Event>> {
    if let Some(cache) = &env.backend.cache
        && let Ok(Some(bytes)) = cache.get(&history_key(run.id)).await
        && let Ok(hit) = serde_json::from_slice::<CachedHistory>(&bytes)
        && hit.last_seq == run.last_seq
        && hit.events.last().map_or(0, |e| e.seq) == hit.last_seq
    {
        return Ok(hit.events);
    }
    env.backend.store.load_events(run.id, 0).await
}

async fn store_history(env: &EngineEnv, run_id: RunId, last_seq: u64, events: &[Event]) {
    let Some(cache) = &env.backend.cache else {
        return;
    };
    let entry = CachedHistory {
        last_seq,
        events: events.to_vec(),
    };
    if let Ok(bytes) = serde_json::to_vec(&entry) {
        let _ = cache
            .set(&history_key(run_id), bytes, Some(HISTORY_TTL))
            .await;
    }
}

struct Exec<'a> {
    env: &'a EngineEnv,
    wf: &'a dyn HostWorkflow,
    run: Run,
    replay: ReplayHandle,
    /// The workflow body, run by its host. `None` until the first rebuild.
    host: Option<Box<dyn HostExecution>>,
    status: ExecStatus,
    history: Vec<Event>,
    /// The log exactly as stored (`history` is normalized for replay).
    raw: Vec<Event>,
    last_seq: u64,
    running: HashMap<String, Running>,
    /// The open commands that `running` does not cover.
    idle: Idle,
    done_tx: tokio::sync::mpsc::UnboundedSender<StepDone>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<StepDone>,
    cancel_sub: Option<futures::stream::BoxStream<'static, Vec<u8>>>,
    cancel_requested: bool,
    lost: Arc<Notify>,
    /// Inbox signals for already-open waits are consumed by the first commit.
    inbox_checked: bool,
    /// Step attempts last published to the in-flight overlay.
    published_steps: BTreeMap<String, u32>,
    /// `running` changed since the overlay was last written.
    steps_dirty: bool,
    last_publish: Option<Instant>,
    /// When a write that `PUBLISH_INTERVAL` held back is due.
    publish_at: Option<Instant>,
    streams: Arc<StreamHub>,
}

impl Drop for Exec<'_> {
    fn drop(&mut self) {
        // A cancelled execution must not leave step tasks running behind it.
        self.abort_all();
    }
}

impl Exec<'_> {
    fn now(&self) -> DateTime<Utc> {
        self.env.backend.clock.now()
    }

    fn abort_all(&mut self) {
        self.running.drain().for_each(|(_, r)| r.cancel());
    }

    fn fault(&self) -> Option<String> {
        self.replay.with(|r| r.fault().map(str::to_string))
    }

    fn frontier(&self) -> Vec<Pending> {
        self.replay.with(|r| r.frontier())
    }

    /// (Re)build the workflow from history. Running steps survive: the engine
    /// owns their tasks, not the workflow body.
    async fn rebuild(&mut self) {
        if let Some(mut old) = self.host.take() {
            old.interrupt();
        }
        self.replay = ReplayHandle::new(self.run.id, self.history.clone());
        let mut exec = self.wf.start(ExecEnv {
            run_id: self.run.id,
            input: self.run.input.clone(),
            replay: self.replay.clone(),
            deps: self.env.deps.clone(),
            pool: self.env.pool.clone(),
        });
        self.status = host::replay(&self.replay, &mut *exec, ExecStatus::Pending).await;
        self.host = Some(exec);
        // The new replayer lists every command it has; start the copy over.
        self.idle = Idle::default();
        self.sync_frontier(true);
    }

    /// Bring `idle` up to date with the replayer, and abort attempts whose
    /// command left the frontier (resolved, or its future was dropped).
    ///
    /// `full` is for a new replayer, whose changes list every command: an
    /// attempt it does not list as open is aborted too.
    fn sync_frontier(&mut self, full: bool) {
        let changes = self.replay.with(Replayer::take_frontier_changes);
        let mut live = HashSet::new();
        for c in changes {
            match c.pending {
                Some(p) => {
                    if full {
                        live.insert(c.key.clone());
                    }
                    if !self.running.contains_key(&c.key) {
                        self.idle.insert(c.handle.0, p);
                    }
                }
                None => {
                    self.idle.remove(&c.key);
                    if let Some(r) = self.running.remove(&c.key) {
                        r.cancel();
                        self.steps_dirty = true;
                    }
                }
            }
        }
        if full {
            let before = self.running.len();
            self.running.retain(|key, r| {
                let keep = live.contains(key);
                if !keep {
                    r.cancel();
                }
                keep
            });
            self.steps_dirty |= self.running.len() != before;
        }
    }

    async fn run_loop(&mut self) -> Result<Outcome> {
        self.rebuild().await;
        loop {
            if let Some(msg) = self.fault() {
                return self.on_fault(msg).await;
            }
            if self.cancel_requested {
                return self.on_cancel().await;
            }
            if let ExecStatus::Finished(res) = self.status.clone() {
                return self.on_finished(res).await;
            }
            self.sync_frontier(false);
            // Steps with a running attempt are left out (see `Idle`).
            let frontier = self.idle.to_vec();
            let decision = durable_sm::decide(&DecideInput {
                frontier: &frontier,
                now: self.now(),
                deadline_at: self.run.deadline_at,
                running: &|key| self.running.contains_key(key),
                running_elsewhere: self.running.len(),
                inbox_checked: self.inbox_checked,
                inline_threshold: self.env.inline_threshold,
            });
            let next_wake = match decision {
                Decision::DeadlineExceeded => return self.on_deadline().await,
                Decision::Fault { message } => return self.on_fault(message).await,
                Decision::Register { events, hooks } => {
                    self.inbox_checked = true;
                    let hooks = hooks
                        .into_iter()
                        .map(|h| HookReg {
                            key: h.key,
                            token: h.token.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                        })
                        .collect();
                    self.commit_running(events, hooks, &frontier).await?;
                    continue;
                }
                Decision::Fire { events } => {
                    self.commit_running(events, vec![], &frontier).await?;
                    continue;
                }
                Decision::Suspend { wake_at } => {
                    self.publish_steps(true).await;
                    if let Some(outcome) = self.suspend(&frontier, wake_at).await? {
                        return Ok(outcome);
                    }
                    continue;
                }
                Decision::Run { launch, wake_at } => {
                    // A host that can no longer start a body (a Rust FnOnce an
                    // earlier attempt took) needs a rebuild; launch the rest then.
                    if !launch.iter().all(|l| self.launch(l)) {
                        self.rebuild().await;
                        continue;
                    }
                    self.publish_steps(false).await;
                    wake_at
                }
            };

            let clock = self.env.backend.clock.clone();
            let publish_at = self.publish_at;
            let (done_rx, cancel_sub, lost) = (&mut self.done_rx, &mut self.cancel_sub, &self.lost);
            let wake = tokio::select! {
                Some(first) = done_rx.recv() => {
                    // Drain what else is ready: one commit for all of them
                    // instead of one turn (frontier, decide, commit) each.
                    let mut batch = vec![first];
                    while batch.len() < MAX_DONE_PER_TURN {
                        match done_rx.try_recv() {
                            Ok(d) => batch.push(d),
                            Err(_) => break,
                        }
                    }
                    Wake::Done(batch)
                }
                _ = async {
                    match next_wake {
                        Some(t) => clock.sleep_until(t).await,
                        None => std::future::pending().await,
                    }
                } => Wake::Timer,
                _ = async {
                    match publish_at {
                        Some(t) => tokio::time::sleep_until(t.into()).await,
                        None => std::future::pending().await,
                    }
                } => Wake::Publish,
                msg = async {
                    match cancel_sub.as_mut() {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => if msg.is_some() { Wake::Cancel } else { Wake::CancelStreamClosed },
                _ = lost.notified() => Wake::Lost,
            };
            match wake {
                Wake::Done(batch) => self.on_steps_done(batch, &frontier).await?,
                Wake::Cancel => self.cancel_requested = true,
                Wake::Lost => return Ok(Outcome::Abandoned),
                Wake::CancelStreamClosed => self.cancel_sub = None,
                Wake::Publish => self.publish_steps(false).await,
                Wake::Timer => {}
            }
        }
    }

    /// Keep the cache's in-flight view of running step attempts current.
    ///
    /// Throttled to one write per `PUBLISH_INTERVAL`: a change inside the
    /// interval is written when it ends (see `Wake::Publish`). `force` writes
    /// now, for when the loop is about to stop turning.
    async fn publish_steps(&mut self, force: bool) {
        let Some(cache) = &self.env.backend.cache else {
            return;
        };
        if !self.steps_dirty {
            return;
        }
        let now = Instant::now();
        if let Some(last) = self.last_publish
            && !force
            && now < last + PUBLISH_INTERVAL
        {
            self.publish_at = Some(last + PUBLISH_INTERVAL);
            return;
        }
        self.steps_dirty = false;
        self.publish_at = None;
        self.last_publish = Some(now);
        let steps: BTreeMap<String, u32> = self
            .running
            .iter()
            .map(|(k, r)| (k.clone(), r.attempt))
            .collect();
        if steps == self.published_steps {
            return;
        }
        if let Some(mut state) = crate::inflight::read(&**cache, self.run.id).await {
            state.steps = steps.clone();
            crate::inflight::write(&**cache, self.run.id, &state).await;
        }
        self.published_steps = steps;
    }

    fn build_commit(
        &self,
        events: Vec<EventKind>,
        hooks: Vec<HookReg>,
        frontier: &[Pending],
        status: RunStatus,
        wake_at: Option<DateTime<Utc>>,
        at: DateTime<Utc>,
    ) -> Commit {
        let waits = durable_sm::open_waits(frontier, &events, at);
        let mut c = Commit::new(self.run.id, self.last_seq, at);
        c.awaiting = waits.iter().map(|w| w.name.clone()).collect();
        c.waits = waits;
        c.events = events;
        c.hooks = hooks;
        c.status = status;
        c.wake_at = wake_at;
        c
    }

    /// The store commit, traced and timed. Watchers are told about the new
    /// events right after it succeeds.
    async fn commit_store(&mut self, c: Commit) -> Result<CommitOutcome> {
        let span = tracing::debug_span!(
            "store.commit",
            durable.run_id = %self.run.id,
            durable.events = c.events.len(),
        );
        let started = Instant::now();
        let res = self.env.backend.store.commit(c).instrument(span).await;
        self.env.backend.observer.record(&Metric::Commit {
            duration: started.elapsed(),
            conflict: matches!(res, Err(Error::Conflict)),
        });
        if let Ok(out) = &res {
            notify_run(&self.env.backend, self.run.id, json!({"seq": out.last_seq})).await;
        }
        res
    }

    /// Commit, then feed everything the store appended into the live driver.
    async fn apply(&mut self, c: Commit) -> Result<CommitOutcome> {
        let at = c.at;
        let mut seq = self.last_seq;
        let mut applied: Vec<Event> = c
            .events
            .iter()
            .map(|k| {
                seq += 1;
                Event {
                    run_id: self.run.id,
                    seq,
                    at,
                    kind: k.clone(),
                }
            })
            .collect();
        let out = self.commit_store(c).await?;
        applied.extend(out.generated.iter().cloned());
        self.last_seq = out.last_seq;
        self.cancel_requested |= out.cancel_requested;
        let applied: Vec<Event> = applied
            .into_iter()
            .filter(|ev| !ev.kind.is_terminal())
            .collect();
        self.raw.extend(applied.iter().cloned());
        self.history.extend(applied.iter().cloned());
        self.replay.with(|r| r.push_live(applied));
        if let Some(exec) = self.host.as_mut() {
            self.status = host::replay(&self.replay, &mut **exec, self.status.clone()).await;
        }
        Ok(out)
    }

    async fn commit_running(
        &mut self,
        events: Vec<EventKind>,
        hooks: Vec<HookReg>,
        frontier: &[Pending],
    ) -> Result<CommitOutcome> {
        let at = self.now();
        let c = self.build_commit(
            events,
            hooks,
            frontier,
            RunStatus::Running,
            Some(saturating_add(at, self.env.lease_grace)),
            at,
        );
        self.apply(c).await
    }

    /// `None` means "keep going": something arrived while suspending.
    async fn suspend(
        &mut self,
        frontier: &[Pending],
        wake_at: Option<DateTime<Utc>>,
    ) -> Result<Option<Outcome>> {
        let at = self.now();
        let c = self.build_commit(vec![], vec![], frontier, RunStatus::Waiting, wake_at, at);
        let out = self.apply(c).await?;
        if !out.generated.is_empty() || out.cancel_requested {
            return Ok(None);
        }
        Ok(Some(Outcome::Suspended { wake_at }))
    }

    /// Start an attempt. `false` when the host can no longer start the body.
    fn launch(&mut self, l: &StepLaunch) -> bool {
        let (key, attempt, timeout) = (l.key.as_str(), l.attempt, l.timeout);
        let Some(Pending::Step { policy, .. }) = self.idle.get(key) else {
            debug_assert!(
                false,
                "`decide` launched `{key}`, which is not an idle step"
            );
            return true;
        };
        let policy = *policy;
        let name = step_name(key);
        let span = tracing::info_span!(
            "step",
            otel.name = %format!("step {name}"),
            durable.run_id = %self.run.id,
            durable.workflow = %self.run.workflow,
            durable.step.name = name,
            durable.step.key = key,
            durable.step.attempt = attempt,
            durable.step.kind = tracing::field::Empty,
            durable.step.outcome = tracing::field::Empty,
            durable.step.error = tracing::field::Empty,
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let heartbeat = self.env.heartbeat.clone();
        let info = StepInfo {
            step_key: key.to_string(),
            attempt,
            idempotency_key: format!("{}:{key}", self.run.id),
            cancelled: cancelled.clone(),
            heartbeat: Arc::new(move || heartbeat.notify_one()),
            deps: self.env.deps.clone(),
            streams: self.streams.clone(),
        };
        let Some(started) = self
            .host
            .as_mut()
            .and_then(|h| h.start_step(key, attempt, info, &span))
        else {
            return false;
        };
        span.record("durable.step.kind", started.kind);
        // Best effort: lets watchers show the attempt as running. Not
        // awaited, so it may land after this step's own `{"seq"}` notice;
        // `watch` drops a `step_started` for an attempt it already saw end.
        let backend = self.env.backend.clone();
        let run_id = self.run.id;
        let notice = json!({"step_started": {"key": key, "attempt": attempt}});
        tokio::spawn(async move { notify_run(&backend, run_id, notice).await });

        let task = tokio::spawn(started.fut.instrument(span.clone()));
        let mut aborts = vec![task.abort_handle()];
        let (done_tx, key_owned, flag) = (self.done_tx.clone(), key.to_string(), cancelled.clone());
        let forward = tokio::spawn(async move {
            // `None`: the attempt was aborted and reports nothing.
            let result = async {
                match task.await {
                    Ok(r) => Some(r),
                    Err(e) if e.is_panic() => Some(Err(StepError::retryable(format!(
                        "step panicked: {}",
                        crate::driver::panic_message(e.into_panic())
                    )))),
                    Err(_) => None,
                }
            };
            let result = match timeout {
                Some(d) => tokio::select! {
                    r = result => r,
                    _ = tokio::time::sleep(d) => {
                        flag.store(true, Ordering::SeqCst);
                        Some(Err(StepError::retryable(format!("step timed out after {d:?}"))))
                    }
                },
                None => result.await,
            };
            if let Some(result) = result {
                let _ = done_tx.send(StepDone {
                    key: key_owned,
                    attempt,
                    result,
                });
            }
        });
        aborts.push(forward.abort_handle());
        self.steps_dirty = true;
        self.idle.remove(key);
        self.running.insert(
            key.to_string(),
            Running {
                attempt,
                policy,
                cancelled,
                aborts,
                started: Instant::now(),
                span,
            },
        );
        true
    }

    /// Record a batch of finished attempts in one commit. Results for attempts
    /// the engine no longer runs (an older attempt, or one it aborted) are
    /// dropped.
    async fn on_steps_done(&mut self, batch: Vec<StepDone>, frontier: &[Pending]) -> Result<()> {
        let mut events = Vec::with_capacity(batch.len());
        for done in batch {
            if let Some(event) = self.step_event(done).await {
                events.push(event);
            }
        }
        if !events.is_empty() {
            self.commit_running(events, vec![], frontier).await?;
        }
        Ok(())
    }

    /// The event for one finished attempt, with its span, metric and stream
    /// bookkeeping done. `None` for a result the engine no longer waits for.
    async fn step_event(&mut self, done: StepDone) -> Option<EventKind> {
        match self.running.get(&done.key) {
            Some(r) if r.attempt == done.attempt => {}
            _ => return None, // stale
        }
        let running = self.running.remove(&done.key).expect("checked above");
        self.steps_dirty = true;
        running.aborts.iter().for_each(AbortHandle::abort);
        let policy = running.policy;
        let step_key = done.key.clone();
        let event = durable_sm::step_outcome(
            done.key,
            &policy,
            done.attempt,
            done.result.map_err(|e| (e.message, e.class)),
            self.now(),
            self.env.max_payload_bytes,
        );
        let (outcome, failed) = match &event {
            EventKind::StepSucceeded { .. } => ("succeeded", false),
            EventKind::StepFailed {
                retry_at: Some(_), ..
            } => ("retrying", true),
            _ => ("failed", true),
        };
        running.span.record("durable.step.outcome", outcome);
        if let EventKind::StepFailed { error, .. } = &event {
            running.span.record("durable.step.error", error.as_str());
        }
        self.env.backend.observer.record(&Metric::StepAttempt {
            namespace: &self.run.namespace,
            workflow: &self.run.workflow,
            step: step_name(&step_key),
            outcome,
            duration: running.started.elapsed(),
        });
        // Readers must see every chunk before the step's event shows up.
        self.streams
            .finish_step(&step_key, done.attempt, failed)
            .await;
        Some(event)
    }

    async fn finish_commit(&mut self, kind: EventKind) -> Result<()> {
        let at = self.now();
        let status = match &kind {
            EventKind::RunSucceeded { .. } => RunStatus::Succeeded,
            EventKind::RunCancelled { .. } => RunStatus::Cancelled,
            _ => RunStatus::Failed,
        };
        let mut c = Commit::new(self.run.id, self.last_seq, at);
        c.events = vec![kind];
        self.commit_store(c).await?;
        self.env.backend.observer.record(&Metric::RunFinished {
            namespace: &self.run.namespace,
            workflow: &self.run.workflow,
            status,
            e2e: (at - self.run.created_at).to_std().unwrap_or_default(),
        });
        // Readers of the run's streams end once the run is over.
        self.streams.shutdown().await;
        close_run_streams(&self.env.backend, self.run.id).await;
        Ok(())
    }

    async fn on_finished(&mut self, res: Result<Value, WorkflowFailure>) -> Result<Outcome> {
        let error = match res {
            Ok(output) => {
                self.finish_commit(EventKind::RunSucceeded { output })
                    .await?;
                return Ok(Outcome::Finished);
            }
            Err(e) => e,
        };
        match error.kind {
            FailureKind::Cancelled => self.on_cancel().await,
            FailureKind::Interrupted => {
                self.on_fault("workflow body returned Interrupted without being interrupted".into())
                    .await
            }
            FailureKind::Fault => self.on_fault(error.message).await,
            FailureKind::Failed | FailureKind::Step | FailureKind::HookConflict => {
                self.finish_commit(EventKind::RunFailed {
                    error: error.message,
                })
                .await?;
                Ok(Outcome::Finished)
            }
        }
    }

    async fn on_cancel(&mut self) -> Result<Outcome> {
        self.abort_all();
        self.replay.with(Replayer::cancel);
        let reason = self
            .env
            .backend
            .store
            .get_run(self.run.id)
            .await?
            .and_then(|r| r.cancel_reason);
        self.finish_commit(EventKind::RunCancelled { reason })
            .await?;
        Ok(Outcome::Finished)
    }

    async fn on_deadline(&mut self) -> Result<Outcome> {
        self.abort_all();
        self.finish_commit(EventKind::RunFailed {
            error: "run deadline exceeded".into(),
        })
        .await?;
        Ok(Outcome::Finished)
    }

    /// Record the fault on the run and back off. The run stays open so a code
    /// fix resumes it.
    async fn on_fault(&mut self, msg: String) -> Result<Outcome> {
        tracing::error!(
            run_id = %self.run.id, workflow = %self.run.workflow,
            "engine fault: {msg}"
        );
        self.env.backend.observer.record(&Metric::EngineFault {
            namespace: &self.run.namespace,
            workflow: &self.run.workflow,
        });
        self.abort_all();
        let at = self.now();
        let attempt = match &self.env.backend.cache {
            Some(cache) => cache
                .incr(
                    &format!("dw:faults:{}", self.run.id),
                    Some(Duration::from_secs(3600)),
                )
                .await
                .unwrap_or(1),
            None => 1,
        };
        let backoff = Duration::from_secs(10)
            .saturating_mul(2u32.saturating_pow(attempt.clamp(1, 16) as u32 - 1))
            .min(Duration::from_secs(300));
        let retry_at = saturating_add(at, backoff);
        let frontier = self.frontier();
        let mut c = self.build_commit(
            vec![],
            vec![],
            &frontier,
            RunStatus::Waiting,
            Some(retry_at),
            at,
        );
        c.last_error = Some(msg);
        self.commit_store(c).await?;
        Ok(Outcome::Fault { retry_at })
    }
}
