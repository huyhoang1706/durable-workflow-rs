//! The execution loop for one claimed run.
//!
//! Load history, replay it through a driver, then act on the frontier of
//! unresolved commands until the run finishes or can only wait, in which case
//! it suspends and holds no worker. Every state change is committed to the
//! store first.

use std::{
    collections::{HashMap, HashSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use durable_core::{
    Backend, Commit, CommitOutcome, ComputePool, Error, Event, EventKind, HookReg, Metric, Result,
    Run, RunId, RunStatus, saturating_add,
};
use durable_sm::{DecideInput, Decision};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::{sync::Notify, task::AbortHandle};
use tracing::Instrument;

use crate::{
    Deps, StepError, WorkflowError, channels,
    core::{Advance, Core, Pending, StepStart},
    driver::{Driver, DriverStatus},
    history,
    step::{self, StepInfo},
    stream::{StreamHub, StreamLimits, close_run_streams},
    workflow::ErasedWorkflow,
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
    cancelled: Arc<AtomicBool>,
    aborts: Vec<AbortHandle>,
    started: Instant,
    /// Closed (and exported) when the attempt's result is committed.
    span: tracing::Span,
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

enum Wake {
    Done(StepDone),
    Timer,
    Cancel,
    CancelStreamClosed,
    Lost,
}

pub(crate) async fn execute(
    env: &EngineEnv,
    wf: &dyn ErasedWorkflow,
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
    wf: &dyn ErasedWorkflow,
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
    let core = Core::new(run_id, env.deps.clone(), vec![]);
    let mut exec = Exec {
        env,
        wf,
        cancel_requested: run.cancel_requested_at.is_some(),
        run,
        core,
        driver: None,
        status: DriverStatus::Pending,
        history: history::normalize(events.clone()),
        last_seq: events.last().map_or(0, |e| e.seq),
        raw: events,
        running: HashMap::new(),
        done_tx,
        done_rx,
        cancel_sub,
        lost,
        inbox_checked: false,
        published_steps: Default::default(),
        streams: StreamHub::new(
            &env.backend,
            run_id,
            env.stream_limits.clone(),
            env.stream_warned.clone(),
        ),
    };
    let result = exec.run_loop().await;
    exec.abort_all();
    if let Some(d) = exec.driver.as_mut() {
        d.interrupt();
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
    wf: &'a dyn ErasedWorkflow,
    run: Run,
    core: Arc<Core>,
    driver: Option<Driver>,
    status: DriverStatus,
    history: Vec<Event>,
    /// The log exactly as stored (`history` is normalized for replay).
    raw: Vec<Event>,
    last_seq: u64,
    running: HashMap<String, Running>,
    done_tx: tokio::sync::mpsc::UnboundedSender<StepDone>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<StepDone>,
    cancel_sub: Option<futures::stream::BoxStream<'static, Vec<u8>>>,
    cancel_requested: bool,
    lost: Arc<Notify>,
    /// Inbox signals for already-open waits are consumed by the first commit.
    inbox_checked: bool,
    /// Step attempts last published to the in-flight overlay.
    published_steps: std::collections::BTreeMap<String, u32>,
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
        for (_, r) in self.running.drain() {
            r.cancelled.store(true, Ordering::SeqCst);
            r.aborts.iter().for_each(AbortHandle::abort);
        }
    }

    /// (Re)build the workflow from history. Running steps survive: the engine
    /// owns their tasks, not the workflow future.
    async fn rebuild(&mut self) {
        if let Some(old) = self.driver.as_mut() {
            old.interrupt();
        }
        self.core = Core::new(self.run.id, self.env.deps.clone(), self.history.clone());
        let mut driver = self.wf.start(self.core.clone(), self.run.input.clone());
        self.status = DriverStatus::Pending;
        // The poll after the newest event is live (`is_replaying` is false): it
        // is the first time the body sees that event, unless an earlier
        // execution crashed before acting on it, in which case acting twice
        // beats never acting.
        while self.core.advance() == Advance::RunCode {
            self.status = driver.poll().await;
            if self.core.fault().is_some() {
                break;
            }
        }
        self.driver = Some(driver);
    }

    async fn run_loop(&mut self) -> Result<Outcome> {
        self.rebuild().await;
        loop {
            if let Some(msg) = self.core.fault() {
                return self.on_fault(msg).await;
            }
            if self.cancel_requested {
                return self.on_cancel().await;
            }
            if let DriverStatus::Finished(res) = self.status.clone() {
                return self.on_finished(res).await;
            }
            let frontier = self.core.frontier();
            self.reap(&frontier);
            let decision = durable_sm::decide(&DecideInput {
                frontier: &frontier,
                now: self.now(),
                deadline_at: self.run.deadline_at,
                running: &|key| self.running.contains_key(key),
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
                    self.publish_steps().await;
                    if let Some(outcome) = self.suspend(&frontier, wake_at).await? {
                        return Ok(outcome);
                    }
                    continue;
                }
                Decision::Run { launch, wake_at } => {
                    // A spent FnOnce body (an earlier attempt took it) needs a rebuild.
                    if let Some(spent) = launch.iter().position(|l| !self.core.has_start(&l.key)) {
                        for l in &launch[..spent] {
                            self.launch(&l.key, l.attempt, l.timeout);
                        }
                        self.rebuild().await;
                        continue;
                    }
                    for l in &launch {
                        self.launch(&l.key, l.attempt, l.timeout);
                    }
                    self.publish_steps().await;
                    wake_at
                }
            };

            let clock = self.env.backend.clock.clone();
            let (done_rx, cancel_sub, lost) = (&mut self.done_rx, &mut self.cancel_sub, &self.lost);
            let wake = tokio::select! {
                Some(done) = done_rx.recv() => Wake::Done(done),
                _ = async {
                    match next_wake {
                        Some(t) => clock.sleep_until(t).await,
                        None => std::future::pending().await,
                    }
                } => Wake::Timer,
                msg = async {
                    match cancel_sub.as_mut() {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => if msg.is_some() { Wake::Cancel } else { Wake::CancelStreamClosed },
                _ = lost.notified() => Wake::Lost,
            };
            match wake {
                Wake::Done(d) => self.on_step_done(d, &frontier).await?,
                Wake::Cancel => self.cancel_requested = true,
                Wake::Lost => return Ok(Outcome::Abandoned),
                Wake::CancelStreamClosed => self.cancel_sub = None,
                Wake::Timer => {}
            }
        }
    }

    /// Keep the cache's in-flight view of running step attempts current.
    async fn publish_steps(&mut self) {
        let Some(cache) = &self.env.backend.cache else {
            return;
        };
        let steps: std::collections::BTreeMap<String, u32> = self
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

    /// Abort tasks whose command is gone (resolved, or its future was dropped).
    fn reap(&mut self, frontier: &[Pending]) {
        let live: HashSet<&str> = frontier.iter().map(Pending::key).collect();
        self.running.retain(|key, r| {
            let keep = live.contains(key.as_str());
            if !keep {
                r.cancelled.store(true, Ordering::SeqCst);
                r.aborts.iter().for_each(AbortHandle::abort);
            }
            keep
        });
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
        self.core.push_live(applied);
        while self.core.advance() == Advance::RunCode {
            if let Some(d) = self.driver.as_mut() {
                self.status = d.poll().await;
            }
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

    fn launch(&mut self, key: &str, attempt: u32, timeout: Option<Duration>) {
        let Some(start) = self.core.take_start(key) else {
            return;
        };
        let name = step_name(key);
        let span = tracing::info_span!(
            "step",
            otel.name = %format!("step {name}"),
            durable.run_id = %self.run.id,
            durable.workflow = %self.run.workflow,
            durable.step.name = name,
            durable.step.key = key,
            durable.step.attempt = attempt,
            durable.step.kind = match &start {
                StepStart::Async(_) => "async",
                StepStart::Blocking(_) => "blocking",
            },
            durable.step.outcome = tracing::field::Empty,
            durable.step.error = tracing::field::Empty,
        );
        // Best effort: lets watchers show the attempt as running. Not
        // awaited, so it may land after this step's own `{"seq"}` notice;
        // `watch` drops a `step_started` for an attempt it already saw end.
        let backend = self.env.backend.clone();
        let run_id = self.run.id;
        let notice = json!({"step_started": {"key": key, "attempt": attempt}});
        tokio::spawn(async move { notify_run(&backend, run_id, notice).await });
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
        let (done_tx, key_owned) = (self.done_tx.clone(), key.to_string());
        let mut aborts = vec![];

        // `result` resolves with the step outcome (or never, if aborted).
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<Value, StepError>>();
        match start {
            StepStart::Async(f) => {
                let task = tokio::spawn(
                    step::scope_async(info, async move { f().await }).instrument(span.clone()),
                );
                aborts.push(task.abort_handle());
                tokio::spawn(async move {
                    let r = match task.await {
                        Ok(r) => r,
                        Err(e) if e.is_panic() => Err(StepError::retryable(format!(
                            "step panicked: {}",
                            crate::driver::panic_message(e.into_panic())
                        ))),
                        Err(_) => return, // aborted
                    };
                    let _ = result_tx.send(r);
                });
            }
            StepStart::Blocking(f) => {
                let span = span.clone();
                self.env.pool.execute(Box::new(move || {
                    let _entered = span.enter();
                    let r = catch_unwind(AssertUnwindSafe(|| step::scope_blocking(info, f)))
                        .unwrap_or_else(|p| {
                            Err(StepError::retryable(format!(
                                "step panicked: {}",
                                crate::driver::panic_message(p)
                            )))
                        });
                    let _ = result_tx.send(r);
                }));
            }
        }
        let flag = cancelled.clone();
        let forward = tokio::spawn(async move {
            let result = match timeout {
                Some(d) => tokio::select! {
                    r = result_rx => r,
                    _ = tokio::time::sleep(d) => {
                        flag.store(true, Ordering::SeqCst);
                        Ok(Err(StepError::retryable(format!("step timed out after {d:?}"))))
                    }
                },
                None => result_rx.await,
            };
            if let Ok(result) = result {
                let _ = done_tx.send(StepDone {
                    key: key_owned,
                    attempt,
                    result,
                });
            }
        });
        aborts.push(forward.abort_handle());
        self.running.insert(
            key.to_string(),
            Running {
                attempt,
                cancelled,
                aborts,
                started: Instant::now(),
                span,
            },
        );
    }

    async fn on_step_done(&mut self, done: StepDone, frontier: &[Pending]) -> Result<()> {
        match self.running.get(&done.key) {
            Some(r) if r.attempt == done.attempt => {}
            _ => return Ok(()), // stale
        }
        let running = self.running.remove(&done.key).expect("checked above");
        running.aborts.iter().for_each(AbortHandle::abort);
        let Some(policy) = frontier.iter().find_map(|p| match p {
            Pending::Step { key, policy, .. } if *key == done.key => Some(*policy),
            _ => None,
        }) else {
            return Ok(());
        };
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
        drop(running);
        self.commit_running(vec![event], vec![], frontier).await?;
        Ok(())
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

    async fn on_finished(&mut self, res: Result<Value, WorkflowError>) -> Result<Outcome> {
        match res {
            Ok(output) => {
                self.finish_commit(EventKind::RunSucceeded { output })
                    .await?;
                Ok(Outcome::Finished)
            }
            Err(WorkflowError::Cancelled) => self.on_cancel().await,
            Err(WorkflowError::Interrupted) => {
                self.on_fault("workflow body returned Interrupted without being interrupted".into())
                    .await
            }
            Err(e) if e.is_engine_fault() => self.on_fault(e.to_string()).await,
            Err(e) => {
                self.finish_commit(EventKind::RunFailed {
                    error: e.to_string(),
                })
                .await?;
                Ok(Outcome::Finished)
            }
        }
    }

    async fn on_cancel(&mut self) -> Result<Outcome> {
        self.abort_all();
        self.core.cancel();
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
        let frontier = self.core.frontier();
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
