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
    time::Duration,
};

use chrono::{DateTime, Utc};
use durable_core::{
    Backend, Commit, CommitOutcome, ComputePool, Error, Event, EventKind, HookReg, Result, Run,
    RunId, RunStatus, WaitReg,
};
use futures::StreamExt;
use serde_json::Value;
use tokio::{sync::Notify, task::AbortHandle};

use crate::{
    Deps, ErrorClass, StepError, WorkflowError, channels,
    core::{Core, Pending, StepStart},
    driver::{Driver, DriverStatus},
    history,
    step::{self, StepInfo},
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
) -> Result<Outcome> {
    let store = &env.backend.store;
    let run = store
        .get_run(run_id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
    if run.status.is_terminal() {
        return Ok(Outcome::Finished);
    }
    let events = load_history(env, &run).await?;
    let cancel_sub = env
        .backend
        .pubsub
        .subscribe(&channels::cancel(run_id))
        .await
        .ok();
    let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
    let core = Core::new(run_id, env.deps.clone());
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
    };
    let result = exec.run_loop().await;
    exec.abort_all();
    if let Some(d) = exec.driver.as_mut() {
        d.interrupt();
    }
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
        self.core = Core::new(self.run.id, self.env.deps.clone());
        let mut driver = self.wf.start(self.core.clone(), self.run.input.clone());
        self.status = DriverStatus::Pending;
        for ev in &self.history {
            self.core.feed(ev);
            self.status = driver.poll().await;
            if self.core.fault().is_some() {
                break;
            }
        }
        if self.history.is_empty() {
            self.status = driver.poll().await;
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
            let now = self.now();
            if self.run.deadline_at.is_some_and(|d| now >= d) {
                return self.on_deadline().await;
            }
            let frontier = self.core.frontier();
            if frontier.is_empty() {
                return self
                    .on_fault(
                        "workflow awaited a non-durable future (only ctx.* calls may be awaited)"
                            .into(),
                    )
                    .await;
            }
            self.reap(&frontier);

            // 1. Register new timers, waits and hooks (and sync open waits).
            let (events, hooks) = self.registrations(&frontier, now);
            let has_open_waits = frontier.iter().any(|p| matches!(p, Pending::Wait { .. }));
            if !events.is_empty() || !hooks.is_empty() || (has_open_waits && !self.inbox_checked) {
                self.inbox_checked = true;
                self.commit_running(events, hooks, &frontier).await?;
                continue;
            }

            // 2. Fire due timers and wait timeouts.
            let due: Vec<EventKind> = frontier
                .iter()
                .filter_map(|p| match p {
                    Pending::Sleep {
                        key,
                        fire_at: Some(t),
                        ..
                    } if *t <= now => Some(EventKind::TimerFired { key: key.clone() }),
                    Pending::Wait {
                        key,
                        timeout_at: Some(t),
                        ..
                    } if *t <= now => Some(EventKind::WaitTimedOut { key: key.clone() }),
                    _ => None,
                })
                .collect();
            if !due.is_empty() {
                self.commit_running(due, vec![], &frontier).await?;
                continue;
            }

            // 3. Start steps that are ready.
            let mut needs_rebuild = false;
            for p in &frontier {
                let Pending::Step {
                    key,
                    attempts,
                    retry_at,
                    timeout,
                    has_start,
                    ..
                } = p
                else {
                    continue;
                };
                if self.running.contains_key(key) || retry_at.is_some_and(|t| t > now) {
                    continue;
                }
                if !has_start {
                    // The FnOnce closure was spent by an earlier attempt.
                    needs_rebuild = true;
                    break;
                }
                self.launch(key, attempts + 1, *timeout);
            }
            if needs_rebuild {
                self.rebuild().await;
                continue;
            }

            self.publish_steps().await;

            // 4. Decide whether to wait here or suspend.
            let next_wake = self.next_wake(&frontier);
            if self.running.is_empty() {
                let inline = next_wake.is_some_and(|t| {
                    (t - now)
                        .to_std()
                        .is_ok_and(|d| d <= self.env.inline_threshold)
                });
                if !inline {
                    if let Some(outcome) = self.suspend(&frontier, next_wake).await? {
                        return Ok(outcome);
                    }
                    continue;
                }
            }

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

    fn registrations(
        &self,
        frontier: &[Pending],
        now: DateTime<Utc>,
    ) -> (Vec<EventKind>, Vec<HookReg>) {
        let (mut events, mut hooks) = (vec![], vec![]);
        for p in frontier {
            match p {
                Pending::Sleep {
                    key,
                    dur,
                    fire_at: None,
                } => events.push(EventKind::TimerScheduled {
                    key: key.clone(),
                    fire_at: now
                        + chrono::Duration::from_std(*dur).unwrap_or(chrono::Duration::MAX),
                }),
                Pending::Wait {
                    key,
                    kind,
                    name,
                    filter,
                    timeout,
                    started: false,
                    ..
                } => events.push(EventKind::WaitStarted {
                    key: key.clone(),
                    kind: *kind,
                    name: name.clone(),
                    filter: filter.clone(),
                    timeout_at: timeout.map(|d| {
                        now + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)
                    }),
                }),
                Pending::Hook { key, token } => hooks.push(HookReg {
                    key: key.clone(),
                    token: token
                        .clone()
                        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                }),
                Pending::HookDispose { key, token } => events.push(EventKind::HookDisposed {
                    key: key.clone(),
                    token: token.clone(),
                }),
                _ => {}
            }
        }
        (events, hooks)
    }

    fn next_wake(&self, frontier: &[Pending]) -> Option<DateTime<Utc>> {
        let timers = frontier.iter().filter_map(|p| match p {
            Pending::Sleep { fire_at, .. } => *fire_at,
            Pending::Wait { timeout_at, .. } => *timeout_at,
            Pending::Step { key, retry_at, .. } if !self.running.contains_key(key) => *retry_at,
            _ => None,
        });
        timers.chain(self.run.deadline_at).min()
    }

    /// Wait registrations for every open wait, minus those closing in `events`.
    fn open_waits(
        &self,
        frontier: &[Pending],
        events: &[EventKind],
        now: DateTime<Utc>,
    ) -> Vec<WaitReg> {
        let closing: HashSet<&str> = events
            .iter()
            .filter_map(|e| match e {
                EventKind::WaitTimedOut { key } => Some(key.as_str()),
                _ => None,
            })
            .collect();
        frontier
            .iter()
            .filter_map(|p| match p {
                Pending::Wait {
                    key,
                    kind,
                    name,
                    filter,
                    timeout,
                    timeout_at,
                    ..
                } if !closing.contains(key.as_str()) => {
                    let timeout_at = timeout_at.or_else(|| {
                        timeout.map(|d| {
                            now + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)
                        })
                    });
                    Some(WaitReg {
                        key: key.clone(),
                        kind: *kind,
                        name: name.clone(),
                        filter: filter.clone(),
                        timeout_at,
                    })
                }
                _ => None,
            })
            .collect()
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
        let waits = self.open_waits(frontier, &events, at);
        let mut c = Commit::new(self.run.id, self.last_seq, at);
        c.awaiting = waits.iter().map(|w| w.name.clone()).collect();
        c.waits = waits;
        c.events = events;
        c.hooks = hooks;
        c.status = status;
        c.wake_at = wake_at;
        c
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
        let out = self.env.backend.store.commit(c).await?;
        applied.extend(out.generated.iter().cloned());
        self.last_seq = out.last_seq;
        self.cancel_requested |= out.cancel_requested;
        for ev in applied {
            if ev.kind.is_terminal() {
                continue;
            }
            self.raw.push(ev.clone());
            self.history.push(ev.clone());
            self.core.feed(&ev);
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
        let grace =
            chrono::Duration::from_std(self.env.lease_grace).unwrap_or(chrono::Duration::MAX);
        let c = self.build_commit(
            events,
            hooks,
            frontier,
            RunStatus::Running,
            Some(at + grace),
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
        let cancelled = Arc::new(AtomicBool::new(false));
        let heartbeat = self.env.heartbeat.clone();
        let info = StepInfo {
            attempt,
            idempotency_key: format!("{}:{key}", self.run.id),
            cancelled: cancelled.clone(),
            heartbeat: Arc::new(move || heartbeat.notify_one()),
            deps: self.env.deps.clone(),
        };
        let (done_tx, key_owned) = (self.done_tx.clone(), key.to_string());
        let mut aborts = vec![];

        // `result` resolves with the step outcome (or never, if aborted).
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<Value, StepError>>();
        match start {
            StepStart::Async(f) => {
                let task =
                    tokio::spawn(
                        async move { step::scope_async(info, async move { f().await }).await },
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
                self.env.pool.execute(Box::new(move || {
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
        let key = done.key;
        let mut result = done.result;
        if let Ok(v) = &result
            && v.to_string().len() > self.env.max_payload_bytes
        {
            result = Err(StepError::fatal(format!(
                "step output exceeds max_payload_bytes ({}); pass large data by reference",
                self.env.max_payload_bytes
            )));
        }
        let at = self.now();
        let event = match result {
            Ok(output) => EventKind::StepSucceeded { key, output },
            Err(e) => {
                let retryable = e.class != ErrorClass::Fatal;
                let retry = retryable && policy.allows_retry(done.attempt);
                let delay = match e.class {
                    ErrorClass::RetryAfter(d) => d,
                    _ => policy.delay_after(done.attempt),
                };
                let retry_at = retry.then(|| {
                    at + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX)
                });
                EventKind::StepFailed {
                    key,
                    attempt: done.attempt,
                    error: e.message,
                    retryable,
                    retry_at,
                }
            }
        };
        self.commit_running(vec![event], vec![], frontier).await?;
        Ok(())
    }

    async fn finish_commit(&mut self, kind: EventKind) -> Result<()> {
        let at = self.now();
        let mut c = Commit::new(self.run.id, self.last_seq, at);
        c.events = vec![kind];
        self.env.backend.store.commit(c).await?;
        let _ = self
            .env
            .backend
            .pubsub
            .publish(&channels::run_finished(self.run.id), b"finished".to_vec())
            .await;
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
        tracing::error!(run = %self.run.id, workflow = %self.run.workflow, "engine fault: {msg}");
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
        let retry_at = at + chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::MAX);
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
        self.env.backend.store.commit(c).await?;
        Ok(Outcome::Fault { retry_at })
    }
}
