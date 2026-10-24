//! Driver-agnostic replay state for one execution of one run.
//!
//! Workflow code issues *commands* (steps, timers, waits, hooks) through the
//! contexts. History events resolve them. Whatever is still unresolved after
//! replay is the *frontier* the engine has to act on.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use chrono::{DateTime, Utc};
use durable_core::{Event, EventKind, RunId, WaitKind};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::{Deps, ErrorClass, RetryPolicy, StepError, WorkflowError};

/// Characters generated keys are built from: `name#1`, `name[0]`, `key[recv]`.
const RESERVED: &[char] = &['#', '[', ']'];

pub(crate) type StepFut = BoxFuture<'static, Result<Value, StepError>>;

pub(crate) enum StepStart {
    /// Runs on tokio.
    Async(Box<dyn FnOnce() -> StepFut + Send>),
    /// Runs on the compute pool.
    Blocking(Box<dyn FnOnce() -> Result<Value, StepError> + Send>),
}

pub(crate) struct StepSpec {
    pub start: Option<StepStart>,
    pub policy: RetryPolicy,
    pub timeout: Option<Duration>,
    /// Failed attempts recorded in history.
    pub attempts: u32,
    pub retry_at: Option<DateTime<Utc>>,
}

pub(crate) enum CmdKind {
    Step(StepSpec),
    Sleep {
        dur: Duration,
        fire_at: Option<DateTime<Utc>>,
    },
    Wait {
        kind: WaitKind,
        name: String,
        filter: Option<Value>,
        timeout: Option<Duration>,
        started: bool,
        timeout_at: Option<DateTime<Utc>>,
    },
    Hook {
        token: Option<String>,
    },
    HookDispose {
        token: String,
    },
}

impl CmdKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Step(_) => "step",
            Self::Sleep { .. } => "sleep",
            Self::Wait { .. } => "wait",
            Self::Hook { .. } => "hook",
            Self::HookDispose { .. } => "hook dispose",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Resolution {
    Step(Result<Value, StepError>),
    Slept,
    /// `None` means the wait timed out.
    Signal(Option<Value>),
    Hook {
        token: String,
        conflict: bool,
    },
    Disposed,
}

struct Cmd {
    order: usize,
    kind: CmdKind,
    resolved: Option<Resolution>,
    /// The workflow future that issued it was dropped (e.g. a `select!` loser).
    dropped: bool,
    waker: Option<Waker>,
}

/// An unresolved command, as the engine sees it.
#[derive(Debug, Clone)]
pub(crate) enum Pending {
    Step {
        key: String,
        attempts: u32,
        retry_at: Option<DateTime<Utc>>,
        policy: RetryPolicy,
        timeout: Option<Duration>,
        has_start: bool,
    },
    Sleep {
        key: String,
        dur: Duration,
        fire_at: Option<DateTime<Utc>>,
    },
    Wait {
        key: String,
        kind: WaitKind,
        name: String,
        filter: Option<Value>,
        timeout: Option<Duration>,
        started: bool,
        timeout_at: Option<DateTime<Utc>>,
    },
    Hook {
        key: String,
        token: Option<String>,
    },
    HookDispose {
        key: String,
        token: String,
    },
}

impl Pending {
    pub fn key(&self) -> &str {
        match self {
            Self::Step { key, .. }
            | Self::Sleep { key, .. }
            | Self::Wait { key, .. }
            | Self::Hook { key, .. }
            | Self::HookDispose { key, .. } => key,
        }
    }
}

struct Inner {
    occurrences: HashMap<String, u32>,
    cmds: HashMap<String, Cmd>,
    next_order: usize,
    now: DateTime<Utc>,
    rng: ChaCha8Rng,
    last_uuid: uuid::Uuid,
    interrupted: bool,
    cancelled: bool,
    fault: Option<String>,
}

pub(crate) struct Core {
    pub run_id: RunId,
    pub deps: Deps,
    /// True while the engine feeds history to the workflow.
    replaying: AtomicBool,
    inner: Mutex<Inner>,
}

impl Core {
    pub fn new(run_id: RunId, deps: Deps) -> Arc<Self> {
        let mut seed = [0u8; 32];
        seed[..16].copy_from_slice(run_id.as_bytes());
        seed[16..].copy_from_slice(run_id.as_bytes());
        Arc::new(Self {
            run_id,
            deps,
            replaying: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                occurrences: HashMap::new(),
                cmds: HashMap::new(),
                next_order: 0,
                now: DateTime::<Utc>::MIN_UTC,
                rng: ChaCha8Rng::from_seed(seed),
                last_uuid: uuid::Uuid::nil(),
                interrupted: false,
                cancelled: false,
                fault: None,
            }),
        })
    }

    pub fn set_replaying(&self, replaying: bool) {
        self.replaying.store(replaying, Ordering::SeqCst);
    }

    pub fn is_replaying(&self) -> bool {
        self.replaying.load(Ordering::SeqCst)
    }

    /// Key for a name chosen by workflow code. Names may not contain the
    /// characters generated keys are built from, or two commands could share
    /// a key; such a name is a code bug and faults the run.
    pub fn user_key(&self, name: &str) -> String {
        self.check_name(name);
        self.key_for(name)
    }

    /// Faults the run if `name` is not a valid user-chosen name.
    pub fn check_name(&self, name: &str) {
        if name.is_empty() || name.contains(RESERVED) {
            self.set_fault(format!(
                "invalid name `{name}`: names must be non-empty and must not contain `#`, `[` or `]`"
            ));
        }
    }

    /// `name`, then `name#1`, `name#2`, ... in call order. Engine-generated
    /// names (`name[0]`, `key[recv]`) go here directly.
    pub fn key_for(&self, name: &str) -> String {
        let mut g = self.inner.lock();
        let n = g.occurrences.entry(name.to_string()).or_insert(0);
        let key = if *n == 0 {
            name.to_string()
        } else {
            format!("{name}#{n}")
        };
        *n += 1;
        key
    }

    /// Timestamp of the latest event consumed: deterministic, zero writes.
    pub fn now(&self) -> DateTime<Utc> {
        self.inner.lock().now
    }

    pub fn random_u64(&self) -> u64 {
        self.inner.lock().rng.next_u64()
    }

    /// A UUIDv7 stamped with [`Self::now`] and filled from the run's seeded
    /// RNG. `now` stands still between events, so ids are kept strictly
    /// increasing within a run by bumping the previous one when needed.
    pub fn uuid(&self) -> uuid::Uuid {
        let mut g = self.inner.lock();
        let millis = u64::try_from(g.now.timestamp_millis()).unwrap_or(0);
        let mut rand = [0u8; 10];
        g.rng.fill_bytes(&mut rand);
        let mut id = uuid::Builder::from_unix_timestamp_millis(millis, &rand).into_uuid();
        if id <= g.last_uuid {
            id = uuid::Uuid::from_u128(g.last_uuid.as_u128() + 1);
        }
        g.last_uuid = id;
        id
    }

    pub fn fault(&self) -> Option<String> {
        self.inner.lock().fault.clone()
    }

    pub fn set_fault(&self, msg: String) {
        self.inner.lock().fault.get_or_insert(msg);
    }

    /// Fail every pending and future ctx call with `Interrupted`.
    pub fn interrupt(&self) {
        let wakers = {
            let mut g = self.inner.lock();
            g.interrupted = true;
            Self::take_wakers(&mut g)
        };
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Fail every pending and future ctx call with `Cancelled`.
    pub fn cancel(&self) {
        let wakers = {
            let mut g = self.inner.lock();
            g.cancelled = true;
            Self::take_wakers(&mut g)
        };
        wakers.into_iter().for_each(Waker::wake);
    }

    fn take_wakers(g: &mut Inner) -> Vec<Waker> {
        g.cmds.values_mut().filter_map(|c| c.waker.take()).collect()
    }

    pub fn take_start(&self, key: &str) -> Option<StepStart> {
        match &mut self.inner.lock().cmds.get_mut(key)?.kind {
            CmdKind::Step(s) => s.start.take(),
            _ => None,
        }
    }

    pub fn frontier(&self) -> Vec<Pending> {
        let g = self.inner.lock();
        let mut cmds: Vec<_> = g
            .cmds
            .iter()
            .filter(|(_, c)| c.resolved.is_none() && !c.dropped)
            .collect();
        cmds.sort_by_key(|(_, c)| c.order);
        cmds.into_iter()
            .map(|(key, c)| {
                let key = key.clone();
                match &c.kind {
                    CmdKind::Step(s) => Pending::Step {
                        key,
                        attempts: s.attempts,
                        retry_at: s.retry_at,
                        policy: s.policy,
                        timeout: s.timeout,
                        has_start: s.start.is_some(),
                    },
                    CmdKind::Sleep { dur, fire_at } => Pending::Sleep {
                        key,
                        dur: *dur,
                        fire_at: *fire_at,
                    },
                    CmdKind::Wait {
                        kind,
                        name,
                        filter,
                        timeout,
                        started,
                        timeout_at,
                    } => Pending::Wait {
                        key,
                        kind: *kind,
                        name: name.clone(),
                        filter: filter.clone(),
                        timeout: *timeout,
                        started: *started,
                        timeout_at: *timeout_at,
                    },
                    CmdKind::Hook { token } => Pending::Hook {
                        key,
                        token: token.clone(),
                    },
                    CmdKind::HookDispose { token } => Pending::HookDispose {
                        key,
                        token: token.clone(),
                    },
                }
            })
            .collect()
    }

    /// Apply one history event. Divergence from the code is recorded as a fault.
    pub fn feed(&self, ev: &Event) {
        let wakers = {
            let mut g = self.inner.lock();
            g.now = ev.at;
            match Self::apply(&mut g, &ev.kind) {
                Ok(woken) => woken,
                Err(msg) => {
                    g.fault.get_or_insert(msg);
                    None
                }
            }
        };
        if let Some(w) = wakers {
            w.wake();
        }
    }

    fn apply(g: &mut Inner, kind: &EventKind) -> Result<Option<Waker>, String> {
        use EventKind::*;
        let Some(key) = kind.key() else {
            return Ok(None);
        };
        let cmd = g.cmds.get_mut(key).ok_or_else(|| {
            format!(
                "non-determinism: history has an event for `{key}` but the workflow did not \
                 issue it (was a step removed or renamed?)"
            )
        })?;
        let mismatch = |want: &str, cmd: &Cmd| {
            format!(
                "non-determinism: history says `{key}` was a {want}, but the workflow issued a {}",
                cmd.kind.label()
            )
        };
        let mut resolve = None;
        match (kind, &mut cmd.kind) {
            (StepSucceeded { output, .. }, CmdKind::Step(_)) => {
                resolve = Some(Resolution::Step(Ok(output.clone())));
            }
            (
                StepFailed {
                    attempt,
                    error,
                    retryable,
                    retry_at,
                    ..
                },
                CmdKind::Step(s),
            ) => match retry_at {
                Some(at) => {
                    s.attempts = *attempt;
                    s.retry_at = Some(*at);
                }
                None => {
                    let class = if *retryable {
                        ErrorClass::Retryable
                    } else {
                        ErrorClass::Fatal
                    };
                    resolve = Some(Resolution::Step(Err(StepError {
                        message: error.clone(),
                        class,
                    })));
                }
            },
            (StepSucceeded { .. } | StepFailed { .. }, _) => return Err(mismatch("step", cmd)),
            (TimerScheduled { fire_at, .. }, CmdKind::Sleep { fire_at: f, .. }) => {
                *f = Some(*fire_at);
            }
            (TimerFired { .. }, CmdKind::Sleep { .. }) => resolve = Some(Resolution::Slept),
            (TimerScheduled { .. } | TimerFired { .. }, _) => return Err(mismatch("sleep", cmd)),
            (
                WaitStarted {
                    kind: k,
                    timeout_at: t,
                    ..
                },
                CmdKind::Wait {
                    kind: want,
                    started,
                    timeout_at,
                    ..
                },
            ) if k == want => {
                *started = true;
                *timeout_at = *t;
            }
            (SignalReceived { payload, .. }, CmdKind::Wait { .. }) => {
                resolve = Some(Resolution::Signal(Some(payload.clone())));
            }
            (WaitTimedOut { .. }, CmdKind::Wait { .. }) => resolve = Some(Resolution::Signal(None)),
            (WaitStarted { .. } | SignalReceived { .. } | WaitTimedOut { .. }, _) => {
                return Err(mismatch("wait", cmd));
            }
            (
                HookCreated {
                    token, conflict, ..
                },
                CmdKind::Hook { .. },
            ) => {
                resolve = Some(Resolution::Hook {
                    token: token.clone(),
                    conflict: *conflict,
                });
            }
            (HookCreated { .. }, _) => return Err(mismatch("hook", cmd)),
            (HookDisposed { .. }, CmdKind::HookDispose { .. }) => {
                resolve = Some(Resolution::Disposed);
            }
            (HookDisposed { .. }, _) => return Err(mismatch("hook dispose", cmd)),
            _ => {}
        }
        if let Some(r) = resolve {
            cmd.resolved = Some(r);
            return Ok(cmd.waker.take());
        }
        Ok(None)
    }
}

/// The future behind every ctx call. Registers its command on first poll.
pub(crate) struct CmdFuture {
    core: Arc<Core>,
    key: String,
    pending: Option<CmdKind>,
    finished: bool,
}

impl CmdFuture {
    pub fn new(core: Arc<Core>, key: String, kind: CmdKind) -> Self {
        Self {
            core,
            key,
            pending: Some(kind),
            finished: false,
        }
    }
}

impl Future for CmdFuture {
    type Output = Result<Resolution, WorkflowError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut g = this.core.inner.lock();
        if g.interrupted {
            this.finished = true;
            return Poll::Ready(Err(WorkflowError::Interrupted));
        }
        if g.cancelled {
            this.finished = true;
            return Poll::Ready(Err(WorkflowError::Cancelled));
        }
        if let Some(msg) = &g.fault {
            this.finished = true;
            return Poll::Ready(Err(WorkflowError::Fault(msg.clone())));
        }
        if let Some(kind) = this.pending.take() {
            if g.cmds.contains_key(&this.key) {
                // Keys are unique by construction; never let a second command
                // silently take over the first one's resolution.
                let msg = format!("two commands share the key `{}`", this.key);
                g.fault.get_or_insert(msg.clone());
                this.finished = true;
                return Poll::Ready(Err(WorkflowError::Fault(msg)));
            }
            let order = g.next_order;
            g.next_order += 1;
            g.cmds.insert(
                this.key.clone(),
                Cmd {
                    order,
                    kind,
                    resolved: None,
                    dropped: false,
                    waker: None,
                },
            );
        }
        let cmd = g.cmds.get_mut(&this.key).expect("command registered");
        if let Some(r) = &cmd.resolved {
            let r = r.clone();
            this.finished = true;
            return Poll::Ready(Ok(r));
        }
        cmd.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for CmdFuture {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(cmd) = self.core.inner.lock().cmds.get_mut(&self.key) {
            cmd.dropped = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker_ref;

    use super::*;

    fn sleep_cmd(core: &Arc<Core>, key: &str) -> CmdFuture {
        let kind = CmdKind::Sleep {
            dur: Duration::from_secs(1),
            fire_at: None,
        };
        CmdFuture::new(core.clone(), key.into(), kind)
    }

    #[test]
    fn a_second_command_with_a_live_key_faults_instead_of_replacing_it() {
        let core = Core::new(RunId::nil(), Deps::default());
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut first = sleep_cmd(&core, "k");
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        let mut second = sleep_cmd(&core, "k");
        assert!(matches!(
            Pin::new(&mut second).poll(&mut cx),
            Poll::Ready(Err(WorkflowError::Fault(_)))
        ));
        assert!(core.fault().unwrap().contains("share the key `k`"));
    }

    #[test]
    fn uuids_are_v7_stamped_with_now_and_strictly_increasing() {
        let core = Core::new(RunId::nil(), Deps::default());
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        core.inner.lock().now = at;
        let ids: Vec<_> = (0..100).map(|_| core.uuid()).collect();
        for id in &ids {
            assert_eq!(id.get_version(), Some(uuid::Version::SortRand));
            let (secs, nanos) = id.get_timestamp().unwrap().to_unix();
            assert_eq!((secs, nanos / 1_000_000), (1_700_000_000, 123));
        }
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn reserved_characters_are_rejected_in_user_names() {
        for bad in ["", "a#1", "x[0]", "h]"] {
            let core = Core::new(RunId::nil(), Deps::default());
            core.user_key(bad);
            assert!(core.fault().is_some(), "`{bad}` was accepted");
        }
        let core = Core::new(RunId::nil(), Deps::default());
        assert_eq!(core.user_key("charge:card.v2"), "charge:card.v2");
        assert_eq!(core.fault(), None);
    }
}
