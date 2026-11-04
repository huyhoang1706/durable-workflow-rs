//! Replay state for one execution of one run.
//!
//! Workflow code issues *commands* (steps, timers, waits, hooks). History
//! events resolve them. Whatever is still unresolved once history is
//! exhausted is the *frontier* the engine has to act on.
//!
//! The replayer never runs user code and never waits. A host (the Rust
//! runtime, or a language binding) runs the code, and alternates:
//!
//! ```text
//! loop {
//!     match replayer.advance() {
//!         Advance::RunCode => { settle(replayer.take_resolved()); run code until it is quiescent }
//!         Advance::Live    => break,
//!     }
//! }
//! ```

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use rand_chacha::{
    ChaCha8Rng,
    rand_core::{RngCore, SeedableRng},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Event, EventKind, RetryPolicy, RunId, WaitKind};

/// Characters generated keys are built from: `name#1`, `name[0]`, `key[recv]`.
const RESERVED: &[char] = &['#', '[', ']'];

/// Identifies one issued command within one execution. Hosts map it to
/// whatever their code awaits (a waker, a Promise, an asyncio future).
/// Unlike a key, it means nothing outside this `Replayer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Handle(pub u32);

/// What workflow code asks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// The body stays with the host; the replayer only knows its options.
    Step {
        policy: RetryPolicy,
        timeout: Option<Duration>,
    },
    Sleep {
        dur: Duration,
    },
    Wait {
        kind: WaitKind,
        name: String,
        filter: Option<Value>,
        timeout: Option<Duration>,
    },
    Hook {
        token: Option<String>,
    },
    HookDispose {
        token: String,
    },
}

impl Command {
    fn label(&self) -> &'static str {
        match self {
            Self::Step { .. } => "step",
            Self::Sleep { .. } => "sleep",
            Self::Wait { .. } => "wait",
            Self::Hook { .. } => "hook",
            Self::HookDispose { .. } => "hook dispose",
        }
    }
}

/// A step's final failure, as recorded in history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepFailure {
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Resolution {
    Step {
        result: Result<Value, StepFailure>,
    },
    Slept,
    /// `payload: None` means the wait timed out.
    Signal {
        payload: Option<Value>,
    },
    Hook {
        token: String,
        conflict: bool,
    },
    Disposed,
}

/// Why every pending and future command fails. Checked before resolutions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "message", rename_all = "snake_case")]
pub enum Abort {
    /// The engine is rebuilding or suspending the execution.
    Interrupted,
    Cancelled,
    /// Non-determinism, an invalid name, a duplicate key, ...: the run needs a code fix.
    Fault(String),
}

/// What the host should do after [`Replayer::advance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Advance {
    /// History was applied (or the code has never run). Settle
    /// [`Replayer::take_resolved`], run the code until it is quiescent, then
    /// call `advance` again.
    RunCode,
    /// Every event has been delivered and the code has seen it. The engine
    /// now acts on [`Replayer::frontier`].
    Live,
}

/// An unresolved command, as the engine sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Pending {
    Step {
        key: String,
        /// Failed attempts recorded in history.
        attempts: u32,
        retry_at: Option<DateTime<Utc>>,
        policy: RetryPolicy,
        timeout: Option<Duration>,
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

/// What history has told us about a command so far.
#[derive(Debug, Default)]
struct Progress {
    /// Step: failed attempts, and when the next one is due.
    attempts: u32,
    retry_at: Option<DateTime<Utc>>,
    /// Sleep: `TimerScheduled`.
    fire_at: Option<DateTime<Utc>>,
    /// Wait: `WaitStarted`.
    started: bool,
    timeout_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct Cmd {
    key: String,
    command: Command,
    progress: Progress,
    resolved: Option<Resolution>,
    /// The code stopped awaiting it (e.g. a `select!` loser).
    released: bool,
    /// Issued under a key another command already holds; never resolves.
    rejected: bool,
}

#[derive(Debug)]
pub struct Replayer {
    run_id: RunId,
    history: Vec<Event>,
    /// Next event of `history` to apply.
    cursor: usize,
    /// Events before this index were already seen by an earlier execution.
    replay_end: usize,
    /// Events were applied since the code last ran (or it never ran).
    need_run: bool,
    /// Indexed by `Handle`; issue order is frontier order.
    cmds: Vec<Cmd>,
    by_key: HashMap<String, Handle>,
    occurrences: HashMap<String, u32>,
    resolved: Vec<(Handle, Resolution)>,
    now: DateTime<Utc>,
    rng: ChaCha8Rng,
    last_uuid: uuid::Uuid,
    interrupted: bool,
    cancelled: bool,
    fault: Option<String>,
}

impl Replayer {
    /// `history` must already be normalized for replay (see `durable_runtime::history::normalize`).
    pub fn new(run_id: RunId, history: Vec<Event>) -> Self {
        let mut seed = [0u8; 32];
        seed[..16].copy_from_slice(run_id.as_bytes());
        seed[16..].copy_from_slice(run_id.as_bytes());
        Self {
            run_id,
            replay_end: history.len(),
            history,
            cursor: 0,
            need_run: true,
            cmds: Vec::new(),
            by_key: HashMap::new(),
            occurrences: HashMap::new(),
            resolved: Vec::new(),
            now: DateTime::<Utc>::MIN_UTC,
            rng: ChaCha8Rng::from_seed(seed),
            last_uuid: uuid::Uuid::nil(),
            interrupted: false,
            cancelled: false,
            fault: None,
        }
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    // ---- keys -------------------------------------------------------------

    /// Key for a name chosen by workflow code. Names may not contain the
    /// characters generated keys are built from, or two commands could share
    /// a key; such a name is a code bug and faults the run.
    pub fn user_key(&mut self, name: &str) -> String {
        self.check_name(name);
        self.key_for(name)
    }

    /// Faults the run if `name` is not a valid user-chosen name.
    pub fn check_name(&mut self, name: &str) {
        if name.is_empty() || name.contains(RESERVED) {
            self.set_fault(format!(
                "invalid name `{name}`: names must be non-empty and must not contain `#`, `[` or `]`"
            ));
        }
    }

    /// `name`, then `name#1`, `name#2`, ... in call order. Engine-generated
    /// names (`name[0]`, `key[recv]`) go here directly.
    pub fn key_for(&mut self, name: &str) -> String {
        let n = self.occurrences.entry(name.to_string()).or_insert(0);
        let key = if *n == 0 {
            name.to_string()
        } else {
            format!("{name}#{n}")
        };
        *n += 1;
        key
    }

    // ---- commands ---------------------------------------------------------

    /// Register a command under `key` (from [`Self::user_key`] or
    /// [`Self::key_for`]). A key that is already taken faults the run; the
    /// returned handle then never resolves.
    pub fn issue(&mut self, key: String, command: Command) -> Handle {
        let handle = Handle(u32::try_from(self.cmds.len()).expect("fewer than 2^32 commands"));
        let rejected = self.by_key.contains_key(&key);
        if rejected {
            // Keys are unique by construction; never let a second command
            // silently take over the first one's resolution.
            self.set_fault(format!("two commands share the key `{key}`"));
        } else {
            self.by_key.insert(key.clone(), handle);
        }
        self.cmds.push(Cmd {
            key,
            command,
            progress: Progress::default(),
            resolved: None,
            released: false,
            rejected,
        });
        handle
    }

    pub fn is_issued(&self, key: &str) -> bool {
        self.by_key.contains_key(key)
    }

    pub fn key_of(&self, h: Handle) -> Option<&str> {
        self.cmds.get(h.0 as usize).map(|c| c.key.as_str())
    }

    /// The code no longer awaits `h`. It leaves the frontier; history for it
    /// is still accepted.
    pub fn release(&mut self, h: Handle) {
        if let Some(c) = self.cmds.get_mut(h.0 as usize) {
            c.released = true;
        }
    }

    pub fn resolution(&self, h: Handle) -> Option<&Resolution> {
        self.cmds.get(h.0 as usize)?.resolved.as_ref()
    }

    /// Resolutions produced since the last call, in history order.
    pub fn take_resolved(&mut self) -> Vec<(Handle, Resolution)> {
        std::mem::take(&mut self.resolved)
    }

    // ---- history ----------------------------------------------------------

    /// Apply history up to and including the next event that resolves a
    /// command, or up to an event whose command the code has not issued yet.
    ///
    /// Call it again after every [`Advance::RunCode`], once the code is
    /// quiescent. Stopping after each resolution keeps replay identical to
    /// the live execution that wrote the history: the code sees one outcome
    /// at a time, so a race (`select!`) picks the same winner.
    pub fn advance(&mut self) -> Advance {
        while self.cursor < self.history.len() {
            let unissued = self.history[self.cursor]
                .kind
                .key()
                .is_some_and(|k| !self.by_key.contains_key(k));
            if unissued && self.need_run {
                // The code may yet issue it.
                self.need_run = false;
                return Advance::RunCode;
            }
            let ev = self.history[self.cursor].clone();
            self.cursor += 1;
            self.need_run = true;
            if self.apply(&ev) {
                self.need_run = false;
                return Advance::RunCode;
            }
        }
        if self.need_run {
            self.need_run = false;
            return Advance::RunCode;
        }
        Advance::Live
    }

    /// Append events the engine just committed. Deliver them with
    /// [`Self::advance`].
    pub fn push_live(&mut self, events: impl IntoIterator<Item = Event>) {
        self.history.extend(events);
    }

    /// True while the code runs on events an earlier execution already saw.
    pub fn is_replaying(&self) -> bool {
        self.cursor < self.replay_end
    }

    /// Apply one event. Returns whether it resolved a command (or faulted).
    fn apply(&mut self, ev: &Event) -> bool {
        self.now = ev.at;
        match self.try_apply(&ev.kind) {
            Ok(Some((h, r))) => {
                self.cmds[h.0 as usize].resolved = Some(r.clone());
                self.resolved.push((h, r));
                true
            }
            Ok(None) => false,
            Err(msg) => {
                self.set_fault(msg);
                true
            }
        }
    }

    fn try_apply(&mut self, kind: &EventKind) -> Result<Option<(Handle, Resolution)>, String> {
        use EventKind::*;
        let Some(key) = kind.key() else {
            return Ok(None);
        };
        let h = *self.by_key.get(key).ok_or_else(|| {
            format!(
                "non-determinism: history has an event for `{key}` but the workflow did not \
                 issue it (was a step removed or renamed?)"
            )
        })?;
        let cmd = &mut self.cmds[h.0 as usize];
        let mismatch = |want: &str, cmd: &Cmd| {
            format!(
                "non-determinism: history says `{key}` was a {want}, but the workflow issued a {}",
                cmd.command.label()
            )
        };
        let p = &mut cmd.progress;
        let resolution = match (kind, &cmd.command) {
            (StepSucceeded { output, .. }, Command::Step { .. }) => Some(Resolution::Step {
                result: Ok(output.clone()),
            }),
            (
                StepFailed {
                    attempt,
                    error,
                    retryable,
                    retry_at,
                    ..
                },
                Command::Step { .. },
            ) => match retry_at {
                Some(at) => {
                    p.attempts = *attempt;
                    p.retry_at = Some(*at);
                    None
                }
                None => Some(Resolution::Step {
                    result: Err(StepFailure {
                        message: error.clone(),
                        retryable: *retryable,
                    }),
                }),
            },
            (StepSucceeded { .. } | StepFailed { .. }, _) => return Err(mismatch("step", cmd)),
            (TimerScheduled { fire_at, .. }, Command::Sleep { .. }) => {
                p.fire_at = Some(*fire_at);
                None
            }
            (TimerFired { .. }, Command::Sleep { .. }) => Some(Resolution::Slept),
            (TimerScheduled { .. } | TimerFired { .. }, _) => return Err(mismatch("sleep", cmd)),
            (
                WaitStarted {
                    kind: k,
                    timeout_at,
                    ..
                },
                Command::Wait { kind: want, .. },
            ) if k == want => {
                p.started = true;
                p.timeout_at = *timeout_at;
                None
            }
            (SignalReceived { payload, .. }, Command::Wait { .. }) => Some(Resolution::Signal {
                payload: Some(payload.clone()),
            }),
            (WaitTimedOut { .. }, Command::Wait { .. }) => {
                Some(Resolution::Signal { payload: None })
            }
            (WaitStarted { .. } | SignalReceived { .. } | WaitTimedOut { .. }, _) => {
                return Err(mismatch("wait", cmd));
            }
            (
                HookCreated {
                    token, conflict, ..
                },
                Command::Hook { .. },
            ) => Some(Resolution::Hook {
                token: token.clone(),
                conflict: *conflict,
            }),
            (HookCreated { .. }, _) => return Err(mismatch("hook", cmd)),
            (HookDisposed { .. }, Command::HookDispose { .. }) => Some(Resolution::Disposed),
            (HookDisposed { .. }, _) => return Err(mismatch("hook dispose", cmd)),
            _ => None,
        };
        Ok(resolution.map(|r| (h, r)))
    }

    // ---- the engine's view ------------------------------------------------

    /// Unresolved commands the code still awaits, in issue order.
    pub fn frontier(&self) -> Vec<Pending> {
        self.cmds
            .iter()
            .filter(|c| c.resolved.is_none() && !c.released && !c.rejected)
            .map(|c| {
                let key = c.key.clone();
                let p = &c.progress;
                match &c.command {
                    Command::Step { policy, timeout } => Pending::Step {
                        key,
                        attempts: p.attempts,
                        retry_at: p.retry_at,
                        policy: *policy,
                        timeout: *timeout,
                    },
                    Command::Sleep { dur } => Pending::Sleep {
                        key,
                        dur: *dur,
                        fire_at: p.fire_at,
                    },
                    Command::Wait {
                        kind,
                        name,
                        filter,
                        timeout,
                    } => Pending::Wait {
                        key,
                        kind: *kind,
                        name: name.clone(),
                        filter: filter.clone(),
                        timeout: *timeout,
                        started: p.started,
                        timeout_at: p.timeout_at,
                    },
                    Command::Hook { token } => Pending::Hook {
                        key,
                        token: token.clone(),
                    },
                    Command::HookDispose { token } => Pending::HookDispose {
                        key,
                        token: token.clone(),
                    },
                }
            })
            .collect()
    }

    // ---- deterministic environment ----------------------------------------

    /// Timestamp of the latest event applied: deterministic, zero writes.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }

    pub fn random_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// A UUIDv7 stamped with [`Self::now`] and filled from the run's seeded
    /// RNG. `now` stands still between events, so ids are kept strictly
    /// increasing within a run by bumping the previous one when needed.
    pub fn uuid(&mut self) -> uuid::Uuid {
        let millis = u64::try_from(self.now.timestamp_millis()).unwrap_or(0);
        let mut rand = [0u8; 10];
        self.rng.fill_bytes(&mut rand);
        let mut id = uuid::Builder::from_unix_timestamp_millis(millis, &rand).into_uuid();
        if id <= self.last_uuid {
            id = uuid::Uuid::from_u128(self.last_uuid.as_u128() + 1);
        }
        self.last_uuid = id;
        id
    }

    // ---- aborting ---------------------------------------------------------

    pub fn fault(&self) -> Option<&str> {
        self.fault.as_deref()
    }

    /// The first fault wins.
    pub fn set_fault(&mut self, msg: String) {
        self.fault.get_or_insert(msg);
    }

    /// Fail every pending and future command with [`Abort::Interrupted`].
    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    /// Fail every pending and future command with [`Abort::Cancelled`].
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn abort(&self) -> Option<Abort> {
        if self.interrupted {
            Some(Abort::Interrupted)
        } else if self.cancelled {
            Some(Abort::Cancelled)
        } else {
            self.fault.clone().map(Abort::Fault)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn at(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap()
    }

    fn log(kinds: Vec<EventKind>) -> Vec<Event> {
        kinds
            .into_iter()
            .enumerate()
            .map(|(i, kind)| Event {
                run_id: RunId::nil(),
                seq: i as u64 + 1,
                at: at(i as i64),
                kind,
            })
            .collect()
    }

    fn step() -> Command {
        Command::Step {
            policy: RetryPolicy::default(),
            timeout: None,
        }
    }

    fn ok(key: &str, v: Value) -> EventKind {
        EventKind::StepSucceeded {
            key: key.into(),
            output: v,
        }
    }

    fn sleep() -> Command {
        Command::Sleep {
            dur: Duration::from_secs(1),
        }
    }

    #[test]
    fn a_second_command_with_a_live_key_faults_instead_of_replacing_it() {
        let mut r = Replayer::new(RunId::nil(), vec![]);
        let first = r.issue("k".into(), sleep());
        let second = r.issue("k".into(), sleep());
        assert_ne!(first, second);
        assert!(r.fault().unwrap().contains("share the key `k`"));
        assert_eq!(r.frontier().len(), 1, "the rejected command is not pending");
        assert_eq!(r.abort(), Some(Abort::Fault(r.fault().unwrap().into())));
    }

    #[test]
    fn uuids_are_v7_stamped_with_now_and_strictly_increasing() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![EventKind::RunStarted { input: json!(1) }]),
        );
        assert_eq!(r.advance(), Advance::RunCode);
        let ids: Vec<_> = (0..100).map(|_| r.uuid()).collect();
        for id in &ids {
            assert_eq!(id.get_version(), Some(uuid::Version::SortRand));
            let (secs, nanos) = id.get_timestamp().unwrap().to_unix();
            assert_eq!((secs, nanos / 1_000_000), (1_700_000_000, 0));
        }
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn reserved_characters_are_rejected_in_user_names() {
        for bad in ["", "a#1", "x[0]", "h]"] {
            let mut r = Replayer::new(RunId::nil(), vec![]);
            r.user_key(bad);
            assert!(r.fault().is_some(), "`{bad}` was accepted");
        }
        let mut r = Replayer::new(RunId::nil(), vec![]);
        assert_eq!(r.user_key("charge:card.v2"), "charge:card.v2");
        assert_eq!(r.fault(), None);
    }

    #[test]
    fn keys_count_occurrences_per_name() {
        let mut r = Replayer::new(RunId::nil(), vec![]);
        let keys: Vec<_> = ["a", "b", "a", "a"].map(|n| r.user_key(n)).into();
        assert_eq!(keys, ["a", "b", "a#1", "a#2"]);
    }

    #[test]
    fn an_empty_history_runs_the_code_once_then_goes_live() {
        let mut r = Replayer::new(RunId::nil(), vec![]);
        assert!(!r.is_replaying());
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(r.advance(), Advance::Live);
    }

    #[test]
    fn sequential_steps_replay_one_resolution_per_run() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                ok("a", json!(1)),
                ok("b", json!(2)),
            ]),
        );
        // RunStarted applied; "a" is not issued yet, so the code runs first.
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(r.now(), at(0));
        assert!(r.is_replaying());
        let a = r.issue("a".into(), step());

        assert_eq!(r.advance(), Advance::RunCode);
        let resolved = r.take_resolved();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, a);
        assert!(r.is_replaying(), "`b` is still to come");
        let b = r.issue("b".into(), step());

        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(
            r.resolution(b),
            Some(&Resolution::Step {
                result: Ok(json!(2))
            })
        );
        assert!(!r.is_replaying(), "the poll after the newest event is live");
        assert_eq!(r.advance(), Advance::Live);
        assert!(r.frontier().is_empty());
    }

    #[test]
    fn non_resolving_events_are_applied_without_running_the_code() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                EventKind::TimerScheduled {
                    key: "t".into(),
                    fire_at: at(60_000),
                },
            ]),
        );
        assert_eq!(r.advance(), Advance::RunCode);
        r.issue("t".into(), sleep());
        // TimerScheduled resolves nothing, but it is the newest event: the
        // code gets one live run after it.
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(r.advance(), Advance::Live);
        assert!(matches!(
            r.frontier()[..],
            [Pending::Sleep { fire_at: Some(t), .. }] if t == at(60_000)
        ));
    }

    #[test]
    fn races_see_one_outcome_at_a_time_in_history_order() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                EventKind::TimerFired { key: "t".into() },
                EventKind::SignalReceived {
                    key: "s".into(),
                    signal_id: "1".into(),
                    payload: json!("late"),
                },
            ]),
        );
        assert_eq!(r.advance(), Advance::RunCode);
        let t = r.issue("t".into(), sleep());
        let s = r.issue(
            "s".into(),
            Command::Wait {
                kind: WaitKind::Signal,
                name: "s".into(),
                filter: None,
                timeout: None,
            },
        );
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(r.take_resolved(), vec![(t, Resolution::Slept)]);
        // The timer won; the code drops the signal wait.
        r.release(s);
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(
            r.take_resolved().len(),
            1,
            "a released command still accepts history"
        );
        assert_eq!(r.advance(), Advance::Live);
        assert!(r.frontier().is_empty());
        assert_eq!(r.fault(), None);
    }

    #[test]
    fn an_event_the_code_never_issues_is_non_determinism() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                ok("renamed", json!(1)),
            ]),
        );
        assert_eq!(r.advance(), Advance::RunCode);
        r.issue("a".into(), step());
        assert_eq!(r.advance(), Advance::RunCode);
        assert!(r.fault().unwrap().contains("did not issue it"));
    }

    #[test]
    fn a_kind_mismatch_is_non_determinism() {
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                ok("a", json!(1)),
            ]),
        );
        r.advance();
        r.issue("a".into(), sleep());
        r.advance();
        assert!(
            r.fault()
                .unwrap()
                .contains("was a step, but the workflow issued a sleep")
        );
    }

    #[test]
    fn retries_update_the_frontier_and_a_final_failure_resolves() {
        let fail = |attempt, retry_at| EventKind::StepFailed {
            key: "a".into(),
            attempt,
            error: "boom".into(),
            retryable: true,
            retry_at,
        };
        let mut r = Replayer::new(
            RunId::nil(),
            log(vec![
                EventKind::RunStarted { input: json!(0) },
                fail(1, Some(at(5_000))),
            ]),
        );
        r.advance();
        let a = r.issue("a".into(), step());
        assert_eq!(r.advance(), Advance::RunCode);
        assert_eq!(r.advance(), Advance::Live);
        assert!(matches!(
            r.frontier()[..],
            [Pending::Step { attempts: 1, retry_at: Some(t), .. }] if t == at(5_000)
        ));

        r.push_live(log(vec![fail(2, None)]));
        assert_eq!(r.advance(), Advance::RunCode);
        assert!(!r.is_replaying(), "live events are not replays");
        assert_eq!(
            r.resolution(a),
            Some(&Resolution::Step {
                result: Err(StepFailure {
                    message: "boom".into(),
                    retryable: true
                })
            })
        );
    }

    #[test]
    fn interrupt_and_cancel_take_precedence_over_a_fault() {
        let mut r = Replayer::new(RunId::nil(), vec![]);
        r.set_fault("x".into());
        r.cancel();
        assert_eq!(r.abort(), Some(Abort::Cancelled));
        r.interrupt();
        assert_eq!(r.abort(), Some(Abort::Interrupted));
    }

    #[test]
    fn commands_and_resolutions_are_tagged_in_json() {
        let v = serde_json::to_value(Resolution::Signal { payload: None }).unwrap();
        assert_eq!(v, json!({"type": "signal", "payload": null}));
        let c: Command = serde_json::from_value(json!({
            "type": "hook", "token": "t"
        }))
        .unwrap();
        assert_eq!(
            c,
            Command::Hook {
                token: Some("t".into())
            }
        );
    }
}
