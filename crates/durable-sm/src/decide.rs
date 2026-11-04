//! The engine's policy, as pure functions.
//!
//! Given the replayer's frontier and a snapshot of the execution, decide what
//! the engine does next: register timers and waits, fire what is due, launch
//! steps, wait in-process or suspend. The engine shell performs the decision
//! (commits, tasks, timers) and feeds the results back into the replayer.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{EventKind, Pending, RetryPolicy, WaitKind, saturating_add};

/// How a failed step attempt is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "after", rename_all = "snake_case")]
pub enum ErrorClass {
    Retryable,
    /// Stops retries immediately.
    Fatal,
    /// Retry no sooner than this.
    RetryAfter(Duration),
}

/// A wait the store indexes, so signals, hooks and events can find the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitReg {
    pub key: String,
    pub kind: WaitKind,
    /// Signal name, hook token or event name.
    pub name: String,
    pub filter: Option<Value>,
    pub timeout_at: Option<DateTime<Utc>>,
}

impl WaitReg {
    /// Inbox name that satisfies this wait.
    pub fn inbox_name(&self) -> String {
        match self.kind {
            WaitKind::Signal => self.name.clone(),
            WaitKind::Hook => format!("hook:{}", self.name),
            WaitKind::Event => format!("event:{}", self.name),
        }
    }
}

/// A hook to register. `token: None` asks the shell to mint a fresh one,
/// which is not deterministic and so is not the decider's job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRequest {
    pub key: String,
    pub token: Option<String>,
}

/// One step attempt to start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepLaunch {
    pub key: String,
    /// 1-based.
    pub attempt: u32,
    pub timeout: Option<Duration>,
}

/// A snapshot of one execution, taken at the top of the engine loop.
pub struct DecideInput<'a> {
    pub frontier: &'a [Pending],
    pub now: DateTime<Utc>,
    pub deadline_at: Option<DateTime<Utc>>,
    /// Whether a step attempt is in flight for this key.
    pub running: &'a dyn Fn(&str) -> bool,
    /// Whether a commit already synced the inbox for open waits.
    pub inbox_checked: bool,
    /// Wakes closer than this are waited out in-process instead of suspending.
    pub inline_threshold: Duration,
}

/// What the engine does next.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Decision {
    /// The run's deadline has passed.
    DeadlineExceeded,
    /// The code awaits nothing durable; it can never make progress.
    Fault { message: String },
    /// Commit new timers, waits and hooks (and sync the inbox for open
    /// waits), then decide again.
    Register {
        events: Vec<EventKind>,
        hooks: Vec<HookRequest>,
    },
    /// Commit due timers and wait timeouts, then decide again.
    Fire { events: Vec<EventKind> },
    /// Launch these attempts, in order, then wait in-process until a step
    /// finishes or `wake_at`. A host that can no longer start a step (its
    /// body was spent) rebuilds instead and decides again.
    Run {
        launch: Vec<StepLaunch>,
        wake_at: Option<DateTime<Utc>>,
    },
    /// Nothing runs and the next wake is not close: release the worker.
    Suspend { wake_at: Option<DateTime<Utc>> },
}

pub fn decide(input: &DecideInput<'_>) -> Decision {
    let DecideInput {
        frontier,
        now,
        deadline_at,
        running,
        inbox_checked,
        inline_threshold,
    } = *input;
    if deadline_at.is_some_and(|d| now >= d) {
        return Decision::DeadlineExceeded;
    }
    if frontier.is_empty() {
        return Decision::Fault {
            message: "workflow awaited a non-durable future (only ctx.* calls may be awaited)"
                .into(),
        };
    }

    // 1. Register new timers, waits and hooks (and sync open waits).
    let (events, hooks) = registrations(frontier, now);
    let has_open_waits = frontier.iter().any(|p| matches!(p, Pending::Wait { .. }));
    if !events.is_empty() || !hooks.is_empty() || (has_open_waits && !inbox_checked) {
        return Decision::Register { events, hooks };
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
        return Decision::Fire { events: due };
    }

    // 3. Launch steps that are ready.
    let launch: Vec<StepLaunch> = frontier
        .iter()
        .filter_map(|p| match p {
            Pending::Step {
                key,
                attempts,
                retry_at,
                timeout,
                ..
            } if !running(key) && !retry_at.is_some_and(|t| t > now) => Some(StepLaunch {
                key: key.clone(),
                attempt: attempts + 1,
                timeout: *timeout,
            }),
            _ => None,
        })
        .collect();

    // 4. Wait here, or suspend.
    let busy = |key: &str| running(key) || launch.iter().any(|l| l.key == key);
    let wake_at = next_wake(frontier, deadline_at, &busy);
    let any_running = !launch.is_empty() || frontier.iter().any(|p| busy(p.key()));
    let inline = wake_at.is_some_and(|t| (t - now).to_std().is_ok_and(|d| d <= inline_threshold));
    if !any_running && !inline {
        return Decision::Suspend { wake_at };
    }
    Decision::Run { launch, wake_at }
}

fn registrations(frontier: &[Pending], now: DateTime<Utc>) -> (Vec<EventKind>, Vec<HookRequest>) {
    let (mut events, mut hooks) = (vec![], vec![]);
    for p in frontier {
        match p {
            Pending::Sleep {
                key,
                dur,
                fire_at: None,
            } => events.push(EventKind::TimerScheduled {
                key: key.clone(),
                fire_at: saturating_add(now, *dur),
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
                timeout_at: timeout.map(|d| saturating_add(now, d)),
            }),
            Pending::Hook { key, token } => hooks.push(HookRequest {
                key: key.clone(),
                token: token.clone(),
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

/// The earliest timer, wait timeout, retry of an idle step, or the deadline.
fn next_wake(
    frontier: &[Pending],
    deadline_at: Option<DateTime<Utc>>,
    busy: &dyn Fn(&str) -> bool,
) -> Option<DateTime<Utc>> {
    let timers = frontier.iter().filter_map(|p| match p {
        Pending::Sleep { fire_at, .. } => *fire_at,
        Pending::Wait { timeout_at, .. } => *timeout_at,
        Pending::Step { key, retry_at, .. } if !busy(key) => *retry_at,
        _ => None,
    });
    timers.chain(deadline_at).min()
}

/// Wait registrations for every open wait, minus those `events` close.
pub fn open_waits(frontier: &[Pending], events: &[EventKind], now: DateTime<Utc>) -> Vec<WaitReg> {
    let closing: Vec<&str> = events
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
            } if !closing.contains(&key.as_str()) => Some(WaitReg {
                key: key.clone(),
                kind: *kind,
                name: name.clone(),
                filter: filter.clone(),
                timeout_at: timeout_at.or_else(|| timeout.map(|d| saturating_add(now, d))),
            }),
            _ => None,
        })
        .collect()
}

/// The event that records a finished step attempt: success, a retry with its
/// backoff, or a final failure. `failure` carries the error's message and
/// class; outputs over `max_payload_bytes` fail fatally.
pub fn step_outcome(
    key: String,
    policy: &RetryPolicy,
    attempt: u32,
    result: Result<Value, (String, ErrorClass)>,
    now: DateTime<Utc>,
    max_payload_bytes: usize,
) -> EventKind {
    let result = match result {
        Ok(v) if v.to_string().len() > max_payload_bytes => Err((
            format!(
                "step output exceeds max_payload_bytes ({max_payload_bytes}); pass large data by reference"
            ),
            ErrorClass::Fatal,
        )),
        other => other,
    };
    match result {
        Ok(output) => EventKind::StepSucceeded { key, output },
        Err((error, class)) => {
            let retryable = class != ErrorClass::Fatal;
            let retry = retryable && policy.allows_retry(attempt);
            let delay = match class {
                ErrorClass::RetryAfter(d) => d,
                _ => policy.delay_after(attempt),
            };
            EventKind::StepFailed {
                key,
                attempt,
                error,
                retryable,
                retry_at: retry.then(|| saturating_add(now, delay)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn step(key: &str, attempts: u32, retry_at: Option<DateTime<Utc>>) -> Pending {
        Pending::Step {
            key: key.into(),
            attempts,
            retry_at,
            policy: RetryPolicy::default(),
            timeout: None,
        }
    }

    fn sleep(key: &str, fire_at: Option<DateTime<Utc>>) -> Pending {
        Pending::Sleep {
            key: key.into(),
            dur: Duration::from_secs(60),
            fire_at,
        }
    }

    fn wait(key: &str, started: bool, timeout_at: Option<DateTime<Utc>>) -> Pending {
        Pending::Wait {
            key: key.into(),
            kind: WaitKind::Signal,
            name: key.into(),
            filter: None,
            timeout: Some(Duration::from_secs(30)),
            started,
            timeout_at,
        }
    }

    fn decide_with(frontier: &[Pending], running: &[&str], inbox_checked: bool) -> Decision {
        decide(&DecideInput {
            frontier,
            now: t(0),
            deadline_at: None,
            running: &|k| running.contains(&k),
            inbox_checked,
            inline_threshold: Duration::from_secs(5),
        })
    }

    #[test]
    fn the_deadline_wins_over_everything() {
        let d = decide(&DecideInput {
            frontier: &[],
            now: t(10),
            deadline_at: Some(t(10)),
            running: &|_| false,
            inbox_checked: true,
            inline_threshold: Duration::ZERO,
        });
        assert_eq!(d, Decision::DeadlineExceeded);
    }

    #[test]
    fn an_empty_frontier_is_a_fault() {
        assert!(matches!(
            decide_with(&[], &[], true),
            Decision::Fault { message } if message.contains("non-durable future")
        ));
    }

    #[test]
    fn new_timers_waits_and_hooks_are_registered_first() {
        let frontier = [
            step("a", 0, None),
            sleep("t", None),
            wait("w", false, None),
            Pending::Hook {
                key: "h".into(),
                token: None,
            },
        ];
        let Decision::Register { events, hooks } = decide_with(&frontier, &[], true) else {
            panic!("expected Register");
        };
        assert_eq!(
            events,
            [
                EventKind::TimerScheduled {
                    key: "t".into(),
                    fire_at: t(60)
                },
                EventKind::WaitStarted {
                    key: "w".into(),
                    kind: WaitKind::Signal,
                    name: "w".into(),
                    filter: None,
                    timeout_at: Some(t(30)),
                },
            ]
        );
        assert_eq!(
            hooks,
            [HookRequest {
                key: "h".into(),
                token: None
            }]
        );
    }

    #[test]
    fn open_waits_sync_the_inbox_once() {
        let frontier = [wait("w", true, None)];
        assert_eq!(
            decide_with(&frontier, &[], false),
            Decision::Register {
                events: vec![],
                hooks: vec![]
            }
        );
        assert_eq!(
            decide_with(&frontier, &[], true),
            Decision::Suspend { wake_at: None }
        );
    }

    #[test]
    fn due_timers_and_timeouts_fire() {
        let frontier = [sleep("t", Some(t(0))), wait("w", true, Some(t(-1)))];
        assert_eq!(
            decide_with(&frontier, &[], true),
            Decision::Fire {
                events: vec![
                    EventKind::TimerFired { key: "t".into() },
                    EventKind::WaitTimedOut { key: "w".into() },
                ]
            }
        );
    }

    #[test]
    fn ready_steps_launch_in_order_and_running_or_backing_off_ones_do_not() {
        let frontier = [
            step("a", 0, None),
            step("b", 2, Some(t(0))),
            step("c", 0, None),
            step("d", 1, Some(t(3))),
        ];
        let Decision::Run { launch, wake_at } = decide_with(&frontier, &["c"], true) else {
            panic!("expected Run");
        };
        let keys: Vec<_> = launch.iter().map(|l| (l.key.as_str(), l.attempt)).collect();
        assert_eq!(keys, [("a", 1), ("b", 3)]);
        assert_eq!(wake_at, Some(t(3)), "d's retry is the next wake");
    }

    #[test]
    fn a_far_wake_with_nothing_running_suspends_and_a_near_one_waits_inline() {
        let far = [step("a", 1, Some(t(600)))];
        assert_eq!(
            decide_with(&far, &[], true),
            Decision::Suspend {
                wake_at: Some(t(600))
            }
        );
        let near = [step("a", 1, Some(t(2)))];
        assert_eq!(
            decide_with(&near, &[], true),
            Decision::Run {
                launch: vec![],
                wake_at: Some(t(2))
            }
        );
    }

    #[test]
    fn a_running_step_keeps_the_worker_even_with_a_far_timer() {
        let frontier = [step("a", 0, None), sleep("t", Some(t(600)))];
        assert_eq!(
            decide_with(&frontier, &["a"], true),
            Decision::Run {
                launch: vec![],
                wake_at: Some(t(600))
            }
        );
    }

    #[test]
    fn open_waits_skip_the_ones_timing_out_now() {
        let frontier = [wait("w1", true, Some(t(5))), wait("w2", false, None)];
        let closing = [EventKind::WaitTimedOut { key: "w1".into() }];
        let regs = open_waits(&frontier, &closing, t(0));
        assert_eq!(regs.len(), 1);
        assert_eq!(regs[0].key, "w2");
        assert_eq!(regs[0].timeout_at, Some(t(30)));
        assert_eq!(regs[0].inbox_name(), "w2");
    }

    #[test]
    fn step_outcomes_follow_the_policy_and_the_error_class() {
        let p = RetryPolicy::exponential(3);
        let out = |attempt, r| step_outcome("s".into(), &p, attempt, r, t(0), 64);
        assert_eq!(
            out(1, Ok(json!(1))),
            EventKind::StepSucceeded {
                key: "s".into(),
                output: json!(1)
            }
        );
        let retry_at = |e: EventKind| match e {
            EventKind::StepFailed { retry_at, .. } => retry_at,
            other => panic!("{other:?}"),
        };
        let err = |c| Err(("boom".to_string(), c));
        assert_eq!(retry_at(out(2, err(ErrorClass::Retryable))), Some(t(2)));
        assert_eq!(
            retry_at(out(3, err(ErrorClass::Retryable))),
            None,
            "budget spent"
        );
        assert_eq!(retry_at(out(1, err(ErrorClass::Fatal))), None);
        let after = ErrorClass::RetryAfter(Duration::from_secs(9));
        assert_eq!(retry_at(out(1, err(after))), Some(t(9)));
    }

    #[test]
    fn an_oversized_output_fails_fatally() {
        let e = step_outcome(
            "s".into(),
            &RetryPolicy::default(),
            1,
            Ok(json!("x".repeat(100))),
            t(0),
            10,
        );
        assert!(matches!(
            e,
            EventKind::StepFailed { retryable: false, retry_at: None, ref error, .. }
                if error.contains("max_payload_bytes")
        ));
    }
}
