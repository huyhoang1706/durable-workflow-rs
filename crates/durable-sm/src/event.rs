use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RunId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    Signal,
    Hook,
    Event,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    RunStarted {
        input: Value,
    },
    RunSucceeded {
        output: Value,
    },
    RunFailed {
        error: String,
    },
    RunCancelled {
        reason: Option<String>,
    },
    /// A failed run was re-run from its failure point.
    RunRetried,
    StepSucceeded {
        key: String,
        output: Value,
    },
    /// `retry_at: Some` means another attempt follows; `None` is final.
    StepFailed {
        key: String,
        attempt: u32,
        error: String,
        retryable: bool,
        retry_at: Option<DateTime<Utc>>,
    },
    TimerScheduled {
        key: String,
        fire_at: DateTime<Utc>,
    },
    TimerFired {
        key: String,
    },
    WaitStarted {
        key: String,
        kind: WaitKind,
        name: String,
        filter: Option<Value>,
        timeout_at: Option<DateTime<Utc>>,
    },
    SignalReceived {
        key: String,
        signal_id: String,
        payload: Value,
    },
    WaitTimedOut {
        key: String,
    },
    /// `conflict` is set when another live run already holds the token.
    HookCreated {
        key: String,
        token: String,
        conflict: bool,
    },
    /// The run released the hook's token; webhooks to it are rejected and
    /// another run may claim it. `key` is the dispose call's own key.
    HookDisposed {
        key: String,
        token: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub run_id: RunId,
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub kind: EventKind,
}

impl EventKind {
    /// The step/timer/wait key this event belongs to, if any.
    pub fn key(&self) -> Option<&str> {
        use EventKind::*;
        match self {
            StepSucceeded { key, .. }
            | StepFailed { key, .. }
            | TimerScheduled { key, .. }
            | TimerFired { key }
            | WaitStarted { key, .. }
            | SignalReceived { key, .. }
            | WaitTimedOut { key }
            | HookCreated { key, .. }
            | HookDisposed { key, .. } => Some(key),
            RunStarted { .. }
            | RunSucceeded { .. }
            | RunFailed { .. }
            | RunCancelled { .. }
            | RunRetried => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::RunSucceeded { .. } | Self::RunFailed { .. } | Self::RunCancelled { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_kind_is_tagged_in_json() {
        let k = EventKind::StepSucceeded {
            key: "a".into(),
            output: json!(1),
        };
        let v = serde_json::to_value(&k).unwrap();
        assert_eq!(v["type"], "step_succeeded");
        assert_eq!(serde_json::from_value::<EventKind>(v).unwrap(), k);
    }

    #[test]
    fn key_is_exposed_for_keyed_events_only() {
        assert_eq!(EventKind::TimerFired { key: "t".into() }.key(), Some("t"));
        assert_eq!(EventKind::RunRetried.key(), None);
    }
}
