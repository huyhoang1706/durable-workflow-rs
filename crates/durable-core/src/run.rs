use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RunId = uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Terminal means finished with any outcome.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl FromStr for RunStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "waiting" => Self::Waiting,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(format!("unknown run status `{other}`")),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub namespace: String,
    pub workflow: String,
    pub idempotency_key: Option<String>,
    pub status: RunStatus,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    /// Optimistic version: the seq of the last event in the log.
    pub last_seq: u64,
    /// When the run next needs a worker (the runnable index).
    pub wake_at: Option<DateTime<Utc>>,
    /// Signal/hook/event names the run is currently waiting on.
    pub awaiting: Vec<String>,
    pub concurrency_key: Option<String>,
    pub throttle_key: Option<String>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub cancel_reason: Option<String>,
    /// Engine fault (panic, non-determinism, ...) that left the run stuck.
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewRun {
    pub namespace: String,
    pub workflow: String,
    pub input: Value,
    pub idempotency_key: Option<String>,
    pub concurrency_key: Option<String>,
    pub throttle_key: Option<String>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum CreateOutcome {
    Created(Run),
    Existing(Run),
}

impl CreateOutcome {
    pub fn run(&self) -> &Run {
        match self {
            Self::Created(r) | Self::Existing(r) => r,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    pub namespace: Option<String>,
    pub workflow: Option<String>,
    pub status: Option<RunStatus>,
    /// Return runs created strictly before this run (newest-first cursor).
    pub after: Option<RunId>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    /// First execution: subject to throttle.
    Start,
    /// Resumption after sleep/signal/retry: wins over starts.
    Resume,
}

/// A request that a run be executed at `due_at`; the unit of the queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueItem {
    pub namespace: String,
    pub workflow: String,
    pub run_id: RunId,
    pub due_at: DateTime<Utc>,
    pub kind: WorkKind,
    pub concurrency_key: Option<String>,
    pub throttle_key: Option<String>,
}

impl Run {
    pub fn queue_item(&self, due_at: DateTime<Utc>) -> QueueItem {
        QueueItem {
            namespace: self.namespace.clone(),
            workflow: self.workflow.clone(),
            run_id: self.id,
            due_at,
            kind: if self.status == RunStatus::Pending && self.last_seq <= 1 {
                WorkKind::Start
            } else {
                WorkKind::Resume
            },
            concurrency_key: self.concurrency_key.clone(),
            throttle_key: self.throttle_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [RunStatus; 6] = [
        RunStatus::Pending,
        RunStatus::Running,
        RunStatus::Waiting,
        RunStatus::Succeeded,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ];

    #[test]
    fn status_round_trips_through_str() {
        for s in ALL {
            assert_eq!(s.as_str().parse::<RunStatus>().unwrap(), s);
        }
        assert!("completed".parse::<RunStatus>().is_err());
    }

    #[test]
    fn only_succeeded_failed_cancelled_are_terminal() {
        let terminal: Vec<_> = ALL.into_iter().filter(|s| s.is_terminal()).collect();
        assert_eq!(
            terminal,
            [
                RunStatus::Succeeded,
                RunStatus::Failed,
                RunStatus::Cancelled
            ]
        );
    }
}
