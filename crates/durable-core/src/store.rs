use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{
    CreateOutcome, Event, EventKind, NewRun, QueueItem, Result, Run, RunFilter, RunId, RunStatus,
    WaitKind, WorkflowDefinition,
};

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct HookReg {
    pub key: String,
    pub token: String,
}

/// The single atomic write path for executions.
#[derive(Debug, Clone)]
pub struct Commit {
    pub run_id: RunId,
    pub expected_seq: u64,
    pub at: DateTime<Utc>,
    pub events: Vec<EventKind>,
    /// Every wait still open after this commit. New ones are registered and
    /// matching inbox signals are consumed (appended as `SignalReceived`).
    pub waits: Vec<WaitReg>,
    /// Hooks to create; the store appends a `HookCreated` event for each.
    pub hooks: Vec<HookReg>,
    /// Requested status (`Running` or `Waiting`); terminal events override it.
    pub status: RunStatus,
    pub wake_at: Option<DateTime<Utc>>,
    pub awaiting: Vec<String>,
    pub last_error: Option<String>,
}

impl Commit {
    pub fn new(run_id: RunId, expected_seq: u64, at: DateTime<Utc>) -> Self {
        Self {
            run_id,
            expected_seq,
            at,
            events: vec![],
            waits: vec![],
            hooks: vec![],
            status: RunStatus::Running,
            wake_at: None,
            awaiting: vec![],
            last_error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitOutcome {
    pub last_seq: u64,
    /// Events the store produced itself (consumed signals, created hooks), in order.
    pub generated: Vec<Event>,
    pub cancel_requested: bool,
}

#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Idempotent on `(namespace, workflow, idempotency_key)`.
    async fn create_run(&self, new: NewRun) -> Result<CreateOutcome>;
    async fn get_run(&self, id: RunId) -> Result<Option<Run>>;
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<Run>>;
    async fn load_events(&self, id: RunId, after_seq: u64) -> Result<Vec<Event>>;

    /// Returns `Error::Conflict` if `expected_seq` is stale.
    async fn commit(&self, commit: Commit) -> Result<CommitOutcome>;

    /// External inputs write to the inbox under the run lock. Each returns the
    /// queue item to enqueue when the run must wake up.
    async fn deliver_signal(
        &self,
        run_id: RunId,
        name: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>>;
    /// `Error::NotFound` when no live run holds the token.
    async fn resolve_hook(
        &self,
        token: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>>;
    /// Matches open event waits by name and JSON containment of `filter` in `data`.
    async fn publish_event(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        at: DateTime<Utc>,
    ) -> Result<Vec<QueueItem>>;
    async fn request_cancel(
        &self,
        run_id: RunId,
        reason: Option<String>,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>>;
    /// Re-run a failed run from its failure point.
    async fn retry_run(&self, run_id: RunId, at: DateTime<Utc>) -> Result<QueueItem>;

    /// Non-terminal runs whose `wake_at` is at or before `before`.
    async fn due_runs(
        &self,
        namespace: &str,
        before: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<QueueItem>>;

    async fn upsert_definitions(&self, defs: Vec<WorkflowDefinition>) -> Result<()>;
    async fn get_definition(&self, name: &str) -> Result<Option<WorkflowDefinition>>;
}
