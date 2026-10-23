use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{QueueItem, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueScope {
    pub namespace: String,
    pub workflow: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThrottleLimit {
    pub limit: u32,
    pub period: Duration,
    /// Starts allowed back-to-back. `1` spaces starts evenly, `period / limit` apart.
    pub burst: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ClaimLimits {
    /// Max executing runs for the whole workflow.
    pub workflow_concurrency: Option<u32>,
    /// Max executing runs per `concurrency_key`.
    pub key_concurrency: Option<u32>,
    /// Applies to `Start` items only; delays runs, never drops them.
    pub throttle: Option<ThrottleLimit>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    pub item: QueueItem,
    /// Generation token: a stale token cannot ack or extend.
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct Claimed {
    pub leases: Vec<Lease>,
    /// Earliest future due time seen, to schedule the next poll.
    pub next_due: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait Queue: Send + Sync + 'static {
    /// Deduped per run; keeps the earliest due time. If the run is leased,
    /// remembers a `rerun` so `ack` re-enqueues it.
    async fn enqueue(&self, items: Vec<QueueItem>) -> Result<()>;
    async fn claim(
        &self,
        scope: &QueueScope,
        limits: &ClaimLimits,
        max: usize,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Claimed>;
    /// Returns the leases that were lost (expired or superseded).
    async fn extend(
        &self,
        leases: &[Lease],
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>>;
    /// Release and reschedule atomically (`next_wake: None` = nothing to run).
    async fn ack(
        &self,
        lease: &Lease,
        next_wake: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<()>;
    async fn nack(&self, lease: &Lease, retry_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<()>;
    async fn requeue_expired(&self, scope: &QueueScope, now: DateTime<Utc>) -> Result<usize>;
}
