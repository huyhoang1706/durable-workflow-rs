//! Best-effort view of a run that is executing right now, kept in the cache.
//!
//! The store only knows a run is `Running`; which worker holds it, how many
//! times it was delivered and which step attempts are in progress live here.
//! It is written by the executing worker and read by the ops API. Losing it
//! (cache flush, no cache configured) loses nothing durable.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use durable_spi::{Cache, RunId};
use serde::{Deserialize, Serialize};

/// Safety net for a worker that dies without cleaning up.
pub(crate) const TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InFlight {
    /// Id of the worker executing the run.
    pub worker: String,
    pub since: DateTime<Utc>,
    /// How many times the queue delivered this run since it last settled.
    pub deliveries: i64,
    /// Step key to attempt number, for steps executing right now.
    pub steps: BTreeMap<String, u32>,
}

pub(crate) fn key(run_id: RunId) -> String {
    format!("dw:inflight:{run_id}")
}

pub(crate) async fn write(cache: &dyn Cache, run_id: RunId, v: &InFlight) {
    if let Ok(bytes) = serde_json::to_vec(v) {
        let _ = cache.set(&key(run_id), bytes, Some(TTL)).await;
    }
}

pub(crate) async fn read(cache: &dyn Cache, run_id: RunId) -> Option<InFlight> {
    let bytes = cache.get(&key(run_id)).await.ok()??;
    serde_json::from_slice(&bytes).ok()
}

pub(crate) async fn clear(cache: &dyn Cache, run_id: RunId) {
    let _ = cache.delete(&key(run_id)).await;
}
