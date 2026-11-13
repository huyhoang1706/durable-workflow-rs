//! Node-API addon behind the durable-workflow TypeScript SDK.
//!
//! - [`replayer::Replayer`]: one execution's `durable_sm::Replayer`, called
//!   synchronously by workflow code.
//! - [`host`]: the engine side of a TypeScript workflow. The engine calls into
//!   JavaScript through `ThreadsafeFunction`s and awaits the promises they
//!   return.
//! - [`worker`]: the backend, the embedded worker and the client.
//!
//! Values cross as JSON in the replay corpus's wire shape: internally tagged
//! enums, `*_ms` durations and RFC 3339 timestamps. `u64` values cross as
//! decimal strings, because JSON numbers lose precision above 2^53 in
//! JavaScript.

mod host;
mod replayer;
mod worker;

use durable_sm::{Handle, Resolution, RunId};
use napi::{Error, Result};
use napi_derive::napi;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

pub(crate) fn from_json<T: DeserializeOwned>(what: &str, v: Value) -> Result<T> {
    serde_json::from_value(v).map_err(|e| Error::from_reason(format!("invalid {what}: {e}")))
}

pub(crate) fn to_json<T: Serialize>(v: &T) -> Result<Value> {
    serde_json::to_value(v).map_err(|e| Error::from_reason(e.to_string()))
}

pub(crate) fn parse_run_id(s: &str) -> Result<RunId> {
    s.parse()
        .map_err(|e| Error::from_reason(format!("invalid run id `{s}`: {e}")))
}

/// `[{handle, resolution}]`, as `Replayer.takeResolved()` returns it.
pub(crate) fn resolved_json(resolved: Vec<(Handle, Resolution)>) -> Result<Value> {
    let items: Vec<Value> = resolved
        .into_iter()
        .map(|(h, res)| Ok(serde_json::json!({"handle": h.0, "resolution": to_json(&res)?})))
        .collect::<Result<_>>()?;
    Ok(Value::Array(items))
}

/// The retry policy a step gets when the code sets none, as JSON.
#[napi]
pub fn default_retry_policy() -> Result<Value> {
    to_json(&durable_sm::RetryPolicy::default())
}
