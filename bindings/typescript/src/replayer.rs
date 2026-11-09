//! [`Replayer`] exposes one execution's `durable_sm::Replayer` to JavaScript.
//!
//! Every method takes the lock for a single call, so the engine (on tokio
//! threads) and workflow code (on the JS main thread) can share it.

use durable_runtime::host::ReplayHandle;
use durable_sm::{Command, Event};
use napi::Result;
use napi_derive::napi;
use serde_json::Value;

use crate::{from_json, parse_run_id, resolved_json, to_json};

/// The replay state machine of one execution. See `durable_sm::Replayer`.
#[napi]
pub struct Replayer {
    handle: ReplayHandle,
}

impl Replayer {
    /// The replayer of an engine execution, for the TypeScript host.
    pub(crate) fn from_handle(handle: ReplayHandle) -> Self {
        Self { handle }
    }
}

#[napi]
impl Replayer {
    /// `history` is a JSON array of events, already normalized for replay.
    #[napi(constructor)]
    pub fn new(run_id: String, history: Value) -> Result<Self> {
        let run_id = parse_run_id(&run_id)?;
        let history: Vec<Event> = from_json("history", history)?;
        Ok(Self {
            handle: ReplayHandle::new(run_id, history),
        })
    }

    #[napi]
    pub fn run_id(&self) -> String {
        self.handle.with(|r| r.run_id().to_string())
    }

    // ---- tracing ----

    /// Record every host call from now on.
    #[napi]
    pub fn record(&self) {
        self.handle.with(|r| r.record());
    }

    /// The trace ops recorded since the last call, as JSON.
    #[napi]
    pub fn take_trace(&self) -> Result<Value> {
        to_json(&self.handle.with(|r| r.take_trace()))
    }

    // ---- workflow code ----

    #[napi]
    pub fn user_key(&self, name: String) -> String {
        self.handle.with(|r| r.user_key(&name))
    }

    #[napi]
    pub fn check_name(&self, name: String) {
        self.handle.with(|r| r.check_name(&name));
    }

    #[napi]
    pub fn key_for(&self, name: String) -> String {
        self.handle.with(|r| r.key_for(&name))
    }

    /// Issue `command` (a JSON `Command`) under `key`; returns its handle.
    #[napi]
    pub fn issue(&self, key: String, command: Value) -> Result<u32> {
        let command: Command = from_json("command", command)?;
        Ok(self.handle.with(|r| r.issue(key, command)).0)
    }

    #[napi]
    pub fn is_issued(&self, key: String) -> bool {
        self.handle.with(|r| r.is_issued(&key))
    }

    #[napi]
    pub fn key_of(&self, handle: u32) -> Option<String> {
        self.handle
            .with(|r| r.key_of(durable_sm::Handle(handle)).map(str::to_string))
    }

    /// The code no longer awaits `handle`.
    #[napi]
    pub fn release(&self, handle: u32) {
        self.handle.with(|r| r.release(durable_sm::Handle(handle)));
    }

    /// How `handle` resolved, if it has.
    #[napi]
    pub fn resolution(&self, handle: u32) -> Result<Option<Value>> {
        self.handle
            .with(|r| r.resolution(durable_sm::Handle(handle)).cloned())
            .map(|res| to_json(&res))
            .transpose()
    }

    /// Why every pending and future command fails, if it does:
    /// `{"type": "interrupted" | "cancelled"}` or `{"type": "fault", "message"}`.
    #[napi]
    pub fn abort(&self) -> Result<Option<Value>> {
        self.handle
            .with(|r| r.abort())
            .map(|a| to_json(&a))
            .transpose()
    }

    /// RFC 3339, as chrono serializes it; the time of the latest applied event.
    #[napi]
    pub fn now(&self) -> String {
        self.handle
            .with(|r| r.now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
    }

    /// A deterministic `u64`, as a decimal string.
    #[napi]
    pub fn random_u64(&self) -> String {
        self.handle.with(|r| r.random_u64()).to_string()
    }

    #[napi]
    pub fn uuid(&self) -> String {
        self.handle.with(|r| r.uuid()).to_string()
    }

    #[napi]
    pub fn is_replaying(&self) -> bool {
        self.handle.with(|r| r.is_replaying())
    }

    #[napi]
    pub fn set_fault(&self, message: String) {
        self.handle.with(|r| r.set_fault(message));
    }

    // ---- engine ----

    /// Apply history up to the next point where the code must run:
    /// `"run_code"` or `"live"`.
    #[napi]
    pub fn advance(&self) -> Result<Value> {
        to_json(&self.handle.with(|r| r.advance()))
    }

    /// Resolutions produced since the last call, in history order:
    /// `[{handle, resolution}]`.
    #[napi]
    pub fn take_resolved(&self) -> Result<Value> {
        resolved_json(self.handle.with(|r| r.take_resolved()))
    }

    /// Append events the engine just committed (a JSON array).
    #[napi]
    pub fn push_live(&self, events: Value) -> Result<()> {
        let events: Vec<Event> = from_json("events", events)?;
        self.handle.with(|r| r.push_live(events));
        Ok(())
    }

    /// Unresolved commands, as JSON `Pending`s in issue order.
    #[napi]
    pub fn frontier(&self) -> Result<Value> {
        to_json(&self.handle.with(|r| r.frontier()))
    }

    #[napi]
    pub fn fault(&self) -> Option<String> {
        self.handle.with(|r| r.fault().map(str::to_string))
    }

    #[napi]
    pub fn interrupt(&self) {
        self.handle.with(|r| r.interrupt());
    }

    #[napi]
    pub fn cancel(&self) {
        self.handle.with(|r| r.cancel());
    }
}
