//! The replay conformance corpus: the cross-language replay contract.
//!
//! Each case in `corpus/*.json` is a history written by a real run of a Rust
//! workflow, plus the [`TraceOp`]s its replay produced: every key reserved,
//! command issued and released, value drawn, and each `advance()` with the
//! resolutions it delivered. It ends with the frontier, fault and result the
//! replay left behind.
//!
//! Two checks use it:
//!
//! - **Replayer conformance**: [`play`] drives a bare [`Replayer`] with the
//!   host calls from the trace and checks every answer. A binding runs the
//!   same check through its FFI layer.
//! - **SDK conformance**: the SDK's version of the case's workflow replays
//!   the history with recording on, and must produce the same trace,
//!   frontier and fault, and the same result: an equal `ok` value, or an
//!   `err` of the same [`FailureKind`](durable_sm::FailureKind). Messages
//!   the host writes (the `err` message, a `set_fault` op's message, and the
//!   final `fault` when the host set it) are the SDK's own and are not
//!   compared; faults the replayer raises come from the shared core and are.
//!
//! The Rust SDK writes the corpus: run
//! `DURABLE_UPDATE_CORPUS=1 cargo test -p durable-runtime --test replay_corpus`.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

use durable_sm::{Event, Handle, Pending, Replayer, RunId, TraceOp, WorkflowFailure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How a replayed workflow body ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Finish {
    Ok(Value),
    /// Compare `kind`; the message is the SDK's own.
    Err(WorkflowFailure),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Case {
    pub name: String,
    /// What the workflow does, for whoever ports it to another SDK.
    pub description: String,
    /// The name the workflow is registered under.
    pub workflow: String,
    pub input: Value,
    /// Seeds `random_u64` and `uuid`.
    pub run_id: RunId,
    /// Already normalized for replay; terminal events are dropped.
    pub history: Vec<Event>,
    pub trace: Vec<TraceOp>,
    pub frontier: Vec<Pending>,
    pub fault: Option<String>,
    /// `None` while the body still waits on the frontier.
    pub result: Option<Finish>,
}

/// `crates/durable-testkit/corpus`.
pub fn dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// The file a case named `name` lives in.
pub fn path(name: &str) -> PathBuf {
    dir().join(format!("{name}.json"))
}

impl Case {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Canonical JSON: one history event and one trace op per line, so a
    /// change to the replay shows up as a readable diff.
    pub fn to_json(&self) -> String {
        fn one<T: Serialize>(v: &T) -> String {
            serde_json::to_string(v).expect("corpus types serialize")
        }
        fn list<T: Serialize>(out: &mut String, name: &str, items: &[T], last: bool) {
            let sep = if last { "" } else { "," };
            if items.is_empty() {
                let _ = writeln!(out, "  \"{name}\": []{sep}");
                return;
            }
            let _ = writeln!(out, "  \"{name}\": [");
            for (i, item) in items.iter().enumerate() {
                let comma = if i + 1 < items.len() { "," } else { "" };
                let _ = writeln!(out, "    {}{comma}", one(item));
            }
            let _ = writeln!(out, "  ]{sep}");
        }
        let mut out = String::from("{\n");
        let _ = writeln!(out, "  \"name\": {},", one(&self.name));
        let _ = writeln!(out, "  \"description\": {},", one(&self.description));
        let _ = writeln!(out, "  \"workflow\": {},", one(&self.workflow));
        let _ = writeln!(out, "  \"input\": {},", one(&self.input));
        let _ = writeln!(out, "  \"run_id\": {},", one(&self.run_id));
        list(&mut out, "history", &self.history, false);
        list(&mut out, "trace", &self.trace, false);
        list(&mut out, "frontier", &self.frontier, false);
        let _ = writeln!(out, "  \"fault\": {},", one(&self.fault));
        let _ = writeln!(out, "  \"result\": {}", one(&self.result));
        out.push_str("}\n");
        out
    }
}

/// Every case in the corpus, sorted by name.
pub fn load_all() -> Result<Vec<Case>, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir())
        .map_err(|e| format!("{}: {e}", dir().display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    paths.iter().map(|p| Case::load(p)).collect()
}

/// Drive a bare [`Replayer`] with the host calls in `case.trace` and check
/// that it answers each one as recorded, then that it ends with the recorded
/// frontier and fault.
pub fn play(case: &Case) -> Result<(), String> {
    let mut r = Replayer::new(case.run_id, case.history.clone());
    r.record();
    let mut handles: Vec<(String, Handle)> = Vec::new();
    for (i, op) in case.trace.iter().enumerate() {
        match op {
            TraceOp::UserKey { name, .. } => {
                r.user_key(name);
            }
            TraceOp::CheckName { name } => r.check_name(name),
            TraceOp::KeyFor { name, .. } => {
                r.key_for(name);
            }
            TraceOp::Issue { key, command } => {
                let h = r.issue(key.clone(), command.clone());
                handles.push((key.clone(), h));
            }
            TraceOp::Release { key } => {
                // The live command under `key`: a rejected duplicate never is.
                let h = handles
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, h)| *h)
                    .ok_or_else(|| {
                        format!("op #{i}: release of `{key}`, which was never issued")
                    })?;
                r.release(h);
            }
            TraceOp::Random { .. } => {
                r.random_u64();
            }
            TraceOp::Uuid { .. } => {
                r.uuid();
            }
            TraceOp::SetFault { message } => r.set_fault(message.clone()),
            TraceOp::Interrupt => r.interrupt(),
            TraceOp::Cancel => r.cancel(),
            TraceOp::Advance { .. } => {
                r.advance();
                r.take_resolved();
            }
        }
        let got = r.take_trace();
        if got.as_slice() != std::slice::from_ref(op) {
            return Err(format!(
                "op #{i}: expected {}, the replayer recorded {}",
                json(op),
                json(&got)
            ));
        }
    }
    let frontier = r.frontier();
    if frontier != case.frontier {
        return Err(format!(
            "frontier: expected {}, got {}",
            json(&case.frontier),
            json(&frontier)
        ));
    }
    if r.fault() != case.fault.as_deref() {
        return Err(format!(
            "fault: expected {:?}, got {:?}",
            case.fault,
            r.fault()
        ));
    }
    Ok(())
}

/// Where two traces first differ, for test failures.
pub fn first_difference(expected: &[TraceOp], got: &[TraceOp]) -> Option<String> {
    let i = expected
        .iter()
        .zip(got)
        .position(|(a, b)| a != b)
        .unwrap_or(expected.len().min(got.len()));
    if i == expected.len() && i == got.len() {
        return None;
    }
    let show = |ops: &[TraceOp]| ops.get(i).map_or_else(|| "<end of trace>".into(), json);
    Some(format!(
        "trace op #{i}: expected {}, got {}",
        show(expected),
        show(got)
    ))
}

fn json<T: Serialize + ?Sized>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|e| format!("<{e}>"))
}
