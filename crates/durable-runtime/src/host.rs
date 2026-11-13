//! Hosting workflows written in any language.
//!
//! The engine owns replay: it drives history through the execution's
//! [`Replayer`] and decides what to do next. A *host* owns the user code: it
//! runs the workflow body, and turns a step key into a running attempt. The
//! Rust SDK is one host; the TypeScript SDK (and later Python) are others.
//!
//! The replayer sits in a [`ReplayHandle`] that the engine and the host share.
//! The host's ctx calls (`user_key`, `issue`, `release`, `random_u64`, ...) go
//! into it directly and synchronously. The engine calls `advance`,
//! `take_resolved`, `push_live`, `frontier` and `fault`.
//!
//! Hidden until the host contract is stable: the shape may still change.

use std::sync::Arc;

use async_trait::async_trait;
use durable_sm::{Advance, Handle, Replayer, Resolution, WorkflowFailure};
use durable_spi::{ComputePool, Event, RunId, WorkflowDefinition};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::Value;

use crate::{Deps, StepError, deps::DepKey, step::StepInfo};

/// The replayer of one execution, shared by the engine and the host.
///
/// The lock is only ever held for one closure, so it can never be held
/// across an `.await`.
#[derive(Clone)]
pub struct ReplayHandle(Arc<Mutex<Replayer>>);

impl ReplayHandle {
    /// `history` must already be normalized for replay.
    pub fn new(run_id: RunId, history: Vec<Event>) -> Self {
        Self(Arc::new(Mutex::new(Replayer::new(run_id, history))))
    }

    /// Run `f` with the replayer locked. Do not call back into the handle
    /// from inside `f`: the lock is not reentrant.
    pub fn with<R>(&self, f: impl FnOnce(&mut Replayer) -> R) -> R {
        f(&mut self.0.lock())
    }
}

/// A registered workflow, written in any language.
pub trait HostWorkflow: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn definition(&self) -> WorkflowDefinition;
    /// Checked by `WorkerBuilder::build()`.
    fn required_deps(&self) -> Vec<DepKey> {
        vec![]
    }
    /// Start the body for one execution. It must not run user code yet: that
    /// waits for the first [`HostExecution::activate`].
    fn start(&self, env: ExecEnv) -> Box<dyn HostExecution>;
}

/// What a host gets to run one execution.
pub struct ExecEnv {
    pub run_id: RunId,
    pub input: Value,
    pub replay: ReplayHandle,
    /// Registered dependencies. Rust-only today; other hosts ignore them.
    pub deps: Deps,
    /// The worker's pool for blocking Rust steps.
    pub pool: Arc<dyn ComputePool>,
}

/// One execution of one workflow body.
#[async_trait]
pub trait HostExecution: Send {
    /// Settle these resolutions (in order), then run user code until it can do
    /// nothing more without new events.
    async fn activate(&mut self, resolved: Vec<(Handle, Resolution)>) -> ExecStatus;

    /// Start attempt `attempt` of the step issued under `key`. `None` means the
    /// host can no longer start it (a Rust `FnOnce` body an earlier attempt
    /// took); the engine then rebuilds the execution.
    ///
    /// The engine runs the returned future on tokio with `span` entered, and
    /// owns its timeout, panics and cancellation. Work the host moves off that
    /// future (to a thread pool) should enter `span` itself.
    fn start_step(
        &mut self,
        key: &str,
        attempt: u32,
        info: StepInfo,
        span: &tracing::Span,
    ) -> Option<StepAttempt>;

    /// The engine is done with this execution: unwind or abandon the body.
    fn interrupt(&mut self);
}

/// Where the body stands after an activation.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecStatus {
    /// Quiescent: everything the body can do without new events is done.
    Pending,
    Finished(Result<Value, WorkflowFailure>),
}

/// A running step attempt.
pub struct StepAttempt {
    /// Shown as `durable.step.kind` on the step's span: `"async"`, `"blocking"`, ...
    pub kind: &'static str,
    pub fut: BoxFuture<'static, Result<Value, StepError>>,
}

/// Deliver history to `exec` until the replayer has none left for it.
///
/// Each activation follows one `advance`, so the body sees one outcome at a
/// time. The activation after the newest event is live (`is_replaying` is
/// false): it is the first time the body sees that event, unless an earlier
/// execution crashed before acting on it, in which case acting twice beats
/// never acting. Stops early on a fault.
pub(crate) async fn replay(
    replay: &ReplayHandle,
    exec: &mut dyn HostExecution,
    mut status: ExecStatus,
) -> ExecStatus {
    while replay.with(Replayer::advance) == Advance::RunCode {
        let resolved = replay.with(Replayer::take_resolved);
        status = exec.activate(resolved).await;
        if replay.with(|r| r.fault().is_some()) {
            break;
        }
    }
    status
}
