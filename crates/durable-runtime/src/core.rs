//! The Rust host's per-execution state: wakers, step closures and deps.
//!
//! All replay logic (keys, history matching, non-determinism, the frontier,
//! now/random/uuid) lives in the replayer. This module only connects it to
//! Rust futures: a ctx call becomes a [`CmdFuture`] that issues a command on
//! first poll and is woken when the replayer resolves its handle.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use chrono::{DateTime, Utc};
use durable_sm::{Abort, Handle, Replayer, StepFailure};
pub(crate) use durable_sm::{Command, Resolution};
use durable_spi::RunId;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::Value;

use crate::{Deps, ErrorClass, StepError, WorkflowError, host::ReplayHandle};

pub(crate) type StepFut = BoxFuture<'static, Result<Value, StepError>>;

pub(crate) enum StepStart {
    /// Runs on tokio. A future is lazy, so a replayed step drops it unpolled.
    Async(StepFut),
    /// Runs on the compute pool.
    Blocking(Box<dyn FnOnce() -> Result<Value, StepError> + Send>),
}

/// A step's final failure from history, as workflow code sees it.
pub(crate) fn step_error(f: StepFailure) -> StepError {
    StepError {
        message: f.message,
        class: if f.retryable {
            ErrorClass::Retryable
        } else {
            ErrorClass::Fatal
        },
    }
}

/// Host-side state of one execution: what the replayer cannot hold.
#[derive(Default)]
struct HostState {
    wakers: HashMap<Handle, Waker>,
    /// Step bodies by key. Taken when an attempt launches; a retry after that
    /// needs a rebuild, because the closures are `FnOnce`.
    starts: HashMap<String, StepStart>,
}

/// The Rust host's view of one execution: the shared replayer plus wakers,
/// step closures and deps.
///
/// Lock order: `host` before the replayer. The engine only ever takes the
/// replayer, for one call at a time, so it cannot deadlock with a ctx call.
pub(crate) struct Core {
    pub run_id: RunId,
    pub deps: Deps,
    replay: ReplayHandle,
    host: Mutex<HostState>,
}

impl Core {
    pub fn new(run_id: RunId, deps: Deps, replay: ReplayHandle) -> Arc<Self> {
        Arc::new(Self {
            run_id,
            deps,
            replay,
            host: Mutex::default(),
        })
    }

    fn sm<R>(&self, f: impl FnOnce(&mut Replayer) -> R) -> R {
        self.replay.with(f)
    }

    pub fn is_replaying(&self) -> bool {
        self.sm(|r| r.is_replaying())
    }

    pub fn user_key(&self, name: &str) -> String {
        self.sm(|r| r.user_key(name))
    }

    pub fn check_name(&self, name: &str) {
        self.sm(|r| r.check_name(name));
    }

    pub fn key_for(&self, name: &str) -> String {
        self.sm(|r| r.key_for(name))
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.sm(|r| r.now())
    }

    pub fn random_u64(&self) -> u64 {
        self.sm(Replayer::random_u64)
    }

    pub fn uuid(&self) -> uuid::Uuid {
        self.sm(Replayer::uuid)
    }

    pub fn set_fault(&self, msg: String) {
        self.sm(|r| r.set_fault(msg));
    }

    /// Fail every pending and future ctx call with `Interrupted`.
    pub fn interrupt(&self) {
        let wakers: Vec<Waker> = {
            let mut g = self.host.lock();
            self.sm(Replayer::interrupt);
            g.wakers.drain().map(|(_, w)| w).collect()
        };
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Wake the futures awaiting these handles.
    pub fn wake(&self, handles: impl IntoIterator<Item = Handle>) {
        let wakers: Vec<Waker> = {
            let mut g = self.host.lock();
            handles
                .into_iter()
                .filter_map(|h| g.wakers.remove(&h))
                .collect()
        };
        wakers.into_iter().for_each(Waker::wake);
    }

    pub fn take_start(&self, key: &str) -> Option<StepStart> {
        self.host.lock().starts.remove(key)
    }
}

/// The future behind every ctx call. Issues its command on first poll.
pub(crate) struct CmdFuture {
    core: Arc<Core>,
    key: String,
    pending: Option<(Command, Option<StepStart>)>,
    handle: Option<Handle>,
    finished: bool,
}

impl CmdFuture {
    pub fn new(core: Arc<Core>, key: String, command: Command) -> Self {
        Self::with_start(core, key, command, None)
    }

    pub fn step(core: Arc<Core>, key: String, command: Command, start: StepStart) -> Self {
        Self::with_start(core, key, command, Some(start))
    }

    fn with_start(
        core: Arc<Core>,
        key: String,
        command: Command,
        start: Option<StepStart>,
    ) -> Self {
        Self {
            core,
            key,
            pending: Some((command, start)),
            handle: None,
            finished: false,
        }
    }
}

impl Future for CmdFuture {
    type Output = Result<Resolution, WorkflowError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let core = this.core.clone();
        let mut g = core.host.lock();
        if let Some(abort) = core.sm(|r| r.abort()) {
            this.finished = true;
            return Poll::Ready(Err(match abort {
                Abort::Interrupted => WorkflowError::Interrupted,
                Abort::Cancelled => WorkflowError::Cancelled,
                Abort::Fault(msg) => WorkflowError::Fault(msg),
            }));
        }
        if let Some((command, start)) = this.pending.take() {
            let (duplicate, handle, fault) = core.sm(|r| {
                let duplicate = r.is_issued(&this.key);
                let handle = r.issue(this.key.clone(), command);
                (duplicate, handle, r.fault().unwrap_or_default().to_string())
            });
            if duplicate {
                this.finished = true;
                return Poll::Ready(Err(WorkflowError::Fault(fault)));
            }
            if let Some(start) = start {
                g.starts.insert(this.key.clone(), start);
            }
            this.handle = Some(handle);
        }
        let handle = this.handle.expect("command issued");
        if let Some(r) = core.sm(|r| r.resolution(handle).cloned()) {
            this.finished = true;
            return Poll::Ready(Ok(r));
        }
        g.wakers.insert(handle, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for CmdFuture {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(h) = self.handle {
            let mut g = self.core.host.lock();
            self.core.sm(|r| r.release(h));
            g.wakers.remove(&h);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker_ref;

    use super::*;

    fn new_core() -> Arc<Core> {
        let replay = ReplayHandle::new(RunId::nil(), vec![]);
        Core::new(RunId::nil(), Deps::default(), replay)
    }

    fn sleep_cmd(core: &Arc<Core>, key: &str) -> CmdFuture {
        let command = Command::Sleep {
            dur: std::time::Duration::from_secs(1),
        };
        CmdFuture::new(core.clone(), key.into(), command)
    }

    #[test]
    fn a_second_command_with_a_live_key_faults_instead_of_replacing_it() {
        let core = new_core();
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut first = sleep_cmd(&core, "k");
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        let mut second = sleep_cmd(&core, "k");
        assert!(matches!(
            Pin::new(&mut second).poll(&mut cx),
            Poll::Ready(Err(WorkflowError::Fault(_)))
        ));
        let fault = core.sm(|r| r.fault().map(str::to_string)).unwrap();
        assert!(fault.contains("share the key `k`"));
    }

    #[test]
    fn a_dropped_future_leaves_the_frontier() {
        let core = new_core();
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut fut = sleep_cmd(&core, "k");
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert_eq!(core.sm(|r| r.frontier()).len(), 1);
        drop(fut);
        assert!(core.sm(|r| r.frontier()).is_empty());
    }

    #[test]
    fn a_final_step_failure_keeps_its_class() {
        let f = StepFailure {
            message: "x".into(),
            retryable: false,
        };
        assert_eq!(step_error(f).class, ErrorClass::Fatal);
    }
}
