//! The Rust host for `durable_sm::Replayer`: wakers, step closures and deps.
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
use durable_core::{Event, RunId};
use durable_sm::{Abort, Handle, Replayer, StepFailure};
pub(crate) use durable_sm::{Advance, Command, Pending, Resolution};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::Value;

use crate::{Deps, ErrorClass, StepError, WorkflowError};

pub(crate) type StepFut = BoxFuture<'static, Result<Value, StepError>>;

pub(crate) enum StepStart {
    /// Runs on tokio.
    Async(Box<dyn FnOnce() -> StepFut + Send>),
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

struct Inner {
    sm: Replayer,
    wakers: HashMap<Handle, Waker>,
    /// Step bodies by key. Taken when an attempt launches; a retry after that
    /// needs a rebuild, because the closures are `FnOnce`.
    starts: HashMap<String, StepStart>,
}

pub(crate) struct Core {
    pub run_id: RunId,
    pub deps: Deps,
    inner: Mutex<Inner>,
}

impl Core {
    /// `history` must already be normalized for replay.
    pub fn new(run_id: RunId, deps: Deps, history: Vec<Event>) -> Arc<Self> {
        Arc::new(Self {
            run_id,
            deps,
            inner: Mutex::new(Inner {
                sm: Replayer::new(run_id, history),
                wakers: HashMap::new(),
                starts: HashMap::new(),
            }),
        })
    }

    pub fn is_replaying(&self) -> bool {
        self.inner.lock().sm.is_replaying()
    }

    pub fn user_key(&self, name: &str) -> String {
        self.inner.lock().sm.user_key(name)
    }

    pub fn check_name(&self, name: &str) {
        self.inner.lock().sm.check_name(name);
    }

    pub fn key_for(&self, name: &str) -> String {
        self.inner.lock().sm.key_for(name)
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.inner.lock().sm.now()
    }

    pub fn random_u64(&self) -> u64 {
        self.inner.lock().sm.random_u64()
    }

    pub fn uuid(&self) -> uuid::Uuid {
        self.inner.lock().sm.uuid()
    }

    pub fn fault(&self) -> Option<String> {
        self.inner.lock().sm.fault().map(str::to_string)
    }

    pub fn set_fault(&self, msg: String) {
        self.inner.lock().sm.set_fault(msg);
    }

    /// Fail every pending and future ctx call with `Interrupted`.
    pub fn interrupt(&self) {
        self.abort_with(Replayer::interrupt);
    }

    /// Fail every pending and future ctx call with `Cancelled`.
    pub fn cancel(&self) {
        self.abort_with(Replayer::cancel);
    }

    fn abort_with(&self, f: fn(&mut Replayer)) {
        let wakers: Vec<Waker> = {
            let mut g = self.inner.lock();
            f(&mut g.sm);
            g.wakers.drain().map(|(_, w)| w).collect()
        };
        wakers.into_iter().for_each(Waker::wake);
    }

    pub fn take_start(&self, key: &str) -> Option<StepStart> {
        self.inner.lock().starts.remove(key)
    }

    pub fn has_start(&self, key: &str) -> bool {
        self.inner.lock().starts.contains_key(key)
    }

    pub fn frontier(&self) -> Vec<Pending> {
        self.inner.lock().sm.frontier()
    }

    /// Apply history up to the next point where the code must run, and wake
    /// the futures it resolved. The caller polls the driver on `RunCode`.
    pub fn advance(&self) -> Advance {
        let (advance, wakers) = {
            let mut g = self.inner.lock();
            let advance = g.sm.advance();
            let resolved = g.sm.take_resolved();
            let wakers: Vec<Waker> = resolved
                .into_iter()
                .filter_map(|(h, _)| g.wakers.remove(&h))
                .collect();
            (advance, wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
        advance
    }

    /// Queue events the engine just committed; deliver them with `advance`.
    pub fn push_live(&self, events: Vec<Event>) {
        self.inner.lock().sm.push_live(events);
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
        let mut g = this.core.inner.lock();
        if let Some(abort) = g.sm.abort() {
            this.finished = true;
            return Poll::Ready(Err(match abort {
                Abort::Interrupted => WorkflowError::Interrupted,
                Abort::Cancelled => WorkflowError::Cancelled,
                Abort::Fault(msg) => WorkflowError::Fault(msg),
            }));
        }
        if let Some((command, start)) = this.pending.take() {
            let duplicate = g.sm.is_issued(&this.key);
            let handle = g.sm.issue(this.key.clone(), command);
            if duplicate {
                this.finished = true;
                let msg = g.sm.fault().unwrap_or_default().to_string();
                return Poll::Ready(Err(WorkflowError::Fault(msg)));
            }
            if let Some(start) = start {
                g.starts.insert(this.key.clone(), start);
            }
            this.handle = Some(handle);
        }
        let handle = this.handle.expect("command issued");
        if let Some(r) = g.sm.resolution(handle) {
            this.finished = true;
            return Poll::Ready(Ok(r.clone()));
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
            let mut g = self.core.inner.lock();
            g.sm.release(h);
            g.wakers.remove(&h);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker_ref;

    use super::*;

    fn sleep_cmd(core: &Arc<Core>, key: &str) -> CmdFuture {
        let command = Command::Sleep {
            dur: std::time::Duration::from_secs(1),
        };
        CmdFuture::new(core.clone(), key.into(), command)
    }

    #[test]
    fn a_second_command_with_a_live_key_faults_instead_of_replacing_it() {
        let core = Core::new(RunId::nil(), Deps::default(), vec![]);
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut first = sleep_cmd(&core, "k");
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        let mut second = sleep_cmd(&core, "k");
        assert!(matches!(
            Pin::new(&mut second).poll(&mut cx),
            Poll::Ready(Err(WorkflowError::Fault(_)))
        ));
        assert!(core.fault().unwrap().contains("share the key `k`"));
    }

    #[test]
    fn a_dropped_future_leaves_the_frontier() {
        let core = Core::new(RunId::nil(), Deps::default(), vec![]);
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut fut = sleep_cmd(&core, "k");
        assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
        assert_eq!(core.frontier().len(), 1);
        drop(fut);
        assert!(core.frontier().is_empty());
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
