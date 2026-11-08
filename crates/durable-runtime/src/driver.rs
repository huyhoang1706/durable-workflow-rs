//! The Rust host: drivers run user workflow code against a `Core`, and
//! [`RustExecution`] plugs them into the engine as a [`HostExecution`].
//!
//! Drivers differ only in how the body runs: an async future polled by the
//! engine, or a blocking body on its own thread. Both are *disposable*: a pure
//! function of history.

use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use async_trait::async_trait;
use durable_sm::{Handle, Resolution};
use durable_spi::ComputePool;
use futures::{FutureExt, future::BoxFuture};
use parking_lot::{Condvar, Mutex};
use serde_json::Value;
use tokio::sync::Notify;

use crate::{
    StepError, WfSyncCtx, WorkflowError,
    core::{Core, StepStart},
    host::{ExecStatus, HostExecution, StepAttempt},
    step::{self, StepInfo},
};

pub(crate) type RunResult = Result<Value, WorkflowError>;

#[derive(Clone)]
pub(crate) enum DriverStatus {
    /// Quiescent: everything the body can do without new events is done.
    Pending,
    Finished(RunResult),
}

pub(crate) enum Driver {
    Async(AsyncDriver),
    Sync(SyncDriver),
}

impl Driver {
    /// Drive the body until it can make no more progress.
    pub async fn poll(&mut self) -> DriverStatus {
        match self {
            Self::Async(d) => d.poll(),
            Self::Sync(d) => d.quiesce().await,
        }
    }

    pub fn interrupt(&mut self) {
        match self {
            Self::Async(d) => d.interrupt(),
            Self::Sync(d) => d.interrupt(),
        }
    }
}

pub(crate) fn panic_message(p: Box<dyn Any + Send>) -> String {
    p.downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "non-string panic payload".into())
}

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(crate) struct AsyncDriver {
    core: Arc<Core>,
    root: Option<BoxFuture<'static, RunResult>>,
    finished: Option<RunResult>,
    flag: Arc<WakeFlag>,
}

impl AsyncDriver {
    pub fn new(core: Arc<Core>, root: BoxFuture<'static, RunResult>) -> Self {
        Self {
            core,
            root: Some(root),
            finished: None,
            flag: Arc::default(),
        }
    }

    fn poll(&mut self) -> DriverStatus {
        if let Some(r) = &self.finished {
            return DriverStatus::Finished(r.clone());
        }
        let Some(root) = self.root.as_mut() else {
            return DriverStatus::Pending;
        };
        let waker = Waker::from(self.flag.clone());
        let mut cx = Context::from_waker(&waker);
        loop {
            self.flag.0.store(false, Ordering::SeqCst);
            let polled = catch_unwind(AssertUnwindSafe(|| root.as_mut().poll(&mut cx)));
            let result = match polled {
                Ok(Poll::Ready(r)) => r,
                Ok(Poll::Pending) if self.flag.0.load(Ordering::SeqCst) => continue,
                Ok(Poll::Pending) => return DriverStatus::Pending,
                Err(p) => Err(WorkflowError::Fault(format!(
                    "workflow panicked: {}",
                    panic_message(p)
                ))),
            };
            self.root = None;
            self.finished = Some(result.clone());
            return DriverStatus::Finished(result);
        }
    }

    fn interrupt(&mut self) {
        self.core.interrupt();
        self.root = None;
    }
}

#[derive(Default)]
struct SyncState {
    blocked: bool,
    /// A wake arrived that the body has not yet acted on.
    runnable: bool,
    done: Option<RunResult>,
}

/// State shared between the driver thread and the engine.
pub(crate) struct SyncShared {
    state: Mutex<SyncState>,
    cv: Condvar,
    notify: Notify,
}

struct ThreadWaker(Arc<SyncShared>);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.state.lock().runnable = true;
        self.0.cv.notify_all();
    }
}

impl SyncShared {
    /// Block the calling (driver) thread until `fut` completes.
    pub fn block_on<F: Future>(self: &Arc<Self>, fut: F) -> F::Output {
        let mut fut = Box::pin(fut);
        let waker = Waker::from(Arc::new(ThreadWaker(self.clone())));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(v) = Pin::new(&mut fut).poll(&mut cx) {
                return v;
            }
            let mut st = self.state.lock();
            if st.runnable {
                st.runnable = false;
                continue;
            }
            st.blocked = true;
            self.notify.notify_one();
            while !st.runnable {
                self.cv.wait(&mut st);
            }
            st.runnable = false;
            st.blocked = false;
        }
    }
}

type SyncBody = Box<dyn FnOnce(&WfSyncCtx) -> RunResult + Send + 'static>;

pub(crate) struct SyncDriver {
    core: Arc<Core>,
    shared: Arc<SyncShared>,
    /// The body, until the first `quiesce` starts its thread.
    body: Option<SyncBody>,
}

impl SyncDriver {
    /// The body's thread starts on the first `quiesce`, so no user code runs
    /// before the engine's first activation.
    pub fn new(core: Arc<Core>, body: SyncBody) -> Self {
        let shared = Arc::new(SyncShared {
            state: Mutex::default(),
            cv: Condvar::new(),
            notify: Notify::new(),
        });
        Self {
            core,
            shared,
            body: Some(body),
        }
    }

    fn spawn(&mut self, body: SyncBody) {
        let (thread_core, thread_shared) = (self.core.clone(), self.shared.clone());
        std::thread::Builder::new()
            .name(format!("durable-sync-{}", self.core.run_id))
            .spawn(move || {
                let ctx = WfSyncCtx::new(thread_core, thread_shared.clone());
                let result = catch_unwind(AssertUnwindSafe(|| body(&ctx))).unwrap_or_else(|p| {
                    Err(WorkflowError::Fault(format!(
                        "workflow panicked: {}",
                        panic_message(p)
                    )))
                });
                thread_shared.state.lock().done = Some(result);
                thread_shared.notify.notify_one();
            })
            .expect("spawn sync driver thread");
    }

    async fn quiesce(&mut self) -> DriverStatus {
        if let Some(body) = self.body.take() {
            self.spawn(body);
        }
        loop {
            let notified = self.shared.notify.notified();
            tokio::pin!(notified);
            // Register interest before checking, so no signal is missed.
            notified.as_mut().enable();
            {
                let st = self.shared.state.lock();
                if let Some(r) = &st.done {
                    return DriverStatus::Finished(r.clone());
                }
                if st.blocked && !st.runnable {
                    return DriverStatus::Pending;
                }
            }
            notified.await;
        }
    }

    fn interrupt(&mut self) {
        // Every pending and future ctx call now fails, so the body unwinds.
        self.body = None;
        self.core.interrupt();
    }
}

impl Drop for SyncDriver {
    fn drop(&mut self) {
        self.core.interrupt();
    }
}

/// A Rust workflow body, run for the engine.
pub(crate) struct RustExecution {
    core: Arc<Core>,
    driver: Driver,
    pool: Arc<dyn ComputePool>,
}

impl RustExecution {
    pub fn new(core: Arc<Core>, driver: Driver, pool: Arc<dyn ComputePool>) -> Self {
        Self { core, driver, pool }
    }
}

#[async_trait]
impl HostExecution for RustExecution {
    async fn activate(&mut self, resolved: Vec<(Handle, Resolution)>) -> ExecStatus {
        self.core.wake(resolved.into_iter().map(|(h, _)| h));
        match self.driver.poll().await {
            DriverStatus::Pending => ExecStatus::Pending,
            DriverStatus::Finished(r) => ExecStatus::Finished(r.map_err(|e| e.failure())),
        }
    }

    fn start_step(
        &mut self,
        key: &str,
        _attempt: u32,
        info: StepInfo,
        span: &tracing::Span,
    ) -> Option<StepAttempt> {
        Some(match self.core.take_start(key)? {
            StepStart::Async(fut) => StepAttempt {
                kind: "async",
                fut: step::scope_async(info, fut).boxed(),
            },
            StepStart::Blocking(f) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let span = span.clone();
                self.pool.execute(Box::new(move || {
                    let _entered = span.enter();
                    let r = catch_unwind(AssertUnwindSafe(|| step::scope_blocking(info, f)))
                        .unwrap_or_else(|p| {
                            Err(StepError::retryable(format!(
                                "step panicked: {}",
                                panic_message(p)
                            )))
                        });
                    let _ = tx.send(r);
                }));
                StepAttempt {
                    kind: "blocking",
                    // A job the pool dropped never reports, as before.
                    fut: async move {
                        match rx.await {
                            Ok(r) => r,
                            Err(_) => std::future::pending().await,
                        }
                    }
                    .boxed(),
                }
            }
        })
    }

    fn interrupt(&mut self) {
        self.driver.interrupt();
    }
}
