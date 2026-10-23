//! The authoring surface: `WfAsyncCtx` and `WfSyncCtx`.

use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Utc};
use durable_core::WaitKind;
use futures::future::BoxFuture;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    Dep, RetryPolicy, StepError, WorkflowError,
    core::{CmdFuture, CmdKind, Core, Resolution, StepSpec, StepStart},
    driver::SyncShared,
};

fn decode<T: DeserializeOwned>(core: &Core, key: &str, v: Value) -> Result<T, WorkflowError> {
    serde_json::from_value(v).map_err(|e| {
        let msg = format!("`{key}` no longer deserializes into the requested type: {e}");
        core.set_fault(format!("non-determinism: {msg}"));
        WorkflowError::NonDeterminism(msg)
    })
}

fn encode_step<T: Serialize>(t: T) -> Result<Value, StepError> {
    serde_json::to_value(t)
        .map_err(|e| StepError::fatal(format!("step output is not serializable: {e}")))
}

fn unexpected(core: &Core, key: &str, got: &Resolution) -> WorkflowError {
    let msg = format!("`{key}` resolved to an unexpected kind: {got:?}");
    core.set_fault(format!("non-determinism: {msg}"));
    WorkflowError::NonDeterminism(msg)
}

/// Context for async workflows. Cheap to clone.
#[derive(Clone)]
pub struct WfAsyncCtx {
    pub(crate) core: Arc<Core>,
}

impl WfAsyncCtx {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// An async step: runs on tokio, memoized by `name` (+ occurrence).
    pub fn step<T, E, F, Fut>(&self, name: &str, f: F) -> StepBuilder<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let start = StepStart::Async(Box::new(move || {
            Box::pin(async move { f().await.map_err(Into::into).and_then(encode_step) })
        }));
        StepBuilder::new(self.core.clone(), self.core.user_key(name), start)
    }

    /// A blocking step: runs on the compute pool, never on a tokio worker thread.
    pub fn step_blocking<T, E, F>(&self, name: &str, f: F) -> StepBuilder<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        self.blocking_step_at(self.core.user_key(name), f)
    }

    fn blocking_step_at<T, E, F>(&self, key: String, f: F) -> StepBuilder<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let start = StepStart::Blocking(Box::new(move || {
            f().map_err(Into::into).and_then(encode_step)
        }));
        StepBuilder::new(self.core.clone(), key, start)
    }

    /// Parallel fan-out on the compute pool; each item is memoized as `name[i]`.
    pub fn map_blocking<I, T, E, F>(
        &self,
        name: &str,
        items: Vec<I>,
        f: F,
    ) -> BoxFuture<'static, Result<Vec<T>, WorkflowError>>
    where
        I: Send + 'static,
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: Fn(I) -> Result<T, E> + Send + Sync + 'static,
    {
        self.core.check_name(name);
        let f = Arc::new(f);
        let steps: Vec<_> = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let f = f.clone();
                let key = self.core.key_for(&format!("{name}[{i}]"));
                self.blocking_step_at(key, move || f(item)).into_future()
            })
            .collect();
        Box::pin(futures::future::try_join_all(steps))
    }

    /// A durable timer: suspends the run, holding no worker.
    pub fn sleep(
        &self,
        name: &str,
        dur: Duration,
    ) -> impl Future<Output = Result<(), WorkflowError>> + Send + 'static {
        let (core, key) = (self.core.clone(), self.core.user_key(name));
        async move {
            match CmdFuture::new(
                core.clone(),
                key.clone(),
                CmdKind::Sleep { dur, fire_at: None },
            )
            .await?
            {
                Resolution::Slept => Ok(()),
                other => Err(unexpected(&core, &key, &other)),
            }
        }
    }

    /// Wait for `ctx`-addressed signal `name` sent via `Client::signal`.
    pub fn wait_for_signal<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> WaitBuilder<T> {
        let key = self.core.user_key(name);
        WaitBuilder::new(self.core.clone(), WaitKind::Signal, key, name)
    }

    /// Wait for a published event `name`; narrow with `.filter(json)`.
    /// Only events published after the wait is committed are seen.
    pub fn wait_for_event<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> WaitBuilder<T> {
        let key = self.core.user_key(name);
        WaitBuilder::new(self.core.clone(), WaitKind::Event, key, name)
    }

    /// A webhook endpoint for this run; `hook.token()` identifies it.
    pub fn create_hook<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Hook<T>, WorkflowError>> + Send + 'static {
        self.hook(name, None)
    }

    /// As `create_hook`, with a deterministic token. A token held by another
    /// live run resolves to `Err(WorkflowError::HookConflict)`.
    pub fn create_hook_with_token<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
        token: &str,
    ) -> impl Future<Output = Result<Hook<T>, WorkflowError>> + Send + 'static {
        self.hook(name, Some(token.to_string()))
    }

    fn hook<T>(
        &self,
        name: &str,
        token: Option<String>,
    ) -> impl Future<Output = Result<Hook<T>, WorkflowError>> + Send + 'static {
        let (core, key) = (self.core.clone(), self.core.user_key(name));
        async move {
            match CmdFuture::new(core.clone(), key.clone(), CmdKind::Hook { token }).await? {
                Resolution::Hook {
                    token,
                    conflict: true,
                } => Err(WorkflowError::HookConflict(token)),
                Resolution::Hook { token, .. } => Ok(Hook {
                    core,
                    key,
                    token,
                    _t: PhantomData,
                }),
                other => Err(unexpected(&core, &key, &other)),
            }
        }
    }

    /// Timestamp of the latest event consumed. Deterministic across replays.
    pub fn now(&self) -> DateTime<Utc> {
        self.core.now()
    }

    pub fn random_u64(&self) -> u64 {
        self.core.random_u64()
    }

    pub fn uuid(&self) -> uuid::Uuid {
        self.core.uuid()
    }

    /// A missing dependency is an engine fault: the run resumes once a fixed
    /// worker is deployed.
    pub fn dep<T: ?Sized + Send + Sync + 'static>(&self) -> Result<Dep<T>, WorkflowError> {
        self.core.deps.get::<T>().ok_or_else(|| {
            let msg = format!(
                "dependency `{}` is not registered",
                std::any::type_name::<T>()
            );
            self.core.set_fault(msg.clone());
            WorkflowError::Fault(msg)
        })
    }
}

/// Builder state: configurable until first polled, then a running future.
enum Lazy<C, O> {
    Config(Option<C>),
    Running(BoxFuture<'static, O>),
}

impl<C, O> Lazy<C, O> {
    fn poll_with(
        &mut self,
        cx: &mut Context<'_>,
        build: impl FnOnce(C) -> BoxFuture<'static, O>,
    ) -> Poll<O> {
        if let Self::Config(c) = self {
            let cfg = c.take().expect("builder polled after completion");
            *self = Self::Running(build(cfg));
        }
        match self {
            Self::Running(f) => f.as_mut().poll(cx),
            Self::Config(_) => unreachable!(),
        }
    }

    fn config(&mut self) -> &mut C {
        match self {
            Self::Config(Some(c)) => c,
            _ => panic!("builder cannot be configured after it has been polled"),
        }
    }
}

struct StepCfg {
    core: Arc<Core>,
    key: String,
    start: StepStart,
    policy: RetryPolicy,
    timeout: Option<Duration>,
}

/// A step call. Configure with `.retry()` / `.timeout()`, then `.await`
/// (or hand it to `try_join!` / `select!`).
pub struct StepBuilder<T> {
    lazy: Lazy<StepCfg, Result<T, WorkflowError>>,
}

impl<T> StepBuilder<T> {
    fn new(core: Arc<Core>, key: String, start: StepStart) -> Self {
        let cfg = StepCfg {
            core,
            key,
            start,
            policy: RetryPolicy::default(),
            timeout: None,
        };
        Self {
            lazy: Lazy::Config(Some(cfg)),
        }
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.lazy.config().policy = policy;
        self
    }

    /// Per-attempt wall-clock limit. A blocking step cannot be preempted: it
    /// is marked failed and its cancel flag is set, but the pool slot stays
    /// busy until the closure returns.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.lazy.config().timeout = Some(d);
        self
    }
}

impl<T: DeserializeOwned + Send + 'static> Future for StepBuilder<T> {
    type Output = Result<T, WorkflowError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.lazy.poll_with(
            cx,
            |StepCfg {
                 core,
                 key,
                 start,
                 policy,
                 timeout,
             }| {
                Box::pin(async move {
                    let spec = StepSpec {
                        start: Some(start),
                        policy,
                        timeout,
                        attempts: 0,
                        retry_at: None,
                    };
                    match CmdFuture::new(core.clone(), key.clone(), CmdKind::Step(spec)).await? {
                        Resolution::Step(Ok(v)) => decode(&core, &key, v),
                        Resolution::Step(Err(e)) => Err(WorkflowError::Step(e)),
                        other => Err(unexpected(&core, &key, &other)),
                    }
                })
            },
        )
    }
}

struct WaitCfg {
    core: Arc<Core>,
    key: String,
    kind: WaitKind,
    name: String,
    filter: Option<Value>,
    timeout: Option<Duration>,
}

impl WaitCfg {
    fn command(self) -> (Arc<Core>, String, CmdFuture) {
        let kind = CmdKind::Wait {
            kind: self.kind,
            name: self.name,
            filter: self.filter,
            timeout: self.timeout,
            started: false,
            timeout_at: None,
        };
        let fut = CmdFuture::new(self.core.clone(), self.key.clone(), kind);
        (self.core, self.key, fut)
    }
}

/// A wait for a signal, event or hook payload.
pub struct WaitBuilder<T> {
    lazy: Lazy<WaitCfg, Result<T, WorkflowError>>,
}

impl<T> WaitBuilder<T> {
    fn new(core: Arc<Core>, kind: WaitKind, key: String, name: &str) -> Self {
        let cfg = WaitCfg {
            core,
            key,
            kind,
            name: name.to_string(),
            filter: None,
            timeout: None,
        };
        Self {
            lazy: Lazy::Config(Some(cfg)),
        }
    }

    /// JSON containment filter applied to published events.
    pub fn filter(mut self, filter: Value) -> Self {
        self.lazy.config().filter = Some(filter);
        self
    }

    /// Give up after `d`; the wait then yields `None`.
    pub fn timeout(mut self, d: Duration) -> TimedWaitBuilder<T> {
        let cfg = match &mut self.lazy {
            Lazy::Config(c) => c.take().expect("builder polled after completion"),
            Lazy::Running(_) => panic!("builder cannot be configured after it has been polled"),
        };
        TimedWaitBuilder {
            lazy: Lazy::Config(Some(WaitCfg {
                timeout: Some(d),
                ..cfg
            })),
        }
    }
}

impl<T: DeserializeOwned + Send + 'static> Future for WaitBuilder<T> {
    type Output = Result<T, WorkflowError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.lazy.poll_with(cx, |cfg| {
            let (core, key, fut) = cfg.command();
            Box::pin(async move {
                match fut.await? {
                    Resolution::Signal(Some(v)) => decode(&core, &key, v),
                    other => Err(unexpected(&core, &key, &other)),
                }
            })
        })
    }
}

/// A wait with a timeout: resolves to `None` when the timeout elapses.
pub struct TimedWaitBuilder<T> {
    lazy: Lazy<WaitCfg, Result<Option<T>, WorkflowError>>,
}

impl<T> TimedWaitBuilder<T> {
    pub fn filter(mut self, filter: Value) -> Self {
        self.lazy.config().filter = Some(filter);
        self
    }
}

impl<T: DeserializeOwned + Send + 'static> Future for TimedWaitBuilder<T> {
    type Output = Result<Option<T>, WorkflowError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.lazy.poll_with(cx, |cfg| {
            let (core, key, fut) = cfg.command();
            Box::pin(async move {
                match fut.await? {
                    Resolution::Signal(Some(v)) => decode(&core, &key, v).map(Some),
                    Resolution::Signal(None) => Ok(None),
                    other => Err(unexpected(&core, &key, &other)),
                }
            })
        })
    }
}

/// A webhook-style endpoint. `recv` may be called repeatedly.
pub struct Hook<T> {
    core: Arc<Core>,
    key: String,
    token: String,
    _t: PhantomData<fn() -> T>,
}

impl<T> Hook<T> {
    /// Resolve with `Client::resume_hook(token, payload)`.
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl<T> Hook<T> {
    /// Release the token: later webhooks to it are rejected and another run
    /// may claim it. The run keeps going.
    pub fn dispose(self) -> impl Future<Output = Result<(), WorkflowError>> + Send + 'static {
        let core = self.core.clone();
        let key = core.key_for(&format!("{}[dispose]", self.key));
        let token = self.token;
        async move {
            match CmdFuture::new(core.clone(), key.clone(), CmdKind::HookDispose { token }).await? {
                Resolution::Disposed => Ok(()),
                other => Err(unexpected(&core, &key, &other)),
            }
        }
    }
}

impl<T: DeserializeOwned + Send + 'static> Hook<T> {
    pub fn recv(&self) -> WaitBuilder<T> {
        let key = self.core.key_for(&format!("{}[recv]", self.key));
        WaitBuilder::<T>::new(self.core.clone(), WaitKind::Hook, key, &self.token)
    }
}

/// Context for sync (blocking) workflows: plain blocking code on a driver
/// thread. Every call blocks that thread, never a tokio worker.
pub struct WfSyncCtx {
    inner: WfAsyncCtx,
    shared: Arc<SyncShared>,
}

impl WfSyncCtx {
    pub(crate) fn new(core: Arc<Core>, shared: Arc<SyncShared>) -> Self {
        Self {
            inner: WfAsyncCtx::new(core),
            shared,
        }
    }

    fn block<F: Future>(&self, fut: F) -> F::Output {
        self.shared.block_on(fut)
    }

    /// A blocking step on the compute pool.
    pub fn step<T, E, F>(&self, name: &str, f: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        self.block(self.inner.step_blocking(name, f).into_future())
    }

    /// As `step`, with retry and timeout settings.
    pub fn step_with<T, E, F>(
        &self,
        name: &str,
        policy: RetryPolicy,
        timeout: Option<Duration>,
        f: F,
    ) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let mut b = self.inner.step_blocking(name, f).retry(policy);
        if let Some(t) = timeout {
            b = b.timeout(t);
        }
        self.block(b.into_future())
    }

    /// An async step on tokio; this thread blocks until it finishes.
    pub fn step_async<T, E, F, Fut>(&self, name: &str, f: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        self.block(self.inner.step(name, f).into_future())
    }

    /// Parallel fan-out on the compute pool, memoized per item.
    pub fn map<I, T, E, F>(&self, name: &str, items: Vec<I>, f: F) -> Result<Vec<T>, WorkflowError>
    where
        I: Send + 'static,
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<StepError>,
        F: Fn(I) -> Result<T, E> + Send + Sync + 'static,
    {
        self.block(self.inner.map_blocking(name, items, f))
    }

    /// Run two blocking steps in parallel.
    pub fn join2<A, B, EA, EB, FA, FB>(
        &self,
        a: (&str, FA),
        b: (&str, FB),
    ) -> Result<(A, B), WorkflowError>
    where
        A: Serialize + DeserializeOwned + Send + 'static,
        B: Serialize + DeserializeOwned + Send + 'static,
        EA: Into<StepError>,
        EB: Into<StepError>,
        FA: FnOnce() -> Result<A, EA> + Send + 'static,
        FB: FnOnce() -> Result<B, EB> + Send + 'static,
    {
        let fa = self.inner.step_blocking(a.0, a.1).into_future();
        let fb = self.inner.step_blocking(b.0, b.1).into_future();
        self.block(futures::future::try_join(fa, fb))
    }

    /// Run a tuple of `(name, closure)` blocking steps (up to 8, each with its
    /// own output type) in parallel: `let (a, b, c) = ctx.join((("a", fa), ("b", fb), ("c", fc)))?`.
    pub fn join<G: StepGroup>(&self, steps: G) -> Result<G::Output, WorkflowError> {
        self.block(steps.run(&self.inner))
    }

    pub fn sleep(&self, name: &str, dur: Duration) -> Result<(), WorkflowError> {
        self.block(self.inner.sleep(name, dur))
    }

    pub fn wait_for_signal<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> Result<T, WorkflowError> {
        self.block(self.inner.wait_for_signal::<T>(name).into_future())
    }

    pub fn wait_for_signal_timeout<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Option<T>, WorkflowError> {
        self.block(
            self.inner
                .wait_for_signal::<T>(name)
                .timeout(timeout)
                .into_future(),
        )
    }

    pub fn wait_for_event<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
        filter: Option<Value>,
    ) -> Result<T, WorkflowError> {
        let mut w = self.inner.wait_for_event::<T>(name);
        if let Some(f) = filter {
            w = w.filter(f);
        }
        self.block(w.into_future())
    }

    pub fn create_hook<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> Result<Hook<T>, WorkflowError> {
        self.block(self.inner.create_hook::<T>(name))
    }

    pub fn create_hook_with_token<T: DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
        token: &str,
    ) -> Result<Hook<T>, WorkflowError> {
        self.block(self.inner.create_hook_with_token::<T>(name, token))
    }

    pub fn recv<T: DeserializeOwned + Send + 'static>(
        &self,
        hook: &Hook<T>,
    ) -> Result<T, WorkflowError> {
        self.block(hook.recv())
    }

    pub fn dispose_hook<T>(&self, hook: Hook<T>) -> Result<(), WorkflowError> {
        self.block(hook.dispose())
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.inner.now()
    }

    pub fn random_u64(&self) -> u64 {
        self.inner.random_u64()
    }

    pub fn uuid(&self) -> uuid::Uuid {
        self.inner.uuid()
    }

    pub fn dep<T: ?Sized + Send + Sync + 'static>(&self) -> Result<Dep<T>, WorkflowError> {
        self.inner.dep::<T>()
    }
}

/// A tuple of `(name, blocking closure)` pairs that [`WfSyncCtx::join`] runs
/// in parallel. Implemented for tuples of 1 to 8 steps.
pub trait StepGroup {
    type Output;
    #[doc(hidden)]
    fn run(self, ctx: &WfAsyncCtx) -> BoxFuture<'static, Result<Self::Output, WorkflowError>>;
}

macro_rules! step_group {
    ($(($T:ident, $E:ident, $F:ident, $v:ident)),+) => {
        impl<'n, $($T, $E, $F),+> StepGroup for ($((&'n str, $F),)+)
        where
            $(
                $T: Serialize + DeserializeOwned + Send + 'static,
                $E: Into<StepError>,
                $F: FnOnce() -> Result<$T, $E> + Send + 'static,
            )+
        {
            type Output = ($($T,)+);
            fn run(self, ctx: &WfAsyncCtx) -> BoxFuture<'static, Result<Self::Output, WorkflowError>> {
                #[allow(non_snake_case)]
                let ($($v,)+) = self;
                $(let $v = ctx.step_blocking($v.0, $v.1).into_future();)+
                Box::pin(async move { futures::try_join!($($v),+) })
            }
        }
    };
}

step_group!((A, EA, FA, a));
step_group!((A, EA, FA, a), (B, EB, FB, b));
step_group!((A, EA, FA, a), (B, EB, FB, b), (C, EC, FC, c));
step_group!(
    (A, EA, FA, a),
    (B, EB, FB, b),
    (C, EC, FC, c),
    (D, ED, FD, d)
);
step_group!(
    (A, EA, FA, a),
    (B, EB, FB, b),
    (C, EC, FC, c),
    (D, ED, FD, d),
    (E, EE, FE, e)
);
step_group!(
    (A, EA, FA, a),
    (B, EB, FB, b),
    (C, EC, FC, c),
    (D, ED, FD, d),
    (E, EE, FE, e),
    (F, EF, FF, f)
);
step_group!(
    (A, EA, FA, a),
    (B, EB, FB, b),
    (C, EC, FC, c),
    (D, ED, FD, d),
    (E, EE, FE, e),
    (F, EF, FF, f),
    (G, EG, FG, g)
);
step_group!(
    (A, EA, FA, a),
    (B, EB, FB, b),
    (C, EC, FC, c),
    (D, ED, FD, d),
    (E, EE, FE, e),
    (F, EF, FF, f),
    (G, EG, FG, g),
    (H, EH, FH, h)
);
