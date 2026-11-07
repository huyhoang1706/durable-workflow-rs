use std::{future::Future, marker::PhantomData, sync::Arc, time::Duration};

use durable_spi::{ConcurrencySpec, RateLimitSpec, ThrottleSpec, WorkflowDefinition};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    WfAsyncCtx, WfSyncCtx, WorkflowError,
    core::Core,
    deps::DepKey,
    driver::{AsyncDriver, Driver, SyncDriver},
};

/// Flow control and limits for a workflow.
#[derive(Debug, Clone, Default)]
pub struct WorkflowOptions {
    pub concurrency: Option<ConcurrencySpec>,
    pub throttle: Option<ThrottleSpec>,
    pub rate_limit: Option<RateLimitSpec>,
    pub timeout: Option<Duration>,
}

impl WorkflowOptions {
    /// At most `limit` runs executing at once.
    pub fn concurrency(mut self, limit: u32) -> Self {
        self.concurrency = Some(ConcurrencySpec { limit, key: None });
        self
    }

    /// At most `limit` executing runs per value at JSON pointer `key` of the input.
    pub fn concurrency_by(mut self, limit: u32, key: &str) -> Self {
        self.concurrency = Some(ConcurrencySpec {
            limit,
            key: Some(key.into()),
        });
        self
    }

    /// Delay (never drop) run starts beyond `limit` per `period`.
    pub fn throttle(mut self, limit: u32, period: Duration) -> Self {
        self.throttle = Some(ThrottleSpec {
            limit,
            period_secs: period.as_secs(),
            burst: 1,
            key: None,
        });
        self
    }

    /// Allow `burst` starts back-to-back before the throttle spaces them out.
    pub fn throttle_burst(mut self, burst: u32) -> Self {
        if let Some(t) = &mut self.throttle {
            t.burst = burst.max(1);
        }
        self
    }

    /// Reject (never queue) starts beyond `limit` per `period`.
    pub fn rate_limit(mut self, limit: u32, period: Duration) -> Self {
        self.rate_limit = Some(RateLimitSpec {
            limit,
            period_secs: period.as_secs(),
            key: None,
        });
        self
    }

    /// Fail a run that has not finished within `d` of starting.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn definition(&self, name: &str) -> WorkflowDefinition {
        WorkflowDefinition {
            name: name.into(),
            concurrency: self.concurrency.clone(),
            throttle: self.throttle.clone(),
            rate_limit: self.rate_limit.clone(),
            timeout_secs: self.timeout.map(|d| d.as_secs()),
        }
    }
}

/// A workflow whose body is `async`. A struct may hold its own dependencies.
pub trait AsyncWorkflow: Send + Sync + 'static {
    const NAME: &'static str;
    type Input: DeserializeOwned + Send + 'static;
    type Output: Serialize + Send + 'static;

    fn options() -> WorkflowOptions {
        WorkflowOptions::default()
    }
    /// Checked by `WorkerBuilder::build()`.
    fn required_deps() -> Vec<DepKey> {
        vec![]
    }
    fn run(
        self: Arc<Self>,
        ctx: WfAsyncCtx,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, WorkflowError>> + Send;
}

/// A workflow whose body is plain blocking code, run on a dedicated thread.
pub trait SyncWorkflow: Send + Sync + 'static {
    const NAME: &'static str;
    type Input: DeserializeOwned + Send + 'static;
    type Output: Serialize + Send + 'static;

    fn options() -> WorkflowOptions {
        WorkflowOptions::default()
    }
    fn required_deps() -> Vec<DepKey> {
        vec![]
    }
    fn run(
        self: Arc<Self>,
        ctx: &WfSyncCtx,
        input: Self::Input,
    ) -> Result<Self::Output, WorkflowError>;
}

/// Marker for [`IntoWorkflow`] implemented by async workflows.
pub struct AsyncKind;
/// Marker for [`IntoWorkflow`] implemented by sync workflows.
pub struct SyncKind;

/// A workflow value that [`WorkerBuilder::register`](crate::WorkerBuilder::register) and
/// [`Client::start`](crate::Client::start) accept, whether async or sync. `#[workflow]` generates a
/// unit struct named after the function, so the function itself is the value:
/// `worker.register(onboarding)`. The marker `M` only keeps the two blanket impls apart and is
/// always inferred.
pub trait IntoWorkflow<M>: Sized {
    const NAME: &'static str;

    #[doc(hidden)]
    fn register(self, builder: crate::WorkerBuilder) -> crate::WorkerBuilder;
}

impl<W: AsyncWorkflow> IntoWorkflow<AsyncKind> for W {
    const NAME: &'static str = <W as AsyncWorkflow>::NAME;

    fn register(self, builder: crate::WorkerBuilder) -> crate::WorkerBuilder {
        builder.push(Arc::new(AsyncAdapter(Arc::new(self))))
    }
}

impl<W: SyncWorkflow> IntoWorkflow<SyncKind> for W {
    const NAME: &'static str = <W as SyncWorkflow>::NAME;

    fn register(self, builder: crate::WorkerBuilder) -> crate::WorkerBuilder {
        builder.push(Arc::new(SyncAdapter(Arc::new(self))))
    }
}

pub(crate) trait ErasedWorkflow: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn definition(&self) -> WorkflowDefinition;
    fn required_deps(&self) -> Vec<DepKey>;
    fn start(&self, core: Arc<Core>, input: Value) -> Driver;
}

fn decode_input<I: DeserializeOwned>(v: Value) -> Result<I, WorkflowError> {
    serde_json::from_value(v)
        .map_err(|e| WorkflowError::failed(format!("invalid workflow input: {e}")))
}

fn encode_output<O: Serialize>(o: O) -> Result<Value, WorkflowError> {
    serde_json::to_value(o)
        .map_err(|e| WorkflowError::failed(format!("workflow output is not serializable: {e}")))
}

pub(crate) struct AsyncAdapter<W>(pub Arc<W>);

impl<W: AsyncWorkflow> ErasedWorkflow for AsyncAdapter<W> {
    fn name(&self) -> &str {
        W::NAME
    }
    fn definition(&self) -> WorkflowDefinition {
        W::options().definition(W::NAME)
    }
    fn required_deps(&self) -> Vec<DepKey> {
        W::required_deps()
    }
    fn start(&self, core: Arc<Core>, input: Value) -> Driver {
        let (w, ctx) = (self.0.clone(), WfAsyncCtx::new(core.clone()));
        Driver::Async(AsyncDriver::new(
            core,
            Box::pin(async move {
                let out = w.run(ctx, decode_input(input)?).await?;
                encode_output(out)
            }),
        ))
    }
}

pub(crate) struct SyncAdapter<W>(pub Arc<W>);

impl<W: SyncWorkflow> ErasedWorkflow for SyncAdapter<W> {
    fn name(&self) -> &str {
        W::NAME
    }
    fn definition(&self) -> WorkflowDefinition {
        W::options().definition(W::NAME)
    }
    fn required_deps(&self) -> Vec<DepKey> {
        W::required_deps()
    }
    fn start(&self, core: Arc<Core>, input: Value) -> Driver {
        let w = self.0.clone();
        Driver::Sync(SyncDriver::spawn(
            core,
            Box::new(move |ctx| encode_output(w.run(ctx, decode_input(input)?)?)),
        ))
    }
}

pub(crate) struct AsyncFn<I, O, F> {
    pub name: String,
    pub options: WorkflowOptions,
    pub f: Arc<F>,
    pub _t: PhantomData<fn(I) -> O>,
}

impl<I, O, F, Fut> ErasedWorkflow for AsyncFn<I, O, F>
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(WfAsyncCtx, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, WorkflowError>> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn definition(&self) -> WorkflowDefinition {
        self.options.definition(&self.name)
    }
    fn required_deps(&self) -> Vec<DepKey> {
        vec![]
    }
    fn start(&self, core: Arc<Core>, input: Value) -> Driver {
        let (f, ctx) = (self.f.clone(), WfAsyncCtx::new(core.clone()));
        Driver::Async(AsyncDriver::new(
            core,
            Box::pin(async move { encode_output(f(ctx, decode_input(input)?).await?) }),
        ))
    }
}

pub(crate) struct SyncFn<I, O, F> {
    pub name: String,
    pub options: WorkflowOptions,
    pub f: Arc<F>,
    pub _t: PhantomData<fn(I) -> O>,
}

impl<I, O, F> ErasedWorkflow for SyncFn<I, O, F>
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(&WfSyncCtx, I) -> Result<O, WorkflowError> + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn definition(&self) -> WorkflowDefinition {
        self.options.definition(&self.name)
    }
    fn required_deps(&self) -> Vec<DepKey> {
        vec![]
    }
    fn start(&self, core: Arc<Core>, input: Value) -> Driver {
        let f = self.f.clone();
        Driver::Sync(SyncDriver::spawn(
            core,
            Box::new(move |ctx| encode_output(f(ctx, decode_input(input)?)?)),
        ))
    }
}
