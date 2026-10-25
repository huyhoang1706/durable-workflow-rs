use std::time::Duration;

use chrono::{DateTime, Utc};
use durable_core::{
    Backend, CreateOutcome, Event, Metric, NewRun, QueueItem, RateDecision, Run, RunFilter, RunId,
    RunStatus, WorkflowDefinition, saturating_add,
};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::Instrument;

use crate::channels;

#[derive(Debug, Clone, Default)]
pub struct StartOptions {
    pub idempotency_key: Option<String>,
}

impl StartOptions {
    /// Starting twice with the same key (per workflow) yields the same run.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("run failed: {0}")]
    Failed(String),
    #[error("run was cancelled")]
    Cancelled,
    #[error("run not found")]
    NotFound,
    #[error("result does not match the requested type: {0}")]
    Decode(String),
    #[error(transparent)]
    Backend(#[from] durable_core::Error),
}

#[derive(Clone)]
pub struct RunHandle {
    id: RunId,
    backend: Backend,
}

impl RunHandle {
    pub fn id(&self) -> RunId {
        self.id
    }

    /// Wait for the run to finish. Uses pub/sub with a polling fallback.
    pub async fn result<T: DeserializeOwned>(&self) -> Result<T, RunError> {
        // Subscribe first so a finish between the check and the wait is seen.
        let mut sub = self
            .backend
            .pubsub
            .subscribe(&channels::run(self.id))
            .await
            .ok();
        loop {
            let run = self
                .backend
                .store
                .get_run(self.id)
                .await?
                .filter(|r| r.namespace == self.backend.namespace)
                .ok_or(RunError::NotFound)?;
            match run.status {
                RunStatus::Succeeded => {
                    let out = run.output.unwrap_or(Value::Null);
                    return serde_json::from_value(out)
                        .map_err(|e| RunError::Decode(e.to_string()));
                }
                RunStatus::Failed => return Err(RunError::Failed(run.error.unwrap_or_default())),
                RunStatus::Cancelled => return Err(RunError::Cancelled),
                _ => {}
            }
            let poll = tokio::time::sleep(Duration::from_millis(200));
            match sub.as_mut() {
                Some(s) => {
                    tokio::select! {
                        m = s.next() => if m.is_none() { sub = None },
                        _ = poll => {}
                    }
                }
                None => poll.await,
            }
        }
    }
}

#[derive(Clone)]
pub enum StartOutcome {
    Started(RunHandle),
    /// The idempotency key matched an existing run.
    Existing(RunHandle),
    /// Start-time rate limit exceeded; try again at `retry_at`.
    RateLimited {
        retry_at: DateTime<Utc>,
    },
}

impl StartOutcome {
    pub fn handle(&self) -> Option<&RunHandle> {
        match self {
            Self::Started(h) | Self::Existing(h) => Some(h),
            Self::RateLimited { .. } => None,
        }
    }
}

/// Talks to the backends directly; works without a worker (e.g. in a web tier).
///
/// Scoped to the backend's namespace: runs, hooks and definitions of other
/// namespaces are invisible to it and every by-id call treats them as missing.
#[derive(Clone)]
pub struct Client {
    backend: Backend,
}

impl Client {
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    fn now(&self) -> DateTime<Utc> {
        self.backend.clock.now()
    }

    /// The run, if it exists in this client's namespace.
    async fn scoped_run(&self, id: RunId) -> durable_core::Result<Run> {
        self.get_run(id)
            .await?
            .ok_or_else(|| durable_core::Error::NotFound(format!("run {id}")))
    }

    async fn enqueue(&self, items: Vec<QueueItem>) -> durable_core::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let workflows: std::collections::HashSet<_> = items
            .iter()
            .map(|i| (i.namespace.clone(), i.workflow.clone()))
            .collect();
        self.backend.queue.enqueue(items).await?;
        for (ns, wf) in workflows {
            let _ = self
                .backend
                .pubsub
                .publish(&channels::nudge(&ns, &wf), vec![])
                .await;
        }
        Ok(())
    }

    pub async fn start_raw(
        &self,
        workflow: &str,
        input: Value,
        opts: StartOptions,
    ) -> durable_core::Result<StartOutcome> {
        let span = tracing::info_span!(
            "workflow.start",
            otel.kind = "producer",
            durable.workflow = workflow,
            durable.namespace = %self.backend.namespace,
            durable.run_id = tracing::field::Empty,
        );
        self.start_traced(workflow, input, opts)
            .instrument(span)
            .await
    }

    async fn start_traced(
        &self,
        workflow: &str,
        input: Value,
        opts: StartOptions,
    ) -> durable_core::Result<StartOutcome> {
        let ns = self.backend.namespace.clone();
        let def = self
            .backend
            .store
            .get_definition(&ns, workflow)
            .await?
            .unwrap_or_else(|| WorkflowDefinition::new(workflow));
        let now = self.now();
        let (concurrency_key, throttle_key) = def.flow_keys(&input);

        if let (Some(spec), Some(limiter)) = (&def.rate_limit, &self.backend.rate_limiter) {
            let bucket = spec
                .key
                .as_deref()
                .and_then(|p| input.pointer(p))
                .map_or_else(|| "-".to_string(), |v| v.to_string());
            let key = format!("dw:rl:{ns}:{workflow}:{bucket}");
            let decision = limiter
                .check(&key, spec.limit, Duration::from_secs(spec.period_secs), now)
                .await?;
            if let RateDecision::Denied { retry_at } = decision {
                return Ok(StartOutcome::RateLimited { retry_at });
            }
        }

        let created = self
            .backend
            .store
            .create_run(NewRun {
                namespace: ns,
                workflow: workflow.to_string(),
                input,
                idempotency_key: opts.idempotency_key,
                concurrency_key,
                throttle_key,
                deadline_at: def
                    .timeout_secs
                    .map(|s| saturating_add(now, Duration::from_secs(s))),
                // Later executions continue this trace.
                trace_context: Some(self.backend.observer.inject(&tracing::Span::current()))
                    .filter(|c| !c.is_empty()),
                at: now,
            })
            .await?;
        tracing::Span::current()
            .record("durable.run_id", tracing::field::display(created.run().id));
        let handle = RunHandle {
            id: created.run().id,
            backend: self.backend.clone(),
        };
        match created {
            CreateOutcome::Created(run) => {
                self.backend.observer.record(&Metric::RunStarted {
                    namespace: &run.namespace,
                    workflow: &run.workflow,
                });
                self.enqueue(vec![run.queue_item(now)]).await?;
                Ok(StartOutcome::Started(handle))
            }
            CreateOutcome::Existing(_) => Ok(StartOutcome::Existing(handle)),
        }
    }

    pub async fn start<W: crate::AsyncWorkflow>(
        &self,
        input: &impl serde::Serialize,
        opts: StartOptions,
    ) -> durable_core::Result<StartOutcome> {
        let input = serde_json::to_value(input)
            .map_err(|e| durable_core::Error::Config(format!("input is not serializable: {e}")))?;
        self.start_raw(W::NAME, input, opts).await
    }

    pub fn handle(&self, id: RunId) -> RunHandle {
        RunHandle {
            id,
            backend: self.backend.clone(),
        }
    }

    pub async fn get_run(&self, id: RunId) -> durable_core::Result<Option<Run>> {
        let run = self.backend.store.get_run(id).await?;
        Ok(run.filter(|r| r.namespace == self.backend.namespace))
    }

    /// Execution state of a run that is being executed right now, if the
    /// backend has a cache and a worker published it.
    pub async fn in_flight(&self, id: RunId) -> Option<crate::InFlight> {
        let cache = self.backend.cache.as_deref()?;
        self.get_run(id).await.ok()??;
        crate::inflight::read(cache, id).await
    }

    pub async fn list_runs(&self, mut filter: RunFilter) -> durable_core::Result<Vec<Run>> {
        filter
            .namespace
            .get_or_insert_with(|| self.backend.namespace.clone());
        self.backend.store.list_runs(filter).await
    }

    pub async fn events(&self, id: RunId) -> durable_core::Result<Vec<Event>> {
        self.scoped_run(id).await?;
        self.backend.store.load_events(id, 0).await
    }

    pub async fn signal(&self, id: RunId, name: &str, payload: Value) -> durable_core::Result<()> {
        let span = tracing::info_span!(
            "workflow.signal",
            otel.kind = "producer",
            durable.run_id = %id,
            durable.signal = name,
        );
        async {
            self.scoped_run(id).await?;
            let item = self
                .backend
                .store
                .deliver_signal(id, name, payload, self.now())
                .await?;
            self.enqueue(item.into_iter().collect()).await
        }
        .instrument(span)
        .await
    }

    /// Webhook entry point: resolves the hook identified by `token`.
    pub async fn resume_hook(&self, token: &str, payload: Value) -> durable_core::Result<()> {
        let span = tracing::info_span!("workflow.resume_hook", otel.kind = "producer");
        async {
            let item = self
                .backend
                .store
                .resolve_hook(&self.backend.namespace, token, payload, self.now())
                .await?;
            self.enqueue(item.into_iter().collect()).await
        }
        .instrument(span)
        .await
    }

    /// Publish an event to every run waiting for it (matching by filter).
    pub async fn send_event(&self, name: &str, data: Value) -> durable_core::Result<usize> {
        let span = tracing::info_span!(
            "workflow.send_event",
            otel.kind = "producer",
            durable.event = name,
        );
        async {
            let items = self
                .backend
                .store
                .publish_event(&self.backend.namespace, name, data, self.now())
                .await?;
            let n = items.len();
            self.enqueue(items).await?;
            Ok(n)
        }
        .instrument(span)
        .await
    }

    pub async fn cancel(&self, id: RunId, reason: Option<String>) -> durable_core::Result<()> {
        let span =
            tracing::info_span!("workflow.cancel", otel.kind = "producer", durable.run_id = %id);
        async {
            self.scoped_run(id).await?;
            let item = self
                .backend
                .store
                .request_cancel(id, reason, self.now())
                .await?;
            self.enqueue(item.into_iter().collect()).await?;
            // Best effort: lets a worker mid-step react immediately.
            let _ = self
                .backend
                .pubsub
                .publish(&channels::cancel(id), vec![])
                .await;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Re-run a failed run from its failure point: succeeded steps stay
    /// memoized and failed steps get a fresh retry budget.
    pub async fn retry(&self, id: RunId) -> durable_core::Result<()> {
        self.scoped_run(id).await?;
        let item = self.backend.store.retry_run(id, self.now()).await?;
        self.enqueue(vec![item]).await
    }

    /// A live, resumable feed of the run's events:
    /// `client.watch(id).after(seq).await?` yields [`RunUpdate`]s in order,
    /// exactly once, and ends after the run's terminal event.
    ///
    /// Streamed chunks (see [`stream`](Self::stream)) are for rendering live;
    /// the durable answer arrives here as `StepSucceeded { output }`. Replace
    /// any partial text with it, which also recovers a client whose stream
    /// expired.
    pub fn watch(&self, id: RunId) -> crate::WatchBuilder {
        crate::WatchBuilder {
            backend: self.backend.clone(),
            id,
            after: 0,
        }
    }

    /// Tail a user stream a step writes with `step::current().stream(name)`.
    /// Chunks are transient (kept for the stream store's TTL); see
    /// [`StreamItem`](crate::StreamItem) for how failed attempts show up.
    pub fn stream<T: DeserializeOwned + Send + 'static>(
        &self,
        id: RunId,
        name: &str,
    ) -> crate::StreamReadBuilder<T> {
        crate::StreamReadBuilder {
            backend: self.backend.clone(),
            id,
            name: name.to_string(),
            after: None,
            _item: std::marker::PhantomData,
        }
    }

    /// The run's user streams that have not expired. Empty when the backend
    /// has no stream store.
    pub async fn streams(&self, id: RunId) -> durable_core::Result<Vec<durable_core::StreamInfo>> {
        self.scoped_run(id).await?;
        match &self.backend.streams {
            Some(store) => store.list(id).await,
            None => Ok(vec![]),
        }
    }
}
