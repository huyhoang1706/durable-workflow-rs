//! The backend, the embedded worker and the client, for TypeScript.

use std::{sync::Arc, time::Duration};

use durable_runtime::{RunError, StartOptions, StartOutcome, WorkerHandle, WorkflowOptions};
use durable_spi::Backend as SpiBackend;
use napi::{Error, Result};
use napi_derive::napi;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    from_json,
    host::{AbortStepFn, ActivateFn, DisposeFn, JsHost, JsWorkflow, RunStepFn, StartFn},
    parse_run_id, to_json,
};

fn backend_err(e: durable_spi::Error) -> Error {
    Error::from_reason(e.to_string())
}

/// The stores, queue and pub/sub a worker and a client share.
#[napi(js_name = "Backend")]
pub struct JsBackend {
    inner: SpiBackend,
}

#[napi]
impl JsBackend {
    /// Everything in this process's memory; nothing survives a restart.
    #[napi(factory)]
    pub fn memory() -> Self {
        Self {
            inner: durable_memory::backend(),
        }
    }

    #[napi(getter)]
    pub fn namespace(&self) -> String {
        self.inner.namespace.clone()
    }
}

/// A workflow as the worker registers it: `{name, timeout_ms?}`.
#[derive(Deserialize)]
struct WorkflowSpec {
    name: String,
    timeout_ms: Option<u64>,
}

/// `WorkerBuilder` settings; every field is optional.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WorkerOptions {
    concurrency: Option<usize>,
    lease_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    reconcile_interval_ms: Option<u64>,
    inline_retry_threshold_ms: Option<u64>,
    max_deliveries: Option<i64>,
    max_payload_bytes: Option<usize>,
    drain_timeout_ms: Option<u64>,
    worker_id: Option<String>,
}

/// An embedded worker running TypeScript workflows.
#[napi(js_name = "Worker")]
pub struct JsWorker {
    backend: SpiBackend,
    workflows: Vec<Arc<JsWorkflow>>,
    options: Arc<WorkerOptions>,
    handle: Mutex<Option<WorkerHandle>>,
}

impl JsWorker {
    fn build(&self, backend: SpiBackend) -> Result<durable_runtime::Worker> {
        let o = &self.options;
        let ms = Duration::from_millis;
        let mut b = durable_runtime::Worker::builder(backend);
        for w in &self.workflows {
            b = b.register_host(w.clone());
        }
        if let Some(v) = o.concurrency {
            b = b.concurrency(v);
        }
        if let Some(v) = o.lease_ms {
            b = b.lease(ms(v));
        }
        if let Some(v) = o.poll_interval_ms {
            b = b.poll_interval(ms(v));
        }
        if let Some(v) = o.reconcile_interval_ms {
            b = b.reconcile_interval(ms(v));
        }
        if let Some(v) = o.inline_retry_threshold_ms {
            b = b.inline_retry_threshold(ms(v));
        }
        if let Some(v) = o.max_deliveries {
            b = b.max_deliveries(v);
        }
        if let Some(v) = o.max_payload_bytes {
            b = b.max_payload_bytes(v);
        }
        if let Some(v) = o.drain_timeout_ms {
            b = b.drain_timeout(ms(v));
        }
        if let Some(v) = &o.worker_id {
            b = b.worker_id(v.clone());
        }
        b.build().map_err(|e| Error::from_reason(e.to_string()))
    }
}

#[napi]
impl JsWorker {
    /// `workflows` is `[{name, timeout_ms?}]`; `options` holds `WorkerBuilder`
    /// settings in snake case with `*_ms` durations. The five functions are
    /// the TypeScript host (see `host.rs`).
    #[napi(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend: &JsBackend,
        workflows: Value,
        options: Value,
        start: StartFn,
        activate: ActivateFn,
        run_step: RunStepFn,
        abort_step: AbortStepFn,
        dispose: DisposeFn,
    ) -> Result<Self> {
        let specs: Vec<WorkflowSpec> = from_json("workflows", workflows)?;
        let options: WorkerOptions = if options.is_null() {
            WorkerOptions::default()
        } else {
            from_json("worker options", options)?
        };
        let host = Arc::new(JsHost::new(start, activate, run_step, abort_step, dispose));
        let workflows = specs
            .into_iter()
            .map(|s| {
                let mut opts = WorkflowOptions::default();
                if let Some(ms) = s.timeout_ms {
                    // Deadlines are stored in whole seconds: round up, so a
                    // short timeout never becomes zero.
                    opts = opts.timeout(Duration::from_secs(ms.div_ceil(1000)));
                }
                Arc::new(JsWorkflow {
                    definition: opts.definition(&s.name),
                    name: s.name,
                    host: host.clone(),
                })
            })
            .collect();
        Ok(Self {
            backend: backend.inner.clone(),
            workflows,
            options: Arc::new(options),
            handle: Mutex::new(None),
        })
    }

    /// Start claiming runs. Resolves once the workflow definitions are synced
    /// to the store.
    #[napi]
    pub async fn start(&self) -> Result<()> {
        let running = || Error::from_reason("the worker is already running");
        if self.handle.lock().is_some() {
            return Err(running());
        }
        let handle = self.build(self.backend.clone())?.spawn();
        handle.ready().await;
        // Two concurrent starts: keep the first.
        let extra = {
            let mut slot = self.handle.lock();
            if slot.is_some() {
                Some(handle)
            } else {
                *slot = Some(handle);
                None
            }
        };
        match extra {
            Some(h) => {
                h.shutdown().await;
                Err(running())
            }
            None => Ok(()),
        }
    }

    /// Stop claiming, drain in-flight runs, then hand back what is left.
    #[napi]
    pub async fn shutdown(&self) -> Result<()> {
        let handle = self.handle.lock().take();
        if let Some(h) = handle {
            h.shutdown().await;
        }
        Ok(())
    }

    /// Simulate a crash: stop at once without acking or handing back leases.
    /// For tests.
    #[napi]
    pub fn abort(&self) {
        if let Some(h) = self.handle.lock().take() {
            h.abort();
        }
    }

    /// Replay a stored history through a registered workflow the way the
    /// engine does, with recording on: `{trace, frontier, fault, result}`.
    /// No store and no step execution. Rejects for an unknown workflow.
    #[napi]
    pub async fn replay(
        &self,
        workflow: String,
        run_id: String,
        input: Value,
        history: Value,
    ) -> Result<Value> {
        let run_id = parse_run_id(&run_id)?;
        let history = from_json("history", history)?;
        let worker = self.build(durable_memory::backend())?;
        let replay = worker
            .replay(&workflow, run_id, input, history)
            .await
            .ok_or_else(|| Error::from_reason(format!("unknown workflow `{workflow}`")))?;
        let result = match replay.result {
            None => Value::Null,
            Some(Ok(v)) => json!({ "ok": v }),
            Some(Err(f)) => json!({ "err": to_json(&f)? }),
        };
        Ok(json!({
            "trace": to_json(&replay.trace)?,
            "frontier": to_json(&replay.frontier)?,
            "fault": replay.fault,
            "result": result,
        }))
    }
}

/// Starts runs and talks to them; works without a worker.
#[napi(js_name = "Client")]
pub struct JsClient {
    inner: durable_runtime::Client,
}

#[napi]
impl JsClient {
    #[napi(constructor)]
    pub fn new(backend: &JsBackend) -> Self {
        Self {
            inner: durable_runtime::Client::new(backend.inner.clone()),
        }
    }

    /// `{outcome: "started" | "existing", run_id}` or
    /// `{outcome: "rate_limited", retry_at}`.
    #[napi]
    pub async fn start(
        &self,
        workflow: String,
        input: Value,
        idempotency_key: Option<String>,
    ) -> Result<Value> {
        let opts = StartOptions { idempotency_key };
        let out = self
            .inner
            .start_raw(&workflow, input, opts)
            .await
            .map_err(backend_err)?;
        Ok(match out {
            StartOutcome::Started(h) => json!({"outcome": "started", "run_id": h.id().to_string()}),
            StartOutcome::Existing(h) => {
                json!({"outcome": "existing", "run_id": h.id().to_string()})
            }
            StartOutcome::RateLimited { retry_at } => {
                json!({"outcome": "rate_limited", "retry_at": to_json(&retry_at)?})
            }
        })
    }

    /// Wait for the run to finish: `{ok: output}` or
    /// `{err: {kind: "failed" | "cancelled" | "not_found", message}}`.
    #[napi]
    pub async fn result(&self, run_id: String) -> Result<Value> {
        let h = self.inner.handle(parse_run_id(&run_id)?);
        Ok(match h.result::<Value>().await {
            Ok(v) => json!({ "ok": v }),
            Err(RunError::Failed(m)) => json!({"err": {"kind": "failed", "message": m}}),
            Err(RunError::Cancelled) => {
                json!({"err": {"kind": "cancelled", "message": "run was cancelled"}})
            }
            Err(RunError::NotFound) => {
                json!({"err": {"kind": "not_found", "message": format!("run {run_id} not found")}})
            }
            Err(e) => return Err(Error::from_reason(e.to_string())),
        })
    }

    /// The run as stored, or `null` if this namespace has no such run.
    #[napi]
    pub async fn get_run(&self, run_id: String) -> Result<Value> {
        let run = self
            .inner
            .get_run(parse_run_id(&run_id)?)
            .await
            .map_err(backend_err)?;
        to_json(&run)
    }

    /// The run's history, as stored.
    #[napi]
    pub async fn events(&self, run_id: String) -> Result<Value> {
        let events = self
            .inner
            .events(parse_run_id(&run_id)?)
            .await
            .map_err(backend_err)?;
        to_json(&events)
    }

    #[napi]
    pub async fn signal(&self, run_id: String, name: String, payload: Value) -> Result<()> {
        self.inner
            .signal(parse_run_id(&run_id)?, &name, payload)
            .await
            .map_err(backend_err)
    }

    /// Publish an event to every run waiting for it; returns how many matched.
    #[napi]
    pub async fn send_event(&self, name: String, data: Value) -> Result<u32> {
        let n = self
            .inner
            .send_event(&name, data)
            .await
            .map_err(backend_err)?;
        Ok(u32::try_from(n).unwrap_or(u32::MAX))
    }

    #[napi]
    pub async fn resume_hook(&self, token: String, payload: Value) -> Result<()> {
        self.inner
            .resume_hook(&token, payload)
            .await
            .map_err(backend_err)
    }

    #[napi]
    pub async fn cancel(&self, run_id: String, reason: Option<String>) -> Result<()> {
        self.inner
            .cancel(parse_run_id(&run_id)?, reason)
            .await
            .map_err(backend_err)
    }
}
