//! The engine side of a TypeScript workflow: [`JsWorkflow`] and
//! [`JsExecution`] implement `durable_runtime::host` by calling into
//! JavaScript.
//!
//! One [`JsHost`] per worker holds the `ThreadsafeFunction`s. The TypeScript
//! side keeps a table of executions by id; every call names the execution it
//! is for:
//!
//! | Rust                         | JavaScript                                     |
//! |------------------------------|------------------------------------------------|
//! | first `activate`             | `start(id, workflow, runId, input, replayer)`  |
//! | `activate(resolved)`         | `activate(id, resolved)` → `Promise<status>`   |
//! | `start_step(key, attempt)`   | `runStep(id, key, attempt, step)` → `Promise`  |
//! | attempt dropped or timed out | `abortStep(id, key, attempt)`                  |
//! | `interrupt`                  | `dispose(id)`                                  |
//!
//! The functions are weak: they do not keep Node alive by themselves. A
//! running worker keeps the event loop alive from TypeScript instead, so a
//! worker that was never started, or a replay, cannot hang the process.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use durable_runtime::{
    StepError,
    host::{ExecEnv, ExecStatus, HostExecution, HostWorkflow, ReplayHandle, StepAttempt},
    step::StepInfo,
};
use durable_sm::{ErrorClass, Handle, Resolution, RunId, WorkflowFailure};
use durable_spi::WorkflowDefinition;
use napi::{
    Status,
    bindgen_prelude::{FnArgs, Promise},
    threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode, UnknownReturnValue},
};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::Value;

use crate::{from_json, replayer::Replayer, resolved_json};

/// A weak `ThreadsafeFunction` whose caller handles errors (no error-first
/// callback in JavaScript).
type Tsfn<Args, Ret> = ThreadsafeFunction<FnArgs<Args>, Ret, FnArgs<Args>, Status, false, true>;

pub type StartFn = Tsfn<(u32, String, String, Value, Replayer), UnknownReturnValue>;
pub type ActivateFn = Tsfn<(u32, Value), Promise<Value>>;
pub type RunStepFn = Tsfn<(u32, String, u32, StepHandle), Promise<Value>>;
pub type AbortStepFn = Tsfn<(u32, String, u32), UnknownReturnValue>;
pub type DisposeFn = ThreadsafeFunction<u32, UnknownReturnValue, u32, Status, false, true>;

/// The JavaScript side of one worker.
pub struct JsHost {
    pub start: StartFn,
    pub activate: ActivateFn,
    pub run_step: RunStepFn,
    pub abort_step: AbortStepFn,
    pub dispose: DisposeFn,
    next_id: AtomicU32,
}

impl JsHost {
    pub fn new(
        start: StartFn,
        activate: ActivateFn,
        run_step: RunStepFn,
        abort_step: AbortStepFn,
        dispose: DisposeFn,
    ) -> Self {
        Self {
            start,
            activate,
            run_step,
            abort_step,
            dispose,
            next_id: AtomicU32::new(1),
        }
    }

    fn abort(&self, id: u32, key: &str, attempt: u32) {
        self.abort_step.call(
            FnArgs::from((id, key.to_string(), attempt)),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

/// A workflow registered from TypeScript.
pub struct JsWorkflow {
    pub name: String,
    pub definition: WorkflowDefinition,
    pub host: Arc<JsHost>,
}

impl HostWorkflow for JsWorkflow {
    fn name(&self) -> &str {
        &self.name
    }

    fn definition(&self) -> WorkflowDefinition {
        self.definition.clone()
    }

    fn start(&self, env: ExecEnv) -> Box<dyn HostExecution> {
        // No JavaScript runs here: the body starts on the first activation.
        Box::new(JsExecution {
            id: self.host.next_id.fetch_add(1, Ordering::Relaxed),
            workflow: self.name.clone(),
            run_id: env.run_id,
            input: Some(env.input),
            replay: env.replay,
            host: self.host.clone(),
            disposed: false,
        })
    }
}

/// How a TypeScript body ended, as `activate` reports it.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Finish {
    Ok(Value),
    Err(WorkflowFailure),
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Activation {
    Pending,
    Finished { result: Finish },
}

/// A step attempt's outcome, as `runStep` reports it.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Ok(Value),
    Err { message: String, class: ErrorClass },
}

pub struct JsExecution {
    id: u32,
    workflow: String,
    run_id: RunId,
    /// Taken by the first activation, which starts the body.
    input: Option<Value>,
    replay: ReplayHandle,
    host: Arc<JsHost>,
    disposed: bool,
}

impl JsExecution {
    fn dispose(&mut self) {
        if !std::mem::replace(&mut self.disposed, true) {
            self.host
                .dispose
                .call(self.id, ThreadsafeFunctionCallMode::NonBlocking);
        }
    }

    async fn try_activate(
        &mut self,
        resolved: Vec<(Handle, Resolution)>,
    ) -> napi::Result<ExecStatus> {
        if let Some(input) = self.input.take() {
            let replayer = Replayer::from_handle(self.replay.clone());
            let args = (
                self.id,
                self.workflow.clone(),
                self.run_id.to_string(),
                input,
                replayer,
            );
            self.host.start.call_async_catch(args.into()).await?;
        }
        let resolved = resolved_json(resolved)?;
        let status = self
            .host
            .activate
            .call_async_catch((self.id, resolved).into())
            .await?
            .await?;
        Ok(
            match from_json::<Activation>("activation status", status)? {
                Activation::Pending => ExecStatus::Pending,
                Activation::Finished {
                    result: Finish::Ok(v),
                } => ExecStatus::Finished(Ok(v)),
                Activation::Finished {
                    result: Finish::Err(f),
                } => ExecStatus::Finished(Err(f)),
            },
        )
    }
}

#[async_trait]
impl HostExecution for JsExecution {
    async fn activate(&mut self, resolved: Vec<(Handle, Resolution)>) -> ExecStatus {
        match self.try_activate(resolved).await {
            Ok(status) => status,
            Err(e) => {
                // The body cannot be trusted any more (the host threw, or Node
                // is shutting down): fault the run so it is retried later.
                let msg = format!("typescript host: activation failed: {}", e.reason);
                self.replay.with(|r| r.set_fault(msg));
                ExecStatus::Pending
            }
        }
    }

    fn start_step(
        &mut self,
        key: &str,
        attempt: u32,
        info: StepInfo,
        _span: &tracing::Span,
    ) -> Option<StepAttempt> {
        let (host, id, key) = (self.host.clone(), self.id, key.to_string());
        Some(StepAttempt {
            kind: "typescript",
            fut: Box::pin(async move {
                let mut guard = AbortGuard {
                    host: host.clone(),
                    id,
                    key: key.clone(),
                    attempt,
                    armed: true,
                };
                let cancelled = info.clone();
                let call = async {
                    let args = (id, key.clone(), attempt, StepHandle { info });
                    host.run_step.call_async_catch(args.into()).await?.await
                };
                tokio::pin!(call);
                // The engine flags a timed-out attempt without dropping it:
                // abort the step's `AbortSignal` then, and keep waiting.
                let result = loop {
                    tokio::select! {
                        r = &mut call => break r,
                        _ = tokio::time::sleep(Duration::from_millis(50)), if guard.armed => {
                            if cancelled.is_cancelled() {
                                host.abort(id, &key, attempt);
                                guard.armed = false;
                            }
                        }
                    }
                };
                guard.armed = false;
                let outcome = result.map_err(|e| {
                    StepError::retryable(format!("typescript step failed to run: {}", e.reason))
                })?;
                match from_json::<Outcome>("step outcome", outcome)
                    .map_err(|e| StepError::fatal(e.reason))?
                {
                    Outcome::Ok(v) => Ok(v),
                    Outcome::Err { message, class } => Err(StepError { message, class }),
                }
            }),
        })
    }

    fn interrupt(&mut self) {
        // A body still running (it awaited something other than a ctx call)
        // sees `InterruptedError` from its next ctx call.
        self.replay.with(|r| r.interrupt());
        self.dispose();
    }
}

impl Drop for JsExecution {
    /// An execution can be dropped without `interrupt` when the engine's task
    /// is aborted (a crash, `Worker.abort()`): free its JavaScript side anyway.
    fn drop(&mut self) {
        self.dispose();
    }
}

/// Aborts the attempt's `AbortSignal` when the engine drops the attempt
/// (reaped, or the execution ended) before JavaScript answered.
struct AbortGuard {
    host: Arc<JsHost>,
    id: u32,
    key: String,
    attempt: u32,
    armed: bool,
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        if self.armed {
            self.host.abort(self.id, &self.key, self.attempt);
        }
    }
}

/// What a step body can ask the engine while it runs.
#[napi]
pub struct StepHandle {
    info: StepInfo,
}

#[napi]
impl StepHandle {
    /// `run_id:key`, stable across retries and replays.
    #[napi]
    pub fn idempotency_key(&self) -> String {
        self.info.idempotency_key().to_string()
    }

    /// Tell the engine a long attempt is alive.
    #[napi]
    pub fn heartbeat(&self) {
        self.info.heartbeat();
    }

    /// True once the engine gave up on this attempt (timeout, cancel).
    #[napi]
    pub fn is_cancelled(&self) -> bool {
        self.info.is_cancelled()
    }
}
