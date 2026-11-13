#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[workflow]
async fn wf(ctx: WfAsyncCtx, n: u32) -> Result<u32, WorkflowError> { let _ = ctx; Ok(n) }
fn main() {}
