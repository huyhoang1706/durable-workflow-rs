#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[workflow(name = "a")]
async fn wf(ctx: WfAsyncCtx, (a, b): (u32, u32)) -> Result<u32, WorkflowError> { let _ = ctx; Ok(a + b) }
fn main() {}
