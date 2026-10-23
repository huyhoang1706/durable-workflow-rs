#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[workflow(name = "a")]
async fn wf(&self, ctx: WfAsyncCtx) -> Result<u32, WorkflowError> { let _ = ctx; Ok(1) }
fn main() {}
