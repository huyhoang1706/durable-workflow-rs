#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[workflow(name = "a")]
async fn wf() -> Result<u32, WorkflowError> { Ok(1) }
fn main() {}
