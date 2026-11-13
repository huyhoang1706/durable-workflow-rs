#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[step(timeout = "30 seconds")]
async fn charge(cents: u64) -> Result<u64, StepError> { Ok(cents) }
fn main() {}
