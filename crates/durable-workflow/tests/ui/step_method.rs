#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[step]
async fn charge(&self, cents: u64) -> Result<u64, StepError> { Ok(cents) }
fn main() {}
