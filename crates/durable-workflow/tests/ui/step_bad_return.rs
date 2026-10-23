#![allow(unused_imports)]
use durable_workflow::{step, workflow, StepError, WfAsyncCtx, WorkflowError};

#[step]
async fn charge(cents: u64) -> u64 { cents }
fn main() {}
