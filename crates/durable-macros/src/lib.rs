//! Procedural macros for durable-workflow: `#[workflow]` and `#[step]`.
//!
//! These are re-exported by the `durable-workflow` crate; depend on that.

mod args;
mod step;
mod util;
mod workflow;

use proc_macro::TokenStream;

/// Turn a function into a workflow definition.
///
/// ```ignore
/// #[workflow(name = "user.onboarding", concurrency(limit = 4, key = "/id"),
///            throttle(limit = 100, period = "1m"), timeout = "1h")]
/// async fn onboarding(ctx: WfAsyncCtx, mailer: Dep<dyn Mailer>, user: Signup)
///     -> Result<Welcome, WorkflowError> { ... }
/// ```
///
/// Accepts `async fn(ctx: WfAsyncCtx, ..)` or `fn(ctx: &WfSyncCtx, ..)`. Arguments typed
/// `Dep<T>` are injected; the one remaining argument is the input. Generates a unit
/// struct named after the function in `PascalCase` (here `Onboarding`) implementing
/// `AsyncWorkflow` / `SyncWorkflow`, which also records each `Dep<T>` so
/// `Worker::build()` can fail fast when one is missing.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    workflow::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Turn a function into a durable step callable from a workflow.
///
/// ```ignore
/// #[step(retry = 5, timeout = "30s")]
/// async fn notify_sales(slack: Dep<Slack>, user_id: UserId) -> Result<(), StepError> { ... }
/// // call site:  notify_sales(&ctx, user_id).await?;
/// ```
///
/// The generated function takes the context first, followed by the non-`Dep`
/// arguments, and runs the body as a memoized step named after the function.
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    step::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
