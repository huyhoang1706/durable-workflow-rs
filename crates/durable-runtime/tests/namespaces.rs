//! Two namespaces sharing one set of backends must not see or touch each
//! other's runs, hooks or workflow definitions.

mod common;

use std::time::Duration;

use common::*;
use durable_core::{Error, RunStatus};
use durable_runtime::*;
use serde_json::{Value, json};

async fn approve(ctx: WfAsyncCtx, _: Value) -> Result<Value, WorkflowError> {
    ctx.wait_for_signal("approval").await
}

#[tokio::test]
async fn a_client_cannot_reach_runs_of_another_namespace() {
    let mut h = Harness::new();
    h.spawn(h.worker().register_fn("approve", approve));
    let id = h.start("approve", json!(null)).await;
    h.suspended(id).await;

    let mut other = h.backend.clone();
    other.namespace = "other".into();
    let foreign = Client::new(other);

    assert!(foreign.get_run(id).await.unwrap().is_none());
    assert!(foreign.in_flight(id).await.is_none());
    let not_found = |r: durable_core::Result<()>| matches!(r, Err(Error::NotFound(_)));
    assert!(matches!(foreign.events(id).await, Err(Error::NotFound(_))));
    assert!(not_found(
        foreign.signal(id, "approval", json!("evil")).await
    ));
    assert!(not_found(foreign.cancel(id, None).await));
    assert!(not_found(foreign.retry(id).await));
    assert!(matches!(
        foreign.handle(id).result::<Value>().await,
        Err(RunError::NotFound)
    ));

    // The run is untouched and still answers to its own namespace.
    let run = h.run(id).await;
    assert_eq!(run.status, RunStatus::Waiting);
    assert!(run.cancel_requested_at.is_none());
    h.client.signal(id, "approval", json!("ok")).await.unwrap();
    assert_eq!(h.succeeded(id).await, json!("ok"));
}

#[tokio::test]
async fn hook_tokens_resolve_only_in_their_namespace() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("hooked", |ctx: WfAsyncCtx, _: Value| async move {
                let hook = ctx
                    .create_hook_with_token::<Value>("cb", "invoice-7")
                    .await?;
                hook.recv().await
            }),
    );
    let id = h.start("hooked", json!(null)).await;
    h.suspended(id).await;

    let mut other = h.backend.clone();
    other.namespace = "other".into();
    let err = Client::new(other)
        .resume_hook("invoice-7", json!("evil"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");

    h.client
        .resume_hook("invoice-7", json!("paid"))
        .await
        .unwrap();
    assert_eq!(h.succeeded(id).await, json!("paid"));
}

#[tokio::test]
async fn each_namespace_keeps_its_own_workflow_definition() {
    let mut h = Harness::new();
    let mut other = h.backend.clone();
    other.namespace = "other".into();
    // Same workflow name; only the default namespace sets a timeout.
    h.spawn(h.worker().register_fn_with(
        WorkflowOptions::default().timeout(Duration::from_secs(60)),
        "approve",
        approve,
    ));
    let other_worker = Worker::builder(other.clone())
        .register_fn("approve", approve)
        .build()
        .unwrap()
        .spawn();
    other_worker.ready().await;

    let here = h.start("approve", json!(null)).await;
    let there = Client::new(other)
        .start_raw("approve", json!(null), StartOptions::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .id();
    assert!(h.run(here).await.deadline_at.is_some());
    let there = h.backend.store.get_run(there).await.unwrap().unwrap();
    assert_eq!(there.namespace, "other");
    assert_eq!(there.deadline_at, None);
    other_worker.abort();
}
