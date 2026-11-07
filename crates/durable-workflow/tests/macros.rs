//! `#[workflow]`, `#[step]` and `Dep<T>` injection, on the memory backend.

use std::{sync::Arc, time::Duration};

use durable_spi::WorkflowDefinition;
use durable_testkit::{ManualClock, t0};
use durable_workflow::{
    AsyncWorkflow, Client, Dep, StepError, SyncWorkflow, WfAsyncCtx, WfSyncCtx, Worker,
    WorkflowError, step, workflow,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;

trait Mailer: Send + Sync {
    fn send(&self, to: &str);
}

#[derive(Default)]
struct FakeMailer(Mutex<Vec<String>>);

impl Mailer for FakeMailer {
    fn send(&self, to: &str) {
        self.0.lock().push(to.to_string());
    }
}

struct Slack(Mutex<Vec<u64>>);
struct AppConfig {
    greeting: String,
}

#[derive(Serialize, Deserialize)]
struct Signup {
    id: u64,
    name: String,
}

#[workflow(
    name = "user.onboarding",
    concurrency(limit = 4, key = "/id"),
    throttle(limit = 100, period = "1m"),
    timeout = "1h"
)]
async fn onboarding(
    ctx: WfAsyncCtx,
    mailer: Dep<dyn Mailer>,
    cfg: Dep<AppConfig>,
    user: Signup,
) -> Result<String, WorkflowError> {
    let greeting = format!("{} {}", cfg.greeting, user.name); // pure dep in the body
    let to = user.name.clone();
    ctx.step("send-welcome", async move {
        mailer.send(&to);
        Ok::<_, StepError>(())
    })
    .await?;
    notify_sales(&ctx, user.id).await?;
    Ok(greeting)
}

#[step(retry = 3, timeout = "30s")]
async fn notify_sales(slack: Dep<Slack>, user_id: u64) -> Result<(), StepError> {
    slack.0.lock().push(user_id);
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Batch {
    items: Vec<u32>,
}

#[workflow(name = "images.resize")]
fn resize(ctx: &WfSyncCtx, cfg: Dep<AppConfig>, batch: Batch) -> Result<String, WorkflowError> {
    let squares: Vec<u32> = ctx.map("sq", batch.items, |x: u32| Ok::<_, StepError>(x * x))?;
    let total = sum(ctx, squares)?;
    Ok(format!("{}:{total}", cfg.greeting))
}

#[step(retry = 2)]
fn sum(values: Vec<u32>) -> Result<u32, StepError> {
    Ok(values.iter().sum())
}

#[workflow(name = "no.input")]
async fn no_input(_ctx: WfAsyncCtx) -> Result<u8, WorkflowError> {
    Ok(7)
}

fn worker_with_deps(
    clock: Arc<ManualClock>,
) -> (
    durable_workflow::WorkerBuilder,
    Arc<FakeMailer>,
    Arc<Slack>,
    Client,
) {
    let backend = durable_workflow::memory::backend_with_clock(clock);
    let mailer = Arc::new(FakeMailer::default());
    let slack = Arc::new(Slack(Mutex::default()));
    let w = Worker::builder(backend.clone())
        .poll_interval(Duration::from_millis(20))
        .dep::<dyn Mailer>(mailer.clone())
        .dep(slack.clone())
        .dep_pure(Arc::new(AppConfig {
            greeting: "hello".into(),
        }));
    (w, mailer, slack, Client::new(backend))
}

#[tokio::test]
async fn async_workflow_with_injected_deps_and_a_step_fn() {
    let (w, mailer, slack, client) = worker_with_deps(ManualClock::new(t0()));
    let handle = w.register(onboarding).build().unwrap().spawn();
    let out = client
        .start(
            onboarding,
            &Signup {
                id: 9,
                name: "ann".into(),
            },
            Default::default(),
        )
        .await
        .unwrap()
        .handle()
        .unwrap()
        .result::<String>()
        .await
        .unwrap();
    assert_eq!(out, "hello ann");
    assert_eq!(mailer.0.lock().as_slice(), ["ann"]);
    assert_eq!(slack.0.lock().as_slice(), [9]);
    handle.shutdown().await;
}

#[tokio::test]
async fn sync_workflow_with_injected_deps_and_a_sync_step_fn() {
    let (w, _, _, client) = worker_with_deps(ManualClock::new(t0()));
    let handle = w.register(resize).build().unwrap().spawn();
    let out = client
        .start_raw(
            "images.resize",
            json!({"items": [1, 2, 3]}),
            Default::default(),
        )
        .await
        .unwrap()
        .handle()
        .unwrap()
        .result::<String>()
        .await
        .unwrap();
    assert_eq!(out, "hello:14");
    handle.shutdown().await;
}

#[tokio::test]
async fn workflow_without_an_input_argument() {
    let (w, _, _, client) = worker_with_deps(ManualClock::new(t0()));
    let handle = w.register(no_input).build().unwrap().spawn();
    let out = client
        .start_raw("no.input", json!(null), Default::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .result::<u8>()
        .await
        .unwrap();
    assert_eq!(out, 7);
    handle.shutdown().await;
}

#[test]
fn macro_records_names_options_and_required_deps() {
    assert_eq!(onboarding::NAME, "user.onboarding");
    assert_eq!(resize::NAME, "images.resize");
    let o = onboarding::options().definition(onboarding::NAME);
    let expected = WorkflowDefinition {
        name: "user.onboarding".into(),
        concurrency: Some(durable_spi::ConcurrencySpec {
            limit: 4,
            key: Some("/id".into()),
        }),
        throttle: Some(durable_spi::ThrottleSpec {
            limit: 100,
            period_secs: 60,
            burst: 1,
            key: None,
        }),
        rate_limit: None,
        timeout_secs: Some(3600),
    };
    assert_eq!(o, expected);
    let names: Vec<_> = onboarding::required_deps().iter().map(|d| d.name).collect();
    assert_eq!(names.len(), 2);
    assert!(
        names.iter().any(|n| n.contains("Mailer")) && names.iter().any(|n| n.contains("AppConfig"))
    );
    let _ = <resize as SyncWorkflow>::required_deps();
}

#[test]
fn build_rejects_a_workflow_whose_macro_declared_dep_is_missing() {
    let backend = durable_workflow::memory::backend();
    let err = Worker::builder(backend)
        .dep_pure(Arc::new(AppConfig {
            greeting: "x".into(),
        }))
        .register(onboarding)
        .build()
        .err()
        .expect("Mailer is not registered");
    assert!(err.to_string().contains("Mailer"), "{err}");
}
