mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use common::*;
use durable_runtime::*;
use parking_lot::Mutex;
use serde_json::{Value, json};

trait Mailer: Send + Sync {
    fn send(&self, to: &str, idempotency_key: &str);
}

#[derive(Default)]
struct FakeMailer {
    sent: Mutex<Vec<(String, String)>>,
}

impl Mailer for FakeMailer {
    fn send(&self, to: &str, key: &str) {
        self.sent.lock().push((to.into(), key.into()));
    }
}

struct AppConfig {
    greeting: String,
}

#[tokio::test]
async fn ctx_dep_lookup_inside_steps_async_and_sync() {
    let mut h = Harness::new();
    let mailer = Arc::new(FakeMailer::default());
    h.spawn(
        h.worker()
            .dep::<dyn Mailer>(mailer.clone())
            .register_fn("a", |ctx: WfAsyncCtx, to: String| async move {
                let m = ctx.dep::<dyn Mailer>()?;
                ctx.step("send", move || async move {
                    m.send(&to, step::current().idempotency_key());
                    Ok::<_, StepError>(())
                })
                .await?;
                Ok(())
            })
            .register_sync_fn("s", |ctx: &WfSyncCtx, to: String| {
                let m = ctx.dep::<dyn Mailer>()?;
                ctx.step("send", move || {
                    m.send(&to, step::current().idempotency_key());
                    Ok::<_, StepError>(())
                })?;
                Ok(())
            }),
    );
    let a = h.start("a", json!("ann@x")).await;
    let s = h.start("s", json!("bob@x")).await;
    h.succeeded(a).await;
    h.succeeded(s).await;
    let sent = mailer.sent.lock().clone();
    assert_eq!(sent.len(), 2);
    assert!(
        sent.iter()
            .any(|(to, k)| to == "ann@x" && k == &format!("{a}:send"))
    );
    assert!(
        sent.iter()
            .any(|(to, k)| to == "bob@x" && k == &format!("{s}:send"))
    );
}

struct Invoice {
    prefix: String,
}

impl AsyncWorkflow for Invoice {
    const NAME: &'static str = "invoice";
    type Input = i64;
    type Output = String;

    async fn run(self: Arc<Self>, ctx: WfAsyncCtx, id: i64) -> Result<String, WorkflowError> {
        let n: i64 = ctx
            .step("charge", move || async move { Ok::<_, StepError>(id * 10) })
            .await?;
        Ok(format!("{}{n}", self.prefix))
    }
}

struct SyncInvoice {
    prefix: String,
}

impl SyncWorkflow for SyncInvoice {
    const NAME: &'static str = "sync-invoice";
    type Input = i64;
    type Output = String;

    fn run(self: Arc<Self>, ctx: &WfSyncCtx, id: i64) -> Result<String, WorkflowError> {
        let n: i64 = ctx.step("charge", move || Ok::<_, StepError>(id * 10))?;
        Ok(format!("{}{n}", self.prefix))
    }
}

#[tokio::test]
async fn constructor_injection_through_struct_fields() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register(Invoice {
                prefix: "inv-".into(),
            })
            .register_sync(SyncInvoice {
                prefix: "sinv-".into(),
            }),
    );
    let a = h.start("invoice", json!(4)).await;
    let b = h.start("sync-invoice", json!(5)).await;
    assert_eq!(h.succeeded(a).await, json!("inv-40"));
    assert_eq!(h.succeeded(b).await, json!("sinv-50"));
}

struct NeedsMailer;

impl AsyncWorkflow for NeedsMailer {
    const NAME: &'static str = "needs-mailer";
    type Input = ();
    type Output = ();

    fn required_deps() -> Vec<DepKey> {
        vec![DepKey::of::<dyn Mailer>()]
    }

    async fn run(self: Arc<Self>, _ctx: WfAsyncCtx, _: ()) -> Result<(), WorkflowError> {
        Ok(())
    }
}

#[test]
fn build_rejects_a_missing_required_dependency() {
    let h = Harness::new();
    let err = h
        .worker()
        .register(NeedsMailer)
        .build()
        .err()
        .expect("must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("needs-mailer") && msg.contains("Mailer"),
        "{msg}"
    );

    h.worker()
        .dep::<dyn Mailer>(Arc::new(FakeMailer::default()))
        .register(NeedsMailer)
        .build()
        .expect("builds once the dependency is registered");
}

#[test]
fn build_rejects_duplicate_workflow_names() {
    let h = Harness::new();
    let err = h
        .worker()
        .register_fn("dup", |_c: WfAsyncCtx, _: Value| async { Ok(()) })
        .register_fn("dup", |_c: WfAsyncCtx, _: Value| async { Ok(()) })
        .build()
        .err()
        .expect("must fail");
    assert!(err.to_string().contains("dup"));
}

#[tokio::test]
async fn effectful_dep_in_the_body_is_an_engine_fault_with_a_clear_message() {
    let mut h = Harness::new();
    let mailer = Arc::new(FakeMailer::default());
    h.spawn(h.worker().dep::<dyn Mailer>(mailer.clone()).register_fn(
        "bad",
        |ctx: WfAsyncCtx, _: Value| async move {
            let m = ctx.dep::<dyn Mailer>()?;
            m.send("x", "k"); // in the body: would re-send on every replay
            Ok(())
        },
    ));
    let id = h.start("bad", json!(null)).await;
    let run = h.until(id, "fault", |r| r.last_error.is_some()).await;
    let err = run.last_error.unwrap();
    assert!(
        err.contains("used outside a step") && err.contains("Mailer"),
        "{err}"
    );
    assert!(!run.status.is_terminal());
    assert!(mailer.sent.lock().is_empty());
}

#[tokio::test]
async fn pure_dep_is_usable_in_the_workflow_body() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .dep_pure(Arc::new(AppConfig {
                greeting: "hello".into(),
            }))
            .register_fn("pure", |ctx: WfAsyncCtx, name: String| async move {
                let cfg = ctx.dep::<AppConfig>()?;
                Ok(format!("{} {name}", cfg.greeting))
            }),
    );
    let id = h.start("pure", json!("ann")).await;
    assert_eq!(h.succeeded(id).await, json!("hello ann"));
}

#[tokio::test]
async fn missing_dep_at_runtime_is_a_fault_not_a_step_failure() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("nodep", |ctx: WfAsyncCtx, _: Value| async move {
                let _m = ctx.dep::<dyn Mailer>()?;
                Ok(())
            }),
    );
    let id = h.start("nodep", json!(null)).await;
    let run = h.until(id, "fault", |r| r.last_error.is_some()).await;
    assert!(run.last_error.unwrap().contains("not registered"));
    assert!(!run.status.is_terminal());
}

#[tokio::test]
async fn replays_do_not_resend_but_a_crash_before_commit_does() {
    let mut h = Harness::new();
    let mailer = Arc::new(FakeMailer::default());
    let invocations = Arc::new(AtomicUsize::new(0));

    // Forced replay: the send step is memoized across suspend/resume.
    let m = mailer.clone();
    let w1 = h.spawn(h.worker().dep::<dyn Mailer>(m).register_fn(
        "replayed",
        |ctx: WfAsyncCtx, _: Value| async move {
            let mailer = ctx.dep::<dyn Mailer>()?;
            ctx.step("send", move || async move {
                mailer.send("a", "k");
                Ok::<_, StepError>(())
            })
            .await?;
            ctx.sleep("nap", Duration::from_secs(60)).await?;
            Ok(())
        },
    ));
    let id = h.start("replayed", json!(null)).await;
    h.suspended(id).await;
    h.advance(60);
    h.succeeded(id).await;
    assert_eq!(
        mailer.sent.lock().len(),
        1,
        "exactly one send across a replay"
    );
    h.stop(w1).await;

    // Crash between send and commit: at least one send, and the run completes.
    let (m, inv) = (mailer.clone(), invocations.clone());
    let w2 = h.spawn(
        h.worker()
            .lease(Duration::from_secs(30))
            .dep::<dyn Mailer>(m)
            .register_fn("crashy", move |ctx: WfAsyncCtx, _: Value| {
                let inv = inv.clone();
                async move {
                    let mailer = ctx.dep::<dyn Mailer>()?;
                    ctx.step("send", move || async move {
                        mailer.send("b", "k2");
                        if inv.fetch_add(1, SeqCst) == 0 {
                            // The worker dies before the result is committed.
                            std::future::pending::<()>().await;
                        }
                        Ok::<_, StepError>(())
                    })
                    .await?;
                    Ok(())
                }
            }),
    );
    let id = h.start("crashy", json!(null)).await;
    let before = mailer.sent.lock().len();
    h.eventually("first send", || async { mailer.sent.lock().len() > before })
        .await;
    h.stop(w2).await; // drains, aborts the in-flight step and hands the run back

    h.spawn(
        h.worker()
            .lease(Duration::from_secs(30))
            .dep::<dyn Mailer>(mailer.clone())
            .register_fn("crashy", {
                let inv = invocations.clone();
                move |ctx: WfAsyncCtx, _: Value| {
                    let inv = inv.clone();
                    async move {
                        let mailer = ctx.dep::<dyn Mailer>()?;
                        ctx.step("send", move || async move {
                            mailer.send("b", "k2");
                            inv.fetch_add(1, SeqCst);
                            Ok::<_, StepError>(())
                        })
                        .await?;
                        Ok(())
                    }
                }
            }),
    );
    h.succeeded(id).await;
    let to_b = mailer
        .sent
        .lock()
        .iter()
        .filter(|(to, _)| to == "b")
        .count();
    assert!(
        to_b >= 2,
        "at-least-once: the step re-ran after the crash ({to_b})"
    );
}
