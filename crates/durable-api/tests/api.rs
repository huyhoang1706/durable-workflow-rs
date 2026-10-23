use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use durable_api::router;
use durable_runtime::{Client, StepError, WfAsyncCtx, Worker, WorkerHandle, WorkflowError};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

static GATE_OPEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

struct Env {
    app: Router,
    client: Client,
    worker: WorkerHandle,
}

async fn env() -> Env {
    let backend = durable_memory::backend();
    let worker = Worker::builder(backend.clone())
        .poll_interval(Duration::from_millis(20))
        .drain_timeout(Duration::from_millis(200))
        .register_fn("echo", |ctx: WfAsyncCtx, v: Value| async move {
            let out: Value = ctx
                .step("s", move || async move { Ok::<_, StepError>(v) })
                .await?;
            Ok(out)
        })
        .register_fn("approve", |ctx: WfAsyncCtx, _: Value| async move {
            let v: Value = ctx.wait_for_signal("approval").await?;
            Ok(v)
        })
        .register_fn("hooked", |ctx: WfAsyncCtx, _: Value| async move {
            let hook = ctx.create_hook::<Value>("cb").await?;
            hook.recv().await
        })
        .register_fn("order", |ctx: WfAsyncCtx, id: i64| async move {
            let v: Value = ctx
                .wait_for_event("order.paid")
                .filter(json!({"order_id": id}))
                .await?;
            Ok(v)
        })
        .register_fn("bad", |_ctx: WfAsyncCtx, _: Value| async {
            Err::<(), _>(WorkflowError::failed("nope"))
        })
        .register_fn("gated", |ctx: WfAsyncCtx, _: Value| async move {
            ctx.step("work", || async {
                while !GATE_OPEN.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, StepError>(1)
            })
            .await?;
            Ok(())
        })
        .register_fn("sleeper", |ctx: WfAsyncCtx, _: Value| async move {
            ctx.sleep("z", Duration::from_secs(3600)).await?;
            Ok(())
        })
        .build()
        .unwrap()
        .spawn();
    worker.ready().await;
    let client = Client::new(backend);
    Env {
        app: router(client.clone()),
        client,
        worker,
    }
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(b) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        // Probes answer in plain text.
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
    };
    (status, json)
}

async fn until(app: &Router, id: &str, pred: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..400 {
        let (_, run) = call(app, "GET", &format!("/v1/runs/{id}"), None).await;
        if pred(&run) {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting on run {id}");
}

#[tokio::test]
async fn get_run_overlays_in_flight_execution_state_from_the_cache() {
    let e = env().await;
    let (_, body) = call(&e.app, "POST", "/v1/workflows/gated/runs", Some(json!({}))).await;
    let id = body["run_id"].as_str().unwrap().to_string();

    let run = until(&e.app, &id, |r| r["in_flight"]["steps"]["work"] == 1).await;
    assert_eq!(run["in_flight"]["deliveries"], 1);
    assert!(
        !run["in_flight"]["worker"].as_str().unwrap().is_empty(),
        "{run}"
    );

    GATE_OPEN.store(true, std::sync::atomic::Ordering::SeqCst);
    let run = until(&e.app, &id, |r| r["status"] == "succeeded").await;
    // Settled runs carry no overlay.
    until(&e.app, &id, |r| r.get("in_flight").is_none()).await;
    assert_eq!(run["workflow"], "gated");
    e.worker.shutdown().await;
}

#[tokio::test]
async fn health_and_readiness() {
    let e = env().await;
    assert_eq!(
        call(&e.app, "GET", "/healthz", None).await.0,
        StatusCode::OK
    );
    assert_eq!(call(&e.app, "GET", "/readyz", None).await.0, StatusCode::OK);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn start_a_run_by_name_and_read_it_back_with_events() {
    let e = env().await;
    let (status, body) = call(
        &e.app,
        "POST",
        "/v1/workflows/echo/runs",
        Some(json!({"input": {"a": 1}})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["outcome"], "started");
    let id = body["run_id"].as_str().unwrap().to_string();

    let run = until(&e.app, &id, |r| r["status"] == "succeeded").await;
    assert_eq!(run["output"], json!({"a": 1}));
    assert_eq!(run["workflow"], "echo");

    let (status, events) = call(&e.app, "GET", &format!("/v1/runs/{id}/events"), None).await;
    assert_eq!(status, StatusCode::OK);
    let types: Vec<_> = events
        .as_array()
        .unwrap()
        .iter()
        .map(|ev| ev["kind"]["type"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["run_started", "step_succeeded", "run_succeeded"]);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn idempotent_start_returns_the_same_run() {
    let e = env().await;
    let body = json!({"input": 1, "idempotency_key": "k1"});
    let (s1, a) = call(
        &e.app,
        "POST",
        "/v1/workflows/echo/runs",
        Some(body.clone()),
    )
    .await;
    let (s2, b) = call(&e.app, "POST", "/v1/workflows/echo/runs", Some(body)).await;
    assert_eq!((s1, s2), (StatusCode::CREATED, StatusCode::OK));
    assert_eq!(
        (a["run_id"].clone(), b["outcome"].clone()),
        (b["run_id"].clone(), json!("existing"))
    );
    e.worker.shutdown().await;
}

#[tokio::test]
async fn signal_a_waiting_run() {
    let e = env().await;
    let (_, body) = call(
        &e.app,
        "POST",
        "/v1/workflows/approve/runs",
        Some(json!({"input": null})),
    )
    .await;
    let id = body["run_id"].as_str().unwrap().to_string();
    until(&e.app, &id, |r| r["status"] == "waiting").await;
    let (status, _) = call(
        &e.app,
        "POST",
        &format!("/v1/runs/{id}/signals/approval"),
        Some(json!({"payload": {"by": "ann"}})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let run = until(&e.app, &id, |r| r["status"] == "succeeded").await;
    assert_eq!(run["output"], json!({"by": "ann"}));

    // Signalling a finished run conflicts.
    let (status, err) = call(
        &e.app,
        "POST",
        &format!("/v1/runs/{id}/signals/approval"),
        Some(json!({"payload": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    e.worker.shutdown().await;
}

#[tokio::test]
async fn webhook_resolves_a_hook_by_token() {
    let e = env().await;
    let (_, body) = call(
        &e.app,
        "POST",
        "/v1/workflows/hooked/runs",
        Some(json!({"input": null})),
    )
    .await;
    let id = body["run_id"].as_str().unwrap().to_string();
    let run = until(&e.app, &id, |r| r["status"] == "waiting").await;
    let token = run["awaiting"][0].as_str().unwrap().to_string();
    // The webhook body is the payload itself.
    let (status, _) = call(
        &e.app,
        "POST",
        &format!("/v1/hooks/{token}"),
        Some(json!({"paid": true})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let run = until(&e.app, &id, |r| r["status"] == "succeeded").await;
    assert_eq!(run["output"], json!({"paid": true}));
    let (status, _) = call(&e.app, "POST", "/v1/hooks/unknown", Some(json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn publish_an_event_to_matching_waits() {
    let e = env().await;
    let mut ids = vec![];
    for order in [1, 2] {
        let (_, b) = call(
            &e.app,
            "POST",
            "/v1/workflows/order/runs",
            Some(json!({"input": order})),
        )
        .await;
        let id = b["run_id"].as_str().unwrap().to_string();
        until(&e.app, &id, |r| r["status"] == "waiting").await;
        ids.push(id);
    }
    let (status, body) = call(
        &e.app,
        "POST",
        "/v1/events",
        Some(json!({"name": "order.paid", "data": {"order_id": 2}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 1);
    until(&e.app, &ids[1], |r| r["status"] == "succeeded").await;
    let (_, still) = call(&e.app, "GET", &format!("/v1/runs/{}", ids[0]), None).await;
    assert_eq!(still["status"], "waiting");
    e.worker.shutdown().await;
}

#[tokio::test]
async fn cancel_and_retry() {
    let e = env().await;
    let (_, b) = call(
        &e.app,
        "POST",
        "/v1/workflows/sleeper/runs",
        Some(json!({"input": null})),
    )
    .await;
    let id = b["run_id"].as_str().unwrap().to_string();
    until(&e.app, &id, |r| r["status"] == "waiting").await;
    let (status, _) = call(
        &e.app,
        "POST",
        &format!("/v1/runs/{id}/cancel"),
        Some(json!({"reason": "stop"})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    until(&e.app, &id, |r| r["status"] == "cancelled").await;

    let (_, b) = call(
        &e.app,
        "POST",
        "/v1/workflows/bad/runs",
        Some(json!({"input": null})),
    )
    .await;
    let bad = b["run_id"].as_str().unwrap().to_string();
    let run = until(&e.app, &bad, |r| r["status"] == "failed").await;
    assert!(run["error"].as_str().unwrap().contains("nope"));
    let (status, _) = call(&e.app, "POST", &format!("/v1/runs/{bad}/retry"), None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    // It fails again (the workflow always does) but was re-run: a RunRetried event exists.
    until(&e.app, &bad, |r| r["status"] == "failed").await;
    let (_, events) = call(&e.app, "GET", &format!("/v1/runs/{bad}/events"), None).await;
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(|ev| ev["kind"]["type"] == "run_retried")
    );

    // Retrying something that is not failed conflicts.
    let (status, _) = call(&e.app, "POST", &format!("/v1/runs/{id}/retry"), None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn list_runs_filters_and_pages() {
    let e = env().await;
    for i in 0..3 {
        call(
            &e.app,
            "POST",
            "/v1/workflows/echo/runs",
            Some(json!({"input": i})),
        )
        .await;
    }
    call(
        &e.app,
        "POST",
        "/v1/workflows/bad/runs",
        Some(json!({"input": null})),
    )
    .await;
    let (_, all) = call(&e.app, "GET", "/v1/runs", None).await;
    assert_eq!(all.as_array().unwrap().len(), 4);
    let (_, echo) = call(&e.app, "GET", "/v1/runs?workflow=echo", None).await;
    assert_eq!(echo.as_array().unwrap().len(), 3);
    let (_, page) = call(&e.app, "GET", "/v1/runs?limit=2", None).await;
    let page = page.as_array().unwrap();
    assert_eq!(page.len(), 2);
    let after = page[1]["id"].as_str().unwrap();
    let (_, rest) = call(
        &e.app,
        "GET",
        &format!("/v1/runs?limit=10&after={after}"),
        None,
    )
    .await;
    assert_eq!(rest.as_array().unwrap().len(), 2);
    let (status, _) = call(&e.app, "GET", "/v1/runs?status=completed", None).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "`completed` is not a status; the outcome is `succeeded`"
    );
    e.worker.shutdown().await;
}

#[tokio::test]
async fn unknown_run_is_404_and_bad_ids_are_400() {
    let e = env().await;
    let missing = uuid::Uuid::now_v7();
    assert_eq!(
        call(&e.app, "GET", &format!("/v1/runs/{missing}"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&e.app, "GET", &format!("/v1/runs/{missing}/events"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&e.app, "GET", "/v1/runs/not-a-uuid", None).await.0,
        StatusCode::BAD_REQUEST
    );
    e.worker.shutdown().await;
}

#[tokio::test]
async fn bearer_auth_guards_everything_but_health() {
    let e = env().await;
    let guarded = durable_api::with_bearer_token(e.app.clone(), "s3cret");
    let req = |auth: Option<&str>, uri: &str| {
        let mut r = Request::builder().uri(uri);
        if let Some(a) = auth {
            r = r.header(header::AUTHORIZATION, a);
        }
        r.body(Body::empty()).unwrap()
    };
    let status = |r: Request<Body>| {
        let app = guarded.clone();
        async move { app.oneshot(r).await.unwrap().status() }
    };
    assert_eq!(
        status(req(None, "/v1/runs")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(req(Some("Bearer wrong"), "/v1/runs")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(req(Some("Bearer s3cret"), "/v1/runs")).await,
        StatusCode::OK
    );
    assert_eq!(status(req(None, "/healthz")).await, StatusCode::OK);
    drop(e.client);
    e.worker.shutdown().await;
}
