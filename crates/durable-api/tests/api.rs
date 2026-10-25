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
    backend: durable_core::Backend,
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
        .register_fn(
            "streamer",
            |ctx: WfAsyncCtx, words: Vec<String>| async move {
                let text: String = ctx
                    .step("gen", move || async move {
                        let w = durable_runtime::step::current().stream::<String>("tokens");
                        for word in &words {
                            w.write(word)?;
                        }
                        Ok::<_, StepError>(words.concat())
                    })
                    .await?;
                Ok(text)
            },
        )
        .register_fn("sleeper", |ctx: WfAsyncCtx, _: Value| async move {
            ctx.sleep("z", Duration::from_secs(3600)).await?;
            Ok(())
        })
        .build()
        .unwrap()
        .spawn();
    worker.ready().await;
    let client = Client::new(backend.clone());
    Env {
        app: router(client.clone()),
        backend,
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

#[derive(Debug)]
struct Sse {
    id: Option<String>,
    event: Option<String>,
    data: Value,
}

/// GET an SSE route and read it to the end (the stream must end by itself).
async fn sse_call(
    app: &Router,
    uri: &str,
    last_event_id: Option<&str>,
) -> (StatusCode, Vec<Sse>, Value) {
    let mut req = Request::builder().method("GET").uri(uri);
    if let Some(id) = last_event_id {
        req = req.header("last-event-id", id);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = tokio::time::timeout(Duration::from_secs(10), resp.into_body().collect())
        .await
        .expect("the SSE stream ends")
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes).to_string();
    if !content_type.starts_with("text/event-stream") {
        let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
        return (status, vec![], body);
    }
    let msgs = text
        .split("\n\n")
        .filter(|b| !b.trim().is_empty() && !b.trim_start().starts_with(':'))
        .map(|block| {
            let field = |name: &str| {
                block
                    .lines()
                    .find_map(|l| l.strip_prefix(&format!("{name}:")))
                    .map(|v| v.trim_start().to_string())
            };
            Sse {
                id: field("id"),
                event: field("event"),
                data: field("data")
                    .map(|d| serde_json::from_str(&d).unwrap_or(Value::String(d)))
                    .unwrap_or(Value::Null),
            }
        })
        .collect();
    (status, msgs, Value::Null)
}

async fn start(app: &Router, workflow: &str, input: Value) -> String {
    let (_, body) = call(
        app,
        "POST",
        &format!("/v1/workflows/{workflow}/runs"),
        Some(json!({ "input": input })),
    )
    .await;
    body["run_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn watch_streams_events_as_sse_and_resumes_from_last_event_id() {
    let e = env().await;
    let id = start(&e.app, "echo", json!({"a": 1})).await;
    until(&e.app, &id, |r| r["status"] == "succeeded").await;

    let (status, all, _) = sse_call(&e.app, &format!("/v1/runs/{id}/watch"), None).await;
    assert_eq!(status, StatusCode::OK);
    let events: Vec<_> = all
        .iter()
        .filter(|m| m.event.as_deref() == Some("event"))
        .collect();
    let kinds: Vec<_> = events
        .iter()
        .map(|m| m.data["kind"]["type"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(kinds, ["run_started", "step_succeeded", "run_succeeded"]);
    assert_eq!(events[0].id.as_deref(), Some("1"));
    assert_eq!(events[2].id.as_deref(), Some("3"));
    assert_eq!(events[1].data["type"], "event");

    // A reconnecting EventSource sends the last id it saw.
    let (_, rest, _) = sse_call(&e.app, &format!("/v1/runs/{id}/watch"), Some("2")).await;
    let ids: Vec<_> = rest
        .iter()
        .filter(|m| m.event.as_deref() == Some("event"))
        .map(|m| m.id.clone().unwrap())
        .collect();
    assert_eq!(ids, ["3"]);
    // ?after= does the same for clients that cannot set headers.
    let (_, rest, _) = sse_call(&e.app, &format!("/v1/runs/{id}/watch?after=1"), None).await;
    assert_eq!(rest.len(), 2);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn watch_rejects_unknown_runs_and_bad_ids() {
    let e = env().await;
    let (status, _, body) = sse_call(
        &e.app,
        &format!("/v1/runs/{}/watch", uuid::Uuid::now_v7()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, _, _) = sse_call(&e.app, "/v1/runs/nope/watch", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let id = start(&e.app, "echo", json!(1)).await;
    let (status, _, _) = sse_call(&e.app, &format!("/v1/runs/{id}/watch"), Some("abc")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    e.worker.shutdown().await;
}

#[tokio::test]
async fn user_streams_are_listed_and_tailed_with_resume() {
    let e = env().await;
    let id = start(&e.app, "streamer", json!(["a", "b", "c"])).await;
    until(&e.app, &id, |r| r["status"] == "succeeded").await;

    let (status, list) = call(&e.app, "GET", &format!("/v1/runs/{id}/streams"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["stream"], "tokens");
    assert_eq!(list[0]["closed"], true);

    let uri = format!("/v1/runs/{id}/streams/tokens");
    let (status, all, _) = sse_call(&e.app, &uri, None).await;
    assert_eq!(status, StatusCode::OK);
    let chunks: Vec<_> = all
        .iter()
        .filter(|m| m.event.as_deref() == Some("chunk"))
        .collect();
    // The attempt announces itself, then its tokens follow.
    assert_eq!(chunks[0].data["body"]["type"], "attempt_started");
    let data: Vec<_> = chunks
        .iter()
        .filter(|m| m.data["body"]["type"] == "data")
        .collect();
    let text: Vec<_> = data
        .iter()
        .map(|m| m.data["body"]["data"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(text, ["a", "b", "c"]);
    assert_eq!(data[0].data["step_key"], "gen");
    assert_eq!(data[0].data["attempt"], 1);

    let cursor = data[0].id.clone().unwrap();
    let (_, rest, _) = sse_call(&e.app, &uri, Some(&cursor)).await;
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[0].data["body"]["data"], "b");
    e.worker.shutdown().await;
}

#[tokio::test]
async fn a_stream_that_never_existed_ends_once_the_run_is_done() {
    let e = env().await;
    let id = start(&e.app, "echo", json!(1)).await;
    until(&e.app, &id, |r| r["status"] == "succeeded").await;
    let (status, msgs, _) = sse_call(&e.app, &format!("/v1/runs/{id}/streams/nothing"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(msgs.is_empty());
    e.worker.shutdown().await;
}

#[tokio::test]
async fn watch_and_streams_are_scoped_to_the_namespace() {
    let e = env().await;
    let id = start(&e.app, "streamer", json!(["x"])).await;
    until(&e.app, &id, |r| r["status"] == "succeeded").await;
    let mut other = e.backend.clone();
    other.namespace = "other".into();
    let other_app = router(Client::new(other));
    for uri in [
        format!("/v1/runs/{id}/watch"),
        format!("/v1/runs/{id}/streams"),
        format!("/v1/runs/{id}/streams/tokens"),
    ] {
        let (status, _, _) = sse_call(&other_app, &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
    }
    e.worker.shutdown().await;
}
