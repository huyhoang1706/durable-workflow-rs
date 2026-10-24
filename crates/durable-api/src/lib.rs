//! Ops HTTP API: start, inspect, cancel, retry and signal runs, resolve
//! webhooks, publish events.
//!
//! [`router`] returns a plain `axum::Router`: nest it into your own app
//! (`.nest("/durable", durable_api::router(client))`) and wrap it with your
//! own auth layers, or serve it standalone behind [`with_bearer_token`].
//!
//! | Route | |
//! |---|---|
//! | `POST /v1/workflows/{name}/runs` | start (`{input, idempotency_key?}`) |
//! | `GET /v1/runs`, `/v1/runs/{id}`, `/v1/runs/{id}/events` | inspect (a running run also carries `in_flight`: worker, deliveries, step attempts) |
//! | `POST /v1/runs/{id}/cancel` `/retry` `/signals/{name}` | control |
//! | `POST /v1/hooks/{token}` | webhook: the body is the payload |
//! | `POST /v1/events` | `{name, data}` |
//! | `GET /healthz`, `/readyz` | probes |

use std::str::FromStr;

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use durable_core::{Error, RunFilter, RunId, RunStatus};
use durable_runtime::{Client, StartOptions, StartOutcome};
use serde::Deserialize;
use serde_json::{Value, json};

pub fn router(client: Client) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/v1/workflows/{name}/runs", post(start_run))
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/events", get(get_events))
        .route("/v1/runs/{id}/cancel", post(cancel_run))
        .route("/v1/runs/{id}/retry", post(retry_run))
        .route("/v1/runs/{id}/signals/{name}", post(signal_run))
        .route("/v1/hooks/{token}", post(resume_hook))
        .route("/v1/events", post(publish_event))
        .with_state(client)
}

/// Require `Authorization: Bearer <token>` on everything except the probes.
pub fn with_bearer_token(app: Router, token: impl Into<String>) -> Router {
    let expected = format!("Bearer {}", token.into());
    app.layer(middleware::from_fn(move |req: Request, next: Next| {
        let expected = expected.clone();
        async move {
            let open = matches!(req.uri().path(), "/healthz" | "/readyz");
            let given = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok());
            if open || given.is_some_and(|g| constant_time_eq(g.as_bytes(), expected.as_bytes())) {
                next.run(req).await
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "missing or invalid bearer token"})),
                )
                    .into_response()
            }
        }
    }))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

struct ApiError(StatusCode, String);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        let status = match &e {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::InvalidState(_) | Error::Conflict | Error::Closed => StatusCode::CONFLICT,
            Error::Config(_) => StatusCode::BAD_REQUEST,
            Error::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

fn run_id(raw: &str) -> ApiResult<RunId> {
    raw.parse()
        .map_err(|_| bad_request(format!("`{raw}` is not a valid run id")))
}

async fn readyz(State(c): State<Client>) -> Response {
    match c
        .list_runs(RunFilter {
            limit: Some(1),
            ..Default::default()
        })
        .await
    {
        Ok(_) => (StatusCode::OK, "ready").into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct StartBody {
    #[serde(default)]
    input: Value,
    idempotency_key: Option<String>,
}

async fn start_run(
    State(c): State<Client>,
    Path(name): Path<String>,
    Json(body): Json<StartBody>,
) -> ApiResult<Response> {
    let opts = StartOptions {
        idempotency_key: body.idempotency_key,
    };
    Ok(match c.start_raw(&name, body.input, opts).await? {
        StartOutcome::Started(h) => (
            StatusCode::CREATED,
            Json(json!({"outcome": "started", "run_id": h.id()})),
        )
            .into_response(),
        StartOutcome::Existing(h) => (
            StatusCode::OK,
            Json(json!({"outcome": "existing", "run_id": h.id()})),
        )
            .into_response(),
        StartOutcome::RateLimited { retry_at } => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"outcome": "rate_limited", "retry_at": retry_at})),
        )
            .into_response(),
    })
}

#[derive(Deserialize)]
struct ListQuery {
    workflow: Option<String>,
    status: Option<String>,
    after: Option<String>,
    limit: Option<usize>,
}

async fn list_runs(State(c): State<Client>, Query(q): Query<ListQuery>) -> ApiResult<Json<Value>> {
    let status = q
        .status
        .map(|s| RunStatus::from_str(&s).map_err(bad_request))
        .transpose()?;
    let after = q.after.as_deref().map(run_id).transpose()?;
    let runs = c
        .list_runs(RunFilter {
            workflow: q.workflow,
            status,
            after,
            limit: Some(q.limit.unwrap_or(50).min(500)),
            ..Default::default()
        })
        .await?;
    Ok(Json(json!(runs)))
}

async fn get_run(State(c): State<Client>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = run_id(&id)?;
    let run = c
        .get_run(id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("run {id}")))?;
    let mut body = json!(run);
    // Executing right now: add the worker and attempt/delivery counters the
    // cache knows about (absent when idle or when there is no cache).
    if !run.status.is_terminal()
        && let Some(in_flight) = c.in_flight(id).await
    {
        body["in_flight"] = json!(in_flight);
    }
    Ok(Json(body))
}

async fn get_events(State(c): State<Client>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(c.events(run_id(&id)?).await?)))
}

#[derive(Deserialize, Default)]
struct CancelBody {
    reason: Option<String>,
}

async fn cancel_run(
    State(c): State<Client>,
    Path(id): Path<String>,
    body: Option<Json<CancelBody>>,
) -> ApiResult<StatusCode> {
    let reason = body.and_then(|Json(b)| b.reason);
    c.cancel(run_id(&id)?, reason).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn retry_run(State(c): State<Client>, Path(id): Path<String>) -> ApiResult<StatusCode> {
    c.retry(run_id(&id)?).await?;
    Ok(StatusCode::ACCEPTED)
}

#[derive(Deserialize, Default)]
struct SignalBody {
    #[serde(default)]
    payload: Value,
}

async fn signal_run(
    State(c): State<Client>,
    Path((id, name)): Path<(String, String)>,
    body: Option<Json<SignalBody>>,
) -> ApiResult<StatusCode> {
    let payload = body.map(|Json(b)| b.payload).unwrap_or_default();
    c.signal(run_id(&id)?, &name, payload).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn resume_hook(
    State(c): State<Client>,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> ApiResult<StatusCode> {
    c.resume_hook(&token, body.map(|Json(v)| v).unwrap_or_default())
        .await?;
    Ok(StatusCode::ACCEPTED)
}

#[derive(Deserialize)]
struct EventBody {
    name: String,
    #[serde(default)]
    data: Value,
}

async fn publish_event(
    State(c): State<Client>,
    Json(e): Json<EventBody>,
) -> ApiResult<Json<Value>> {
    let matched = c.send_event(&e.name, e.data).await?;
    Ok(Json(json!({"matched": matched})))
}
