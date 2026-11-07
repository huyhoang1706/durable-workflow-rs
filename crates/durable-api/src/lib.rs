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
//! | `GET /v1/runs/{id}/watch` | SSE of the run's events (`id` = event seq, resume with `Last-Event-ID` or `?after=`); ends after the terminal event |
//! | `GET /v1/runs/{id}/streams` | the run's user streams (name, length, closed) |
//! | `GET /v1/runs/{id}/streams/{name}` | SSE of a user stream's chunks (`id` = cursor, resume with `Last-Event-ID` or `?after=`); transient, ends when the stream closes |
//! | `POST /v1/runs/{id}/cancel` `/retry` `/signals/{name}` | control |
//! | `POST /v1/hooks/{token}` | webhook: the body is the payload |
//! | `POST /v1/events` | `{name, data}` |
//! | `GET /healthz`, `/readyz` | probes |

use std::{convert::Infallible, str::FromStr};

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event as SseEvent, KeepAlive, Sse},
    },
    routing::{get, post},
};
use durable_runtime::{Client, RunError, RunUpdate, StartOptions, StartOutcome};
use durable_spi::{Error, RunFilter, RunId, RunStatus, StreamCursor};
use futures::{Stream, StreamExt};
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
        .route("/v1/runs/{id}/watch", get(watch_run))
        .route("/v1/runs/{id}/streams", get(list_streams))
        .route("/v1/runs/{id}/streams/{name}", get(tail_stream))
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

impl From<RunError> for ApiError {
    fn from(e: RunError) -> Self {
        match e {
            RunError::NotFound => Self(StatusCode::NOT_FOUND, "run not found".into()),
            RunError::Backend(e) => e.into(),
            other => Self(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        }
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

#[derive(Deserialize, Default)]
struct ResumeQuery {
    after: Option<String>,
}

/// The resume point: `Last-Event-ID` (what an `EventSource` sends on
/// reconnect) or, for clients that cannot set headers, `?after=`.
fn resume_point(headers: &HeaderMap, q: &ResumeQuery) -> Option<String> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| q.after.clone())
        .filter(|v| !v.is_empty())
}

/// Wrap an item stream as SSE. A failure ends the stream with an `error`
/// event (the status line is long gone by then).
fn sse<S>(items: S) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>>
where
    S: Stream<Item = Result<SseEvent, RunError>> + Send + 'static,
{
    let events = items.map(|item| {
        Ok(item.unwrap_or_else(|e| {
            SseEvent::default()
                .event("error")
                .data(json!({"error": e.to_string()}).to_string())
        }))
    });
    Sse::new(events).keep_alive(KeepAlive::default())
}

async fn watch_run(
    State(c): State<Client>,
    Path(id): Path<String>,
    Query(q): Query<ResumeQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut watch = c.watch(run_id(&id)?);
    if let Some(after) = resume_point(&headers, &q) {
        let seq = after
            .parse()
            .map_err(|_| bad_request(format!("`{after}` is not an event seq")))?;
        watch = watch.after(seq);
    }
    let updates = watch.await?;
    let events = updates.map(|u| {
        u.and_then(|u| {
            let mut ev = SseEvent::default()
                .json_data(&u)
                .map_err(|e| RunError::Decode(e.to_string()))?;
            ev = match &u {
                RunUpdate::Event(e) => ev.id(e.seq.to_string()).event("event"),
                RunUpdate::StepStarted { .. } => ev.event("step_started"),
            };
            Ok(ev)
        })
    });
    Ok(sse(events).into_response())
}

async fn list_streams(State(c): State<Client>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(c.streams(run_id(&id)?).await?)))
}

async fn tail_stream(
    State(c): State<Client>,
    Path((id, name)): Path<(String, String)>,
    Query(q): Query<ResumeQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut read = c.stream::<Value>(run_id(&id)?, &name);
    if let Some(after) = resume_point(&headers, &q) {
        read = read.after(StreamCursor(after));
    }
    let items = read.await?;
    let events = items.map(|i| {
        i.and_then(|item| {
            SseEvent::default()
                .id(item.cursor.0.clone())
                .event("chunk")
                .json_data(&item)
                .map_err(|e| RunError::Decode(e.to_string()))
        })
    });
    Ok(sse(events).into_response())
}
