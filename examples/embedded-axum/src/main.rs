//! An existing axum app that embeds durable workflows.
//!
//! The app keeps its own sqlx pool, which the Postgres store reuses, and
//! Redis for the queue. The worker runs inside this process's tokio runtime
//! and the ops API is mounted under `/durable`, behind whatever auth the host
//! app wants.
//!
//! ```text
//! docker compose up -d
//! DATABASE_URL=postgres://postgres:postgres@localhost/durable REDIS_URL=redis://localhost cargo run -p embedded-axum
//! curl -XPOST localhost:3000/signup -H 'content-type: application/json' -d '{"email":"ann@example.com"}'
//! curl -XPOST localhost:3000/durable/v1/runs/<run_id>/signals/verified -H 'content-type: application/json' -d '{"payload":true}'
//! curl localhost:3000/durable/v1/runs/<run_id>
//! ```

use std::{sync::Arc, time::Duration};

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use durable_workflow::{
    Backend, Client, Dep, StartOutcome, StepError, WfAsyncCtx, Worker, WorkflowError,
    postgres::PostgresStore, redis as dw_redis, step, workflow,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;

/// The host app's own client; workflows reach it through `Dep<dyn Mailer>`.
trait Mailer: Send + Sync {
    fn send(&self, to: &str, subject: &str, idempotency_key: &str);
}

struct LogMailer;

impl Mailer for LogMailer {
    fn send(&self, to: &str, subject: &str, key: &str) {
        println!("mail to={to} subject={subject:?} idempotency_key={key}");
    }
}

#[derive(Serialize, Deserialize)]
struct Signup {
    email: String,
}

#[workflow(name = "user.onboarding", timeout = "7d")]
async fn onboarding(
    ctx: WfAsyncCtx,
    _mailer: Dep<dyn Mailer>, // declared here so `Worker::build()` fails fast if it is missing
    user: Signup,
) -> Result<String, WorkflowError> {
    send_email(&ctx, user.email.clone(), "Welcome!".into()).await?;
    // Suspends the run (no worker is held) until verified or a day has passed.
    let verified: Option<bool> = ctx
        .wait_for_signal("verified")
        .timeout(Duration::from_secs(24 * 3600))
        .await?;
    if verified.is_none() {
        send_email(&ctx, user.email, "Please verify your address".into()).await?;
        return Ok("reminded".into());
    }
    Ok("verified".into())
}

#[step(retry = 5, timeout = "30s")]
async fn send_email(mailer: Dep<dyn Mailer>, to: String, subject: String) -> Result<(), StepError> {
    // The idempotency key is stable across retries and replays: pass it to the provider.
    mailer.send(&to, &subject, step::current().idempotency_key());
    Ok(())
}

async fn signup(
    State(client): State<Client>,
    Json(body): Json<Signup>,
) -> (StatusCode, Json<Value>) {
    let opts =
        durable_workflow::StartOptions::default().idempotency_key(format!("signup:{}", body.email));
    match client.start::<Onboarding>(&body, opts).await {
        Ok(StartOutcome::Started(h)) => (StatusCode::CREATED, Json(json!({"run_id": h.id()}))),
        Ok(StartOutcome::Existing(h)) => (StatusCode::OK, Json(json!({"run_id": h.id()}))),
        Ok(StartOutcome::RateLimited { retry_at }) => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"retry_at": retry_at})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let redis_url = std::env::var("REDIS_URL")?;

    // The host app's own pool, shared with the durable store.
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    let store = PostgresStore::from_pool(pool).schema("durable");
    store.migrate().await?;
    let redis = dw_redis::connect(&redis_url, "dw").await?;
    let backend = Backend::builder()
        .namespace("embedded-demo")
        .store(store)
        .queue(redis.queue)
        .pubsub(redis.pubsub)
        .cache(redis.cache)
        .rate_limiter(redis.rate_limiter)
        .build()?;

    let worker = Worker::builder(backend.clone())
        .dep::<dyn Mailer>(Arc::new(LogMailer))
        .register_default::<Onboarding>()
        .build()?
        .spawn();

    let client = Client::new(backend);
    let app = Router::new()
        .route("/signup", post(signup))
        .with_state(client.clone())
        // The host app wraps this with its own auth layers.
        .nest("/durable", durable_api::router(client));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    worker.shutdown().await;
    Ok(())
}
