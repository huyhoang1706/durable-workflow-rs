//! Stream an LLM answer to a browser while the step runs, with the final
//! answer stored durably.
//!
//! Chunks are transient: the step writes them to a named stream (Redis, with a
//! TTL), the page renders them live, and nothing about them is persisted in
//! Postgres. The durable part is the step's return value, the full text, which
//! is memoized in the run's event log like any step output. The page swaps it
//! in when `watch` reports `step_succeeded`, so a client whose stream expired
//! (or that connected late) still ends up with the right answer.
//!
//! Without `ANTHROPIC_API_KEY` a fake generator stands in for the model. It
//! fails its first attempt once, to show the `attempt_failed` marker: the page
//! discards the partial text and the retry streams afresh.
//!
//! ```text
//! docker compose up -d
//! DATABASE_URL=postgres://postgres:postgres@localhost/durable REDIS_URL=redis://localhost \
//!   cargo run -p llm-streaming          # then open http://localhost:3000
//! # optional: ANTHROPIC_API_KEY=... (ANTHROPIC_MODEL, default claude-opus-5-5)
//! curl -N localhost:3000/durable/v1/runs/<run_id>/streams/tokens   # raw SSE
//! ```

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    response::Html,
    routing::{get, post},
};
use durable_workflow::{
    Client, Error, StartOutcome, StepError, WfAsyncCtx, Worker, WorkflowError,
    connect::BackendConfig, step,
};
use futures::{Stream, StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Serialize, Deserialize)]
struct Prompt {
    prompt: String,
}

/// The first fake attempt fails once, halfway through.
static FAILED_ONCE: AtomicBool = AtomicBool::new(false);

async fn chat(ctx: WfAsyncCtx, prompt: String) -> Result<String, WorkflowError> {
    // The step's return value is the durable answer. Everything written to the
    // stream while it runs is a live preview of it.
    generate(&ctx, prompt).await
}

#[step]
async fn generate(prompt: String) -> Result<String, StepError> {
    let writer = step::current().stream::<String>("tokens");
    let (tokens, failure) = generator(&prompt);
    let parts = writer.forward(tokens).await?;
    if let Some(err) = failure.lock().unwrap().take() {
        return Err(StepError::retryable(err));
    }
    Ok(parts.concat())
}

type Failure = std::sync::Arc<std::sync::Mutex<Option<String>>>;

/// Token stream from the Messages API when a key is set, else a fake one.
/// A failure of the real call is reported through the second value because
/// `forward` takes plain items.
fn generator(prompt: &str) -> (BoxStream<'static, String>, Failure) {
    let failure = Failure::default();
    match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) => (
            claude(key, prompt.to_string(), failure.clone()).boxed(),
            failure,
        ),
        Err(_) => (fake(prompt.to_string(), failure.clone()).boxed(), failure),
    }
}

fn fake(prompt: String, failure: Failure) -> impl Stream<Item = String> {
    let attempt = step::current().attempt();
    let answer = format!(
        "Durable execution records each step's result, so after a crash the run resumes where it \
         stopped instead of starting over. You asked: \"{prompt}\"."
    );
    let words: Vec<String> = answer.split_inclusive(' ').map(String::from).collect();
    let fail_at =
        (attempt == 1 && !FAILED_ONCE.swap(true, Ordering::SeqCst)).then_some(words.len() / 2);
    futures::stream::unfold(
        (words.into_iter().enumerate(), failure),
        move |(mut it, failure)| async move {
            let (i, word) = it.next()?;
            tokio::time::sleep(Duration::from_millis(60)).await;
            if Some(i) == fail_at {
                *failure.lock().unwrap() = Some("the fake model dropped the connection".into());
                return None;
            }
            Some((word, (it, failure)))
        },
    )
}

/// Anthropic Messages API, streamed. There is no official Rust SDK, so this is
/// plain HTTP + SSE: `content_block_delta` events with a `text_delta` carry the text.
fn claude(key: String, prompt: String, failure: Failure) -> impl Stream<Item = String> {
    let model = std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-opus-5-5".into());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let result: Result<(), String> = async {
            let resp = reqwest::Client::new()
                .post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", key)
                .header("anthropic-version", "2023-06-01")
                .json(&json!({
                    "model": model,
                    "max_tokens": 2048,
                    "stream": true,
                    "messages": [{"role": "user", "content": prompt}],
                }))
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if !resp.status().is_success() {
                return Err(format!(
                    "Messages API returned {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ));
            }
            let mut body = resp.bytes_stream();
            let mut buf = String::new();
            while let Some(chunk) = body.next().await {
                buf.push_str(&String::from_utf8_lossy(&chunk.map_err(|e| e.to_string())?));
                while let Some(end) = buf.find("\n\n") {
                    let event: String = buf.drain(..end + 2).collect();
                    let Some(data) = event.lines().find_map(|l| l.strip_prefix("data: ")) else {
                        continue;
                    };
                    let v: Value = serde_json::from_str(data).map_err(|e| e.to_string())?;
                    match v["type"].as_str() {
                        Some("content_block_delta") if v["delta"]["type"] == "text_delta" => {
                            let _ = tx
                                .send(v["delta"]["text"].as_str().unwrap_or_default().to_string());
                        }
                        Some("error") => {
                            return Err(v["error"]["message"]
                                .as_str()
                                .unwrap_or("stream error")
                                .to_string());
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            *failure.lock().unwrap() = Some(e);
        }
    });
    futures::stream::unfold(rx, |mut rx| async { rx.recv().await.map(|t| (t, rx)) })
}

async fn start_chat(State(client): State<Client>, Json(p): Json<Prompt>) -> Json<Value> {
    match client
        .start_raw("chat", json!(p.prompt), Default::default())
        .await
    {
        Ok(StartOutcome::Started(h) | StartOutcome::Existing(h)) => Json(json!({"run_id": h.id()})),
        Ok(StartOutcome::RateLimited { retry_at }) => {
            Json(json!({"error": "rate limited", "retry_at": retry_at}))
        }
        Err(e) => Json(json!({"error": e.to_string()})),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let backend = match BackendConfig::from_env() {
        Ok(config) => config.build().await?,
        // Without a database the example still runs, in memory.
        Err(_) => durable_workflow::memory::backend(),
    };
    let worker = Worker::builder(backend.clone())
        .register_fn("chat", |ctx: WfAsyncCtx, prompt: String| chat(ctx, prompt))
        .build()
        .map_err(|e| Error::Config(e.to_string()))?
        .spawn();
    let client = Client::new(backend);
    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/chat", post(start_chat))
        .with_state(client.clone())
        .nest("/durable", durable_api::router(client));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|e| Error::Config(e.to_string()))?;
    println!("open http://localhost:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
    worker.shutdown().await;
    Ok(())
}
