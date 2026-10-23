//! Run a worker as its own process: signal handling, health endpoints, an
//! optional ops API and tracing setup.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), durable_workflow::Error> {
//!     durable_workflow::standalone::run(Config::from_env()?, |worker| {
//!         worker.dep(mailer()).register_default::<Onboarding>()
//!     })
//!     .await
//! }
//! ```

use std::{future::Future, net::SocketAddr, time::Duration};

use axum::{Router, routing::get};
use durable_core::{Backend, Error};
use durable_runtime::{Client, Worker, WorkerBuilder};
use tokio::net::TcpListener;

use crate::connect::BackendConfig;

/// Everything the HTTP side and the worker need beyond the backend itself.
#[derive(Clone)]
pub struct ServeOptions {
    /// Mount the ops API under `/durable`.
    pub enable_api: bool,
    /// Require this bearer token on the ops API (probes stay open).
    pub api_token: Option<String>,
    /// Serve the ops API without a token on a non-loopback address. Off by
    /// default: anyone who can reach the port could start, cancel and signal
    /// runs.
    pub allow_insecure_api: bool,
    pub concurrency: Option<usize>,
    pub compute_threads: Option<usize>,
    pub drain_timeout: Duration,
}

impl std::fmt::Debug for ServeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeOptions")
            .field("enable_api", &self.enable_api)
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("allow_insecure_api", &self.allow_insecure_api)
            .field("concurrency", &self.concurrency)
            .field("compute_threads", &self.compute_threads)
            .field("drain_timeout", &self.drain_timeout)
            .finish()
    }
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            enable_api: false,
            api_token: None,
            allow_insecure_api: false,
            concurrency: None,
            compute_threads: None,
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// Process configuration, read from the environment by [`Config::from_env`].
#[derive(Debug, Clone)]
pub struct Config {
    pub backend: BackendConfig,
    pub http_addr: SocketAddr,
    pub serve: ServeOptions,
}

/// Loopback only, so health and ops endpoints are not exposed by accident;
/// set `DURABLE_HTTP_ADDR=0.0.0.0:17080` to accept outside connections.
pub const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:17080";

impl Config {
    /// Backend: see [`BackendConfig::from_env`]. Plus `DURABLE_HTTP_ADDR`
    /// (default [`DEFAULT_HTTP_ADDR`]), `DURABLE_ENABLE_API`, `DURABLE_API_TOKEN`,
    /// `DURABLE_API_INSECURE`, `DURABLE_CONCURRENCY`, `DURABLE_COMPUTE_THREADS`,
    /// `DURABLE_DRAIN_SECS`.
    pub fn from_env() -> Result<Self, Error> {
        let var = |k: &str| std::env::var(k).ok();
        let flag = |k: &str| matches!(var(k).as_deref(), Some("1" | "true" | "yes"));
        let parse = |k: &str| -> Result<Option<usize>, Error> {
            var(k)
                .map(|v| {
                    v.parse()
                        .map_err(|_| Error::Config(format!("{k} must be a number")))
                })
                .transpose()
        };
        let http_addr = var("DURABLE_HTTP_ADDR")
            .unwrap_or_else(|| DEFAULT_HTTP_ADDR.into())
            .parse()
            .map_err(|e| Error::Config(format!("DURABLE_HTTP_ADDR: {e}")))?;
        let api_token = var("DURABLE_API_TOKEN");
        let mut serve = ServeOptions {
            enable_api: api_token.is_some() || flag("DURABLE_ENABLE_API"),
            api_token,
            allow_insecure_api: flag("DURABLE_API_INSECURE"),
            concurrency: parse("DURABLE_CONCURRENCY")?,
            compute_threads: parse("DURABLE_COMPUTE_THREADS")?,
            ..Default::default()
        };
        if let Some(secs) = parse("DURABLE_DRAIN_SECS")? {
            serve.drain_timeout = Duration::from_secs(secs as u64);
        }
        Ok(Self {
            backend: BackendConfig::from_env()?,
            http_addr,
            serve,
        })
    }
}

/// Build the backend, serve until SIGTERM/SIGINT, then drain and exit.
pub async fn run<F>(config: Config, configure: F) -> Result<(), Error>
where
    F: FnOnce(WorkerBuilder) -> WorkerBuilder,
{
    init_tracing();
    let backend = config.backend.build().await?;
    let listener = TcpListener::bind(config.http_addr)
        .await
        .map_err(|e| Error::Config(format!("cannot bind {}: {e}", config.http_addr)))?;
    tracing::info!(addr = %config.http_addr, namespace = %config.backend.namespace, "durable worker starting");
    serve(
        backend,
        listener,
        config.serve,
        configure,
        shutdown_signal(),
    )
    .await
}

/// Serve on an already-bound listener until `shutdown` resolves. Starts the
/// worker, `/healthz`, `/readyz` and, if enabled, the ops API under `/durable`.
///
/// Refuses to serve the ops API without a token on a non-loopback address
/// unless [`ServeOptions::allow_insecure_api`] is set.
pub async fn serve<F, S>(
    backend: Backend,
    listener: TcpListener,
    opts: ServeOptions,
    configure: F,
    shutdown: S,
) -> Result<(), Error>
where
    F: FnOnce(WorkerBuilder) -> WorkerBuilder,
    S: Future<Output = ()> + Send + 'static,
{
    if opts.enable_api && opts.api_token.is_none() && !opts.allow_insecure_api {
        let addr = listener
            .local_addr()
            .map_err(|e| Error::Config(format!("listener address: {e}")))?;
        if !addr.ip().is_loopback() {
            return Err(Error::Config(format!(
                "refusing to serve the ops API on {addr} without a token: set \
                 DURABLE_API_TOKEN, bind to a loopback address, or set DURABLE_API_INSECURE=1"
            )));
        }
    }
    let mut builder = Worker::builder(backend.clone()).drain_timeout(opts.drain_timeout);
    if let Some(n) = opts.concurrency {
        builder = builder.concurrency(n);
    }
    if let Some(n) = opts.compute_threads {
        builder = builder.compute_threads(n);
    }
    let worker = configure(builder)
        .build()
        .map_err(|e| Error::Config(e.to_string()))?
        .spawn();

    let client = Client::new(backend);
    let mut app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get({
                let client = client.clone();
                move || readyz(client.clone())
            }),
        );
    if opts.enable_api {
        let mut api = durable_api::router(client);
        if let Some(token) = opts.api_token {
            api = durable_api::with_bearer_token(api, token);
        }
        app = app.nest("/durable", api);
    }

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    tracing::info!("shutting down: draining in-flight runs");
    worker.shutdown().await;
    served.map_err(|e| Error::Backend(format!("http server: {e}")))
}

async fn readyz(client: Client) -> (axum::http::StatusCode, String) {
    match client
        .list_runs(durable_core::RunFilter {
            limit: Some(1),
            ..Default::default()
        })
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "ready".into()),
        Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
