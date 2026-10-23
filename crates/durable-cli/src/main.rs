//! `durable`: operate durable workflows from the command line.
//!
//! Talks to the backends directly (there is no server). Without `--redis-url`
//! new work is not pushed to the queue; running workers pick it up through
//! their reconciler, which reads `wake_at` from Postgres.

use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use durable_workflow::{
    Client, RunFilter, RunStatus, StartOptions, StartOutcome, connect::BackendConfig,
};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(name = "durable", version, about = "Operate durable workflows")]
struct Cli {
    #[arg(
        long,
        env = "DURABLE_DATABASE_URL",
        global = true,
        hide_env_values = true
    )]
    database_url: Option<String>,
    #[arg(long, env = "DURABLE_REDIS_URL", global = true, hide_env_values = true)]
    redis_url: Option<String>,
    #[arg(
        long,
        env = "DURABLE_NAMESPACE",
        default_value = "default",
        global = true
    )]
    namespace: String,
    /// Postgres schema holding the durable tables.
    #[arg(long, env = "DURABLE_SCHEMA", default_value = "durable", global = true)]
    schema: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create or upgrade the Postgres tables.
    Migrate,
    /// Start a run of a workflow by name.
    Start {
        workflow: String,
        /// Input as JSON.
        #[arg(long, default_value = "null")]
        input: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Inspect and control runs.
    Runs {
        #[command(subcommand)]
        command: RunsCommand,
    },
    /// Send a signal to a waiting run.
    Signal {
        run_id: String,
        name: String,
        /// Payload as JSON.
        #[arg(long, default_value = "null")]
        payload: String,
    },
    /// Publish and inspect events.
    Event {
        #[command(subcommand)]
        command: EventCommand,
    },
}

#[derive(Subcommand)]
enum RunsCommand {
    List(ListArgs),
    Get {
        run_id: String,
    },
    Events {
        run_id: String,
    },
    Cancel {
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Re-run a failed run from its failure point.
    Retry {
        run_id: String,
    },
}

#[derive(Args)]
struct ListArgs {
    #[arg(long)]
    workflow: Option<String>,
    /// pending, running, waiting, succeeded, failed or cancelled.
    #[arg(long)]
    status: Option<String>,
    #[arg(long, default_value_t = 50)]
    limit: usize,
    /// Continue after this run id (newest-first paging).
    #[arg(long)]
    after: Option<String>,
}

#[derive(Subcommand)]
enum EventCommand {
    /// Publish an event to every run waiting for it.
    Send {
        name: String,
        /// Event data as JSON.
        #[arg(long, default_value = "null")]
        data: String,
    },
}

fn json_arg(what: &str, raw: &str) -> Result<Value, String> {
    serde_json::from_str(raw).map_err(|e| format!("--{what} is not valid JSON: {e}"))
}

fn run_id(raw: &str) -> Result<uuid::Uuid, String> {
    raw.parse()
        .map_err(|_| format!("`{raw}` is not a valid run id"))
}

async fn run(cli: Cli) -> Result<Value, String> {
    let url = cli
        .database_url
        .ok_or("a database URL is required: --database-url or DURABLE_DATABASE_URL")?;
    let mut config = BackendConfig::new(url);
    config.redis_url = cli.redis_url;
    config.namespace = cli.namespace;
    config.schema = cli.schema;
    // Only `migrate` changes the schema.
    config.migrate = matches!(cli.command, Command::Migrate);
    let backend = config.build().await.map_err(|e| e.to_string())?;
    let client = Client::new(backend);
    let e = |e: durable_workflow::Error| e.to_string();

    Ok(match cli.command {
        Command::Migrate => json!({"migrated": true}),
        Command::Start {
            workflow,
            input,
            idempotency_key,
        } => {
            let input = json_arg("input", &input)?;
            let opts = StartOptions { idempotency_key };
            match client.start_raw(&workflow, input, opts).await.map_err(e)? {
                StartOutcome::Started(h) => json!({"outcome": "started", "run_id": h.id()}),
                StartOutcome::Existing(h) => json!({"outcome": "existing", "run_id": h.id()}),
                StartOutcome::RateLimited { retry_at } => {
                    json!({"outcome": "rate_limited", "retry_at": retry_at})
                }
            }
        }
        Command::Runs { command } => match command {
            RunsCommand::List(a) => {
                let status = a.status.map(|s| s.parse::<RunStatus>()).transpose()?;
                let after = a.after.as_deref().map(run_id).transpose()?;
                let runs = client
                    .list_runs(RunFilter {
                        workflow: a.workflow,
                        status,
                        after,
                        limit: Some(a.limit),
                        ..Default::default()
                    })
                    .await
                    .map_err(e)?;
                json!(runs)
            }
            RunsCommand::Get { run_id: id } => {
                let id = run_id(&id)?;
                let run = client.get_run(id).await.map_err(e)?;
                json!(run.ok_or(format!("run {id} not found"))?)
            }
            RunsCommand::Events { run_id: id } => {
                json!(client.events(run_id(&id)?).await.map_err(e)?)
            }
            RunsCommand::Cancel { run_id: id, reason } => {
                client.cancel(run_id(&id)?, reason).await.map_err(e)?;
                json!({"cancel_requested": true})
            }
            RunsCommand::Retry { run_id: id } => {
                client.retry(run_id(&id)?).await.map_err(e)?;
                json!({"retried": true})
            }
        },
        Command::Signal {
            run_id: id,
            name,
            payload,
        } => {
            client
                .signal(run_id(&id)?, &name, json_arg("payload", &payload)?)
                .await
                .map_err(e)?;
            json!({"signalled": true})
        }
        Command::Event {
            command: EventCommand::Send { name, data },
        } => {
            let matched = client
                .send_event(&name, json_arg("data", &data)?)
                .await
                .map_err(e)?;
            json!({"matched": matched})
        }
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(v) => {
            println!("{}", serde_json::to_string_pretty(&v).expect("json"));
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}
