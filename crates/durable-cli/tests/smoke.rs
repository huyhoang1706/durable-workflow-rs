//! Drives the real `durable` binary against Postgres and Redis containers.

use std::process::{Command, Output};

use serde_json::Value;
use testcontainers_modules::{
    postgres::Postgres,
    redis::Redis,
    testcontainers::{ImageExt, runners::AsyncRunner},
};

struct Cli {
    database_url: String,
    redis_url: Option<String>,
    schema: String,
}

impl Cli {
    fn run(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_durable"));
        cmd.args([
            "--database-url",
            &self.database_url,
            "--schema",
            &self.schema,
            "--namespace",
            "cli",
        ]);
        if let Some(r) = &self.redis_url {
            cmd.args(["--redis-url", r]);
        }
        cmd.args(args).output().expect("run durable")
    }

    /// Output of a streaming command: one JSON object per line.
    fn lines(&self, args: &[&str]) -> Vec<Value> {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "`durable {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
            .collect()
    }

    fn ok(&self, args: &[&str]) -> Value {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "`durable {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
            panic!(
                "stdout is not JSON ({e}): {}",
                String::from_utf8_lossy(&out.stdout)
            )
        })
    }
}

/// `DURABLE_TEST_POSTGRES_URL` and `DURABLE_TEST_REDIS_URL` use servers that
/// are already running instead of starting containers.
async fn setup() -> (Cli, Box<dyn std::any::Any>) {
    if let (Ok(pg), Ok(redis)) = (
        std::env::var("DURABLE_TEST_POSTGRES_URL"),
        std::env::var("DURABLE_TEST_REDIS_URL"),
    ) {
        let cli = Cli {
            database_url: pg,
            redis_url: Some(redis),
            schema: format!("cli_{}", uuid::Uuid::now_v7().simple()),
        };
        return (cli, Box::new(()));
    }
    let pg = Postgres::default()
        .with_tag("17")
        .start()
        .await
        .expect("postgres container");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("redis container");
    let rport = redis.get_host_port_ipv4(6379).await.unwrap();
    // Wait for postgres to accept connections.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let cli = Cli {
        database_url: format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
        redis_url: Some(format!("redis://127.0.0.1:{rport}")),
        schema: "durable_cli".into(),
    };
    (cli, Box::new((pg, redis)))
}

#[tokio::test(flavor = "multi_thread")]
async fn operate_runs_from_the_command_line() {
    let (cli, _containers) = setup().await;

    // Migrate (twice: idempotent).
    cli.ok(&["migrate"]);
    cli.ok(&["migrate"]);

    // Start by name. No worker is running, so the run stays pending.
    let started = cli.ok(&[
        "start",
        "demo",
        "--input",
        r#"{"n": 1}"#,
        "--idempotency-key",
        "k1",
    ]);
    assert_eq!(started["outcome"], "started");
    let id = started["run_id"].as_str().unwrap().to_string();
    let again = cli.ok(&["start", "demo", "--input", "{}", "--idempotency-key", "k1"]);
    assert_eq!(
        (again["outcome"].as_str(), again["run_id"].as_str()),
        (Some("existing"), Some(id.as_str()))
    );

    let run = cli.ok(&["runs", "get", &id]);
    assert_eq!(
        (run["workflow"].as_str(), run["status"].as_str()),
        (Some("demo"), Some("pending"))
    );
    assert_eq!(run["input"], serde_json::json!({"n": 1}));

    let list = cli.ok(&["runs", "list", "--workflow", "demo"]);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(
        cli.ok(&["runs", "list", "--status", "failed"])
            .as_array()
            .unwrap()
            .len(),
        0
    );

    let events = cli.ok(&["runs", "events", &id]);
    assert_eq!(events[0]["kind"]["type"], "run_started");

    // A signal to a live run is accepted (it waits in the inbox).
    cli.ok(&["signal", &id, "go", "--payload", "42"]);
    cli.ok(&["event", "send", "thing.happened", "--data", r#"{"k": 1}"#]);

    // Retry only applies to failed runs.
    let retry = cli.run(&["runs", "retry", &id]);
    assert!(!retry.status.success());
    assert!(
        String::from_utf8_lossy(&retry.stderr).contains("failed"),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );

    cli.ok(&["runs", "cancel", &id, "--reason", "done testing"]);
    let run = cli.ok(&["runs", "get", &id]);
    assert!(run["cancel_requested_at"].is_string());

    // Unknown ids and bad JSON are reported with a non-zero exit status.
    let missing = cli.run(&["runs", "get", &uuid::Uuid::now_v7().to_string()]);
    assert!(!missing.status.success());
    let bad = cli.run(&["start", "demo", "--input", "{not json"]);
    assert!(!bad.status.success());
    assert!(
        String::from_utf8_lossy(&bad.stderr)
            .to_lowercase()
            .contains("json")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_runs_and_tail_streams_from_the_command_line() {
    use durable_workflow::{StepError, WfAsyncCtx, Worker, connect::BackendConfig, step};

    let (cli, _containers) = setup().await;
    cli.ok(&["migrate"]);

    let mut config = BackendConfig::new(cli.database_url.clone());
    config.redis_url = cli.redis_url.clone();
    config.namespace = "cli".into();
    config.schema = cli.schema.clone();
    let backend = config.build().await.unwrap();
    let worker = Worker::builder(backend)
        .poll_interval(std::time::Duration::from_millis(50))
        .register_fn("tokens", |ctx: WfAsyncCtx, words: Vec<String>| async move {
            let text: String = ctx
                .step("gen", move || async move {
                    let w = step::current().stream::<String>("tokens");
                    for word in &words {
                        w.write(word)?;
                    }
                    Ok::<_, StepError>(words.concat())
                })
                .await?;
            Ok(text)
        })
        .build()
        .unwrap()
        .spawn();
    worker.ready().await;

    let started = cli.ok(&["start", "tokens", "--input", r#"["he", "llo"]"#]);
    let id = started["run_id"].as_str().unwrap().to_string();

    // `watch` ends by itself once the run is terminal.
    let updates = cli.lines(&["runs", "watch", &id]);
    let kinds: Vec<_> = updates
        .iter()
        .filter(|u| u["type"] == "event")
        .map(|u| u["kind"]["type"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(kinds, ["run_started", "step_succeeded", "run_succeeded"]);
    let resumed = cli.lines(&["runs", "watch", &id, "--after", "2"]);
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0]["kind"]["type"], "run_succeeded");

    let list = cli.ok(&["streams", "list", &id]);
    assert_eq!(list[0]["stream"], "tokens");
    assert_eq!(list[0]["closed"], true);

    // Without a name, `tail` follows the run's only stream. The attempt's
    // first line is its start marker.
    let chunks = cli.lines(&["streams", "tail", &id]);
    assert_eq!(chunks[0]["body"]["attempt_started"], true);
    let data: Vec<_> = chunks
        .iter()
        .filter(|c| c["body"]["data"].is_string())
        .collect();
    let words: Vec<_> = data
        .iter()
        .map(|c| c["body"]["data"].as_str().unwrap())
        .collect();
    assert_eq!(words, ["he", "llo"]);
    assert_eq!(data[0]["step_key"], "gen");
    let cursor = data[0]["cursor"].as_str().unwrap();
    let rest = cli.lines(&["streams", "tail", &id, "tokens", "--after", cursor]);
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0]["body"]["data"], "llo");

    // Streams live in Redis, so the commands refuse to run without it.
    let no_redis = Cli {
        database_url: cli.database_url.clone(),
        redis_url: None,
        schema: cli.schema.clone(),
    };
    let out = no_redis.run(&["streams", "list", &id]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--redis-url"));
    worker.shutdown().await;
}
