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

async fn setup() -> (Cli, impl Sized) {
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
    (cli, (pg, redis))
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
