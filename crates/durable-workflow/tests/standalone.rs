//! The standalone runner: health endpoints, optional ops API, graceful stop.

use std::time::Duration;

use durable_workflow::{
    StepError, WfAsyncCtx, WorkerBuilder,
    standalone::{ServeOptions, serve},
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

async fn http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: &str,
) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let auth = auth
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let status = out.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = out.split("\r\n\r\n").nth(1).unwrap_or_default().to_string();
    (status, body)
}

fn configure(b: WorkerBuilder) -> WorkerBuilder {
    b.poll_interval(Duration::from_millis(20)).register_fn(
        "double",
        |ctx: WfAsyncCtx, n: i64| async move {
            let v: i64 = ctx
                .step("d", move || async move { Ok::<_, StepError>(n * 2) })
                .await?;
            Ok(v)
        },
    )
}

async fn start(
    opts: ServeOptions,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let backend = durable_workflow::memory::backend();
    let task = tokio::spawn(async move {
        serve(backend, listener, opts, configure, async {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    // Wait until the listener answers.
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (addr, tx, task)
}

#[tokio::test]
async fn serves_health_probes_and_stops_gracefully() {
    let (addr, stop, task) = start(ServeOptions::default()).await;
    assert_eq!(http(addr, "GET", "/healthz", None, "").await.0, 200);
    assert_eq!(http(addr, "GET", "/readyz", None, "").await.0, 200);
    // The ops API is opt-in.
    assert_eq!(http(addr, "GET", "/durable/v1/runs", None, "").await.0, 404);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("stops promptly")
        .unwrap();
}

#[tokio::test]
async fn optional_ops_api_runs_workflows_registered_on_the_worker() {
    let opts = ServeOptions {
        enable_api: true,
        api_token: Some("tok".into()),
        ..Default::default()
    };
    let (addr, stop, task) = start(opts).await;

    assert_eq!(http(addr, "GET", "/durable/v1/runs", None, "").await.0, 401);
    let auth = Some("Bearer tok");
    let (status, body) = http(
        addr,
        "POST",
        "/durable/v1/workflows/double/runs",
        auth,
        r#"{"input": 21}"#,
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let id = serde_json::from_str::<Value>(&body).unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let mut output = Value::Null;
    for _ in 0..200 {
        let (_, b) = http(addr, "GET", &format!("/durable/v1/runs/{id}"), auth, "").await;
        let run: Value = serde_json::from_str(&b).unwrap();
        if run["status"] == "succeeded" {
            output = run["output"].clone();
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(output, serde_json::json!(42));
    // Probes stay open for orchestrators.
    assert_eq!(http(addr, "GET", "/healthz", None, "").await.0, 200);
    stop.send(()).unwrap();
    task.await.unwrap();
}
