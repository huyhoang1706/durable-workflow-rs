//! End to end with a real worker: the runtime's spans and a step's own logs
//! land in the local sinks and can be joined on `durable.run_id`.

use std::{fs, io::Write, sync::Arc, time::Duration};

use durable_workflow::{
    Client, StepError, WfAsyncCtx, Worker,
    observable::{self, ObservableConfig},
};
use parking_lot::Mutex;
use serde_json::{Value, json};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn json_lines(text: &str) -> Vec<Value> {
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_spans_and_step_logs_join_on_the_run_id() {
    let dir = std::env::temp_dir().join(format!("dw-observable-e2e-{}", uuid::Uuid::now_v7()));
    let spans = Capture::default();
    let sink = spans.clone();
    let guard = observable::init(
        ObservableConfig::default()
            .log_dir(&dir)
            .filter("info")
            .span_writer(move || sink.clone()),
    )
    .unwrap();
    assert!(guard.observer().is_none(), "no OTLP, so no observer");

    let backend = durable_workflow::memory::backend();
    let worker = Worker::builder(backend.clone())
        .poll_interval(Duration::from_millis(20))
        .register_fn("greet", |ctx: WfAsyncCtx, name: String| async move {
            let msg: String = ctx
                .step("compose", async move {
                    tracing::info!(who = %name, "composing a greeting");
                    Ok::<_, StepError>(format!("hello {name}"))
                })
                .await?;
            Ok(msg)
        })
        .build()
        .unwrap()
        .spawn();
    worker.ready().await;
    let client = Client::new(backend);
    let handle = client
        .start_raw("greet", json!("ada"), Default::default())
        .await
        .unwrap()
        .handle()
        .unwrap()
        .clone();
    let out: String = handle.result().await.unwrap();
    assert_eq!(out, "hello ada");
    let run_id = handle.id().to_string();
    worker.shutdown().await;
    drop(guard);

    // Logs: the step's line carries the run id and step key from its spans.
    let mut logs = vec![];
    for entry in fs::read_dir(&dir).unwrap() {
        logs.extend(json_lines(
            &fs::read_to_string(entry.unwrap().path()).unwrap(),
        ));
    }
    let line = logs
        .iter()
        .find(|l| l["fields"]["message"] == "composing a greeting")
        .unwrap_or_else(|| panic!("step log missing: {logs:?}"));
    assert_eq!(line["span"]["durable.step.key"], "compose");
    assert_eq!(line["span"]["durable.step.attempt"], 1);
    assert_eq!(line["span"]["durable.run_id"], run_id.as_str());
    assert_eq!(line["span"]["durable.workflow"], "greet");

    // Spans: one record per closed span, with the outcome set at the end.
    let text = String::from_utf8(spans.0.lock().clone()).unwrap();
    let closed = json_lines(&text);
    let named = |name: &str| {
        closed
            .iter()
            .find(|r| r["span"]["name"] == name)
            .unwrap_or_else(|| panic!("no closed `{name}` span in {closed:?}"))
    };
    assert_eq!(
        named("workflow.start")["span"]["durable.run_id"],
        run_id.as_str()
    );
    let exec = named("workflow.execute");
    assert_eq!(exec["span"]["durable.run_id"], run_id.as_str());
    assert_eq!(exec["span"]["durable.outcome"], "finished");
    let step = named("step");
    assert_eq!(step["span"]["durable.step.outcome"], "succeeded");
    assert!(step["fields"]["time.busy"].is_string());
    fs::remove_dir_all(&dir).ok();
}
