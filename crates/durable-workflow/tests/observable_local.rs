//! The local sinks of `observable`, driven through scoped subscribers (no
//! global state, so tests run side by side).

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use durable_workflow::{
    Error,
    observable::{self, LogSink, ObservableConfig, Rotation, SpanSink},
};
use parking_lot::Mutex;
use serde_json::Value;
use tracing_subscriber::{Registry, layer::SubscriberExt};

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("dw-observable-{tag}-{}", uuid::Uuid::now_v7()))
}

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

impl Capture {
    fn lines(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().clone())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
            .collect()
    }
}

/// Run `f` under a subscriber built from `config`, then flush.
fn run_with(config: ObservableConfig, f: impl FnOnce()) -> Result<(), Error> {
    let parts = observable::layers::<Registry>(config)?;
    let subscriber = tracing_subscriber::registry().with(parts.layers);
    tracing::subscriber::with_default(subscriber, f);
    drop(parts.guard);
    Ok(())
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    v.sort();
    v
}

fn read_json_lines(dir: &Path) -> Vec<Value> {
    log_files(dir)
        .iter()
        .flat_map(|f| {
            fs::read_to_string(f)
                .unwrap()
                .lines()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .map(|l| serde_json::from_str(&l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
        .collect()
}

#[test]
fn file_logs_are_json_lines_carrying_the_enclosing_span_fields() {
    let dir = temp_dir("json");
    let cfg = ObservableConfig::default()
        .log_dir(&dir)
        .spans(SpanSink::None)
        .filter("info");
    run_with(cfg, || {
        let exec = tracing::info_span!(
            "workflow.execute",
            durable.run_id = "run-1",
            durable.workflow = "wf"
        );
        let _e = exec.enter();
        let step = tracing::info_span!(
            "step",
            durable.step.key = "gen",
            durable.step.attempt = 2u32
        );
        let _s = step.enter();
        tracing::info!(extra = 7, "hello from a step");
        tracing::debug!("filtered out at info");
    })
    .unwrap();

    let lines = read_json_lines(&dir);
    assert_eq!(lines.len(), 1, "{lines:?}");
    let line = &lines[0];
    assert_eq!(line["level"], "INFO");
    assert_eq!(line["fields"]["message"], "hello from a step");
    assert_eq!(line["fields"]["extra"], 7);
    assert!(line["timestamp"].is_string() && line["target"].is_string());
    // The current span is the innermost; the list adds the enclosing ones.
    assert_eq!(line["span"]["durable.step.key"], "gen");
    assert_eq!(line["span"]["durable.step.attempt"], 2);
    let outer = line["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "workflow.execute")
        .expect("enclosing span in the list");
    assert_eq!(outer["durable.run_id"], "run-1");
    assert_eq!(outer["durable.workflow"], "wf");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_missing_log_dir_is_created() {
    let dir = temp_dir("fresh").join("nested").join("logs");
    assert!(!dir.exists());
    let cfg = ObservableConfig::default()
        .log_dir(&dir)
        .spans(SpanSink::None);
    run_with(cfg, || tracing::info!("first line")).unwrap();
    assert!(dir.is_dir());
    assert_eq!(read_json_lines(&dir).len(), 1);
    fs::remove_dir_all(dir.parent().unwrap().parent().unwrap()).ok();
}

#[test]
fn an_unwritable_log_dir_fails_startup_naming_the_path_and_the_variable() {
    // A directory cannot be created below a regular file, whoever runs this.
    let file = temp_dir("blocked");
    fs::write(&file, b"not a directory").unwrap();
    let dir = file.join("logs");
    let err = observable::layers::<Registry>(ObservableConfig::default().log_dir(&dir))
        .err()
        .expect("startup fails");
    match err {
        Error::Config(msg) => {
            assert!(msg.contains(&dir.display().to_string()), "{msg}");
            assert!(msg.contains("DURABLE_LOG_DIR"), "{msg}");
        }
        other => panic!("expected a config error, got {other:?}"),
    }
    fs::remove_file(&file).ok();
}

#[test]
fn an_unwritable_dir_is_harmless_when_file_logs_are_off() {
    let file = temp_dir("blocked-off");
    fs::write(&file, b"x").unwrap();
    let cfg = ObservableConfig::default()
        .log_dir(file.join("logs"))
        .logs(LogSink::None);
    run_with(cfg, || tracing::info!("dropped")).unwrap();
    fs::remove_file(&file).ok();
}

#[test]
fn rotation_keeps_at_most_max_files() {
    let dir = temp_dir("prune");
    fs::create_dir_all(&dir).unwrap();
    // Stale files from earlier minutes, created oldest first.
    for minute in 0..4 {
        fs::write(
            dir.join(format!("durable.2020-01-01-00-0{minute}.log")),
            b"{}\n",
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(15));
    }
    let cfg = ObservableConfig::default()
        .log_dir(&dir)
        .log_rotation(Rotation::Minutely)
        .log_max_files(2)
        .spans(SpanSink::None);
    run_with(cfg, || tracing::info!("new")).unwrap();
    let files = log_files(&dir);
    assert!(files.len() <= 2, "{files:?}");
    let names: Vec<_> = files
        .iter()
        .map(|f| f.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names.contains(&"durable.2020-01-01-00-00.log".to_string()),
        "oldest file pruned: {names:?}"
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn closed_spans_go_to_the_span_writer_as_json_with_timings_and_events_do_not() {
    let dir = temp_dir("spans");
    let capture = Capture::default();
    let c = capture.clone();
    let cfg = ObservableConfig::default()
        .log_dir(&dir)
        .span_writer(move || c.clone());
    run_with(cfg, || {
        let span = tracing::info_span!(
            "workflow.execute",
            durable.run_id = "run-9",
            durable.outcome = tracing::field::Empty
        );
        span.in_scope(|| tracing::info!("an event, not a span"));
        span.record("durable.outcome", "finished");
    })
    .unwrap();

    let lines = capture.lines();
    assert_eq!(lines.len(), 1, "one record per closed span: {lines:?}");
    let rec = &lines[0];
    assert_eq!(rec["fields"]["message"], "close");
    assert!(rec["fields"]["time.busy"].is_string() && rec["fields"]["time.idle"].is_string());
    assert_eq!(rec["span"]["name"], "workflow.execute");
    assert_eq!(rec["span"]["durable.run_id"], "run-9");
    assert_eq!(rec["span"]["durable.outcome"], "finished");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn turning_everything_off_writes_nothing() {
    let dir = temp_dir("off");
    let capture = Capture::default();
    let c = capture.clone();
    let cfg = ObservableConfig::default()
        .log_dir(&dir)
        .spans(SpanSink::None)
        .logs(LogSink::None)
        .span_writer(move || c.clone());
    run_with(cfg, || {
        tracing::info_span!("s").in_scope(|| tracing::info!("x"));
    })
    .unwrap();
    assert!(capture.lines().is_empty());
    assert!(!dir.exists());
}

#[test]
fn a_bad_filter_is_a_config_error() {
    let err = observable::layers::<Registry>(ObservableConfig::default().filter("=[invalid"))
        .err()
        .unwrap();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}
