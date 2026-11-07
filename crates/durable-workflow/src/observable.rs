//! Local-first observability with no OpenTelemetry dependency.
//!
//! The runtime emits `tracing` spans (`workflow.start`, `workflow.execute`,
//! `step`) and ordinary `tracing` events. This module turns them into
//! something useful on a laptop: JSON spans on stdout and rotating JSON log
//! files whose lines carry the enclosing span fields (`durable.run_id`,
//! `durable.workflow`, `durable.step.key`, `durable.step.attempt`), so logs
//! and spans can be joined on the run id.
//!
//! Each signal resolves on its own: an explicit `DURABLE_{TRACES,LOGS,METRICS}`
//! wins, otherwise OTLP takes the signal when the `otel` feature is on and
//! that signal has OTLP configured, otherwise the local sink is used.
//!
//! | Signal | Local default | Other values |
//! |---|---|---|
//! | spans | `stdout`: one JSON line per closed span, with `time.busy`/`time.idle` | `none` |
//! | logs | `file`: rotating JSON lines | `stdout` (pretty), `none` |
//! | metrics | `none` | `none` |
//!
//! | Variable | Default | |
//! |---|---|---|
//! | `DURABLE_TRACES` | `stdout` | `stdout`, `none` |
//! | `DURABLE_LOGS` | `file` | `file`, `stdout`, `none` |
//! | `DURABLE_METRICS` | `none` | `none` |
//! | `DURABLE_LOG_DIR` | `./logs` | created if missing |
//! | `DURABLE_LOG_FILE_PREFIX` | `durable` | |
//! | `DURABLE_LOG_ROTATION` | `daily` | `minutely`, `hourly`, `daily`, `never` |
//! | `DURABLE_LOG_MAX_FILES` | `7` | older files are pruned |
//!
//! `RUST_LOG` filters as usual (default `info`).
//!
//! ```ignore
//! let _guard = durable_workflow::observable::init(ObservableConfig::from_env()?)?;
//! ```
//! Keep the guard alive until shutdown so buffered output is flushed. Apps
//! that already own a `tracing` subscriber call [`layers`] and add the result
//! to it instead.

use std::{io, path::PathBuf, sync::Arc};

use durable_spi::{Error, Observer};
use tracing::Subscriber;
use tracing_appender::{non_blocking::WorkerGuard, rolling};
use tracing_subscriber::{
    EnvFilter, Layer,
    filter::{FilterExt, filter_fn},
    fmt::{self, MakeWriter, format::FmtSpan},
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
};

pub type BoxLayer<S> = Box<dyn Layer<S> + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanSink {
    /// One JSON line per closed span.
    Stdout,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSink {
    /// Rotating JSON lines.
    File,
    /// Human-readable, for development.
    Stdout,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricSink {
    /// Local metrics are not implemented; use the `otel` feature.
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rotation {
    Minutely,
    Hourly,
    Daily,
    Never,
}

impl Rotation {
    fn to_appender(self) -> rolling::Rotation {
        match self {
            Self::Minutely => rolling::Rotation::MINUTELY,
            Self::Hourly => rolling::Rotation::HOURLY,
            Self::Daily => rolling::Rotation::DAILY,
            Self::Never => rolling::Rotation::NEVER,
        }
    }
}

type SpanWriterFactory = Arc<dyn Fn() -> Box<dyn io::Write + Send> + Send + Sync>;

/// Where each signal goes. `None` for a sink means "not chosen": OTLP takes
/// the signal if it is configured, otherwise the local default applies.
#[derive(Clone)]
pub struct ObservableConfig {
    pub spans: Option<SpanSink>,
    pub logs: Option<LogSink>,
    pub metrics: Option<MetricSink>,
    pub log_dir: PathBuf,
    pub log_file_prefix: String,
    pub log_rotation: Rotation,
    pub log_max_files: usize,
    /// Overrides `RUST_LOG`.
    pub filter: Option<String>,
    span_writer: Option<SpanWriterFactory>,
}

impl std::fmt::Debug for ObservableConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservableConfig")
            .field("spans", &self.spans)
            .field("logs", &self.logs)
            .field("metrics", &self.metrics)
            .field("log_dir", &self.log_dir)
            .field("log_file_prefix", &self.log_file_prefix)
            .field("log_rotation", &self.log_rotation)
            .field("log_max_files", &self.log_max_files)
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

impl Default for ObservableConfig {
    fn default() -> Self {
        Self {
            spans: None,
            logs: None,
            metrics: None,
            log_dir: PathBuf::from("./logs"),
            log_file_prefix: "durable".into(),
            log_rotation: Rotation::Daily,
            log_max_files: 7,
            filter: None,
            span_writer: None,
        }
    }
}

impl ObservableConfig {
    /// Reads the `DURABLE_*` variables listed in the module docs.
    pub fn from_env() -> Result<Self, Error> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// As [`from_env`](Self::from_env), over any variable lookup.
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Self, Error> {
        let mut c = Self::default();
        let choice = |var: &str, allowed: &[&str]| -> Result<Option<String>, Error> {
            match get(var).map(|v| v.trim().to_ascii_lowercase()) {
                Some(v) if allowed.contains(&v.as_str()) => Ok(Some(v)),
                Some(v) => Err(Error::Config(format!(
                    "{var}=`{v}` is not valid; expected one of: {}",
                    allowed.join(", ")
                ))),
                None => Ok(None),
            }
        };
        c.spans = choice("DURABLE_TRACES", &["stdout", "none"])?.map(|v| match v.as_str() {
            "stdout" => SpanSink::Stdout,
            _ => SpanSink::None,
        });
        c.logs = choice("DURABLE_LOGS", &["file", "stdout", "none"])?.map(|v| match v.as_str() {
            "file" => LogSink::File,
            "stdout" => LogSink::Stdout,
            _ => LogSink::None,
        });
        c.metrics = choice("DURABLE_METRICS", &["none"])?.map(|_| MetricSink::None);
        if let Some(dir) = get("DURABLE_LOG_DIR").filter(|d| !d.trim().is_empty()) {
            c.log_dir = PathBuf::from(dir);
        }
        if let Some(prefix) = get("DURABLE_LOG_FILE_PREFIX").filter(|p| !p.is_empty()) {
            c.log_file_prefix = prefix;
        }
        if let Some(r) = choice(
            "DURABLE_LOG_ROTATION",
            &["minutely", "hourly", "daily", "never"],
        )? {
            c.log_rotation = match r.as_str() {
                "minutely" => Rotation::Minutely,
                "hourly" => Rotation::Hourly,
                "daily" => Rotation::Daily,
                _ => Rotation::Never,
            };
        }
        if let Some(n) = get("DURABLE_LOG_MAX_FILES") {
            c.log_max_files = n.trim().parse().ok().filter(|n| *n >= 1).ok_or_else(|| {
                Error::Config(format!(
                    "DURABLE_LOG_MAX_FILES=`{n}` must be a number of at least 1"
                ))
            })?;
        }
        Ok(c)
    }

    pub fn spans(mut self, sink: SpanSink) -> Self {
        self.spans = Some(sink);
        self
    }
    pub fn logs(mut self, sink: LogSink) -> Self {
        self.logs = Some(sink);
        self
    }
    /// Directory for log files (`DURABLE_LOG_DIR`), created if missing.
    pub fn log_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.log_dir = dir.into();
        self
    }
    pub fn log_file_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.log_file_prefix = prefix.into();
        self
    }
    pub fn log_rotation(mut self, rotation: Rotation) -> Self {
        self.log_rotation = rotation;
        self
    }
    pub fn log_max_files(mut self, n: usize) -> Self {
        self.log_max_files = n.max(1);
        self
    }
    /// A `RUST_LOG`-style filter, instead of the environment's.
    pub fn filter(mut self, directives: impl Into<String>) -> Self {
        self.filter = Some(directives.into());
        self
    }
    /// Send span JSON to `make` (one writer per record) instead of stdout.
    pub fn span_writer<W>(mut self, make: impl Fn() -> W + Send + Sync + 'static) -> Self
    where
        W: io::Write + Send + 'static,
    {
        self.span_writer = Some(Arc::new(move || Box::new(make())));
        self
    }
}

/// Which signals OTLP has configured (and so takes unless chosen otherwise).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtlpSignals {
    pub traces: bool,
    pub logs: bool,
    pub metrics: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<L> {
    Local(L),
    Otlp,
}

/// The outcome of resolving each signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolution {
    pub spans: Route<SpanSink>,
    pub logs: Route<LogSink>,
    pub metrics: Route<MetricSink>,
}

impl Resolution {
    /// Signals OTLP exports.
    pub fn otlp(&self) -> OtlpSignals {
        OtlpSignals {
            traces: self.spans == Route::Otlp,
            logs: self.logs == Route::Otlp,
            metrics: self.metrics == Route::Otlp,
        }
    }
}

/// Explicit choice, else OTLP if configured, else the local default.
pub fn resolve(config: &ObservableConfig, otlp: OtlpSignals) -> Resolution {
    fn pick<L: Copy>(explicit: Option<L>, otlp: bool, default: L) -> Route<L> {
        match explicit {
            Some(l) => Route::Local(l),
            None if otlp => Route::Otlp,
            None => Route::Local(default),
        }
    }
    Resolution {
        spans: pick(config.spans, otlp.traces, SpanSink::Stdout),
        logs: pick(config.logs, otlp.logs, LogSink::File),
        metrics: pick(config.metrics, otlp.metrics, MetricSink::None),
    }
}

/// Flushes buffered output when dropped. Hold it until after
/// `worker.shutdown()`.
#[must_use = "dropping the guard flushes and stops the log writer"]
pub struct ObservableGuard {
    observer: Option<Arc<dyn Observer>>,
    #[cfg(feature = "otel")]
    otel: Option<durable_otel::OtelGuard>,
    file: Option<WorkerGuard>,
}

impl ObservableGuard {
    /// The metrics/trace-propagation observer to put on the `Backend`
    /// (`BackendBuilder::observer`); `None` unless OTLP is active.
    pub fn observer(&self) -> Option<Arc<dyn Observer>> {
        self.observer.clone()
    }
}

impl Drop for ObservableGuard {
    fn drop(&mut self) {
        // Exporters first, so their own logging still reaches the file.
        #[cfg(feature = "otel")]
        drop(self.otel.take());
        drop(self.file.take());
    }
}

/// Layers for an app that owns its subscriber.
pub struct ObservableLayers<S> {
    pub layers: Vec<BoxLayer<S>>,
    pub guard: ObservableGuard,
    pub resolution: Resolution,
}

struct DynWriter(SpanWriterFactory);

impl<'a> MakeWriter<'a> for DynWriter {
    type Writer = Box<dyn io::Write + Send>;
    fn make_writer(&'a self) -> Self::Writer {
        (self.0)()
    }
}

fn env_filter(config: &ObservableConfig) -> Result<EnvFilter, Error> {
    match &config.filter {
        Some(d) => EnvFilter::try_new(d)
            .map_err(|e| Error::Config(format!("invalid log filter `{d}`: {e}"))),
        None => Ok(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))),
    }
}

fn file_writer(config: &ObservableConfig) -> Result<rolling::RollingFileAppender, Error> {
    let dir = &config.log_dir;
    let unwritable = |e: &dyn std::fmt::Display| {
        Error::Config(format!(
            "cannot write logs to `{}` ({e}); set DURABLE_LOG_DIR to a writable directory \
             or DURABLE_LOGS=none",
            dir.display()
        ))
    };
    // Created here, so a bad path fails with a clear error and the appender
    // does not complain about pruning a directory that is not there yet.
    std::fs::create_dir_all(dir).map_err(|e| unwritable(&e))?;
    let appender = rolling::RollingFileAppender::builder()
        .rotation(config.log_rotation.to_appender())
        .filename_prefix(&config.log_file_prefix)
        .filename_suffix("log")
        .max_log_files(config.log_max_files)
        .build(dir)
        .map_err(|e| unwritable(&e))?;
    Ok(appender)
}

/// Build the local layers (and, with the `otel` feature, the OTLP ones) for
/// any subscriber.
pub fn layers<S>(config: ObservableConfig) -> Result<ObservableLayers<S>, Error>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    #[cfg(feature = "otel")]
    let otel_cfg = durable_otel::OtelConfig::from_env();
    #[cfg(feature = "otel")]
    let configured = {
        let c = otel_cfg.configured();
        OtlpSignals {
            traces: c.traces,
            logs: c.logs,
            metrics: c.metrics,
        }
    };
    #[cfg(not(feature = "otel"))]
    let configured = OtlpSignals::default();

    let resolution = resolve(&config, configured);
    let mut out: Vec<BoxLayer<S>> = vec![];
    #[allow(unused_mut)]
    let mut observer: Option<Arc<dyn Observer>> = None;

    #[cfg(feature = "otel")]
    let otel = {
        let wanted = resolution.otlp();
        if wanted.traces || wanted.logs || wanted.metrics {
            let otel_cfg = otel_cfg
                .traces(wanted.traces)
                .logs(wanted.logs)
                .metrics(wanted.metrics);
            let parts = durable_otel::layers(otel_cfg).map_err(|e| Error::Config(e.to_string()))?;
            for layer in parts.layers::<S>() {
                out.push(Box::new(layer.with_filter(env_filter(&config)?)));
            }
            observer = Some(parts.observer());
            Some(parts.into_guard())
        } else {
            None
        }
    };

    if resolution.spans == Route::Local(SpanSink::Stdout) {
        let filter = env_filter(&config)?.and(filter_fn(|m| m.is_span()));
        let layer = fmt::layer()
            .json()
            .with_span_events(FmtSpan::CLOSE)
            .with_current_span(true)
            .with_span_list(true);
        out.push(match &config.span_writer {
            Some(w) => Box::new(layer.with_writer(DynWriter(w.clone())).with_filter(filter)),
            None => Box::new(layer.with_writer(io::stdout).with_filter(filter)),
        });
    }

    let mut file_guard = None;
    match resolution.logs {
        Route::Local(LogSink::File) => {
            let appender = file_writer(&config)?;
            let (writer, guard) = tracing_appender::non_blocking(appender);
            file_guard = Some(guard);
            out.push(Box::new(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_writer(writer)
                    .with_filter(env_filter(&config)?),
            ));
        }
        Route::Local(LogSink::Stdout) => out.push(Box::new(
            fmt::layer().pretty().with_filter(env_filter(&config)?),
        )),
        Route::Local(LogSink::None) | Route::Otlp => {}
    }

    Ok(ObservableLayers {
        layers: out,
        guard: ObservableGuard {
            observer,
            #[cfg(feature = "otel")]
            otel,
            file: file_guard,
        },
        resolution,
    })
}

/// Install the global subscriber. A subscriber installed earlier stays; the
/// guard still flushes whatever this call set up.
pub fn init(config: ObservableConfig) -> Result<ObservableGuard, Error> {
    let parts = layers::<tracing_subscriber::Registry>(config)?;
    let _ = tracing_subscriber::registry().with(parts.layers).try_init();
    Ok(parts.guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    fn cfg(pairs: &[(&str, &str)]) -> ObservableConfig {
        ObservableConfig::from_env_with(env(pairs)).unwrap()
    }

    #[test]
    fn no_configuration_means_stdout_spans_file_logs_and_no_metrics() {
        let r = resolve(&cfg(&[]), OtlpSignals::default());
        assert_eq!(r.spans, Route::Local(SpanSink::Stdout));
        assert_eq!(r.logs, Route::Local(LogSink::File));
        assert_eq!(r.metrics, Route::Local(MetricSink::None));
        assert_eq!(r.otlp(), OtlpSignals::default());
    }

    #[test]
    fn otlp_takes_only_the_signals_it_has_configured() {
        let only_traces = OtlpSignals {
            traces: true,
            ..Default::default()
        };
        let r = resolve(&cfg(&[]), only_traces);
        assert_eq!(r.spans, Route::Otlp);
        assert_eq!(r.logs, Route::Local(LogSink::File), "logs stay local");
        assert_eq!(r.metrics, Route::Local(MetricSink::None));
        let all = OtlpSignals {
            traces: true,
            logs: true,
            metrics: true,
        };
        let r = resolve(&cfg(&[]), all);
        assert_eq!(
            (r.spans, r.logs, r.metrics),
            (Route::Otlp, Route::Otlp, Route::Otlp)
        );
    }

    #[test]
    fn an_explicit_choice_beats_otlp() {
        let all = OtlpSignals {
            traces: true,
            logs: true,
            metrics: true,
        };
        let r = resolve(
            &cfg(&[
                ("DURABLE_TRACES", "none"),
                ("DURABLE_LOGS", "stdout"),
                ("DURABLE_METRICS", "none"),
            ]),
            all,
        );
        assert_eq!(r.spans, Route::Local(SpanSink::None));
        assert_eq!(r.logs, Route::Local(LogSink::Stdout));
        assert_eq!(r.metrics, Route::Local(MetricSink::None));
        assert_eq!(r.otlp(), OtlpSignals::default());
    }

    #[test]
    fn env_values_are_parsed_and_validated() {
        let c = cfg(&[
            ("DURABLE_LOG_DIR", "/var/log/dw"),
            ("DURABLE_LOG_FILE_PREFIX", "svc"),
            ("DURABLE_LOG_ROTATION", "Hourly"),
            ("DURABLE_LOG_MAX_FILES", "3"),
        ]);
        assert_eq!(c.log_dir, PathBuf::from("/var/log/dw"));
        assert_eq!(c.log_file_prefix, "svc");
        assert_eq!(c.log_rotation, Rotation::Hourly);
        assert_eq!(c.log_max_files, 3);

        for (k, v) in [
            ("DURABLE_TRACES", "otlp"),
            ("DURABLE_LOGS", "syslog"),
            ("DURABLE_METRICS", "prometheus"),
            ("DURABLE_LOG_ROTATION", "weekly"),
            ("DURABLE_LOG_MAX_FILES", "0"),
            ("DURABLE_LOG_MAX_FILES", "many"),
        ] {
            let err = ObservableConfig::from_env_with(env(&[(k, v)])).unwrap_err();
            assert!(
                matches!(&err, Error::Config(m) if m.contains(k)),
                "{k}={v}: {err}"
            );
        }
    }

    #[test]
    fn defaults_match_the_documented_ones() {
        let c = cfg(&[]);
        assert_eq!(c.log_dir, PathBuf::from("./logs"));
        assert_eq!(c.log_file_prefix, "durable");
        assert_eq!(c.log_rotation, Rotation::Daily);
        assert_eq!(c.log_max_files, 7);
    }
}
