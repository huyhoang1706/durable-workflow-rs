use std::{collections::HashMap, time::Duration};

use opentelemetry_sdk::{
    logs::{LogExporter, LoggerProviderBuilder},
    metrics::{MeterProviderBuilder, PeriodicReader, exporter::PushMetricExporter},
    trace::{SpanExporter, TracerProviderBuilder},
};

/// One flag per signal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Signals {
    pub traces: bool,
    pub logs: bool,
    pub metrics: bool,
}

pub(crate) type TraceSetup = Box<dyn FnOnce(TracerProviderBuilder) -> TracerProviderBuilder + Send>;
pub(crate) type LogSetup = Box<dyn FnOnce(LoggerProviderBuilder) -> LoggerProviderBuilder + Send>;
/// Gets the metric export interval along with the builder.
pub(crate) type MetricSetup =
    Box<dyn FnOnce(MeterProviderBuilder, Duration) -> MeterProviderBuilder + Send>;

/// Which signals to export, and where.
///
/// A signal is exported when it has OTLP configured in the environment (a
/// per-signal or global `OTEL_EXPORTER_OTLP_*ENDPOINT`, or
/// `OTEL_{TRACES,METRICS,LOGS}_EXPORTER=otlp`) or a programmatic exporter, and
/// has not been switched off with [`traces`](Self::traces) & co. or
/// `OTEL_SDK_DISABLED` / `OTEL_*_EXPORTER=none`.
pub struct OtelConfig {
    vars: HashMap<String, String>,
    traces: Option<bool>,
    logs: Option<bool>,
    metrics: Option<bool>,
    pub(crate) set_global: bool,
    pub(crate) service_name: Option<String>,
    pub(crate) trace_setup: Vec<TraceSetup>,
    pub(crate) log_setup: Vec<LogSetup>,
    pub(crate) metric_setup: Vec<MetricSetup>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    Grpc,
    HttpProto,
}

impl OtelConfig {
    /// Reads the process's `OTEL_*` variables.
    pub fn from_env() -> Self {
        Self::from_vars(std::env::vars().filter(|(k, _)| k.starts_with("OTEL_")))
    }

    /// As [`from_env`](Self::from_env), over explicit variables.
    pub fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            vars: vars.into_iter().collect(),
            traces: None,
            logs: None,
            metrics: None,
            set_global: true,
            service_name: None,
            trace_setup: vec![],
            log_setup: vec![],
            metric_setup: vec![],
        }
    }

    /// Force the traces signal on or off, whatever the environment says.
    pub fn traces(mut self, on: bool) -> Self {
        self.traces = Some(on);
        self
    }
    pub fn logs(mut self, on: bool) -> Self {
        self.logs = Some(on);
        self
    }
    pub fn metrics(mut self, on: bool) -> Self {
        self.metrics = Some(on);
        self
    }

    /// Resource `service.name`, when `OTEL_SERVICE_NAME` is not set.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Do not touch the process-wide OpenTelemetry providers and propagator
    /// (they are set by default).
    pub fn without_globals(mut self) -> Self {
        self.set_global = false;
        self
    }

    /// Export spans with `exporter` (batched), in addition to any OTLP one.
    pub fn span_exporter(mut self, exporter: impl SpanExporter + 'static) -> Self {
        self.trace_setup
            .push(Box::new(move |b| b.with_batch_exporter(exporter)));
        self
    }

    /// As [`span_exporter`](Self::span_exporter), but each span is exported as
    /// it ends. Meant for tests.
    pub fn span_exporter_simple(mut self, exporter: impl SpanExporter + 'static) -> Self {
        self.trace_setup
            .push(Box::new(move |b| b.with_simple_exporter(exporter)));
        self
    }

    /// Export log records with `exporter` (batched).
    pub fn log_exporter(mut self, exporter: impl LogExporter + 'static) -> Self {
        self.log_setup
            .push(Box::new(move |b| b.with_batch_exporter(exporter)));
        self
    }

    /// As [`log_exporter`](Self::log_exporter), exporting each record at once.
    pub fn log_exporter_simple(mut self, exporter: impl LogExporter + 'static) -> Self {
        self.log_setup
            .push(Box::new(move |b| b.with_simple_exporter(exporter)));
        self
    }

    /// Export metrics with `exporter`, periodically
    /// (`OTEL_METRIC_EXPORT_INTERVAL`, default 60 s; flushed on guard drop).
    pub fn metric_exporter(mut self, exporter: impl PushMetricExporter) -> Self {
        self.metric_setup.push(Box::new(move |b, every| {
            b.with_reader(
                PeriodicReader::builder(exporter)
                    .with_interval(every)
                    .build(),
            )
        }));
        self
    }

    fn var(&self, k: &str) -> Option<&str> {
        self.vars
            .get(k)
            .map(String::as_str)
            .filter(|v| !v.is_empty())
    }

    fn sdk_disabled(&self) -> bool {
        self.var("OTEL_SDK_DISABLED")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// `signal` is `TRACES`, `METRICS` or `LOGS`.
    fn env_otlp(&self, signal: &str) -> bool {
        if self.sdk_disabled() {
            return false;
        }
        match self.var(&format!("OTEL_{signal}_EXPORTER")) {
            Some(v) => v.split(',').any(|e| e.trim() == "otlp"),
            None => {
                self.var(&format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT"))
                    .is_some()
                    || self.var("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
            }
        }
    }

    /// The signals that will be exported.
    pub fn configured(&self) -> Signals {
        let off = self.sdk_disabled();
        let pick = |forced: Option<bool>, signal: &str, programmatic: bool| {
            forced.unwrap_or_else(|| !off && (self.env_otlp(signal) || programmatic))
        };
        Signals {
            traces: pick(self.traces, "TRACES", !self.trace_setup.is_empty()),
            logs: pick(self.logs, "LOGS", !self.log_setup.is_empty()),
            metrics: pick(self.metrics, "METRICS", !self.metric_setup.is_empty()),
        }
    }

    /// True when the signal should also go to an OTLP endpoint (as opposed to
    /// only to programmatic exporters).
    pub(crate) fn wants_otlp(&self, signal: &str, programmatic: bool) -> bool {
        self.env_otlp(signal) || !programmatic
    }

    pub(crate) fn protocol(&self, signal: &str) -> Result<Protocol, String> {
        let explicit = self
            .var(&format!("OTEL_EXPORTER_OTLP_{signal}_PROTOCOL"))
            .or_else(|| self.var("OTEL_EXPORTER_OTLP_PROTOCOL"));
        match explicit {
            Some("grpc") => Ok(Protocol::Grpc),
            Some("http/protobuf") => Ok(Protocol::HttpProto),
            Some(other) => Err(format!(
                "unsupported OTLP protocol `{other}` (use `grpc` or `http/protobuf`)"
            )),
            None => {
                // 4318 is the conventional OTLP/HTTP port.
                let endpoint = self
                    .var(&format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT"))
                    .or_else(|| self.var("OTEL_EXPORTER_OTLP_ENDPOINT"))
                    .unwrap_or_default();
                Ok(if endpoint.contains(":4318") {
                    Protocol::HttpProto
                } else {
                    Protocol::Grpc
                })
            }
        }
    }

    pub(crate) fn metric_interval(&self) -> Duration {
        self.var("OTEL_METRIC_EXPORT_INTERVAL")
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::from_secs(60), Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(pairs: &[(&str, &str)]) -> OtelConfig {
        OtelConfig::from_vars(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    }

    #[test]
    fn nothing_is_exported_without_configuration() {
        assert_eq!(cfg(&[]).configured(), Signals::default());
    }

    #[test]
    fn a_global_endpoint_enables_every_signal() {
        let s = cfg(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")]).configured();
        assert_eq!(
            s,
            Signals {
                traces: true,
                logs: true,
                metrics: true
            }
        );
    }

    #[test]
    fn a_per_signal_endpoint_enables_only_that_signal() {
        let s = cfg(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://c:4317")]).configured();
        assert_eq!(
            s,
            Signals {
                traces: true,
                ..Default::default()
            }
        );
    }

    #[test]
    fn the_exporter_variable_selects_and_none_disables() {
        let s = cfg(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4317"),
            ("OTEL_LOGS_EXPORTER", "none"),
            ("OTEL_METRICS_EXPORTER", "otlp"),
        ])
        .configured();
        assert_eq!(
            s,
            Signals {
                traces: true,
                logs: false,
                metrics: true
            }
        );
        let only = cfg(&[("OTEL_TRACES_EXPORTER", "otlp")]).configured();
        assert!(only.traces && !only.logs && !only.metrics);
    }

    #[test]
    fn sdk_disabled_turns_everything_off() {
        let s = cfg(&[
            ("OTEL_SDK_DISABLED", "true"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4317"),
        ])
        .configured();
        assert_eq!(s, Signals::default());
    }

    #[test]
    fn explicit_flags_override_the_environment() {
        let c = cfg(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4317")])
            .traces(false)
            .metrics(false);
        assert_eq!(
            c.configured(),
            Signals {
                traces: false,
                logs: true,
                metrics: false
            }
        );
    }

    #[test]
    fn protocol_follows_the_variables_then_the_port() {
        assert_eq!(cfg(&[]).protocol("TRACES"), Ok(Protocol::Grpc));
        assert_eq!(
            cfg(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318")]).protocol("TRACES"),
            Ok(Protocol::HttpProto)
        );
        assert_eq!(
            cfg(&[
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
                ("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf"),
            ])
            .protocol("METRICS"),
            Ok(Protocol::HttpProto)
        );
        assert!(
            cfg(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")])
                .protocol("TRACES")
                .is_err()
        );
    }
}
