use std::{sync::Arc, time::Duration};

use durable_spi::Observer;
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol as OtlpProtocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::{SdkTracer, SdkTracerProvider},
};
use tracing::Subscriber;
use tracing_subscriber::{Layer, filter::filter_fn, registry::LookupSpan};

use crate::{
    OtelObserver,
    config::{OtelConfig, Protocol, Signals},
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum OtelError {
    #[error("invalid OpenTelemetry configuration: {0}")]
    Config(String),
    #[error("cannot build the {signal} exporter: {error}")]
    Exporter { signal: &'static str, error: String },
}

fn exporter_err(signal: &'static str, e: impl std::fmt::Display) -> OtelError {
    OtelError::Exporter {
        signal,
        error: e.to_string(),
    }
}

/// Flushes and shuts the providers down when dropped.
#[must_use = "dropping the guard flushes and shuts down the exporters"]
pub struct OtelGuard {
    tracer: Option<SdkTracerProvider>,
    logger: Option<SdkLoggerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl OtelGuard {
    /// Push everything buffered to the exporters now.
    pub fn force_flush(&self) {
        if let Some(p) = &self.tracer {
            let _ = p.force_flush();
        }
        if let Some(p) = &self.logger {
            let _ = p.force_flush();
        }
        if let Some(p) = &self.meter {
            let _ = p.force_flush();
        }
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        self.force_flush();
        if let Some(p) = self.tracer.take() {
            let _ = p.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
        if let Some(p) = self.logger.take() {
            let _ = p.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
        if let Some(p) = self.meter.take() {
            let _ = p.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        }
    }
}

/// What [`layers`] built.
pub struct OtelParts {
    /// The signals that are exported.
    pub signals: Signals,
    tracer: Option<SdkTracer>,
    logger: Option<SdkLoggerProvider>,
    observer: Arc<OtelObserver>,
    guard: OtelGuard,
}

/// Exporters log through `tracing` too; keep that out of the log pipeline or
/// an export failure would produce the log record that fails to export.
fn is_export_internal(target: &str) -> bool {
    [
        "opentelemetry",
        "tonic",
        "h2",
        "hyper",
        "hyper_util",
        "reqwest",
        "tower",
    ]
    .iter()
    .any(|p| {
        target == *p
            || target.starts_with(&format!("{p}::"))
            || target.starts_with(&format!("{p}_"))
    })
}

impl OtelParts {
    /// `tracing` layers: spans (when traces are exported) and log records
    /// (when logs are). Add them to the subscriber.
    pub fn layers<S>(&self) -> Vec<Box<dyn Layer<S> + Send + Sync + 'static>>
    where
        S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
    {
        let mut out: Vec<Box<dyn Layer<S> + Send + Sync + 'static>> = vec![];
        if let Some(tracer) = &self.tracer {
            out.push(Box::new(
                tracing_opentelemetry::layer().with_tracer(tracer.clone()),
            ));
        }
        if let Some(logger) = &self.logger {
            out.push(Box::new(
                OpenTelemetryTracingBridge::new(logger)
                    .with_filter(filter_fn(|m| !is_export_internal(m.target()))),
            ));
        }
        out
    }

    /// Put this on the `Backend`: it propagates trace context and records
    /// the runtime's metrics.
    pub fn observer(&self) -> Arc<dyn Observer> {
        self.observer.clone()
    }

    /// The observer with its concrete type.
    pub fn otel_observer(&self) -> Arc<OtelObserver> {
        self.observer.clone()
    }

    /// Flush now without giving up the parts.
    pub fn force_flush(&self) {
        self.guard.force_flush();
    }

    /// The guard that flushes and shuts the exporters down on drop. Keep it
    /// until the end of the process.
    pub fn into_guard(self) -> OtelGuard {
        self.guard
    }
}

fn resource(config: &OtelConfig) -> Resource {
    let detected = Resource::builder().build();
    let unnamed = detected
        .get(&opentelemetry::Key::new("service.name"))
        .is_none_or(|v| v.as_str().starts_with("unknown_service"));
    let mut b = Resource::builder();
    if unnamed {
        b = b.with_service_name(
            config
                .service_name
                .clone()
                .unwrap_or_else(|| "durable-workflow".to_string()),
        );
    }
    b.with_attribute(KeyValue::new("telemetry.distro.name", "durable-workflow"))
        .build()
}

/// Build the providers for the signals `config` selects. Needs a Tokio
/// runtime for gRPC.
pub fn layers(config: OtelConfig) -> Result<OtelParts, OtelError> {
    let signals = config.configured();
    let resource = resource(&config);
    let mut config = config;
    let mut guard = OtelGuard {
        tracer: None,
        logger: None,
        meter: None,
    };
    let mut tracer = None;
    let mut logger = None;

    if signals.traces {
        let programmatic = !config.trace_setup.is_empty();
        let mut b = SdkTracerProvider::builder().with_resource(resource.clone());
        for setup in std::mem::take(&mut config.trace_setup) {
            b = setup(b);
        }
        if config.wants_otlp("TRACES", programmatic) {
            let exporter = match config.protocol("TRACES").map_err(OtelError::Config)? {
                Protocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .build(),
                Protocol::HttpProto => opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_protocol(OtlpProtocol::HttpBinary)
                    .build(),
            }
            .map_err(|e| exporter_err("trace", e))?;
            b = b.with_batch_exporter(exporter);
        }
        let provider = b.build();
        tracer = Some(provider.tracer("durable-workflow"));
        if config.set_global {
            global::set_tracer_provider(provider.clone());
        }
        guard.tracer = Some(provider);
    }

    if signals.logs {
        let programmatic = !config.log_setup.is_empty();
        let mut b = SdkLoggerProvider::builder().with_resource(resource.clone());
        for setup in std::mem::take(&mut config.log_setup) {
            b = setup(b);
        }
        if config.wants_otlp("LOGS", programmatic) {
            let exporter = match config.protocol("LOGS").map_err(OtelError::Config)? {
                Protocol::Grpc => opentelemetry_otlp::LogExporter::builder()
                    .with_tonic()
                    .build(),
                Protocol::HttpProto => opentelemetry_otlp::LogExporter::builder()
                    .with_http()
                    .with_protocol(OtlpProtocol::HttpBinary)
                    .build(),
            }
            .map_err(|e| exporter_err("log", e))?;
            b = b.with_batch_exporter(exporter);
        }
        let provider = b.build();
        logger = Some(provider.clone());
        guard.logger = Some(provider);
    }

    if signals.metrics {
        let programmatic = !config.metric_setup.is_empty();
        let mut b = SdkMeterProvider::builder().with_resource(resource);
        for setup in std::mem::take(&mut config.metric_setup) {
            b = setup(b, config.metric_interval());
        }
        if config.wants_otlp("METRICS", programmatic) {
            let exporter = match config.protocol("METRICS").map_err(OtelError::Config)? {
                Protocol::Grpc => opentelemetry_otlp::MetricExporter::builder()
                    .with_tonic()
                    .build(),
                Protocol::HttpProto => opentelemetry_otlp::MetricExporter::builder()
                    .with_http()
                    .with_protocol(OtlpProtocol::HttpBinary)
                    .build(),
            }
            .map_err(|e| exporter_err("metric", e))?;
            b = b.with_reader(
                PeriodicReader::builder(exporter)
                    .with_interval(config.metric_interval())
                    .build(),
            );
        }
        let provider = b.build();
        if config.set_global {
            global::set_meter_provider(provider.clone());
        }
        guard.meter = Some(provider);
    }

    if config.set_global {
        global::set_text_map_propagator(
            opentelemetry::propagation::TextMapCompositePropagator::new(vec![
                Box::new(TraceContextPropagator::new()),
                Box::new(BaggagePropagator::new()),
            ]),
        );
    }

    Ok(OtelParts {
        signals,
        tracer,
        logger,
        observer: Arc::new(OtelObserver::new(guard.meter.as_ref())),
        guard,
    })
}
