//! Initializes and configures distributed tracing for the application.
//!
//! Sets up OpenTelemetry with an optional OTLP exporter and integrates it with
//! the tracing subscriber. Initialization is graceful. If exporter
//! configuration fails, the system continues with a no-op tracer. An optional
//! additional subscriber layer can be added.

use opentelemetry::global::set_meter_provider;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{
    ExporterBuildError, MetricExporter, Protocol, SpanExporter, WithExportConfig,
};
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::metrics::{
    Aggregation, Instrument, InstrumentKind, PeriodicReader, SdkMeterProvider, Stream,
};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use std::env;
use std::sync::OnceLock;
use thiserror::Error;
use tracing::error;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::{SetGlobalDefaultError, set_global_default};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::ParseError;
#[cfg(not(test))]
use tracing_subscriber::fmt;
use tracing_subscriber::layer::Identity as TracingIdentity;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer, Registry};

/// A layer that does nothing
pub type Identity = TracingIdentity;

#[cfg(test)]
mod tests;

/// Provider handles retained by [`initialize_tracing`].
///
/// `force_flush` and `shutdown` are methods on the providers. The global
/// subscriber keeps the export pipeline alive for the life of the process.
/// Unless the handles are retained here, telemetry buffered in the batch span
/// processor and periodic metric reader can never be exported before exit.
/// [`flush_telemetry`] and [`shutdown_telemetry`] read this slot.
struct OtelProviders {
    tracer: SdkTracerProvider,
    meter: SdkMeterProvider,
}

static PROVIDERS: OnceLock<OtelProviders> = OnceLock::new();

/// Initializes the tracing system with optional OpenTelemetry and OTLP
/// exporter.
///
/// Sets up the tracing subscriber with an OpenTelemetry layer and installs it
/// as the global default subscriber. Exporter initialization is attempted but
/// failures are handled gracefully. If the exporter cannot be configured (for
/// example a missing endpoint or a protocol error), the function continues with
/// a no-op tracer that does not export traces. The application still runs with
/// local tracing even when telemetry infrastructure is unavailable.
///
/// An optional additional layer can be added to the tracing subscriber; pass
/// `Identity` for `T` when there is none.
///
/// The tracer and meter provider handles are retained for the life of the
/// process so [`flush_telemetry`] and [`shutdown_telemetry`] can export
/// buffered telemetry before exit.
///
/// # Errors
///
/// This function returns an error if:
/// - Setting the global default subscriber fails
/// - Filter directive parsing fails
///
/// Note: OTLP exporter errors (missing endpoint, unknown protocol, exporter
/// build failures) are logged to stderr but do not cause the function to fail.
pub fn initialize_tracing<T>(layer: Option<T>) -> Result<(), TracingError>
where
    T: Layer<Layered<OpenTelemetryLayer<Registry, Tracer>, Registry>> + Send + Sync,
{
    // Filter traces using an environment variable directive
    let env_filter = EnvFilter::builder()
        .with_env_var("PROSODY_LOG")
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("scylla=warn".parse()?);

    // Create a tracing subscriber with OpenTelemetry layer
    #[allow(clippy::print_stderr, reason = "tracing is not initialized yet")]
    let exporter = build_exporter()
        .inspect_err(|error| eprintln!("failed to initialize OpenTelemetry OTLP exporter: {error}"))
        .ok();

    let trace_provider = match exporter {
        None => SdkTracerProvider::builder().build(),
        Some(exporter) => SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build(),
    };

    let telemetry = tracing_opentelemetry::layer().with_tracer(trace_provider.tracer("prosody"));

    let subscriber = Registry::default()
        .with(telemetry)
        .with(layer)
        .with(env_filter);

    // Set the subscriber as the global default
    set_global_default(subscriber)?;

    // Initialize the OTel meter provider alongside the tracer provider.
    #[allow(clippy::print_stderr, reason = "tracing is not initialized yet")]
    let meter_provider = match build_metric_exporter() {
        Ok(exporter) => SdkMeterProvider::builder()
            .with_view(exponential_histograms)
            .with_reader(PeriodicReader::builder(exporter).build())
            .build(),
        Err(e) => {
            eprintln!("failed to initialize OpenTelemetry OTLP metric exporter: {e}");
            SdkMeterProvider::builder()
                .with_view(exponential_histograms)
                .build()
        }
    };
    set_meter_provider(meter_provider.clone());

    // `set_global_default` succeeds at most once per process, so the slot is
    // necessarily empty here. The guard covers a future reordering of these steps.
    PROVIDERS
        .set(OtelProviders {
            tracer: trace_provider,
            meter: meter_provider,
        })
        .map_err(|_| TracingError::AlreadyInitialized)?;

    Ok(())
}

/// Selects base-2 exponential aggregation for all Prosody histograms.
pub(crate) fn exponential_histograms(instrument: &Instrument) -> Option<Stream> {
    if instrument.kind() != InstrumentKind::Histogram || !instrument.name().starts_with("prosody.")
    {
        return None;
    }

    match Stream::builder()
        .with_aggregation(Aggregation::Base2ExponentialHistogram {
            max_size: 160,
            max_scale: 20,
            record_min_max: false,
        })
        .build()
    {
        Ok(stream) => Some(stream),
        Err(error) => {
            error!(%error, "failed to configure a Prosody exponential histogram");
            None
        }
    }
}

/// Exports all buffered telemetry (spans and metrics) without tearing the
/// pipeline down.
///
/// The batch span processor and periodic metric reader export on an interval.
/// Telemetry recorded shortly before process exit is lost unless exported
/// explicitly. Use this when the process keeps running, for example when one
/// of several clients shuts down. Call [`shutdown_telemetry`] at process exit
/// instead. A safe no-op when [`initialize_tracing`] has not run.
///
/// Blocks until the export completes. Call it after async work has settled,
/// never from inside a handler.
///
/// # Errors
///
/// Returns an error if the span or metric exporter fails to flush. Both are
/// attempted regardless.
pub fn flush_telemetry() -> Result<(), TracingError> {
    let Some(providers) = PROVIDERS.get() else {
        return Ok(());
    };
    let spans = providers.tracer.force_flush();
    let metrics = providers.meter.force_flush();
    spans?;
    metrics?;
    Ok(())
}

/// Flushes all buffered telemetry and shuts the export pipeline down.
///
/// Call once at process exit, after all clients have stopped. Spans and
/// metrics recorded afterwards are silently dropped. A safe no-op when
/// [`initialize_tracing`] has not run. See [`flush_telemetry`] for a mid-run
/// flush that keeps the pipeline alive.
///
/// Blocks until the final export completes. Call it after async work has
/// settled, never from inside a handler.
///
/// # Errors
///
/// Returns an error if the span or metric pipeline fails to shut down, which
/// includes shutting down twice. Both are attempted regardless.
pub fn shutdown_telemetry() -> Result<(), TracingError> {
    let Some(providers) = PROVIDERS.get() else {
        return Ok(());
    };
    let spans = providers.tracer.shutdown();
    let metrics = providers.meter.shutdown();
    spans?;
    metrics?;
    Ok(())
}

/// Builds the OTLP span exporter for OpenTelemetry.
///
/// Creates an OTLP span exporter configured via environment variables.
/// The protocol is determined by `OTEL_EXPORTER_OTLP_PROTOCOL` (defaults to
/// "http/protobuf"). Fails if `OTEL_EXPORTER_OTLP_ENDPOINT` is unset,
/// `OTEL_EXPORTER_OTLP_PROTOCOL` names an unsupported protocol, or the
/// exporter itself fails to initialize.
///
/// # Environment Variables
///
/// * `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP endpoint URL (required)
/// * `OTEL_EXPORTER_OTLP_PROTOCOL` - Transport protocol: "grpc",
///   "http/protobuf", or "http/json" (defaults to "http/protobuf")
fn build_exporter() -> Result<SpanExporter, TracingError> {
    // Check if the OTLP endpoint is configured
    if env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Err(TracingError::MissingOtlpEndpoint);
    }

    // Determine the transport protocol, defaulting to http/protobuf
    let protocol =
        env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "http/protobuf".to_owned());

    let exporter = match protocol.as_str() {
        "http/protobuf" => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?,
        "http/json" => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .build()?,
        "grpc" => SpanExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .build()?,
        _ => return Err(TracingError::UnknownOtlpProtocol),
    };

    Ok(exporter)
}

/// Builds the OTLP metric exporter for OpenTelemetry.
///
/// Mirrors [`build_exporter`] but uses `MetricExporter` instead of
/// `SpanExporter`. The protocol is determined by
/// `OTEL_EXPORTER_OTLP_PROTOCOL` (defaults to "http/protobuf").
fn build_metric_exporter() -> Result<MetricExporter, TracingError> {
    if env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Err(TracingError::MissingOtlpEndpoint);
    }

    let protocol =
        env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "http/protobuf".to_owned());

    let exporter = match protocol.as_str() {
        "http/protobuf" => MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?,
        "http/json" => MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .build()?,
        "grpc" => MetricExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .build()?,
        _ => return Err(TracingError::UnknownOtlpProtocol),
    };

    Ok(exporter)
}

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    /// No OTLP endpoint was configured, so the exporter cannot be built.
    #[error(
        "missing OTEL_EXPORTER_OTLP_ENDPOINT environment variable; can't initialize OTLP exporter"
    )]
    MissingOtlpEndpoint,

    /// Unknown OTLP protocol specified in environment variable
    #[error(
        "unknown OTEL_EXPORTER_OTLP_PROTOCOL value; supported values are 'grpc', 'http/protobuf', \
         'http/json'"
    )]
    UnknownOtlpProtocol,

    /// Indicates a failure to initialize the trace exporter.
    #[error("failed to initialize the trace exporter: {0:#}")]
    Exporter(#[from] ExporterBuildError),

    /// Indicates a failure to set the default tracing subscriber.
    #[error("failed to set default tracing subscriber: {0:#}")]
    SetDefault(#[from] SetGlobalDefaultError),

    /// Tracing was already initialized by an earlier call.
    #[error("tracing already initialized")]
    AlreadyInitialized,

    /// Indicates a failure to flush or shut down the telemetry pipeline.
    #[error("failed to flush telemetry: {0:#}")]
    Flush(#[from] OTelSdkError),

    /// Indicates a failure to parse filter directive.
    #[error("failed to parse filter directive: {0:#}")]
    FilterParse(#[from] ParseError),
}

/// Initializes test tracing infrastructure.
///
/// This function is thread-safe and can be called multiple times. The
/// initialization happens only once. Call this at the beginning of any test
/// that uses tracing or OpenTelemetry span operations.
///
/// Defaults to ERROR level to reduce test noise. Set `PROSODY_LOG` environment
/// variable to override (e.g., `PROSODY_LOG=debug cargo test`).
#[cfg(test)]
pub fn init_test_logging() {
    use crate::test_util::init_global_test_tracing;

    let _ = init_global_test_tracing();
}

#[cfg(not(test))]
/// Initializes test tracing infrastructure.
pub fn init_test_logging() {
    use std::sync::Once;

    static INIT: Once = Once::new();

    INIT.call_once(|| {
        // Use ERROR level by default for tests to reduce noise.
        // Suppress "failed to set parent span" errors from modules that use
        // OpenTelemetry span parenting (not available in test environment).
        let env_filter = EnvFilter::builder()
            .with_env_var("PROSODY_LOG")
            .with_default_directive(LevelFilter::ERROR.into())
            .from_env_lossy()
            .add_directive("prosody::consumer::decode=off".parse().unwrap_or_default())
            .add_directive(
                "prosody::timers::store::cassandra=off"
                    .parse()
                    .unwrap_or_default(),
            );

        let subscriber = Registry::default()
            .with(fmt::layer().compact())
            .with(env_filter);

        let _ = set_global_default(subscriber);
    });
}
