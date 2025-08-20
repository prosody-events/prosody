//! Initializes and configures distributed tracing for the application.
//!
//! This module sets up OpenTelemetry with an OTLP exporter and integrates it
//! with the tracing subscriber. It provides functionality to create a
//! customizable tracing setup with optional additional layers.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{ExporterBuildError, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use std::env;
use thiserror::Error;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::{SetGlobalDefaultError, set_global_default};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::layer::Identity as TracingIdentity;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer, Registry};

/// A layer that does nothing
pub type Identity = TracingIdentity;

/// Initializes the tracing system with OpenTelemetry and OTLP exporter.
///
/// This function sets up the OpenTelemetry tracer with OTLP exporter,
/// creates a tracing subscriber with the OpenTelemetry layer, and sets
/// it as the global default subscriber. It also allows for an optional
/// additional layer to be added to the tracing subscriber.
///
/// # Arguments
///
/// * `layer` - An optional additional layer to be added to the tracing
///   subscriber. Use Identity for T if layer is None.
///
/// # Returns
///
/// Returns `Ok(())` if the tracing system is successfully initialized,
/// or a `TracingError` if an error occurs during the process.
///
/// # Errors
///
/// This function returns an error if:
/// - The OTLP endpoint is not configured via `OTEL_EXPORTER_OTLP_ENDPOINT`
/// - An unknown protocol is specified in `OTEL_EXPORTER_OTLP_PROTOCOL`
/// - The trace exporter initialization fails
/// - Setting the global default subscriber fails
/// - Filter directive parsing fails
pub fn initialize_tracing<T>(layer: Option<T>) -> Result<(), TracingError>
where
    T: Layer<Layered<Option<OpenTelemetryLayer<Registry, Tracer>>, Registry>> + Send + Sync,
{
    // Filter traces using an environment variable directive
    let env_filter = EnvFilter::builder()
        .with_env_var("PROSODY_LOG")
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("scylla=warn".parse()?);

    // Create a tracing subscriber with OpenTelemetry layer
    #[allow(clippy::print_stderr, reason = "tracing is not initialized yet")]
    let telemetry = build_telemetry_layer()
        .inspect_err(|error| eprintln!("failed to initialize OpenTelemetry: {error}"))
        .ok();

    let subscriber = Registry::default()
        .with(telemetry)
        .with(layer)
        .with(env_filter);

    // Set the subscriber as the global default
    set_global_default(subscriber)?;

    Ok(())
}

/// Builds the OpenTelemetry layer for the tracing subscriber.
///
/// Creates an OpenTelemetry tracer with an OTLP exporter configured via
/// environment variables and wraps it in a [`tracing_opentelemetry::Layer`].
/// The protocol is determined by `OTEL_EXPORTER_OTLP_PROTOCOL` (defaults to
/// "grpc").
///
/// # Environment Variables
///
/// * `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP endpoint URL (required)
/// * `OTEL_EXPORTER_OTLP_PROTOCOL` - Transport protocol: "grpc",
///   "http/protobuf", or "http/json" (defaults to "grpc")
///
/// # Errors
///
/// This function returns an error if:
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable is not set
/// - `OTEL_EXPORTER_OTLP_PROTOCOL` contains an unsupported protocol value
/// - The trace exporter initialization fails
fn build_telemetry_layer() -> Result<OpenTelemetryLayer<Registry, Tracer>, TracingError> {
    // Check if the OTLP endpoint is configured
    if env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Err(TracingError::MissingOtlpEndpoint);
    }

    // Create and install the OpenTelemetry tracer
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

    let tracer = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build()
        .tracer("prosody");

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    /// OTLP exporter could not be configured because no endpoint was configured
    #[error("missing OTEL_EXPORTER_OTLP_ENDPOINT environment variable; can't initialize tracing")]
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

    /// Indicates a failure to parse filter directive.
    #[error("failed to parse filter directive: {0:#}")]
    FilterParse(#[from] ParseError),
}
