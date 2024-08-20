//! Initializes and configures distributed tracing for the application.
//!
//! This module sets up OpenTelemetry with an OTLP exporter and integrates it
//! with the tracing subscriber. It provides functionality to create a
//! customizable tracing setup with optional additional layers.

use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry_otlp::{new_exporter, new_pipeline};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::Tracer;
use std::env;
use thiserror::Error;
use tonic::transport::ClientTlsConfig;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::{set_global_default, SetGlobalDefaultError};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer, Registry};

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
/// This function can return a `TracingError` in the following cases:
/// - If the trace exporter initialization fails
/// - If setting the global default subscriber fails
pub fn initialize_tracing<T>(layer: Option<T>) -> Result<(), TracingError>
where
    T: Layer<Layered<Option<OpenTelemetryLayer<Registry, Tracer>>, Registry>> + Send + Sync,
{
    // Filter traces using an environment variable directive
    let env_filter = EnvFilter::builder()
        .with_env_var("PROSODY_LOG")
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    // Create a tracing subscriber with OpenTelemetry layer
    let telemetry = build_telemetry_layer().ok();
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
/// This function creates an OpenTelemetry tracer with an OTLP exporter
/// and wraps it in a `tracing_opentelemetry::Layer`.
///
/// # Returns
///
/// Returns `Ok(OpenTelemetryLayer<Registry, Tracer>)` if successful,
/// or a `TracingError` if an error occurs during the process.
///
/// # Errors
///
/// This function can return a `TracingError` in the following cases:
/// - If the OTLP endpoint is not configured (`MissingOtlpEndpoint`)
/// - If the trace exporter initialization fails (Exporter)
fn build_telemetry_layer() -> Result<OpenTelemetryLayer<Registry, Tracer>, TracingError> {
    // Check if the OTLP endpoint is configured
    if env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return Err(TracingError::MissingOtlpEndpoint);
    }

    // Create and install the OpenTelemetry tracer
    let tracer = new_pipeline()
        .tracing()
        .with_exporter(
            new_exporter()
                .tonic()
                .with_tls_config(ClientTlsConfig::default().with_native_roots()),
        )
        .install_batch(Tokio)?
        .tracer("prosody");

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    /// OTLP exporter could not be configured because no endpoint was configured
    #[error("missing OTEL_EXPORTER_OTLP_ENDPOINT environment variable; can't initialize tracing")]
    MissingOtlpEndpoint,

    /// Indicates a failure to initialize the trace exporter.
    #[error("failed to initialize the trace exporter: {0:#}")]
    Exporter(#[from] TraceError),

    /// Indicates a failure to set the default tracing subscriber.
    #[error("failed to set default tracing subscriber: {0:#}")]
    SetDefault(#[from] SetGlobalDefaultError),
}
