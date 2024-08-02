//! Initializes and configures distributed tracing for the application.
//!
//! This module sets up OpenTelemetry with OTLP exporter and integrates it
//! with the tracing subscriber.

use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry_otlp::{new_exporter, new_pipeline};
use opentelemetry_sdk::runtime::Tokio;
use thiserror::Error;
use tonic::transport::ClientTlsConfig;
use tracing::subscriber::{set_global_default, SetGlobalDefaultError};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

/// Initializes the tracing system with OpenTelemetry and OTLP exporter.
///
/// This function sets up the OpenTelemetry tracer with OTLP exporter,
/// creates a tracing subscriber with the OpenTelemetry layer, and sets
/// it as the global default subscriber.
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
pub fn initialize_tracing() -> Result<(), TracingError> {
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

    // Create a tracing subscriber with OpenTelemetry layer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);

    // Set the subscriber as the global default
    set_global_default(subscriber)?;

    Ok(())
}

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    /// Indicates a failure to initialize the trace exporter.
    #[error("failed to initialize the trace exporter: {0:#}")]
    Exporter(#[from] TraceError),

    /// Indicates a failure to set the default tracing subscriber.
    #[error("failed to set default tracing subscriber: {0:#}")]
    SetDefault(#[from] SetGlobalDefaultError),
}
