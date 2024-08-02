//! Initializes and configures distributed tracing for the application.
//!
//! This module sets up OpenTelemetry with OTLP exporter and integrates it
//! with the tracing subscriber.

use opentelemetry::global;
use opentelemetry::trace::TraceError;
use opentelemetry_otlp::{new_exporter, new_pipeline};
use opentelemetry_sdk::runtime::Tokio;
use thiserror::Error;
use tonic::transport::ClientTlsConfig;
use tracing::dispatcher::SetGlobalDefaultError;
use tracing::subscriber::set_global_default;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Initializes the tracing system with OpenTelemetry and OTLP exporter.
///
/// This function sets up the OpenTelemetry tracer with OTLP exporter,
/// configures it as the global tracer, and sets up a tracing subscriber
/// with an OpenTelemetry layer. It uses `EnvFilter` for filtering, which
/// is applied to the OpenTelemetry layer. Finally, it sets the configured
/// subscriber as the global default.
///
/// The `EnvFilter` is configured with a default level of "info" and can be
/// overridden using the `RUST_LOG` environment variable.
///
/// # Returns
///
/// Returns `Ok(())` if the tracing system is successfully initialized,
/// or a `TracingError` if an error occurs during the process.
///
/// # Errors
///
/// This function can return a `TracingError` if the trace exporter
/// initialization fails or if setting the global subscriber fails.
pub fn initialize_tracing() -> Result<(), TracingError> {
    // Create the OpenTelemetry tracer
    let tracer = new_pipeline()
        .tracing()
        .with_exporter(
            new_exporter()
                .tonic()
                .with_tls_config(ClientTlsConfig::default().with_native_roots()),
        )
        .install_batch(Tokio)?;

    // Set the tracer as the global default
    global::set_tracer_provider(tracer);

    // Create an EnvFilter with a default "info" level, overridable by RUST_LOG
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();

    // Set up the subscriber with OpenTelemetry and filtering
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_filter(env_filter));

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

    /// Indicates a failure to initialize the tracing subscriber.
    #[error("Failed to initialize the tracing subscriber: {0}")]
    SubscriberInit(#[from] tracing_subscriber::util::TryInitError),
}
