//! Initializes and configures distributed tracing for the application.
//!
//! This module sets up OpenTelemetry with an optional OTLP exporter and
//! integrates it with the tracing subscriber. OpenTelemetry initialization is
//! graceful - if exporter configuration fails, the system continues with a
//! no-op tracer. This provides functionality to create a customizable tracing
//! setup with optional additional layers.
//!
//! ## Sentry Integration
//!
//! If `SENTRY_DSN` is set, Sentry error tracking is automatically enabled.
//! Events are sent asynchronously as they occur; the guard is held in a static
//! so callers don't need to manage its lifetime.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{ExporterBuildError, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use sentry::ClientInitGuard;
use sentry::integrations::tracing::layer as sentry_tracing_layer;
use std::env;
use std::sync::OnceLock;
use thiserror::Error;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::{SetGlobalDefaultError, set_global_default};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::layer::Identity as TracingIdentity;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt};

/// A layer that does nothing
pub type Identity = TracingIdentity;

/// Sentry guard held for the lifetime of the process.
static SENTRY_GUARD: OnceLock<ClientInitGuard> = OnceLock::new();

/// Initializes the tracing system with optional OpenTelemetry and OTLP
/// exporter.
///
/// This function sets up the tracing subscriber with an OpenTelemetry layer and
/// sets it as the global default subscriber. OpenTelemetry exporter
/// initialization is attempted but failures are handled gracefully - if the
/// exporter cannot be configured (e.g., missing endpoint, protocol errors), the
/// function continues with a no-op tracer that doesn't export traces. This
/// ensures the application can still run with local tracing even when telemetry
/// infrastructure is unavailable.
///
/// An optional additional layer can be added to the tracing subscriber.
///
/// # Sentry Integration
///
/// If `SENTRY_DSN` is set, Sentry error tracking is automatically enabled.
/// The guard is stored in a static and held for the process lifetime.
///
/// # Arguments
///
/// * `layer` - An optional additional layer to be added to the tracing
///   subscriber. Use Identity for T if layer is None.
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
    // Initialize Sentry if SENTRY_DSN is set (guard stored in static)
    let sentry_enabled = init_sentry();

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
        None => SdkTracerProvider::builder().build().tracer("prosody"),
        Some(exporter) => SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build()
            .tracer("prosody"),
    };

    let telemetry = tracing_opentelemetry::layer().with_tracer(trace_provider);

    // Add Sentry tracing layer if Sentry is enabled
    let sentry_layer = sentry_enabled.then(sentry_tracing_layer);

    let subscriber = Registry::default()
        .with(telemetry)
        .with(layer)
        .with(sentry_layer)
        .with(env_filter);

    // Set the subscriber as the global default
    set_global_default(subscriber)?;

    Ok(())
}

/// Initializes Sentry if `SENTRY_DSN` environment variable is set.
///
/// The guard is stored in [`SENTRY_GUARD`] and held for the process lifetime.
/// Events are sent asynchronously as they occur.
///
/// # Configuration
///
/// * `in_app_include` is set to `["prosody"]` so Sentry marks frames from this
///   crate as "in-app" code, making them stand out in stack traces.
/// * `server_name` is set to the hostname for identifying which instance
///   reported the error.
///
/// Returns `true` if Sentry was successfully initialized, `false` otherwise.
#[allow(clippy::print_stderr)]
fn init_sentry() -> bool {
    let Some(dsn) = env::var("SENTRY_DSN").ok().filter(|s| !s.is_empty()) else {
        eprintln!("SENTRY_DSN not set, skipping Sentry initialization");
        return false;
    };

    eprintln!(
        "Initializing Sentry with DSN: {}...",
        &dsn[..50.min(dsn.len())]
    );

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            server_name: whoami::hostname().ok().map(Into::into),
            in_app_include: vec!["prosody"],
            ..sentry::ClientOptions::default()
        },
    ));

    if guard.is_enabled() {
        eprintln!("Sentry initialized successfully, guard stored");
        let _ = SENTRY_GUARD.set(guard);
        true
    } else {
        eprintln!("Sentry guard not enabled after init");
        false
    }
}

/// Builds the OTLP span exporter for OpenTelemetry.
///
/// Creates an OTLP span exporter configured via environment variables.
/// The protocol is determined by `OTEL_EXPORTER_OTLP_PROTOCOL` (defaults to
/// "http/protobuf").
///
/// # Environment Variables
///
/// * `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP endpoint URL (required)
/// * `OTEL_EXPORTER_OTLP_PROTOCOL` - Transport protocol: "grpc",
///   "http/protobuf", or "http/json" (defaults to "http/protobuf")
///
/// # Errors
///
/// This function returns an error if:
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable is not set
/// - `OTEL_EXPORTER_OTLP_PROTOCOL` contains an unsupported protocol value
/// - The trace exporter initialization fails
fn build_exporter() -> Result<SpanExporter, TracingError> {
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

    Ok(exporter)
}

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    /// OTLP exporter could not be configured because no endpoint was configured
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

    /// Indicates a failure to parse filter directive.
    #[error("failed to parse filter directive: {0:#}")]
    FilterParse(#[from] ParseError),
}

/// Initializes test tracing infrastructure.
///
/// This function is thread-safe and can be called multiple times - the
/// initialization will only happen once. Call this at the beginning of any
/// test that uses tracing or OpenTelemetry span operations.
///
/// Defaults to ERROR level to reduce test noise. Set `PROSODY_LOG` environment
/// variable to override (e.g., `PROSODY_LOG=debug cargo test`).
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
