//! Log levels for librdkafka callbacks.
//!
//! librdkafka reconnects to a broker by itself after a connection fails, but
//! the default rdkafka client context logs each failure at error. A failed
//! connection needs action only when it persists, and stall detection reports
//! a client that stops. So Prosody logs connection failures at warn. Every
//! other librdkafka error, including authentication and SSL failures, stays at
//! error.

use rdkafka::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use tracing::{Level, debug, error, info, warn};

#[cfg(test)]
mod tests;

/// The librdkafka log facility for a failed broker connection.
const CONNECTION_FAILURE: &str = "FAIL";

/// A client context that applies Prosody's librdkafka log levels.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogContext;

impl ClientContext for LogContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        log_line(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        log_error(&error, reason);
    }
}

impl ConsumerContext for LogContext {}

/// Logs one librdkafka log line at the level that [`line_level`] selects.
pub(crate) fn log_line(level: RDKafkaLogLevel, fac: &str, message: &str) {
    // A tracing callsite has a static level, so each level needs its own call.
    match line_level(level, fac) {
        Level::ERROR => error!(target: "librdkafka", "librdkafka: {fac} {message}"),
        Level::WARN => warn!(target: "librdkafka", "librdkafka: {fac} {message}"),
        Level::INFO => info!(target: "librdkafka", "librdkafka: {fac} {message}"),
        _ => debug!(target: "librdkafka", "librdkafka: {fac} {message}"),
    }
}

/// Logs one librdkafka error at the level that [`error_level`] selects.
pub(crate) fn log_error(error: &KafkaError, reason: &str) {
    if error_level(error) == Level::WARN {
        warn!(target: "rdkafka::client", "librdkafka: {error}: {reason}");
    } else {
        error!(target: "rdkafka::client", "librdkafka: {error}: {reason}");
    }
}

/// Maps a librdkafka log line to a tracing level. An error-level line about a
/// failed broker connection logs at warn.
fn line_level(level: RDKafkaLogLevel, fac: &str) -> Level {
    let level = match level {
        RDKafkaLogLevel::Emerg
        | RDKafkaLogLevel::Alert
        | RDKafkaLogLevel::Critical
        | RDKafkaLogLevel::Error => Level::ERROR,
        RDKafkaLogLevel::Warning => Level::WARN,
        RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => Level::INFO,
        RDKafkaLogLevel::Debug => Level::DEBUG,
    };
    // A more verbose tracing level compares greater.
    if fac == CONNECTION_FAILURE {
        level.max(Level::WARN)
    } else {
        level
    }
}

/// Maps a librdkafka client error to a tracing level. A connection failure
/// that librdkafka retries logs at warn.
fn error_level(error: &KafkaError) -> Level {
    match error.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::NetworkException,
        ) => Level::WARN,
        _ => Level::ERROR,
    }
}
