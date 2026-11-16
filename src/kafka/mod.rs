//! Shared setup for every librdkafka client: connection settings and log
//! levels.
//!
//! Build each client configuration with [`client_config`], and create each
//! client with [`LogContext`].
//!
//! librdkafka reconnects to a broker by itself after a connection fails, but
//! the default rdkafka client context logs each failure at error. A failed
//! connection needs action only when it persists, and stall detection reports
//! a client that stops. So Prosody logs connection failures at warn. A `FAIL`
//! log line can also carry an authentication or SSL failure. The error
//! callback reports that failure with its own code, which stays at error with
//! every other librdkafka error.

use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use tracing::Level;

use crate::error::log_at;

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

/// The base configuration for a librdkafka client that connects to
/// `bootstrap_servers`. It forwards librdkafka log lines at error and above.
///
/// The client closes a broker connection after 230 s without traffic, and it
/// connects again when it next needs that broker. Some networks silently drop
/// a flow that stays idle past a limit: 350 s for EC2 connection tracking on
/// Nitro v6 instances, NAT gateways, and NLBs, and 4 minutes on Azure. A
/// request on a dropped flow stalls until `socket.timeout.ms` ends it.
/// librdkafka uses the same 230 s by default for Azure brokers.
pub(crate) fn client_config(bootstrap_servers: impl Into<String>) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_servers)
        .set("connections.max.idle.ms", "230000")
        .set_log_level(RDKafkaLogLevel::Error);
    config
}

/// Logs one librdkafka log line at the level that [`line_level`] selects.
pub(crate) fn log_line(level: RDKafkaLogLevel, fac: &str, message: &str) {
    log_at!(line_level(level, fac), target: "librdkafka", "librdkafka: {fac} {message}");
}

/// Logs one librdkafka error at the level that [`error_level`] selects.
pub(crate) fn log_error(error: &KafkaError, reason: &str) {
    log_at!(error_level(error), target: "rdkafka::client", "librdkafka: {error}: {reason}");
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
