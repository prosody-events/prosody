use super::{CONNECTION_FAILURE, error_level, line_level};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use tracing::Level;

/// Every librdkafka level with the level that rdkafka's default context uses.
const DEFAULT_LEVELS: [(RDKafkaLogLevel, Level); 8] = [
    (RDKafkaLogLevel::Emerg, Level::ERROR),
    (RDKafkaLogLevel::Alert, Level::ERROR),
    (RDKafkaLogLevel::Critical, Level::ERROR),
    (RDKafkaLogLevel::Error, Level::ERROR),
    (RDKafkaLogLevel::Warning, Level::WARN),
    (RDKafkaLogLevel::Notice, Level::INFO),
    (RDKafkaLogLevel::Info, Level::INFO),
    (RDKafkaLogLevel::Debug, Level::DEBUG),
];

/// A connection-failure line is never more severe than warn. Every other line
/// keeps rdkafka's default level.
#[test]
fn connection_failure_lines_log_at_most_warn() {
    for (level, default) in DEFAULT_LEVELS {
        assert_eq!(line_level(level, "BROKER"), default, "{level:?}");
        assert_eq!(
            line_level(level, CONNECTION_FAILURE),
            default.max(Level::WARN),
            "{level:?}"
        );
    }
}

/// Connection failures that librdkafka retries log at warn. An error that
/// needs a person, or that carries no code, logs at error.
#[test]
fn only_connection_errors_log_at_warn() {
    let warn = [
        RDKafkaErrorCode::BrokerTransportFailure,
        RDKafkaErrorCode::Resolve,
        RDKafkaErrorCode::OperationTimedOut,
        RDKafkaErrorCode::RequestTimedOut,
        RDKafkaErrorCode::NetworkException,
    ];
    let error = [
        RDKafkaErrorCode::AllBrokersDown,
        RDKafkaErrorCode::Authentication,
        RDKafkaErrorCode::SaslAuthenticationFailed,
        RDKafkaErrorCode::SSL,
        RDKafkaErrorCode::Fatal,
        RDKafkaErrorCode::MessageSizeTooLarge,
    ];

    for code in warn {
        assert_eq!(
            error_level(&KafkaError::Global(code)),
            Level::WARN,
            "{code:?}"
        );
    }
    for code in error {
        assert_eq!(
            error_level(&KafkaError::Global(code)),
            Level::ERROR,
            "{code:?}"
        );
    }
    assert_eq!(error_level(&KafkaError::NoMessageReceived), Level::ERROR);
}
