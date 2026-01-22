//! Error types and classification for producer operations.
//!
//! This module defines error types for Kafka producer operations and implements
//! error classification for retry logic. Errors are classified as:
//!
//! - **Terminal**: Configuration or infrastructure errors that prevent
//!   operation. Requires code/config changes or deployment fix.
//! - **Permanent**: Data-dependent failures specific to this message. Other
//!   messages will succeed. Message should be dropped or moved to dead letter
//!   queue.
//! - **Transient**: Network, timeout, or broker state issues that may resolve
//!   on retry. Retry with backoff recommended.

use crate::error::{ClassifyError, ErrorCategory};
use rdkafka::error::KafkaError;
use std::time::SystemTimeError;
use thiserror::Error;
use validator::ValidationErrors;

#[cfg(target_arch = "arm")]
use serde_json as json;

#[cfg(not(target_arch = "arm"))]
use simd_json as json;

/// Errors that can occur during producer operations.
#[derive(Debug, Error)]
pub enum ProducerError {
    /// Indicates invalid producer configuration.
    #[error("invalid producer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates a failure to serialize the payload.
    #[error("failed to serialize payload: {0:#}")]
    Serialization(#[from] json::Error),

    /// Indicates a failure to set the message timestamp.
    #[error("failed to set timestamp: {0:#}")]
    SystemTime(#[from] SystemTimeError),

    /// Indicates a failure to retrieve the hostname.
    #[error("failed to get hostname: {0:#}")]
    Hostname(#[from] whoami::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka error: {0:#}")]
    Kafka(#[from] KafkaError),
}

impl ClassifyError for ProducerError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Configuration validation failed or failed to retrieve hostname. Programming
            // or infrastructure errors that require code/config fix. Will fail for all
            // messages until deployment.
            Self::Configuration(_) | Self::Hostname(_) => ErrorCategory::Terminal,

            // Payload serialization failed. Data-dependent - this specific payload cannot
            // be serialized (e.g., invalid UTF-8, unsupported types). Other messages will
            // succeed.
            Self::Serialization(_) => ErrorCategory::Permanent,

            // System time error (clock before UNIX epoch). Extremely rare infrastructure
            // issue. May resolve if system clock is corrected.
            Self::SystemTime(_) => ErrorCategory::Transient,

            // Kafka operation error. Delegate to shared classification.
            Self::Kafka(e) => e.classify_error(),
        }
    }
}
