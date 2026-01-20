//! Error classification for Kafka operations.
//!
//! This module provides [`ClassifyError`] implementations for rdkafka error
//! types, enabling consistent retry logic across producer and consumer
//! components.
//!
//! # Classification Rubric
//!
//! - **Terminal**: Fatal errors where the client is unusable and must shutdown.
//!   The Kafka client cannot recover; a new instance must be created.
//! - **Permanent**: Message-level issues (corruption, serialization failures,
//!   invalid data) where retrying forever won't help. Data loss is inevitable
//!   for this specific message, but the client can continue processing others.
//! - **Transient**: Everything else - errors that could be fixed by retry,
//!   waiting, configuration changes, or code changes.

use super::{ClassifyError, ErrorCategory};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

impl ClassifyError for KafkaError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // === Errors with RDKafkaErrorCode - delegate to code classification ===
            Self::AdminOp(code)
            | Self::ConsumerCommit(code)
            | Self::ConsumerQueueClose(code)
            | Self::Flush(code)
            | Self::Global(code)
            | Self::GroupListFetch(code)
            | Self::MessageConsumption(code)
            | Self::MessageProduction(code)
            | Self::MetadataFetch(code)
            | Self::MockCluster(code)
            | Self::OffsetFetch(code)
            | Self::Rebalance(code)
            | Self::SetPartitionOffset(code)
            | Self::StoreOffset(code) => code.classify_error(),

            // === Terminal: Client is unusable, must shutdown ===

            // Fatal consumption error - librdkafka marks errors as fatal when the client
            // instance becomes unusable (e.g., idempotent producer guarantee violations,
            // static consumer fencing). rd_kafka_event_error_is_fatal() returns true.
            // There is "no way for a producer instance to recover from fatal errors" per
            // librdkafka docs. The client must be destroyed and recreated.
            Self::MessageConsumptionFatal(code) => match code.classify_error() {
                // If the underlying code is permanent (message corruption), keep it permanent
                // to preserve information about message-level issues for logging/metrics.
                ErrorCategory::Permanent => ErrorCategory::Permanent,
                // Otherwise this is a fatal/terminal condition requiring client shutdown.
                _ => ErrorCategory::Terminal,
            },

            // Transaction errors - check using rdkafka's built-in classification methods.
            // RDKafkaError provides is_fatal(), is_retriable(), and txn_requires_abort().
            Self::Transaction(err) => {
                if err.is_fatal() {
                    // Fatal transaction errors (e.g., producer fenced, invalid producer epoch)
                    // indicate the transactional producer is permanently unusable.
                    ErrorCategory::Terminal
                } else if err.is_retriable() || err.txn_requires_abort() {
                    // Retriable errors can be retried directly.
                    // Abortable errors (txn_requires_abort) mean the current transaction failed
                    // but the producer can abort and start a new transaction.
                    ErrorCategory::Transient
                } else {
                    // Non-retriable, non-fatal, non-abortable transaction errors are permanent
                    // (e.g., invalid transaction state that won't resolve).
                    ErrorCategory::Permanent
                }
            }

            // === Permanent: Programming/FFI errors that indicate bugs ===

            // FFI null pointer error - indicates a programming bug in string handling.
            // The application passed a string containing an interior null byte.
            Self::Nul(_) => ErrorCategory::Permanent,

            // === Transient: Could be fixed by retry, config change, or waiting ===
            //
            // Includes:
            // - ClientConfig/ClientCreation: config errors, fixable by changing config
            // - AdminOpCreation: usually config/argument issue, fixable
            // - NoMessageReceived: normal poll timeout, not an error
            // - PartitionEOF: normal operation, partition exhausted
            // - PauseResume/Seek/Subscription: operational issues, can retry
            // - Canceled: operation canceled, can retry with new client
            // - Unknown variants: KafkaError is #[non_exhaustive], default to Transient
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for RDKafkaErrorCode {
    /// Classifies an [`RDKafkaErrorCode`] based on recoverability.
    ///
    /// Note: [`RDKafkaErrorCode`] is `#[non_exhaustive]`, so we only explicitly
    /// enumerate Terminal and Permanent cases. All other errors (including new
    /// variants added in future rdkafka versions) default to Transient to avoid
    /// data loss.
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // =================================================================
            // TERMINAL: Fatal errors where client is unusable
            // These indicate the Kafka client instance cannot recover and must
            // be destroyed. A new client instance must be created.
            //
            // - Fatal (-150): librdkafka sets this when unrecoverable conditions occur (e.g.,
            //   idempotent producer guarantee violations). "There is no way for a producer instance
            //   to recover from fatal errors."
            // - Fenced (-144): Instance permanently excluded from consumer group. Occurs with
            //   static group membership when another consumer joins with the same instance ID.
            // - FencedInstanceId (82): Broker response indicating this static member has been
            //   replaced by a newer instance.
            // - ProducerFenced (90): Another producer with the same transactional.id started,
            //   invalidating this producer's epoch. Kafka's zombie fencing.
            // - CriticalSystemResource (-194): Resource exhaustion (fd, memory). Retrying would
            //   cause leaks; must shutdown and recover.
            // =================================================================
            Self::Fatal
            | Self::Fenced
            | Self::FencedInstanceId
            | Self::ProducerFenced
            | Self::CriticalSystemResource => ErrorCategory::Terminal,

            // =================================================================
            // PERMANENT: Message-level errors where retry won't help
            // These indicate the message data itself is broken/invalid.
            // The client can continue processing other messages.
            //
            // Message corruption:
            // - BadMessage (-199): Client detected message is malformed
            // - InvalidMessage (2): CRC failure, invalid size, null key on compacted topic
            // - InvalidRecord (87): Broker failed to validate record
            //
            // Size violations:
            // - MessageSizeTooLarge (10): Exceeds broker's max.message.bytes
            // - InvalidMessageSize (4): Protocol violation (negative size)
            //
            // Serialization failures:
            // - KeySerialization (-162): Key cannot be serialized
            // - ValueSerialization (-161): Value cannot be serialized
            // - KeyDeserialization (-160): Key bytes cannot be deserialized
            // - ValueDeserialization (-159): Value bytes cannot be deserialized
            //
            // Compression failures:
            // - BadCompression (-198): Corrupted compressed payload
            // - UnsupportedCompressionType (76): Client lacks codec support
            //
            // Timestamp validation:
            // - InvalidTimestamp (32): Timestamp outside broker's acceptable range
            //
            // Batch-level failures:
            // - InvalidDifferentRecord (-138): Another record in batch was invalid. This record was
            //   valid but batch rejected. By the time this surfaces, librdkafka exhausted retries
            //   and culprit cannot be identified.
            // =================================================================
            Self::BadMessage
            | Self::InvalidMessage
            | Self::InvalidRecord
            | Self::MessageSizeTooLarge
            | Self::InvalidMessageSize
            | Self::KeySerialization
            | Self::ValueSerialization
            | Self::KeyDeserialization
            | Self::ValueDeserialization
            | Self::BadCompression
            | Self::UnsupportedCompressionType
            | Self::InvalidTimestamp
            | Self::InvalidDifferentRecord => ErrorCategory::Permanent,

            // =================================================================
            // TRANSIENT: Everything else - can be fixed by retry, waiting,
            // config change, or code change
            //
            // This includes: broker errors, network errors, auth errors,
            // timeouts, rebalances, unknown topics (can be created), quota
            // violations, leader elections, etc.
            //
            // We default to Transient to avoid unnecessary data loss.
            // =================================================================
            _ => ErrorCategory::Transient,
        }
    }
}
