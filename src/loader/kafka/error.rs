use std::sync::Arc;

use rdkafka::error::KafkaError;
use thiserror::Error;

use crate::error::{ClassifyError, ErrorCategory};
use crate::{Offset, Partition, Topic};

/// Errors that can occur during Kafka message loading.
#[derive(Clone, Debug, Error)]
pub enum KafkaLoaderError {
    /// Failed to decode the message payload.
    #[error("Failed to decode message {0}/{1}:{2}")]
    DecodeError(Topic, Partition, Offset),

    /// The loader has been shut down and cannot process requests.
    #[error("Loader has shut down")]
    LoaderShutdown,

    /// Every loader permit is held.
    #[error("Loader capacity is exhausted")]
    CapacityExhausted,

    /// The requested offset no longer exists due to retention or compaction.
    ///
    /// `next_offset` is the offset of the first message the broker delivered
    /// after seeking to `requested_offset`:
    ///
    /// - **Truncation** (retention/`delete_records`): the broker auto-resets to
    ///   the Log Start Offset (LSO), so `next_offset` equals the LSO.
    /// - **Compaction hole**: the broker skips the missing key and delivers the
    ///   next surviving message, so `next_offset` is that message's offset. The
    ///   LSO is unchanged and may be much lower.
    ///
    /// In both cases `next_offset` is the lowest offset that can currently be
    /// read from this partition at or after `requested_offset`.
    #[error(
        "Offset {requested_offset} has been deleted from partition {topic}/{partition} (next \
         offset: {next_offset}). The requested message no longer exists due to retention or \
         compaction."
    )]
    OffsetDeleted {
        /// The topic containing the deleted offset.
        topic: Topic,
        /// The partition containing the deleted offset.
        partition: Partition,
        /// The offset that was requested but no longer exists.
        requested_offset: Offset,
        /// The offset of the first message the broker delivered after seeking
        /// to `requested_offset`. For truncation this equals the partition LSO;
        /// for a compaction hole it is the next surviving message after the
        /// gap.
        next_offset: Offset,
    },

    /// Failed to create the Kafka consumer.
    #[error("Failed to create Kafka consumer: {0:#}")]
    ConsumerCreation(KafkaError),

    /// A Kafka operation error occurred.
    #[error("Kafka error: {0:#}")]
    Kafka(KafkaError),

    /// Failed to retrieve the hostname for the consumer client ID.
    #[error("failed to get hostname: {0:#}")]
    Hostname(Arc<whoami::Error>),
}

impl ClassifyError for KafkaLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal errors - system cannot operate
            Self::LoaderShutdown | Self::ConsumerCreation(_) | Self::Hostname(_) => {
                ErrorCategory::Terminal
            }

            Self::CapacityExhausted => ErrorCategory::Transient,

            // Classify Kafka operation errors using shared implementation
            Self::Kafka(kafka_error) => kafka_error.classify_error(),

            // Permanent errors - data issues that won't resolve
            Self::DecodeError(..) | Self::OffsetDeleted { .. } => ErrorCategory::Permanent,
        }
    }
}
