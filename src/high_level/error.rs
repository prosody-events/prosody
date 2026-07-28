//! What can go wrong building or driving a
//! [`HighLevelClient`](super::HighLevelClient).

use crate::Topic;
use crate::consumer::ConsumerError;
use crate::consumer::middleware::scheduler::SchedulerInitError;
use crate::high_level::config::ModeConfigurationError;
use crate::producer::{ProducerConfigurationBuilderError, ProducerError};
use crate::state_reader::StateReaderError;
use crate::telemetry::EmitterError;
use thiserror::Error;

/// Errors that can occur in the `HighLevelClient` operations.
#[derive(Debug, Error)]
pub enum HighLevelClientError<E> {
    /// Error when the producer configuration is invalid.
    #[error("invalid producer configuration: {0:#}")]
    ProducerConfiguration(#[from] ProducerConfigurationBuilderError),

    /// Error when initializing the producer fails.
    #[error("failed to initialize producer: {0:#}")]
    Producer(#[from] ProducerError<E>),

    /// Error when initializing the consumer fails.
    #[error("failed to initialize consumer: {0:#}")]
    Consumer(#[from] ConsumerError),

    /// Error when the scheduler configuration is invalid.
    #[error("invalid scheduler configuration: {0:#}")]
    SchedulerConfiguration(#[from] SchedulerInitError),

    /// Error when attempting to use an unconfigured consumer.
    #[error("unconfigured consumer; client does not have a valid consumer configuration")]
    UnconfiguredConsumer,

    /// Error when the consumer configuration failed during build.
    #[error("consumer configuration failed: {0:#}")]
    ConsumerConfiguration(ModeConfigurationError),

    /// Error when attempting to subscribe an already subscribed consumer.
    #[error("consumer is already subscribed")]
    AlreadySubscribed,

    /// Error when attempting to unsubscribe a not subscribed consumer.
    #[error("consumer is not subscribed")]
    NotSubscribed,

    /// Error when required topics are not found in the Kafka cluster.
    #[error("topics not found: {}", .0.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "))]
    TopicsNotFound(Vec<Topic>),

    /// Error when the telemetry emitter cannot be started.
    #[error("failed to start telemetry emitter: {0:#}")]
    TelemetryEmitter(#[from] EmitterError),

    /// Error building or using a standalone state reader from the shared bundle
    /// (a connect failure, or a descriptor the reader rejects).
    #[error("state reader failed: {0:#}")]
    StateReader(#[from] StateReaderError),
}
