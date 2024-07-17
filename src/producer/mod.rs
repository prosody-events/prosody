//! Kafka producer implementation with support for distributed tracing.
//!
//! This module provides a high-level abstraction for producing messages to
//! Kafka topics. It handles configuration, message serialization, and injection
//! of OpenTelemetry context for distributed tracing.

use std::io;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

use derive_builder::Builder;
use educe::Educe;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde_json::{to_vec, Value};
use thiserror::Error;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::producer::injector::RecordInjector;
use crate::propagator::new_propagator;
use crate::util::{from_env_with_fallback, from_option_duration_env_with_fallback, from_vec_env};
use crate::Topic;

mod injector;

/// Configuration for the Kafka producer.
///
/// This struct holds all the necessary configuration options for creating a
/// Kafka producer. It uses the Builder pattern for flexible initialization and
/// supports loading values from environment variables.
#[derive(Builder, Clone, Debug, Validate)]
pub struct ProducerConfiguration {
    /// List of Kafka bootstrap servers.
    ///
    /// Environment variable: `PROSODY_BOOTSTRAP_SERVERS`
    /// Default: None (must be specified)
    ///
    /// At least one server must be specified.
    #[builder(default = "from_vec_env(\"PROSODY_BOOTSTRAP_SERVERS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Timeout for send operations.
    ///
    /// Environment variable: `PROSODY_SEND_TIMEOUT`
    /// Default: 1 second
    ///
    /// If set to None (or if the environment variable is set to "none"), the
    /// sender will retry indefinitely and never timeout. This may be an
    /// appropriate setting for consumers that produce to topics, as the
    /// consumed message may not be marked as complete until the message is
    /// produced.
    #[builder(
        default = "from_option_duration_env_with_fallback(\"PROSODY_SEND_TIMEOUT\", \
                   Duration::from_secs(1))?",
        setter(into)
    )]
    pub send_timeout: Option<Duration>,

    /// Use a mock producer for testing purposes.
    ///
    /// Environment variable: `PROSODY_MOCK`
    /// Default: false
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MOCK\", false)?",
        setter(into)
    )]
    pub mock: bool,
}

impl ProducerConfiguration {
    /// Creates a new `ProducerConfigurationBuilder`.
    ///
    /// This method is a convenient way to start building a
    /// `ProducerConfiguration`.
    ///
    /// # Returns
    ///
    /// A new instance of `ProducerConfigurationBuilder`.
    #[must_use]
    pub fn builder() -> ProducerConfigurationBuilder {
        ProducerConfigurationBuilder::default()
    }
}

/// High-level Kafka producer implementation.
///
/// This struct encapsulates the Kafka producer functionality, including
/// message sending and OpenTelemetry context propagation.
#[derive(Educe)]
#[educe(Debug)]
pub struct ProsodyProducer {
    /// Timeout for send operations.
    send_timeout: Timeout,

    /// Underlying Kafka producer.
    #[educe(Debug(ignore))]
    producer: FutureProducer,

    /// OpenTelemetry context propagator.
    #[educe(Debug(ignore))]
    propagator: TextMapCompositePropagator,
}

impl Clone for ProsodyProducer {
    fn clone(&self) -> Self {
        Self {
            send_timeout: self.send_timeout,
            producer: self.producer.clone(),
            propagator: new_propagator(),
        }
    }
}

impl ProsodyProducer {
    /// Creates a new `ProsodyProducer` instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The producer configuration.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `ProsodyProducer` instance or a
    /// `ProducerError`.
    ///
    /// # Errors
    ///
    /// Returns a `ProducerError` if:
    /// - The configuration is invalid
    /// - The hostname cannot be retrieved
    /// - The Kafka producer cannot be created
    pub fn new(config: &ProducerConfiguration) -> Result<Self, ProducerError> {
        // Validate the configuration
        config.validate()?;

        // Set the send timeout
        let send_timeout = match config.send_timeout {
            None => Timeout::Never,
            Some(duration) => Timeout::After(duration),
        };

        // Create and configure the Kafka producer
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("compression.codec", "lz4")
            .set("enable.idempotence", "true")
            .set_log_level(RDKafkaLogLevel::Error);

        // Set up mock broker if configured
        if config.mock {
            client_config.set("test.mock.num.brokers", "3");
        }

        Ok(Self {
            send_timeout,
            producer: client_config.create()?,
            propagator: new_propagator(),
        })
    }

    /// Sends a message to a Kafka topic.
    ///
    /// This method serializes the payload, injects OpenTelemetry context,
    /// and sends the message to the specified Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to send the message to.
    /// * `key` - The message key.
    /// * `payload` - The message payload as a JSON Value.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the send operation.
    ///
    /// # Errors
    ///
    /// Returns a `ProducerError` if:
    /// - The payload cannot be serialized
    /// - The system time cannot be retrieved
    /// - The Kafka send operation fails
    pub async fn send(&self, topic: Topic, key: &str, payload: Value) -> Result<(), ProducerError> {
        // Serialize the payload
        let serialized = to_vec(&payload)?;

        // Create the record with timestamp
        let mut record = FutureRecord::to(&topic)
            .key(key)
            .payload(&serialized)
            .timestamp(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);

        // Inject OpenTelemetry context into the record headers
        self.propagator.inject_context(
            &Span::current().context(),
            &mut RecordInjector::new(&mut record),
        );

        // Send the record
        self.producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(error, _)| ProducerError::Kafka(error))?;

        Ok(())
    }
}

/// Errors that can occur during producer operations.
#[derive(Debug, Error)]
pub enum ProducerError {
    /// Indicates invalid producer configuration.
    #[error("invalid producer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates a failure to serialize the payload.
    #[error("failed to serialize payload: {0:#}")]
    Serialization(#[from] serde_json::Error),

    /// Indicates a failure to set the message timestamp.
    #[error("failed to set timestamp: {0:#}")]
    SystemTime(#[from] SystemTimeError),

    /// Indicates a failure to retrieve the hostname.
    #[error("failed to get hostname: {0:#}")]
    Hostname(#[from] io::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka error: {0:#}")]
    Kafka(#[from] KafkaError),
}
