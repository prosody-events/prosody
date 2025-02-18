//! Kafka producer implementation with support for distributed tracing.
//!
//! This module provides a high-level abstraction for producing messages to
//! Kafka topics. It handles configuration, message serialization, and injection
//! of OpenTelemetry context for distributed tracing.

use derive_builder::Builder;
use educe::Educe;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use parking_lot::Mutex;
use rdkafka::client::{Client, DefaultClientContext};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::FutureProducerContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::io;
use std::mem::take;
use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use thiserror::Error;
use tracing::{instrument, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::deduplication::IdempotenceCache;
use crate::producer::injector::RecordInjector;
use crate::propagator::new_propagator;
use crate::util::{
    from_env, from_env_with_fallback, from_option_duration_env_with_fallback, from_vec_env,
};
use crate::{Payload, Topic, MOCK_CLUSTER_BOOTSTRAP, SOURCE_SYSTEM_HEADER};

#[cfg(target_arch = "arm")]
use serde_json as json;

#[cfg(not(target_arch = "arm"))]
use simd_json as json;

mod injector;

/// Configuration for the Kafka producer.
#[derive(Builder, Clone, Debug, Validate)]
pub struct ProducerConfiguration {
    /// List of Kafka bootstrap servers.
    ///
    /// Environment variable: `PROSODY_BOOTSTRAP_SERVERS`
    /// Default: None (must be specified)
    #[builder(default = "from_vec_env(\"PROSODY_BOOTSTRAP_SERVERS\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Timeout for send operations.
    ///
    /// Environment variable: `PROSODY_SEND_TIMEOUT`
    /// Default: 1 second
    ///
    /// If set to `None`, the sender will retry indefinitely.
    #[builder(
        default = "from_option_duration_env_with_fallback(\"PROSODY_SEND_TIMEOUT\", \
                   Duration::from_secs(1))?",
        setter(into)
    )]
    pub send_timeout: Option<Duration>,

    /// Idempotence cache size.
    ///
    /// Environment variable: `PROSODY_IDEMPOTENCE_CACHE_SIZE`
    /// Default: 4096
    ///
    /// Set to 0 to disable the producer idempotence cache.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_IDEMPOTENCE_CACHE_SIZE\", 4096)?",
        setter(into)
    )]
    pub idempotence_cache_size: usize,

    /// Name of producing system.
    ///
    /// Environment variable: `PROSODY_SOURCE_SYSTEM`
    /// Default: None (must be specified)
    #[builder(default = "from_env(\"PROSODY_SOURCE_SYSTEM\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub source_system: String,

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

impl ProducerConfigurationBuilder {
    /// Currently configured source system
    ///
    /// # Returns
    ///
    /// An option containing the source system if configured
    #[must_use]
    pub fn configured_source_system(&self) -> Option<&str> {
        self.source_system.as_deref()
    }
}

impl ProducerConfiguration {
    /// Creates a new `ProducerConfigurationBuilder`.
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
#[derive(Educe)]
#[educe(Debug)]
pub struct ProsodyProducer {
    /// Timeout for send operations.
    send_timeout: Timeout,

    /// Name of producing system.
    source_system: Box<str>,

    /// Underlying Kafka producer.
    #[educe(Debug(ignore))]
    producer: FutureProducer,

    /// Idempotence cache
    idempotence_cache: Arc<Mutex<IdempotenceCache>>,

    /// OpenTelemetry context propagator.
    #[educe(Debug(ignore))]
    propagator: TextMapCompositePropagator,
}

impl Clone for ProsodyProducer {
    fn clone(&self) -> Self {
        Self {
            send_timeout: self.send_timeout,
            source_system: self.source_system.clone(),
            producer: self.producer.clone(),
            idempotence_cache: self.idempotence_cache.clone(),
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
        config.validate()?;

        let send_timeout = config.send_timeout.map_or(Timeout::Never, Timeout::After);

        let bootstrap = if config.mock {
            MOCK_CLUSTER_BOOTSTRAP.clone()
        } else {
            config.bootstrap_servers.join(",")
        };

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", bootstrap)
            .set("client.id", hostname()?)
            .set("compression.codec", "lz4")
            .set("enable.idempotence", "true")
            .set_log_level(RDKafkaLogLevel::Error);

        Ok(Self {
            send_timeout,
            source_system: config.source_system.clone().into_boxed_str(),
            producer: client_config.create()?,
            idempotence_cache: Arc::new(Mutex::new(IdempotenceCache::new(
                config.idempotence_cache_size,
            ))),
            propagator: new_propagator(),
        })
    }

    /// Creates a new `ProsodyProducer` instance for pipeline processing.
    ///
    /// This configuration sets the send timeout to `None`, allowing for
    /// indefinite retries.
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
    /// Returns a `ProducerError` if the producer creation fails.
    pub fn pipeline_producer(mut config: ProducerConfiguration) -> Result<Self, ProducerError> {
        config.send_timeout = None;
        Self::new(&config)
    }

    /// Creates a new `ProsodyProducer` instance optimized for low latency.
    ///
    /// This configuration ensures a send timeout is set, defaulting to 1 second
    /// if not specified.
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
    /// Returns a `ProducerError` if the producer creation fails.
    pub fn low_latency_producer(mut config: ProducerConfiguration) -> Result<Self, ProducerError> {
        if config.send_timeout.is_none() {
            config.send_timeout = Some(Duration::from_secs(1));
        }
        Self::new(&config)
    }

    /// Creates a new `ProsodyProducer` instance optimized for best-effort.
    ///
    /// This configuration ensures a send timeout is set, defaulting to 1 second
    /// if not specified.
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
    /// Returns a `ProducerError` if the producer creation fails.
    pub fn best_effort_producer(mut config: ProducerConfiguration) -> Result<Self, ProducerError> {
        if config.send_timeout.is_none() {
            config.send_timeout = Some(Duration::from_secs(1));
        }
        Self::new(&config)
    }

    /// Sends a message to a Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `headers` - An iterator of key-value pairs to be added as headers.
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
    #[instrument(
        skip(self, topic, headers, payload),
        fields(topic = topic.as_ref(), payload_size, partition, offset),
        err
    )]
    pub async fn send<'a, H>(
        &'a self,
        headers: H,
        topic: Topic,
        key: &str,
        payload: &Payload,
    ) -> Result<(), ProducerError>
    where
        H: IntoIterator<Item = (&'static str, &'a str), IntoIter: ExactSizeIterator>,
    {
        // Skip messages with duplicate event IDs
        if let Some(event_id) = self
            .idempotence_cache
            .lock()
            .check_duplicate(&key.into(), payload)
        {
            warn!("message with id {event_id} already produced; skipping",);
            return Ok(());
        }

        let serialized = json::to_vec(&payload)?;
        Span::current().record("payload_size", serialized.len());

        let mut record = FutureRecord::to(&topic)
            .key(key)
            .payload(&serialized)
            .timestamp(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);

        self.propagator.inject_context(
            &Span::current().context(),
            &mut RecordInjector::new(&mut record),
        );

        let headers = headers.into_iter();
        let owned_headers = record
            .headers
            .get_or_insert_with(|| OwnedHeaders::new_with_capacity(headers.len() + 1));

        *owned_headers = take(owned_headers).insert(Header {
            key: SOURCE_SYSTEM_HEADER,
            value: Some(self.source_system.as_bytes()),
        });

        for (key, value) in headers {
            *owned_headers = take(owned_headers).insert(Header {
                key,
                value: Some(value.as_bytes()),
            });
        }

        let (partition, offset) = self
            .producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(error, _)| ProducerError::Kafka(error))?;

        Span::current()
            .record("partition", partition)
            .record("offset", offset);

        Ok(())
    }

    /// Retrieves the underlying Kafka client.
    ///
    /// This method is primarily used for internal purposes.
    pub(crate) fn kafka_client(&self) -> &Client<FutureProducerContext<DefaultClientContext>> {
        self.producer.client()
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
    Serialization(#[from] json::Error),

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
