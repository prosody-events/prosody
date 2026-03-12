//! Kafka producer implementation with support for distributed tracing.
//!
//! This module provides a high-level abstraction for producing messages to
//! Kafka topics. It handles configuration, message serialization, and injection
//! of OpenTelemetry context for distributed tracing.

use ahash::RandomState;
use derive_builder::Builder;
use educe::Educe;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use quick_cache::UnitWeighter;
use quick_cache::sync::Cache;
use rdkafka::ClientConfig;
use rdkafka::client::{Client, DefaultClientContext};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::FutureProducerContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::env::var;
use std::mem::take;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{Span, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::Validate;

use crate::producer::injector::RecordInjector;
use crate::propagator::new_propagator;
use crate::telemetry::sender::TelemetrySender;
use crate::util::{
    from_env, from_env_with_fallback, from_option_duration_env_with_fallback, from_vec_env,
};
use crate::{
    EventId, EventIdentity, Key, MOCK_CLUSTER_BOOTSTRAP, Payload, SOURCE_SYSTEM_HEADER, Topic,
};

#[cfg(target_arch = "arm")]
use serde_json as json;

#[cfg(not(target_arch = "arm"))]
use simd_json as json;
use tracing::field::debug;
use tracing::log::info;
use whoami::hostname;

mod error;
mod injector;

pub use error::ProducerError;

/// Environment variable name for the source system identifier.
const PROSODY_SOURCE_SYSTEM: &str = "PROSODY_SOURCE_SYSTEM";

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
    #[builder(default = "from_env(PROSODY_SOURCE_SYSTEM)?", setter(into))]
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
    pub(crate) fn configured_source_system(&self) -> Option<String> {
        self.source_system
            .clone()
            .or_else(|| var(PROSODY_SOURCE_SYSTEM).ok())
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
///
/// `ProsodyProducer` handles message serialization, OpenTelemetry context
/// propagation, and provides idempotent delivery semantics through an optional
/// cache.
///
/// The producer can be configured in three operational modes:
/// - Pipeline: Focuses on reliable delivery with indefinite retries
/// - Low-latency: Balances speed and reliability with configurable timeouts
/// - Best-effort: Optimized for throughput with reasonable timeout defaults
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

    /// Idempotence cache for deduplicating messages.
    idempotence_cache: Option<Arc<Cache<Key, EventId, UnitWeighter, RandomState>>>,

    /// OpenTelemetry context propagator.
    #[educe(Debug(ignore))]
    propagator: TextMapCompositePropagator,

    /// Telemetry sender for emitting producer events.
    #[educe(Debug(ignore))]
    telemetry: TelemetrySender,
}

impl Clone for ProsodyProducer {
    fn clone(&self) -> Self {
        Self {
            send_timeout: self.send_timeout,
            source_system: self.source_system.clone(),
            producer: self.producer.clone(),
            idempotence_cache: self.idempotence_cache.clone(),
            propagator: new_propagator(),
            telemetry: self.telemetry.clone(),
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
    /// A `Result` containing the new `ProsodyProducer` instance if successful.
    ///
    /// # Errors
    ///
    /// Returns a `ProducerError` if:
    /// - The configuration is invalid
    /// - The hostname cannot be retrieved
    /// - The Kafka producer cannot be created
    pub fn new(
        config: &ProducerConfiguration,
        telemetry: TelemetrySender,
    ) -> Result<Self, ProducerError> {
        config.validate()?;

        let send_timeout = config.send_timeout.map_or(Timeout::Never, Timeout::After);

        let bootstrap = if config.mock {
            MOCK_CLUSTER_BOOTSTRAP.clone()
        } else {
            config.bootstrap_servers.join(",")
        };

        // Configure the Kafka client with sensible defaults
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", bootstrap)
            .set("client.id", hostname()?)
            .set("compression.codec", "lz4")
            .set("enable.idempotence", "true")
            .set_log_level(RDKafkaLogLevel::Error);

        // Create idempotence cache if size is non-zero
        let idempotence_cache = NonZeroUsize::new(config.idempotence_cache_size)
            .map(|size| Arc::new(Cache::new(size.into())));

        Ok(Self {
            send_timeout,
            source_system: config.source_system.clone().into_boxed_str(),
            producer: client_config.create()?,
            idempotence_cache,
            propagator: new_propagator(),
            telemetry,
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
    pub fn pipeline_producer(
        mut config: ProducerConfiguration,
        telemetry: TelemetrySender,
    ) -> Result<Self, ProducerError> {
        config.send_timeout = None;
        Self::new(&config, telemetry)
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
    pub(crate) fn low_latency_producer(
        mut config: ProducerConfiguration,
        telemetry: TelemetrySender,
    ) -> Result<Self, ProducerError> {
        if config.send_timeout.is_none() {
            config.send_timeout = Some(Duration::from_secs(1));
        }
        Self::new(&config, telemetry)
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
    pub(crate) fn best_effort_producer(
        mut config: ProducerConfiguration,
        telemetry: TelemetrySender,
    ) -> Result<Self, ProducerError> {
        if config.send_timeout.is_none() {
            config.send_timeout = Some(Duration::from_secs(1));
        }
        Self::new(&config, telemetry)
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
        fields(topic = topic.as_ref(), payload_size, partition, offset, timestamp),
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
        let maybe_event_id = payload.event_id();
        let key: Key = key.into();

        // Handle idempotence cache logic if enabled
        if let (Some(cache), Some(event_id)) = (&self.idempotence_cache, maybe_event_id)
            && matches!(
                cache.get(&key),
                Some(previous_event_id) if previous_event_id == event_id
            )
        {
            let span = info_span!("message.filtered", reason = "duplicate-event-id", event_id);
            span.in_scope(|| {
                info!("message with id {event_id} already produced; skipping");
            });
            return Ok(());
        }

        // Serialize the payload to JSON
        let serialized = json::to_vec(&payload)?;
        Span::current().record("payload_size", serialized.len());

        // Build the Kafka record
        let mut record = FutureRecord::to(&topic)
            .key(key.as_ref())
            .payload(&serialized)
            .timestamp(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);

        // Inject OpenTelemetry context
        self.propagator.inject_context(
            &Span::current().context(),
            &mut RecordInjector::new(&mut record),
        );

        // Add custom headers
        let headers = headers.into_iter();
        let owned_headers = record
            .headers
            .get_or_insert_with(|| OwnedHeaders::new_with_capacity(headers.len() + 1));

        // Add source system header
        *owned_headers = take(owned_headers).insert(Header {
            key: SOURCE_SYSTEM_HEADER,
            value: Some(self.source_system.as_bytes()),
        });

        // Add all custom headers
        for (key, value) in headers {
            *owned_headers = take(owned_headers).insert(Header {
                key,
                value: Some(value.as_bytes()),
            });
        }

        // Send the message to Kafka
        let delivery = self
            .producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(error, _)| ProducerError::Kafka(error))?;

        // Record partition, offset, and timestamp in the current span
        Span::current()
            .record("partition", delivery.partition)
            .record("offset", delivery.offset)
            .record("timestamp", debug(delivery.timestamp));

        // Emit producer telemetry event
        self.telemetry.message_sent(
            topic,
            delivery.partition,
            delivery.offset,
            key.clone(),
            Arc::from(self.source_system.as_ref()),
        );

        // Update the idempotence cache if needed
        let Some(cache) = &self.idempotence_cache else {
            return Ok(());
        };

        let Some(event_id) = maybe_event_id else {
            // If no event ID exists, remove it from the cache
            cache.remove(&key);
            return Ok(());
        };

        // Insert the new event ID into the cache
        cache.insert(key, event_id.into());
        Ok(())
    }

    /// Retrieves the underlying Kafka client.
    ///
    /// This method is primarily used for internal purposes like checking topic
    /// existence.
    ///
    /// # Returns
    ///
    /// A reference to the underlying Kafka client.
    pub(crate) fn kafka_client(&self) -> &Client<FutureProducerContext<DefaultClientContext>> {
        self.producer.client()
    }
}
