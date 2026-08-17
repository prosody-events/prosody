//! Kafka message loader for loading messages by offset.
//!
//! This module provides [`KafkaLoader`], which loads specific messages from
//! Kafka by their exact offset coordinates (topic, partition, offset). The
//! deferral and keyed state share the same loader.
//!
//! # Architecture
//!
//! - **Dedicated consumer**: Uses a separate [`BaseConsumer`] with manual
//!   partition assignment to avoid interfering with the primary consumer's
//!   group coordination
//! - **Background polling thread**: Single blocking thread polls Kafka and
//!   fulfills load requests
//! - **Backpressure**: Semaphore-based permits limit concurrent decoding
//!   operations
//! - **Lazy validation**: Detects deleted offsets by comparing requested vs
//!   received offsets after polling
//! - **Seek optimization**: Balances seeking cost against sequential read
//!   bandwidth
//!
//! # Deleted Offset Handling
//!
//! Two Kafka mechanisms can make an offset unreadable:
//!
//! - **Truncation** (time-based retention, `delete_records`): a contiguous
//!   prefix of the partition is removed and the Log Start Offset (LSO)
//!   advances. Seeking to a deleted offset auto-resets to the LSO.
//! - **Compaction**: individual messages are removed by key, leaving holes
//!   anywhere in the log. The LSO does not move; seeking to a compacted offset
//!   delivers the next surviving message after the hole.
//!
//! The loader handles both cases identically:
//!
//! 1. **Assign/seek**: rdkafka automatically positions past the missing offset
//!    in both cases.
//! 2. **Lazy validation**: when a message arrives at offset M, any pending
//!    requests in `[pending_seek, M)` are classified as deleted.
//! 3. **Explicit errors**: returns [`KafkaLoaderError::OffsetDeleted`] carrying
//!    `next_offset` — the offset of the first message the broker actually
//!    delivered. For truncation this equals the LSO; for a compaction hole it
//!    is the next surviving message after the gap.
//!
//! This approach avoids upfront offset validation (which requires metadata
//! queries) and lets rdkafka handle offset recovery automatically.

use super::{MessageLoader, PermitMode};
use crate::consumer::ConsumerConfiguration;
use crate::consumer::decode::{DecodedMessage, DecodedRecord, decode_record};
use crate::consumer::message::{ConsumerMessage, ConsumerRecord};
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::propagator::new_propagator;
use crate::{JsonCodec, Offset, Partition, Topic};
use ahash::HashMap;
use opentelemetry::propagation::TextMapCompositePropagator;
use quick_cache::sync::Cache;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Semaphore, TryAcquireError, mpsc, oneshot};
use tokio::task::spawn_blocking;
use tracing::field::Empty;
use tracing::{Span, debug, error, instrument, warn};

use crate::Codec;
use crate::otel::SpanRelation;
use crate::related_span;
use crate::state::RESOLVE_FANOUT;
use crate::subsystem::SubsystemName;
use tokio::select;
use whoami::hostname;

#[cfg(test)]
mod tests;

/// Multiple response channels waiting for the same offset, with one shared
/// permit.
///
/// When multiple callers request the same offset, only one permit is needed for
/// decoding. Subsequent callers' permits are dropped, but they still acquire
/// permits initially to maintain backpressure semantics.
type Responses<P> = SmallVec<[oneshot::Sender<Result<DecodedRecord<P>, KafkaLoaderError>>; 1]>;

/// Per-partition state for the loader poll loop.
///
/// Groups the pending offset map with the `pending_seek` target so both are
/// scoped to the same partition and dropped together when the partition is
/// removed from `active`.
///
/// `pending_seek` holds the offset we last sought to. It is `Some` while we
/// are waiting for the broker to deliver a message at or past that offset, and
/// `None` once the seek has materialised. A new seek is issued (replacing the
/// stored value) whenever `min_offset < pending_seek`, which handles the case
/// where a lower-offset request arrives after the seek was already dispatched.
struct PartitionState<P> {
    offsets: BTreeMap<Offset, Responses<P>>,
    pending_seek: Option<Offset>,
}

impl<P> Default for PartitionState<P> {
    fn default() -> Self {
        Self {
            offsets: BTreeMap::default(),
            pending_seek: None,
        }
    }
}

/// Active load requests indexed by topic-partition.
///
/// Each partition tracks its requested offsets in sorted order (via
/// [`BTreeMap`] inside [`PartitionState`]) to efficiently find the minimum
/// offset for seek optimization. `pending_seek` is managed per-partition so
/// a message on one partition does not clear the flag for another.
type ActiveRequests<P> = HashMap<(Topic, Partition), PartitionState<P>>;

mod config;
mod error;
mod request;
mod worker;

pub use config::{KafkaLoaderConfigError, KafkaLoaderConfiguration, LoaderConfiguration};
pub use error::KafkaLoaderError;
pub type KafkaLoaderConfigurationBuilder = config::KafkaLoaderConfigurationBuilder;
use worker::{create_load_span, poll_loop};

/// Cache key and decoded record storage for the shared loader.
type LoadedCache<P> = Arc<Cache<(Topic, Partition, Offset), DecodedRecord<P>>>;

/// Kafka message loader for retrieving messages by exact offset.
///
/// Uses a dedicated Kafka consumer with manual partition assignment to load
/// specific messages without interfering with the primary consumer's group
/// coordination. A background polling thread fulfills load requests and
/// semaphore-based permits provide backpressure. Messages are cached to avoid
/// redundant Kafka reads.
pub struct KafkaLoader<C: Codec = JsonCodec> {
    tx: mpsc::Sender<Request<C::Payload>>,
    semaphore: Arc<Semaphore>,
    cache: LoadedCache<C::Payload>,
    message_spans: SpanRelation,
}

impl<C: Codec> Clone for KafkaLoader<C>
where
    C::Payload: Clone,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            semaphore: self.semaphore.clone(),
            cache: self.cache.clone(),
            message_spans: self.message_spans,
        }
    }
}

impl<C: Codec> MessageLoader for KafkaLoader<C>
where
    C::Payload: Clone,
{
    type Error = KafkaLoaderError;
    type Payload = C::Payload;

    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerRecord<C::Payload>, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset, PermitMode::Wait)
    }

    fn try_load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerRecord<C::Payload>, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset, PermitMode::Available)
    }
}

impl<C: Codec> KafkaLoader<C>
where
    C::Payload: Clone,
{
    /// Creates a new Kafka message loader.
    ///
    /// Creates a dedicated `BaseConsumer` for loading messages and spawns
    /// a background thread that polls Kafka for requested messages. The loader
    /// uses manual partition assignment (`assign()`) and never commits offsets,
    /// so it does not participate in consumer group coordination.
    ///
    /// The consumer is configured with:
    /// - `client.id`: hostname or UUID (unique per instance)
    /// - `group.id`: `{config.group_id}.loader`
    /// - `auto.offset.reset=earliest` for recovery from deleted offsets
    /// - `enable.auto.commit=false` (manual offset management)
    /// - `enable.auto.offset.store=false` (manual seek/assign)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration validation fails
    /// - Consumer creation fails
    pub fn new(
        config: LoaderConfiguration,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self, KafkaLoaderError> {
        let group_id = format!("{}.loader", config.group_id);
        let client_id = hostname().map_err(|error| KafkaLoaderError::Hostname(Arc::new(error)))?;

        // Point lookups don't benefit from prefetching. Start with small fetch
        // sizes for ~1KB messages; rdkafka auto-adjusts if larger messages appear.
        // fetch.max.bytes >= message.max.bytes is required by rdkafka.
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", &client_id)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("queued.min.messages", "1")
            .set("queued.max.messages.kbytes", "64")
            .set("fetch.message.max.bytes", "16384")
            .set("message.max.bytes", "1048576")
            .set("fetch.max.bytes", "1048576")
            .set("fetch.wait.max.ms", "100")
            .set_log_level(RDKafkaLogLevel::Error)
            .create()
            .map_err(KafkaLoaderError::ConsumerCreation)?;

        let (tx, rx) = mpsc::channel(config.max_permits);
        let semaphore = Arc::new(Semaphore::new(config.max_permits));
        let cache = Arc::new(Cache::new(config.cache_size));

        let message_spans = config.message_spans;
        let heartbeat = heartbeats.register("kafka loader");
        spawn_blocking(move || poll_loop::<C>(rx, &consumer, &config, &heartbeat));

        Ok(Self {
            tx,
            semaphore,
            cache,
            message_spans,
        })
    }

    /// Builds a [`KafkaLoader`] configured from the surrounding consumer.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying `BaseConsumer` cannot be created.
    pub fn for_consumer(
        consumer_config: &ConsumerConfiguration,
        responder: Option<&SubsystemName>,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self, KafkaLoaderError> {
        Self::new(
            LoaderConfiguration::for_consumer(consumer_config, responder),
            heartbeats,
        )
    }

    /// Loads a specific message from Kafka by offset.
    ///
    /// Checks the cache first for a fast path. Cache hits return immediately,
    /// while cache misses load from Kafka and populate the cache. In both
    /// cases, creates a new span linked to the parent trace context from the
    /// original Kafka message headers, ensuring span lifecycles are independent
    /// of cache eviction.
    ///
    /// Fails with [`KafkaLoaderError::LoaderShutdown`] if the semaphore or
    /// either channel has closed, or with a decode/Kafka error if the message
    /// can't be found or decoded.
    #[instrument(level = "debug", skip(self, mode), fields(cached = Empty), err)]
    async fn load_message_impl(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
        mode: PermitMode,
    ) -> Result<ConsumerRecord<C::Payload>, KafkaLoaderError> {
        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Acquiring permit for message load"
        );

        let semaphore = self.semaphore.clone();
        let load_permit = match mode {
            PermitMode::Wait => semaphore
                .acquire_owned()
                .await
                .map_err(|_| KafkaLoaderError::LoaderShutdown)?,
            PermitMode::Available => {
                semaphore.try_acquire_owned().map_err(|error| match error {
                    TryAcquireError::Closed => KafkaLoaderError::LoaderShutdown,
                    TryAcquireError::NoPermits => KafkaLoaderError::CapacityExhausted,
                })?
            }
        };
        let instrument_span = Span::current();
        let cache_key = (topic, partition, offset);

        // Get decoded message from cache or load from Kafka, tracking cache status
        let (decoded_message, cached) = if let Some(cached) = self.cache.get(&cache_key) {
            instrument_span.record("cached", true);
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "Loading message from cache"
            );
            (cached, true)
        } else {
            instrument_span.record("cached", false);
            let decoded = self.load_from_kafka(topic, partition, offset).await?;
            (decoded, false)
        };

        Ok(match decoded_message {
            DecodedRecord::Message(decoded) => {
                let span = create_load_span(&decoded, cached, self.message_spans);
                ConsumerRecord::Message(ConsumerMessage::from_decoded(
                    decoded.value,
                    span,
                    load_permit,
                ))
            }
            DecodedRecord::Excise(decoded) => {
                let span = create_load_span(&decoded, cached, self.message_spans);
                ConsumerRecord::Excise(ConsumerMessage::from_decoded(
                    decoded.value,
                    span,
                    load_permit,
                ))
            }
        })
    }

    /// Loads a message from Kafka and caches the decoded result.
    ///
    /// Sends a load request to the background poll loop, which handles Kafka
    /// polling and message decoding. The decoded message is cached using
    /// `quick_cache`'s S3-FIFO eviction policy for efficient repeated access.
    #[instrument(skip(self), level = "debug", err)]
    async fn load_from_kafka(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<DecodedRecord<C::Payload>, KafkaLoaderError> {
        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Loading message from Kafka"
        );

        // Create response channel
        let (tx, rx) = oneshot::channel();

        // Send load request with decode permit
        self.tx
            .send(Request {
                topic,
                partition,
                offset,
                tx,
            })
            .await
            .map_err(|_| KafkaLoaderError::LoaderShutdown)?;

        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Load request queued, awaiting response"
        );

        // Wait for decoded message (decode permit was dropped in poll loop)
        let decoded = rx.await.map_err(|_| KafkaLoaderError::LoaderShutdown)??;

        // Cache the decoded message (no permit needed, quick_cache manages capacity)
        let cache_key = (topic, partition, offset);
        self.cache.insert(cache_key, decoded.clone());

        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Message cached for future loads"
        );

        Ok(decoded)
    }
}

/// A load request for a specific message offset.
struct Request<P> {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    tx: oneshot::Sender<Result<DecodedRecord<P>, KafkaLoaderError>>,
}

/// Bounds loader permits to the maximum semaphore capacity.
fn loader_capacity(max_uncommitted: usize) -> usize {
    max_uncommitted
        .saturating_mul(RESOLVE_FANOUT)
        .min(Semaphore::MAX_PERMITS)
}
