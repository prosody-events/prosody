//! Kafka message loader for loading messages by offset.
//!
//! This module provides [`KafkaLoader`], which loads specific messages from
//! Kafka by their exact offset coordinates (topic, partition, offset). The
//! loader is used by the defer middleware to reload failed messages for retry.
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

use super::MessageLoader;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::decode::{DecodedMessage, decode_message};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::DeferConfiguration;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::propagator::new_propagator;
use crate::{Offset, Partition, Topic};
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
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::spawn_blocking;
use tracing::field::Empty;
use tracing::{Span, debug, error, instrument, warn};

use crate::otel::SpanRelation;
use crate::related_span;
use crate::Codec;
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
type Responses<P> = SmallVec<[oneshot::Sender<Result<DecodedMessage<P>, KafkaLoaderError>>; 1]>;

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

/// Configuration for the Kafka message loader.
///
/// Controls performance characteristics and resource usage of the defer
/// middleware loader that loads failed messages for retry.
#[derive(Clone, Debug)]
pub struct LoaderConfiguration {
    /// Kafka broker addresses.
    ///
    /// List of host:port pairs for initial connection to the Kafka cluster.
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID base name.
    ///
    /// The loader will append `-deferred-loader` to create a unique group
    /// ID, ensuring no conflicts with the primary consumer.
    pub group_id: String,

    /// Maximum number of concurrent message decoding operations.
    ///
    /// Controls the size of the semaphore used for decoding permits
    /// and the capacity of the request channel.
    pub max_permits: usize,

    /// Maximum number of messages to cache.
    ///
    /// The cache uses S3-FIFO eviction policy which quickly evicts "one-hit
    /// wonders" while keeping frequently accessed items. Cache capacity is
    /// managed purely by `quick_cache`'s internal mechanisms.
    pub cache_size: usize,

    /// Interval between poll operations when no messages are available.
    pub poll_interval: Duration,

    /// Timeout for seek operations.
    ///
    /// How long to wait for a seek operation to complete before failing.
    pub seek_timeout: Duration,

    /// Number of messages to read sequentially before performing a seek.
    ///
    /// If the next requested offset is within this threshold, we continue
    /// reading and discard intermediate messages rather than performing an
    /// expensive seek operation. This provides significant performance
    /// benefits:
    /// - Kafka seeks: ~10-100ms (network round trips, index lookups)
    /// - Reading 100 messages: ~1-10ms (sequential, already buffered)
    /// - Bandwidth cost: ~10-100KB per 100 messages
    pub discard_threshold: i64,

    /// Span relation for loaded message spans.
    pub message_spans: SpanRelation,
}

/// Kafka message loader for retrieving messages by exact offset.
///
/// Uses a dedicated Kafka consumer with manual partition assignment to load
/// specific messages without interfering with the primary consumer's group
/// coordination. A background polling thread fulfills load requests and
/// semaphore-based permits provide backpressure. Messages are cached to avoid
/// redundant Kafka reads.
pub struct KafkaLoader<C: Codec = crate::codec::JsonCodec> {
    tx: mpsc::Sender<Request<C::Payload>>,
    semaphore: Arc<Semaphore>,
    cache: Arc<Cache<(Topic, Partition, Offset), DecodedMessage<C::Payload>>>,
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
    type Payload = C::Payload;
    type Error = KafkaLoaderError;

    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage<C::Payload>, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset)
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
    /// - `group.id`: `{config.group_id}-deferred-loader`
    /// - `auto.offset.reset=earliest` for recovery from deleted offsets
    /// - `enable.auto.commit=false` (manual offset management)
    /// - `enable.auto.offset.store=false` (manual seek/assign)
    ///
    /// # Arguments
    ///
    /// * `config` - Loader configuration (bootstrap servers, group ID, permits,
    ///   thresholds, etc.)
    /// * `heartbeats` - Registry for monitoring background polling loop
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
        let group_id = format!("{}-deferred-loader", config.group_id);
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

    /// Builds a [`KafkaLoader`] configured from the surrounding consumer and
    /// defer settings.
    ///
    /// Derives a `{group_id}.defer-loader` consumer group so the loader does
    /// not conflict with the primary consumer's group coordination.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying `BaseConsumer` cannot be created.
    pub fn for_consumer(
        consumer_config: &ConsumerConfiguration,
        defer_config: &DeferConfiguration,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self, KafkaLoaderError> {
        let loader_config = LoaderConfiguration {
            bootstrap_servers: consumer_config.bootstrap_servers.clone(),
            group_id: format!("{}.defer-loader", consumer_config.group_id),
            max_permits: consumer_config.max_uncommitted,
            cache_size: defer_config.cache_size,
            poll_interval: consumer_config.poll_interval,
            seek_timeout: defer_config.seek_timeout,
            discard_threshold: defer_config.discard_threshold,
            message_spans: consumer_config.message_spans,
        };
        Self::new(loader_config, heartbeats)
    }

    /// Loads a specific message from Kafka by offset.
    ///
    /// Checks the cache first for a fast path. Cache hits return immediately,
    /// while cache misses load from Kafka and populate the cache. In both
    /// cases, creates a new span linked to the parent trace context from the
    /// original Kafka message headers, ensuring span lifecycles are independent
    /// of cache eviction.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic containing the message
    /// * `partition` - The partition containing the message
    /// * `offset` - The exact offset of the message
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The semaphore is closed (loader shut down)
    /// - The request channel is closed (loader shut down)
    /// - The response channel is closed (loader shut down)
    /// - The message cannot be found or decoded
    /// - A Kafka error occurs during loading
    #[instrument(level = "debug", skip(self), fields(cached = Empty), err)]
    async fn load_message_impl(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage<C::Payload>, KafkaLoaderError> {
        let instrument_span = Span::current();

        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Acquiring permit for deferred message load"
        );

        // Acquire load permit for the returned message (backpressure)
        let load_permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| KafkaLoaderError::LoaderShutdown)?;

        let cache_key = (topic, partition, offset);

        // Get decoded message from cache or load from Kafka, tracking cache status
        let (decoded_message, cached) = if let Some(cached) = self.cache.get(&cache_key) {
            instrument_span.record("cached", true);
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "Loading deferred message from cache"
            );
            (cached, true)
        } else {
            instrument_span.record("cached", false);
            let decoded = self.load_from_kafka(topic, partition, offset).await?;
            (decoded, false)
        };

        // Create span linked to parent context (independent of cache lifetime)
        let load_span = create_load_span(&decoded_message, cached, self.message_spans);

        // Create consumer message from decoded message with new load span
        Ok(ConsumerMessage::from_decoded(
            decoded_message.value,
            load_span,
            load_permit,
        ))
    }

    /// Loads a message from Kafka and caches the decoded result.
    ///
    /// Sends a load request to the background poll loop, which handles Kafka
    /// polling and message decoding. The decoded message is cached using
    /// `quick_cache`'s S3-FIFO eviction policy for efficient repeated access.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic containing the message
    /// * `partition` - The partition containing the message
    /// * `offset` - The exact offset of the message
    ///
    /// # Errors
    ///
    /// Returns an error if Kafka loading fails or the loader is shut down.
    #[instrument(skip(self), level = "debug", err)]
    async fn load_from_kafka(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<DecodedMessage<C::Payload>, KafkaLoaderError> {
        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Loading deferred message from Kafka"
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
            "Deferred message cached for future loads"
        );

        Ok(decoded)
    }
}

/// A load request for a specific message offset.
struct Request<P> {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    tx: oneshot::Sender<Result<DecodedMessage<P>, KafkaLoaderError>>,
}

/// Background polling loop that fulfills load requests.
///
/// Runs on a blocking thread to avoid blocking the async runtime. Drains all
/// pending requests, seeks to optimize reads, polls Kafka, and fulfills
/// requests via lazy validation.
fn poll_loop<C: Codec>(
    mut rx: mpsc::Receiver<Request<C::Payload>>,
    consumer: &BaseConsumer,
    config: &LoaderConfiguration,
    heartbeat: &Heartbeat,
) where
    C::Payload: Clone,
{
    let mut active: ActiveRequests<C::Payload> = HashMap::default();
    let propagator = new_propagator();
    let mut codec = C::default();

    debug!("Deferred message loader poll loop started");

    loop {
        heartbeat.beat();

        // Drain all pending requests
        loop {
            match rx.try_recv() {
                Ok(request) => handle_request(request, &mut active, consumer),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    debug!("Deferred message loader poll loop shutting down");
                    return;
                }
            }
        }

        // If idle, wait for a request (with heartbeat timeout)
        if active.is_empty() {
            debug!("Poll loop idle, waiting for requests");
            if let Some(request) = Handle::current().block_on(async {
                select! {
                    r = rx.recv() => r,
                    () = heartbeat.next() => None,
                }
            }) {
                handle_request(request, &mut active, consumer);
            }

            // Channel close (None from recv) detected on next drain iteration
            continue;
        }

        debug!(
            active_partitions = active.len(),
            total_pending_offsets = active.values().map(|s| s.offsets.len()).sum::<usize>(),
            "Poll loop iteration with active requests"
        );

        // Seek partitions that need it. Per-partition pending_seek flags prevent
        // re-seeking a partition before the broker's auto-reset delivers the LSO
        // message. Partitions with pending_seek are skipped inside the function.
        if let Err(error) = seek_to_first_active_offset(
            &mut active,
            consumer,
            config.discard_threshold,
            config.seek_timeout,
        ) {
            warn!("Seek failed, retrying next iteration: {error:#}");
            // Do NOT fall through to poll(). If the seek failed, the
            // consumer's position is unknown — polling and running
            // split_off with an untrustworthy position would
            // misclassify pending offsets as deleted, which is data
            // corruption. Skip this iteration and retry the seek next
            // time around. The seek_timeout provides implicit backoff
            // (~5s) so this does not spin.
            continue;
        }

        // Poll once per iteration
        let Some(result) = consumer.poll(Timeout::After(config.poll_interval)) else {
            debug!("Poll returned no message");
            continue;
        };

        process_poll_result::<C>(
            result,
            &propagator,
            &mut codec,
            &mut active,
            consumer,
        );
    }
}

/// Processes a poll result and fulfills any matching active requests.
///
/// Performs lazy validation to detect deleted offsets by comparing requested
/// offsets against the received offset. Decodes the message using the first
/// response's permit and sends the result to all waiting channels. Unassigns
/// the partition if all requests are fulfilled.
fn process_poll_result<C: Codec>(
    result: Result<BorrowedMessage, KafkaError>,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
    active: &mut ActiveRequests<C::Payload>,
    consumer: &BaseConsumer,
) where
    C::Payload: Clone,
{
    let message = match result {
        Ok(message) => message,
        Err(error) => {
            error!(error = %format_args!("{error:#}"), "Error polling for deferred message");
            return;
        }
    };

    let msg_topic = Topic::from(message.topic());
    let msg_partition = message.partition();
    let msg_offset = message.offset();

    debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset, "Polled message");

    let Some(state) = active.get_mut(&(msg_topic, msg_partition)) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Received message for partition with no active requests");
        return;
    };

    // LAZY VALIDATION: Detect deleted offsets (LSO moved forward).
    //
    // `pending_seek` records the offset we sought to when the seek was
    // dispatched. Requests that arrived in the channel AFTER the seek was
    // issued have offsets below `pending_seek` and are NOT deleted — they
    // just arrived late and need a fresh seek. Only offsets in the range
    // `[pending_seek, msg_offset)` were genuinely skipped by the broker
    // (i.e., deleted via retention/compaction).
    //
    // When `pending_seek` is None (sequential read, no seek was issued), fall
    // back to the current minimum offset as the split boundary — the original
    // behaviour before this fix was introduced.
    let split_start = state
        .pending_seek
        .take()
        .unwrap_or_else(|| state.offsets.keys().next().copied().unwrap_or(msg_offset));

    // Split out entries that were present at seek time: [split_start..)
    // Entries in [0..split_start) are late arrivals; they stay in state.offsets.
    let working_set = state.offsets.split_off(&split_start);

    // Within the seek-time entries, partition into deleted and future:
    //   deleted  = [split_start..msg_offset)
    //   remaining = [msg_offset..)
    let mut deleted_offsets = working_set;
    let remaining = deleted_offsets.split_off(&msg_offset);

    // Merge future entries back so they aren't lost.
    state.offsets.extend(remaining);

    notify_deleted_offsets(deleted_offsets, msg_topic, msg_partition, msg_offset);

    let Some(senders) = state.offsets.remove(&msg_offset) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Discarding intermediate message (not requested)");
        cleanup_if_empty(active, consumer, msg_topic, msg_partition);
        return;
    };

    fulfill_requests::<C>(
        senders,
        &message,
        propagator,
        codec,
        msg_topic,
        msg_partition,
        msg_offset,
    );

    cleanup_if_empty(active, consumer, msg_topic, msg_partition);
}

/// Notifies senders about deleted offsets and logs warnings.
fn notify_deleted_offsets<P>(
    deleted_offsets: BTreeMap<Offset, Responses<P>>,
    topic: Topic,
    partition: Partition,
    next_offset: Offset,
) {
    for (requested_offset, senders) in deleted_offsets {
        warn!(
            topic = %topic,
            partition = partition,
            requested_offset = requested_offset,
            next_offset = next_offset,
            affected_requests = senders.len(),
            "Deferred message offset no longer exists (deleted by retention or compaction)"
        );
        let error = KafkaLoaderError::OffsetDeleted {
            topic,
            partition,
            requested_offset,
            next_offset,
        };
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

/// Decodes and fulfills requests for a specific offset.
fn fulfill_requests<C: Codec>(
    senders: Responses<C::Payload>,
    message: &BorrowedMessage<'_>,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
    topic: Topic,
    partition: Partition,
    offset: Offset,
) where
    C::Payload: Clone,
{
    let request_count = senders.len();
    debug!(topic = %topic, partition = partition, offset = offset, request_count = request_count,
        "Fulfilling active requests for deferred message");

    let decoded_message = decode_message(message, propagator, codec);

    if let Some(decoded) = decoded_message {
        debug!(topic = %topic, partition = partition, offset = offset, request_count = request_count,
            "Deferred message loaded successfully");
        for sender in senders {
            let _ = sender.send(Ok(decoded.clone()));
        }
    } else {
        error!(topic = %topic, partition = partition, offset = offset,
            "Failed to decode deferred message");
        let error = KafkaLoaderError::DecodeError(topic, partition, offset);
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

/// Cleans up partition entry and unassigns if no more requests remain.
fn cleanup_if_empty<P>(
    active: &mut ActiveRequests<P>,
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
) {
    let should_cleanup = active
        .get(&(topic, partition))
        .is_some_and(|s| s.offsets.is_empty());

    if should_cleanup {
        active.remove(&(topic, partition));
        if let Err(error) = unassign_partition(consumer, topic, partition) {
            warn!(topic = %topic, partition = partition, error = %format_args!("{error:#}"),
                "Failed to unassign partition after fulfilling all requests");
        }
    }
}

/// Handles a single load request by assigning the partition and adding to
/// active requests.
///
/// For the first request to an offset, stores the permit for decoding.
/// For subsequent requests to the same offset, adds the sender but drops the
/// permit (only one permit needed per offset for decoding).
///
/// # Arguments
///
/// * `request` - The load request to process
/// * `active` - Active requests map
/// * `consumer` - Kafka consumer for assignment
fn handle_request<P>(request: Request<P>, active: &mut ActiveRequests<P>, consumer: &BaseConsumer) {
    use std::collections::btree_map::Entry;

    let Request {
        topic,
        partition,
        offset,
        tx,
    } = request;

    debug!(
        topic = %topic,
        partition = partition,
        offset = offset,
        "Processing load request for deferred message"
    );

    if let Err(error) = assign_if_needed(active, consumer, topic, partition, offset) {
        error!(
            topic = %topic,
            partition = partition,
            offset = offset,
            error = %format_args!("{error:#}"),
            "Failed to assign partition for deferred message load"
        );
        let _ = tx.send(Err(KafkaLoaderError::Kafka(error)));
        return;
    }

    let state = active.entry((topic, partition)).or_default();

    match state.offsets.entry(offset) {
        Entry::Vacant(entry) => {
            // First request for this offset
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "First request for offset, will decode when polled"
            );
            let mut senders = SmallVec::new();
            senders.push(tx);
            entry.insert(senders);
        }
        Entry::Occupied(mut entry) => {
            // Subsequent request - coalesce with existing
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                coalesced_count = entry.get().len() + 1,
                "Coalescing with existing request for same offset"
            );
            entry.get_mut().push(tx);
        }
    }
}

/// Seeks partitions to their first active offset when beneficial.
///
/// Seeks are a performance optimization: reading 100 messages (~1-10ms) is
/// faster than seeking (~10-100ms) when within the discard threshold.
///
/// **Strategy:** Seeks to deleted offsets succeed and auto-position at LSO
/// (same as assign behavior). Lazy validation in [`process_poll_result`]
/// detects deletions when messages arrive. Seek failures (network errors,
/// Kafka down) trigger retry via the caller's continue loop.
///
/// Partitions whose `pending_seek` flag is already set are skipped: a seek is
/// only materialised (position advances out of Invalid) after the consumer
/// receives a message from the broker. Re-seeking before that point resets the
/// consumer back to the (possibly deleted) target offset on every iteration,
/// preventing the broker's auto-reset from delivering the LSO message.
/// `pending_seek` is set here for each partition whose seek succeeds and is
/// cleared in [`process_poll_result`] when a message is received.
///
/// # Arguments
///
/// * `active` - Map of active requests by topic-partition (mutated to set
///   `pending_seek` flags)
/// * `consumer` - The Kafka consumer to seek
/// * `discard_threshold` - Number of messages to read before seeking
/// * `seek_timeout` - Timeout for seek operations
///
/// # Errors
///
/// Returns a `KafkaError` if seeking fails. The caller must NOT poll after
/// a seek failure — the consumer's position is unknown and polling would
/// risk misclassifying pending offsets as deleted. The caller should retry
/// the seek on the next iteration.
fn seek_to_first_active_offset<P>(
    active: &mut ActiveRequests<P>,
    consumer: &BaseConsumer,
    discard_threshold: i64,
    seek_timeout: Duration,
) -> KafkaResult<()> {
    if active.is_empty() {
        return Ok(());
    }

    let mut seek_list = TopicPartitionList::new();

    // One call retrieves positions for all assigned partitions from local
    // librdkafka state — no network round-trip.
    let positions = consumer.position()?;

    for ((topic, partition), state) in active.iter() {
        let Some((&min_offset, _)) = state.offsets.first_key_value() else {
            continue;
        };

        // Skip if we already have a seek in flight that will land at or before
        // min_offset. If a new lower-offset request arrived after the seek was
        // dispatched, min_offset < pending_seek and we must re-seek.
        if state.pending_seek.is_some_and(|s| s <= min_offset) {
            continue;
        }

        let current_position = positions
            .find_partition(topic.as_ref(), *partition)
            .and_then(|elem| match elem.offset() {
                rdkafka::Offset::Offset(offset) => Some(offset),
                _ => None,
            });

        // Avoid expensive seeks when close enough to read sequentially.
        // Seek (~10-100ms) vs sequential read of N messages (~1-10ms):
        // - Don't seek: within threshold and before target (sequential read cheaper)
        // - Seek: past target (backward), too far behind
        // - Seek: unknown position (Invalid) — position() returns Invalid after
        //   incremental_assign() until the first message is consumed. assign_if_needed
        //   only assigns on the first request; concurrent lower-offset requests skip
        //   re-assignment, so the consumer may be anchored above min_offset. Always
        //   seek when Invalid to land at the correct starting point.
        let should_seek = match current_position {
            None => true,
            Some(position) => {
                let past_target = position > min_offset;
                let too_far_behind = position + discard_threshold < min_offset;
                past_target || too_far_behind
            }
        };

        debug!(
            topic = %AsRef::<str>::as_ref(topic),
            partition = partition,
            min_offset = min_offset,
            current_position = ?current_position,
            should_seek = should_seek,
            "Evaluating seek decision for partition"
        );

        if should_seek {
            debug!(
                topic = %AsRef::<str>::as_ref(topic),
                partition = partition,
                target_offset = min_offset,
                "Adding partition to seek list"
            );
            seek_list.add_partition_offset(
                topic.as_ref(),
                *partition,
                rdkafka::Offset::Offset(min_offset),
            )?;
        }
    }

    if seek_list.count() == 0 {
        return Ok(());
    }

    debug!(
        partition_count = seek_list.count(),
        "Executing seek operation"
    );
    let result = consumer.seek_partitions(seek_list, Timeout::After(seek_timeout))?;

    // Set pending_seek for each partition that succeeded before checking for
    // errors. This way, if partition A succeeds and partition B fails, A's flag
    // is correctly set before we return the error.
    for elem in result.elements() {
        if let Err(e) = elem.error() {
            warn!(
                topic = elem.topic(),
                partition = elem.partition(),
                offset = ?elem.offset(),
                "Seek failed for partition: {e:#}"
            );
            return Err(e);
        }
        debug!(
            topic = elem.topic(),
            partition = elem.partition(),
            offset = ?elem.offset(),
            "Seek succeeded for partition"
        );
        if let rdkafka::Offset::Offset(sought_offset) = elem.offset() {
            let key = (Topic::from(elem.topic()), elem.partition());
            if let Some(state) = active.get_mut(&key) {
                state.pending_seek = Some(sought_offset);
            }
        }
    }

    Ok(())
}

/// Assigns a partition at the requested offset if not already assigned.
///
/// Uses manual partition assignment with the exact offset. If the offset has
/// been deleted, rdkafka auto-positions at the Log Start Offset (LSO). Lazy
/// validation in [`process_poll_result`] detects this case by comparing
/// requested vs received offsets.
///
/// # Arguments
///
/// * `active` - Active requests map (checked to avoid duplicate assignments)
/// * `consumer` - Kafka consumer to assign partition
/// * `topic` - Topic to assign
/// * `partition` - Partition number to assign
/// * `offset` - Offset to start reading from
///
/// # Errors
///
/// Returns a [`KafkaError`] if the assignment operation fails.
fn assign_if_needed<P>(
    active: &ActiveRequests<P>,
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
    offset: Offset,
) -> KafkaResult<()> {
    if active.contains_key(&(topic, partition)) {
        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Partition already assigned, skipping assignment"
        );
        return Ok(());
    }

    // Incrementally assign partition at requested offset.
    // Must use incremental_assign() to ADD to existing assignments, not replace
    // them. This pairs with incremental_unassign() used when partitions are
    // fulfilled.
    //
    // Note: incremental_assign() with deleted offset auto-resets to LSO cleanly.
    // Lazy validation in process_poll_result detects when received != requested.
    let mut to_assign = TopicPartitionList::new();
    to_assign.add_partition_offset(topic.as_ref(), partition, rdkafka::Offset::Offset(offset))?;
    consumer.incremental_assign(&to_assign)?;

    debug!(
        topic = %topic,
        partition = partition,
        offset = offset,
        "Assigned partition for deferred message loading"
    );

    Ok(())
}

/// Unassigns a partition from the consumer.
///
/// Removes the partition assignment since all requested offsets have been
/// fulfilled. This keeps resource usage minimal by only holding assignments
/// for partitions with active requests.
///
/// # Arguments
///
/// * `consumer` - The Kafka consumer to unassign from
/// * `topic` - The topic of the partition to unassign
/// * `partition` - The partition number to unassign
///
/// # Errors
///
/// Returns a `KafkaError` if the unassign operation fails
fn unassign_partition(
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
) -> KafkaResult<()> {
    let mut to_unassign = TopicPartitionList::new();
    to_unassign.add_partition(topic.as_ref(), partition);
    consumer.incremental_unassign(&to_unassign)?;
    debug!(
        topic = %topic,
        partition = partition,
        "Unassigned partition after fulfilling all deferred load requests"
    );
    Ok(())
}

/// Creates a tracing span for a loaded message connected to its upstream
/// context.
///
/// Creates a span named "load" with message metadata attributes and connects
/// it to the upstream trace via the configured [`SpanRelation`]. Span
/// lifecycles are independent of cache eviction.
///
/// # Arguments
///
/// * `decoded` - The decoded message containing parent context and metadata
/// * `cached` - Whether this message was loaded from cache (true) or Kafka
///   (false)
///
/// # Returns
///
/// A tracing span linked to the parent context with message metadata recorded.
fn create_load_span<P>(decoded: &DecodedMessage<P>, cached: bool, relation: SpanRelation) -> Span {
    related_span!(
        relation,
        decoded.parent_context.clone(),
        "load",
        partition = decoded.value.partition,
        offset = decoded.value.offset,
        topic = %decoded.value.topic,
        key = %decoded.value.key,
        cached = cached,
    )
}

/// Errors that can occur during Kafka message loading.
#[derive(Clone, Debug, Error)]
pub enum KafkaLoaderError {
    /// Failed to decode the message payload.
    #[error("Failed to decode message {0}/{1}:{2}")]
    DecodeError(Topic, Partition, Offset),

    /// The loader has been shut down and cannot process requests.
    #[error("Loader has shut down")]
    LoaderShutdown,

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

            // Classify Kafka operation errors using shared implementation
            Self::Kafka(kafka_error) => kafka_error.classify_error(),

            // Permanent errors - data issues that won't resolve
            Self::DecodeError(..) | Self::OffsetDeleted { .. } => ErrorCategory::Permanent,
        }
    }
}
