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
//! When messages are deleted via retention or compaction, the Log Start Offset
//! (LSO) moves forward. The loader handles this by:
//!
//! 1. **Assign/seek auto-reset**: rdkafka automatically positions at LSO when
//!    assigning or seeking to deleted offsets
//! 2. **Lazy validation**: Compares `requested_offset` < `received_offset` to
//!    detect deletions after messages arrive
//! 3. **Explicit errors**: Returns [`KafkaLoaderError::OffsetDeleted`] with the
//!    LSO
//!
//! This approach avoids upfront offset validation (which requires metadata
//! queries) and lets rdkafka handle offset recovery automatically.

use super::MessageLoader;
use crate::consumer::decode::{DecodedMessage, decode_message};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::heartbeat::{Heartbeat, HeartbeatRegistry};
use crate::propagator::new_propagator;
use crate::{Offset, Partition, Topic};
use ahash::HashMap;
use opentelemetry::propagation::TextMapCompositePropagator;
use quick_cache::sync::Cache;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::spawn_blocking;
use tracing::field::Empty;
use tracing::{Span, debug, error, instrument, warn};
use whoami::fallible::hostname;

#[cfg(not(target_arch = "arm"))]
use simd_json::Buffers;
use tokio::select;

#[cfg(test)]
mod tests;

/// Multiple response channels waiting for the same offset, with one shared
/// permit.
///
/// When multiple callers request the same offset, only one permit is needed for
/// decoding. Subsequent callers' permits are dropped, but they still acquire
/// permits initially to maintain backpressure semantics.
type Responses = SmallVec<[oneshot::Sender<Result<DecodedMessage, KafkaLoaderError>>; 1]>;

/// Active load requests indexed by topic-partition, then by offset.
///
/// Each partition tracks its requested offsets in sorted order (via
/// [`BTreeMap`]) to efficiently find the minimum offset for seek optimization.
type ActiveRequests = HashMap<(Topic, Partition), BTreeMap<Offset, Responses>>;

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
}

/// Kafka message loader for retrieving messages by exact offset.
///
/// Uses a dedicated Kafka consumer with manual partition assignment to load
/// specific messages without interfering with the primary consumer's group
/// coordination. A background polling thread fulfills load requests and
/// semaphore-based permits provide backpressure. Messages are cached to avoid
/// redundant Kafka reads.
#[derive(Clone)]
pub struct KafkaLoader {
    tx: mpsc::Sender<Request>,
    semaphore: Arc<Semaphore>,
    cache: Arc<Cache<(Topic, Partition, Offset), DecodedMessage>>,
}

impl MessageLoader for KafkaLoader {
    type Error = KafkaLoaderError;

    fn load_message(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<ConsumerMessage, Self::Error>> + Send {
        self.load_message_impl(topic, partition, offset)
    }
}

impl KafkaLoader {
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
        let group_id = format!("{}-deferred", config.group_id);
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

        let heartbeat = heartbeats.register("kafka loader");
        spawn_blocking(move || poll_loop(rx, &consumer, &config, &heartbeat));

        Ok(Self {
            tx,
            semaphore,
            cache,
        })
    }

    /// Loads a specific message from Kafka by offset.
    ///
    /// Checks the cache first for a fast path. Cache hits return immediately,
    /// while cache misses load from Kafka and populate the cache. In both
    /// cases, returns a new message instance with a deferred span that has a
    /// `follows_from` relationship to the original span.
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
    ) -> Result<ConsumerMessage, KafkaLoaderError> {
        let span = Span::current();

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

        // Get decoded message from cache or load from Kafka
        let decoded_message = if let Some(cached) = self.cache.get(&cache_key) {
            span.record("cached", true);
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "Loading deferred message from cache"
            );
            cached
        } else {
            span.record("cached", false);
            self.load_from_kafka(topic, partition, offset).await?
        };

        span.follows_from(&decoded_message.span);

        // Create consumer message from decoded message with load permit
        Ok(ConsumerMessage::from_decoded(
            decoded_message.value,
            decoded_message.span,
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
    ) -> Result<DecodedMessage, KafkaLoaderError> {
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
struct Request {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    tx: oneshot::Sender<Result<DecodedMessage, KafkaLoaderError>>,
}

/// Background polling loop that fulfills load requests.
///
/// Runs on a blocking thread to avoid blocking the async runtime. Drains all
/// pending requests, seeks to optimize reads, polls Kafka, and fulfills
/// requests via lazy validation.
fn poll_loop(
    mut rx: mpsc::Receiver<Request>,
    consumer: &BaseConsumer,
    config: &LoaderConfiguration,
    heartbeat: &Heartbeat,
) {
    let mut active: ActiveRequests = HashMap::default();
    let propagator = new_propagator();

    #[cfg(not(target_arch = "arm"))]
    let mut buffers = Buffers::default();

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
            total_pending_offsets = active.values().map(BTreeMap::len).sum::<usize>(),
            "Poll loop iteration with active requests"
        );

        // Seek to first active offset if beneficial
        if let Err(error) = seek_to_first_active_offset(
            &active,
            consumer,
            config.discard_threshold,
            config.seek_timeout,
        ) {
            warn!(error = %format_args!("{error:#}"), "Seek failed, will retry after poll");
            // Fall through to poll() which provides backoff via its timeout
        }

        // Poll once per iteration
        let Some(result) = consumer.poll(Timeout::After(config.poll_interval)) else {
            debug!("Poll returned no message");
            continue;
        };

        process_poll_result(
            result,
            &propagator,
            #[cfg(not(target_arch = "arm"))]
            &mut buffers,
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
fn process_poll_result(
    result: Result<BorrowedMessage, KafkaError>,
    propagator: &TextMapCompositePropagator,
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
    active: &mut ActiveRequests,
    consumer: &BaseConsumer,
) {
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

    let Some(partition_requests) = active.get_mut(&(msg_topic, msg_partition)) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Received message for partition with no active requests");
        return;
    };

    // LAZY VALIDATION: Detect deleted offsets (LSO moved forward)
    let mut deleted_offsets = partition_requests.split_off(&msg_offset);
    mem::swap(&mut deleted_offsets, partition_requests);
    notify_deleted_offsets(deleted_offsets, msg_topic, msg_partition, msg_offset);

    let Some(senders) = partition_requests.remove(&msg_offset) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Discarding intermediate message (not requested)");
        cleanup_if_empty(active, consumer, msg_topic, msg_partition);
        return;
    };

    fulfill_requests(
        senders,
        &message,
        propagator,
        #[cfg(not(target_arch = "arm"))]
        buffers,
        msg_topic,
        msg_partition,
        msg_offset,
    );

    cleanup_if_empty(active, consumer, msg_topic, msg_partition);
}

/// Notifies senders about deleted offsets and logs warnings.
fn notify_deleted_offsets(
    deleted_offsets: BTreeMap<Offset, Responses>,
    topic: Topic,
    partition: Partition,
    log_start_offset: Offset,
) {
    for (deleted_offset, senders) in deleted_offsets {
        warn!(
            topic = %topic,
            partition = partition,
            requested_offset = deleted_offset,
            log_start_offset = log_start_offset,
            affected_requests = senders.len(),
            "Deferred message was deleted due to retention/compaction"
        );
        let error =
            KafkaLoaderError::OffsetDeleted(topic, partition, deleted_offset, log_start_offset);
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

/// Decodes and fulfills requests for a specific offset.
fn fulfill_requests(
    senders: Responses,
    message: &BorrowedMessage<'_>,
    propagator: &TextMapCompositePropagator,
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
    topic: Topic,
    partition: Partition,
    offset: Offset,
) {
    let request_count = senders.len();
    debug!(topic = %topic, partition = partition, offset = offset, request_count = request_count,
        "Fulfilling active requests for deferred message");

    let decoded_message = decode_message(
        message,
        propagator,
        #[cfg(not(target_arch = "arm"))]
        buffers,
    );

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
fn cleanup_if_empty(
    active: &mut ActiveRequests,
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
) {
    let should_cleanup = active
        .get(&(topic, partition))
        .is_some_and(BTreeMap::is_empty);

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
fn handle_request(request: Request, active: &mut ActiveRequests, consumer: &BaseConsumer) {
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

    let partition_requests = active.entry((topic, partition)).or_default();

    match partition_requests.entry(offset) {
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
/// # Arguments
///
/// * `active` - Map of active requests by topic-partition and offset
/// * `consumer` - The Kafka consumer to seek
/// * `discard_threshold` - Number of messages to read before seeking
/// * `seek_timeout` - Timeout for seek operations
///
/// # Errors
///
/// Returns a `KafkaError` if seeking fails (caller should poll to recover)
fn seek_to_first_active_offset(
    active: &ActiveRequests,
    consumer: &BaseConsumer,
    discard_threshold: i64,
    seek_timeout: Duration,
) -> KafkaResult<()> {
    if active.is_empty() {
        return Ok(());
    }

    let mut seek_list = TopicPartitionList::new();

    for ((topic, partition), offsets) in active {
        let Some((&min_offset, _)) = offsets.first_key_value() else {
            continue;
        };

        let current_position = consumer
            .position()?
            .find_partition(topic.as_ref(), *partition)
            .and_then(|elem| match elem.offset() {
                rdkafka::Offset::Offset(offset) => Some(offset),
                _ => None,
            });

        // Avoid expensive seeks when close enough to read sequentially.
        // Seek (~10-100ms) vs sequential read of N messages (~1-10ms):
        // - Don't seek: within threshold and before target (sequential read cheaper)
        // - Seek: past target (backward), too far behind
        // - Don't seek: unknown position (Invalid) - trust assign() positioned
        //   correctly
        //
        // Note: position() returns Invalid after assign() until first message is
        // consumed. In this state, assign() already positioned the consumer at
        // the requested offset, so seeking is unnecessary and adds latency.
        let should_seek = current_position.is_some_and(|position| {
            let past_target = position > min_offset;
            let too_far_behind = position + discard_threshold < min_offset;
            past_target || too_far_behind
        });

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

    if seek_list.count() > 0 {
        debug!(
            partition_count = seek_list.count(),
            "Executing seek operation"
        );
        let result = consumer.seek_partitions(seek_list, Timeout::After(seek_timeout))?;

        // Check for seek failures (network errors, timeouts, etc.)
        for elem in result.elements() {
            if let Err(e) = elem.error() {
                warn!(
                    topic = elem.topic(),
                    partition = elem.partition(),
                    offset = ?elem.offset(),
                    error = %format_args!("{e:#}"),
                    "Seek failed for partition"
                );
                return Err(e);
            }
            debug!(
                topic = elem.topic(),
                partition = elem.partition(),
                offset = ?elem.offset(),
                "Seek succeeded for partition"
            );
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
fn assign_if_needed(
    active: &ActiveRequests,
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

/// Errors that can occur during Kafka message loading.
#[derive(Clone, Debug, Error)]
pub enum KafkaLoaderError {
    /// Failed to decode the message payload.
    #[error("Failed to decode message {0}/{1}:{2}")]
    DecodeError(Topic, Partition, Offset),

    /// The loader has been shut down and cannot process requests.
    #[error("Loader has shut down")]
    LoaderShutdown,

    /// The requested offset has been deleted due to retention or compaction.
    ///
    /// Contains the topic, partition, requested offset, and current log start
    /// offset.
    #[error(
        "Offset {2} has been deleted from partition {0}/{1} (log start offset: {3}). The \
         requested message no longer exists due to retention or compaction."
    )]
    OffsetDeleted(Topic, Partition, Offset, Offset),

    /// Failed to create the Kafka consumer.
    #[error("Failed to create Kafka consumer: {0:#}")]
    ConsumerCreation(KafkaError),

    /// A Kafka operation error occurred.
    #[error("Kafka error: {0:#}")]
    Kafka(KafkaError),

    /// Failed to retrieve the hostname for the consumer client ID.
    #[error("failed to get hostname: {0:#}")]
    Hostname(Arc<io::Error>),
}

impl ClassifyError for KafkaLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal errors - system cannot operate
            Self::LoaderShutdown | Self::ConsumerCreation(_) | Self::Hostname(_) => {
                ErrorCategory::Terminal
            }

            // Classify Kafka operation errors as transient or permanent
            Self::Kafka(kafka_error) => classify_kafka_error(kafka_error),

            // Permanent errors - data issues that won't resolve
            Self::DecodeError(..) | Self::OffsetDeleted(..) => ErrorCategory::Permanent,
        }
    }
}

/// Classifies a [`KafkaError`] as transient or permanent.
///
/// Transient errors (network failures, timeouts) may succeed on retry.
/// Permanent errors will never succeed - accept data loss and continue.
///
/// Note: [`KafkaError`] is marked `#[non_exhaustive]`, so a wildcard is
/// required. New error variants added by rdkafka will be treated as transient
/// by default, since the loader is reloading messages that previously existed.
fn classify_kafka_error(error: &KafkaError) -> ErrorCategory {
    match error {
        // Classify based on error code for errors that contain RDKafkaErrorCode
        KafkaError::MessageProduction(code)
        | KafkaError::MessageConsumption(code)
        | KafkaError::MetadataFetch(code)
        | KafkaError::Global(code)
        | KafkaError::ConsumerCommit(code)
        | KafkaError::OffsetFetch(code)
        | KafkaError::AdminOp(code)
        | KafkaError::StoreOffset(code)
        | KafkaError::SetPartitionOffset(code) => classify_kafka_error_code(*code),

        // Client creation and configuration errors are terminal - loader cannot operate
        KafkaError::ClientCreation(_) | KafkaError::ClientConfig(..) => ErrorCategory::Terminal,

        // All other errors treated as transient (unknown variants, string errors, RDKafkaError
        // types) In the loader context, we're reloading messages that existed before,
        // so unknown errors are more likely to be transient issues
        _ => ErrorCategory::Transient,
    }
}

/// Classifies an [`RDKafkaErrorCode`] as transient or permanent.
///
/// Note: [`RDKafkaErrorCode`] is marked `#[non_exhaustive]`, so a wildcard is
/// required. New error codes added by rdkafka will be treated as transient by
/// default, since the loader is reloading messages that previously existed.
///
/// # Classification Rubric
///
/// - **Permanent**: The message itself is corrupt, malformed, or invalid. No
///   configuration change can salvage it. Data loss is unavoidable.
/// - **Transient**: The error is due to authorization, configuration, network,
///   or availability issues. These can potentially be resolved by changing
///   broker/client configuration, fixing ACLs, or waiting for recovery.
///
/// We bias heavily toward transient to avoid data loss. Only message-level
/// corruption errors that cannot be fixed by any configuration change are
/// marked permanent.
fn classify_kafka_error_code(code: RDKafkaErrorCode) -> ErrorCategory {
    match code {
        // Permanent: Message content is fundamentally broken
        // These indicate the message data itself is corrupt or malformed.
        // No configuration change can fix the message - data loss is unavoidable.
        //
        // - BadMessage (-199): librdkafka client detected received message is incorrect
        // - InvalidMessage (2): Kafka CORRUPT_MESSAGE - CRC failure, invalid size, null key on
        //   compacted topic, or other corruption
        // - InvalidRecord (87): Broker failed to validate record content
        RDKafkaErrorCode::BadMessage
        | RDKafkaErrorCode::InvalidMessage
        | RDKafkaErrorCode::InvalidRecord => ErrorCategory::Permanent,

        // All other errors are transient - they can potentially be resolved by:
        // - Fixing ACLs (authorization errors)
        // - Adjusting broker/client config (size limits, timeouts, etc.)
        // - Creating topics or waiting for metadata refresh
        // - Upgrading brokers or refreshing tokens
        // - Network recovery or rebalancing
        //
        // Note: InvalidMessageSize (4) is Kafka's INVALID_FETCH_SIZE - about fetch
        // request parameters, not message content. MessageSizeTooLarge (10) can be
        // fixed by increasing broker's max.message.bytes.
        _ => ErrorCategory::Transient,
    }
}
