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

#![allow(dead_code)]

use crate::consumer::decode::decode_message;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::propagator::new_propagator;
use crate::{Offset, Partition, Topic};
use ahash::HashMap;
use opentelemetry::propagation::TextMapCompositePropagator;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::spawn_blocking;
use tracing::{debug, error, warn};
use validator::Validate;
use whoami::fallible::hostname;

#[cfg(not(target_arch = "arm"))]
use simd_json::Buffers;

#[cfg(test)]
mod tests;

/// A response channel paired with a semaphore permit for backpressure.
struct Response {
    sender: oneshot::Sender<Result<ConsumerMessage, KafkaLoaderError>>,
    permit: OwnedSemaphorePermit,
}

/// Multiple response channels waiting for the same offset, with one shared
/// permit.
///
/// When multiple callers request the same offset, only one permit is needed for
/// decoding. Subsequent callers' permits are dropped, but they still acquire
/// permits initially to maintain backpressure semantics.
struct Responses {
    permit: OwnedSemaphorePermit,
    senders: SmallVec<[oneshot::Sender<Result<ConsumerMessage, KafkaLoaderError>>; 1]>,
}

/// Active load requests indexed by topic-partition, then by offset.
///
/// Each partition tracks its requested offsets in sorted order (via
/// [`BTreeMap`]) to efficiently find the minimum offset for seek optimization.
type ActiveRequests = HashMap<(Topic, Partition), BTreeMap<Offset, Responses>>;

/// Configuration for the Kafka message loader.
///
/// Controls performance characteristics and resource usage of the defer
/// middleware loader that loads failed messages for retry.
#[derive(Clone, Debug, Validate)]
pub struct LoaderConfiguration {
    /// Kafka broker addresses.
    ///
    /// List of host:port pairs for initial connection to the Kafka cluster.
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    /// Consumer group ID base name.
    ///
    /// The loader will append `-deferred-loader` to create a unique group
    /// ID, ensuring no conflicts with the primary consumer.
    #[validate(length(min = 1_u64))]
    pub group_id: String,

    /// Maximum number of concurrent message decoding operations.
    ///
    /// Controls the size of the semaphore used for decoding permits
    /// and the capacity of the request channel.
    #[validate(range(min = 1_usize))]
    pub max_permits: usize,

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
    #[validate(range(min = 0_i64))]
    pub discard_threshold: i64,
}

/// Kafka message loader for retrieving messages by exact offset.
///
/// Uses a dedicated Kafka consumer with manual partition assignment to load
/// specific messages without interfering with the primary consumer's group
/// coordination. A background polling thread fulfills load requests and
/// semaphore-based permits provide backpressure.
pub struct KafkaLoader {
    tx: mpsc::Sender<Request>,
    semaphore: Arc<Semaphore>,
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
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration validation fails
    /// - Consumer creation fails
    pub fn new(config: LoaderConfiguration) -> Result<Self, KafkaLoaderError> {
        // Validate configuration
        config
            .validate()
            .map_err(|e| KafkaLoaderError::Configuration(e.to_string()))?;

        // Create unique instance identifier from hostname or UUID
        let client_id = hostname().unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        // Create consumer with configuration optimized for random access
        let group_id = format!("{}-deferred-loader", config.group_id);

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", &client_id)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set_log_level(RDKafkaLogLevel::Error)
            .create()
            .map_err(KafkaLoaderError::ConsumerCreation)?;

        let (tx, rx) = mpsc::channel(config.max_permits);
        let semaphore = Arc::new(Semaphore::new(config.max_permits));

        spawn_blocking(move || poll_loop(rx, &consumer, &config));

        Ok(Self { tx, semaphore })
    }

    /// Loads a specific message from Kafka by offset.
    ///
    /// Acquires a semaphore permit, sends a load request to the background
    /// thread, and waits for the message to be decoded.
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
    pub async fn load(
        &self,
        topic: Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<ConsumerMessage, KafkaLoaderError> {
        // Acquire permit for backpressure
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| KafkaLoaderError::LoaderShutdown)?;

        // Create response channel
        let (tx, rx) = oneshot::channel();

        // Send load request
        self.tx
            .send(Request {
                topic,
                partition,
                offset,
                response: Response { sender: tx, permit },
            })
            .await
            .map_err(|_| KafkaLoaderError::LoaderShutdown)?;

        // Wait for response
        rx.await.map_err(|_| KafkaLoaderError::LoaderShutdown)?
    }
}

/// A load request for a specific message offset.
struct Request {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    response: Response,
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
) {
    let mut active: ActiveRequests = HashMap::default();
    let propagator = new_propagator();

    #[cfg(not(target_arch = "arm"))]
    let mut buffers = Buffers::default();

    loop {
        // Drain all pending requests (non-blocking)
        loop {
            match rx.try_recv() {
                Ok(request) => {
                    handle_request(request, &mut active, consumer);
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("Loader poll loop shutdown");
                    return;
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        // If no active requests, block until we receive one
        if active.is_empty() {
            let Some(request) = rx.blocking_recv() else {
                debug!("Loader poll loop shutdown");
                return;
            };

            handle_request(request, &mut active, consumer);
            continue;
        }

        // Seek to first active offset if beneficial
        if let Err(error) = seek_to_first_active_offset(
            &active,
            consumer,
            config.discard_threshold,
            config.seek_timeout,
        ) {
            warn!("seek failed: {error:#}, skipping poll and retrying");
            continue;
        }

        // Poll once per iteration (normal operation or recovery from seek failure)
        let Some(result) = consumer.poll(Timeout::After(config.poll_interval)) else {
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
///
/// # Arguments
///
/// * `result` - Poll result containing message or error
/// * `propagator` - OpenTelemetry propagator for distributed tracing
/// * `buffers` - (Non-ARM only) SIMD JSON parsing buffers
/// * `active` - Active requests map to update
/// * `consumer` - Kafka consumer for unassigning partitions
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
            error!("error polling for message: {error:#}");
            return;
        }
    };

    let msg_topic = Topic::from(message.topic());
    let msg_partition = message.partition();
    let msg_offset = message.offset();

    debug!(
        "Received message: {}/{}:{}",
        msg_topic.as_ref(),
        msg_partition,
        msg_offset
    );

    // Check if this message fulfills any active requests
    let Some(partition_requests) = active.get_mut(&(msg_topic, msg_partition)) else {
        warn!(
            "received message for partition with no active requests: \
             {}/{msg_partition}:{msg_offset}",
            msg_topic.as_ref(),
        );
        return;
    };

    // LAZY VALIDATION: Detect deleted offsets
    // If we received offset N but have requests for offsets < N, those offsets
    // were deleted (LSO moved forward due to retention/compaction).
    // Experiments show: assign() with deleted offset auto-resets to LSO.
    let mut deleted_offsets = partition_requests.split_off(&msg_offset);
    mem::swap(&mut deleted_offsets, partition_requests);
    // Now `deleted_offsets` has < msg_offset, `partition_requests` has >=
    // msg_offset

    for (deleted_offset, Responses { senders, .. }) in deleted_offsets {
        warn!(
            "offset {deleted_offset} in {}/{msg_partition} was deleted (LSO >= {msg_offset})",
            msg_topic.as_ref()
        );
        let error =
            KafkaLoaderError::OffsetDeleted(msg_topic, msg_partition, deleted_offset, msg_offset);
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }

    // Check if this offset has active requests
    let Some(Responses { permit, senders }) = partition_requests.remove(&msg_offset) else {
        // No active requests for this offset - discard without decoding
        debug!(
            "discarding intermediate message: {}/{}/{}",
            msg_topic.as_ref(),
            msg_partition,
            msg_offset
        );
        return;
    };

    // This offset has active requests - decode and fulfill them
    debug!(
        "fulfilling {} active requests: {}/{msg_partition}:{msg_offset}",
        senders.len(),
        msg_topic.as_ref(),
    );

    // Decode using the permit
    let consumer_message = decode_message(
        &message,
        permit,
        propagator,
        #[cfg(not(target_arch = "arm"))]
        buffers,
    );

    // Send to all requesters
    if let Some(msg) = consumer_message {
        for sender in senders {
            let _ = sender.send(Ok(msg.clone()));
        }
    } else {
        // Decoding failed - send error to all
        error!(
            "failed to decode message: {}/{msg_partition}/{msg_offset}",
            msg_topic.as_ref(),
        );
        let error = KafkaLoaderError::DecodeError(msg_topic, msg_partition, msg_offset);
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }

    // Clean up empty partition entries and unassign
    if partition_requests.is_empty() {
        active.remove(&(msg_topic, msg_partition));

        if let Err(error) = unassign_partition(consumer, msg_topic, msg_partition) {
            warn!(
                "failed to unassign partition {}/{msg_partition}: {error:#}",
                msg_topic.as_ref(),
            );
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
        response: Response { sender, permit },
    } = request;

    debug!("Handling request for {topic}/{partition}:{offset}");

    if let Err(error) = assign_if_needed(active, consumer, topic, partition, offset) {
        error!("Failed to assign {topic}/{partition}:{offset}: {error:#}");
        let _ = sender.send(Err(KafkaLoaderError::Kafka(error)));
        return;
    }

    let partition_requests = active.entry((topic, partition)).or_default();

    match partition_requests.entry(offset) {
        Entry::Vacant(entry) => {
            // First request for this offset - use the permit
            let mut senders = SmallVec::new();
            senders.push(sender);
            entry.insert(Responses { permit, senders });
        }
        Entry::Occupied(mut entry) => {
            // Subsequent request - just add sender, permit drops
            entry.get_mut().senders.push(sender);
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

        let should_seek = match current_position {
            Some(pos) if pos + discard_threshold >= min_offset && pos <= min_offset => false,
            Some(pos) if pos > min_offset => true,
            Some(_) | None => true,
        };

        debug!(
            "Seek decision for {}/{}: min_offset={}, current_pos={:?}, should_seek={}",
            AsRef::<str>::as_ref(topic),
            partition,
            min_offset,
            current_position,
            should_seek
        );

        if should_seek {
            debug!(
                "Adding to seek list: {}/{}:{}",
                AsRef::<str>::as_ref(topic),
                partition,
                min_offset
            );
            seek_list.add_partition_offset(
                topic.as_ref(),
                *partition,
                rdkafka::Offset::Offset(min_offset),
            )?;
        }
    }

    if seek_list.count() > 0 {
        debug!("Executing seek for {} partitions", seek_list.count());
        let result = consumer.seek_partitions(seek_list, Timeout::After(seek_timeout))?;

        // Check for seek failures (network errors, timeouts, etc.)
        for elem in result.elements() {
            if let Err(e) = elem.error() {
                debug!(
                    "Seek failed for {}/{}:{:?} - {}",
                    elem.topic(),
                    elem.partition(),
                    elem.offset(),
                    e
                );
                return Err(e);
            }
            debug!(
                "Seek succeeded for {}/{}:{:?}",
                elem.topic(),
                elem.partition(),
                elem.offset()
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
        return Ok(());
    }

    // Assign partition at requested offset
    // Experiments show assign() with deleted offset auto-resets
    // to LSO cleanly (no erroneous state). Lazy validation in process_poll_result
    // will detect when received offset != requested offset.
    let mut to_assign = TopicPartitionList::new();
    to_assign.add_partition_offset(topic.as_ref(), partition, rdkafka::Offset::Offset(offset))?;
    consumer.assign(&to_assign)?;

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
    debug!("unassigned partition: {}/{}", topic.as_ref(), partition);
    Ok(())
}

#[derive(Clone, Debug, Error)]
pub enum KafkaLoaderError {
    #[error("Message {0}/{1}:{2} not found")]
    NotFound(Topic, Partition, Offset),

    #[error("Failed to decode message {0}/{1}:{2}")]
    DecodeError(Topic, Partition, Offset),

    #[error("Loader has shut down")]
    LoaderShutdown,

    #[error(
        "Offset {2} has been deleted from partition {0}/{1} (log start offset: {3}). The \
         requested message no longer exists due to retention or compaction."
    )]
    OffsetDeleted(Topic, Partition, Offset, Offset),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Failed to create Kafka consumer: {0}")]
    ConsumerCreation(KafkaError),

    #[error("Kafka error: {0:#}")]
    Kafka(KafkaError),
}

impl ClassifyError for KafkaLoaderError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal errors - system cannot operate
            Self::LoaderShutdown | Self::Configuration(_) | Self::ConsumerCreation(_) => {
                ErrorCategory::Terminal
            }

            // Classify Kafka operation errors as transient or permanent
            Self::Kafka(kafka_error) => classify_kafka_error(kafka_error),

            // Permanent errors - data issues that won't resolve
            Self::NotFound(..) | Self::DecodeError(..) | Self::OffsetDeleted(..) => {
                ErrorCategory::Permanent
            }
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
        | KafkaError::SetPartitionOffset(code) => classify_rdkafka_error_code(*code),

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
fn classify_rdkafka_error_code(code: RDKafkaErrorCode) -> ErrorCategory {
    match code {
        // Permanent authorization and configuration errors
        RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
        | RDKafkaErrorCode::InvalidConfig
        | RDKafkaErrorCode::UnknownTopicOrPartition
        | RDKafkaErrorCode::InvalidRequest
        | RDKafkaErrorCode::MessageSizeTooLarge
        | RDKafkaErrorCode::UnsupportedVersion
        | RDKafkaErrorCode::InvalidRequiredAcks
        | RDKafkaErrorCode::IllegalGeneration
        | RDKafkaErrorCode::InconsistentGroupProtocol
        | RDKafkaErrorCode::InvalidGroupId
        | RDKafkaErrorCode::UnknownMemberId
        | RDKafkaErrorCode::InvalidSessionTimeout
        | RDKafkaErrorCode::InvalidCommitOffsetSize
        | RDKafkaErrorCode::OffsetMetadataTooLarge
        | RDKafkaErrorCode::UnsupportedForMessageFormat
        | RDKafkaErrorCode::PolicyViolation
        | RDKafkaErrorCode::DelegationTokenAuthDisabled
        | RDKafkaErrorCode::DelegationTokenNotFound
        | RDKafkaErrorCode::DelegationTokenOwnerMismatch
        | RDKafkaErrorCode::DelegationTokenRequestNotAllowed
        | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
        | RDKafkaErrorCode::DelegationTokenExpired
        | RDKafkaErrorCode::InvalidPrincipalType => ErrorCategory::Permanent,

        // Transient network and availability errors
        RDKafkaErrorCode::NetworkException
        | RDKafkaErrorCode::RequestTimedOut
        | RDKafkaErrorCode::BrokerNotAvailable
        | RDKafkaErrorCode::LeaderNotAvailable
        | RDKafkaErrorCode::NotEnoughReplicas
        | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
        | RDKafkaErrorCode::NotCoordinator
        | RDKafkaErrorCode::NotController
        | RDKafkaErrorCode::ReplicaNotAvailable
        | _ => ErrorCategory::Transient,
    }
}
