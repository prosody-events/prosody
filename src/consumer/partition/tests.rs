//! Unit tests for the `PartitionManager` in the consumer partition module.
#![allow(clippy::expect_used, reason = "Test module uses expect for test setup")]

use super::*;
use crate::Key;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue, UncommittedMessage};
use crate::consumer::{DemandType, EventContext, EventHandler, Uncommitted};
use crate::loader::MemoryLoader;
use crate::state::SharedStateBackend;
use crate::state::manager::StateManagerProvider;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::tests::support::FixedOracle;
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::tracing::init_test_logging;
use aho_corasick::StartKind;
use color_eyre::eyre::eyre;
use crossbeam_utils::CachePadded;
use serde_json::json;
use std::array::from_fn;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::Span;

/// Helper trait for waiting on processed offsets.
trait HasProcessedOffsets {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>>;
    fn notify(&self) -> &Arc<Notify>;
}

/// Partition-agnostic memory keyed-state provider used by the partition
/// tests: state is always wired, so even tests that never touch state mint a
/// real (empty-registry) provider over the in-memory backend.
type MemoryStateProvider = StateManagerProvider<
    SharedStateBackend<MemoryCellStore<FixedOracle>, MemoryDescriptorIdentityStore, FixedOracle>,
    MemoryLoader<serde_json::Value>,
>;

/// Builds a [`MemoryStateProvider`] with the given collection registry.
fn memory_state_provider(registry: CollectionDefRegistry) -> MemoryStateProvider {
    let registry = Arc::new(registry);
    StateManagerProvider::new(
        SharedStateBackend::new(
            MemoryCellStore::new(
                MemoryCells::new(),
                FixedOracle::committed(),
                registry.clone(),
            ),
            MemoryDescriptorIdentityStore::new(),
            FixedOracle::committed(),
        ),
        MemoryLoader::new(),
        registry,
        Arc::from("test-group"),
        CompactDuration::new(30),
    )
}

/// Returns a default `PartitionConfiguration` with sensible defaults.
fn default_config()
-> PartitionConfiguration<InMemoryTriggerStoreProvider, MemoryStateProvider, serde_json::Value> {
    PartitionConfiguration {
        group_id: Arc::from("test-group"),
        version: Arc::from("1"),
        buffer_size: 10,
        max_uncommitted: 10,
        allowed_events: None,
        shutdown_timeout: Duration::from_secs(1),
        stall_threshold: Duration::from_secs(1),
        watermark_version: Arc::new(CachePadded::new(AtomicUsize::new(0))),
        trigger_provider: InMemoryTriggerStoreProvider::new(),
        state_provider: memory_state_provider(CollectionDefRegistry::new(None)),
        timer_slab_size: CompactDuration::new(30),
        timer_semaphores: Arc::new(from_fn(|_| Arc::new(Semaphore::new(10)))),
        telemetry_sender: Telemetry::new().sender(),
        timer_spans: SpanRelation::default(),
        _payload: PhantomData,
    }
}

#[tokio::test]
async fn test_partition_manager_capacity() {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    config.buffer_size = 5;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages up to buffer capacity
    for i in 0..5u8 {
        let message = create_test_message(Offset::from(i), "key");
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Send one more message; it should be rejected because the buffer is full
    let message = create_test_message(5, "key");
    assert!(
        partition_manager.try_send(message).is_err(),
        "Message send should fail when buffer is full"
    );

    partition_manager.shutdown().await;
}

/// Same-key events are strictly serialized, in order: each handler holds its
/// key "in processing" for a real delay, so any second same-key dispatch
/// before the first completes would trip the concurrency flag.
#[tokio::test]
async fn test_partition_manager_ordering() {
    init_test_logging();

    let handler = TestHandler::with_delay(Duration::from_millis(20));
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages with the same key and increasing offsets
    let offsets = vec![0, 1, 2, 3];
    for &offset in &offsets {
        let message = create_test_message(offset, "key");
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for all messages to be processed
    wait_for_processed_offsets(&handler, offsets.len(), Duration::from_secs(1))
        .await
        .expect("Messages should be processed");

    // Verify messages were processed in order
    let processed_offsets = handler.processed_offsets.lock().await;
    assert_eq!(
        &*processed_offsets, &offsets,
        "Messages should be processed in order"
    );
    drop(processed_offsets);

    // Verify no two handlers ever ran the same key concurrently
    assert!(
        !*handler.has_concurrent_processing.lock().await,
        "No concurrent processing of the same key should occur"
    );

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_watermark() {
    init_test_logging();

    let handler = TestHandler::new();
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send sequential messages
    for i in 0..5 {
        let message = create_test_message(i, "key");
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for all messages to be processed
    wait_for_processed_offsets(&handler, 5, Duration::from_secs(1))
        .await
        .expect("Messages should be processed");

    // Verify that watermark was updated correctly
    let watermark = partition_manager.watermark();
    assert_eq!(watermark, Some(4), "Watermark should be updated to 4");

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_max_uncommitted() {
    init_test_logging();

    let handler = TestHandler::new();
    let max_uncommitted = 5;
    let mut config = default_config();
    config.max_uncommitted = max_uncommitted;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send more messages than max_uncommitted
    for i in 0..(max_uncommitted + 5) {
        let message = create_test_message(i as Offset, "key");
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Verify that only max_uncommitted messages are processed before backpressure
    wait_for_processed_offsets(&handler, max_uncommitted, Duration::from_secs(1))
        .await
        .expect("Should process up to max_uncommitted messages");

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_is_stalled() -> color_eyre::Result<()> {
    // Handler that introduces a delay to simulate a stall
    #[derive(Clone)]
    struct StallTestHandler {
        processed_offsets: Arc<Mutex<Vec<Offset>>>,
        notify: Arc<Notify>,
    }

    impl StallTestHandler {
        fn new() -> Self {
            Self {
                processed_offsets: Arc::new(Mutex::new(Vec::new())),
                notify: Arc::new(Notify::new()),
            }
        }
    }

    impl EventHandler for StallTestHandler {
        type Payload = serde_json::Value;

        fn on_message<C>(
            &self,
            _context: C,
            message: UncommittedMessage<serde_json::Value>,
            _demand_type: DemandType,
        ) -> impl Future<Output = ()> + Send
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let offset = message.offset();
            let processed = self.processed_offsets.clone();
            let notify = self.notify.clone();
            async move {
                sleep(Duration::from_secs(2)).await;
                {
                    let mut vec = processed.lock().await;
                    vec.push(offset);
                };
                notify.notify_waiters();
                message.commit().await;
            }
        }

        async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
        where
            C: EventContext<Payload = Self::Payload>,
            U: UncommittedTimer,
        {
            // todo: add timer test
        }

        async fn shutdown(self) {}
    }

    impl HasProcessedOffsets for StallTestHandler {
        fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>> {
            &self.processed_offsets
        }

        fn notify(&self) -> &Arc<Notify> {
            &self.notify
        }
    }

    init_test_logging();

    let handler = StallTestHandler::new();
    let mut config = default_config();
    let stall_threshold = Duration::from_millis(100);
    config.stall_threshold = stall_threshold;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send a message that is delayed in processing
    let message = create_test_message(0, "key");
    assert!(
        partition_manager.try_send(message).is_ok(),
        "Message send should succeed"
    );

    wait_for_partition_stalled(&partition_manager, true, Duration::from_secs(2)).await?;

    wait_for_processed_offsets(&handler, 1, Duration::from_secs(3)).await?;

    wait_for_partition_stalled(&partition_manager, false, Duration::from_secs(2)).await?;

    partition_manager.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_partition_manager_event_type_filtering() {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    // Only allow events whose "type" field contains "allowed"
    config.allowed_events = Some(
        AhoCorasick::builder()
            .start_kind(StartKind::Anchored)
            .build(["allowed"])
            .expect("Invalid event pattern"),
    );
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    let test_semaphore = Arc::new(Semaphore::new(10));

    // 1) a disallowed event ("type": "disallowed")
    let disallowed = ConsumerMessage::new(
        ConsumerMessageValue {
            offset: Offset::from(0u8),
            key: "key".into(),
            payload: json!({ "type": "disallowed" }),
            ..Default::default()
        },
        Span::current(),
        test_semaphore
            .clone()
            .try_acquire_owned()
            .expect("Failed to acquire permit"),
    );
    assert!(partition_manager.try_send(disallowed).is_ok());

    // 2) an allowed event ("type": "allowed")
    let allowed = ConsumerMessage::new(
        ConsumerMessageValue {
            offset: Offset::from(1u8),
            key: "key".into(),
            payload: json!({ "type": "allowed" }),
            ..Default::default()
        },
        Span::current(),
        test_semaphore
            .clone()
            .try_acquire_owned()
            .expect("Failed to acquire permit"),
    );
    assert!(partition_manager.try_send(allowed).is_ok());

    // Only the allowed message should make it through
    wait_for_processed_offsets(&handler, 1, Duration::from_secs(1))
        .await
        .expect("Only the allowed message should be processed");

    let processed = handler.processed_offsets.lock().await;
    assert_eq!(
        processed.as_slice(),
        &[Offset::from(1_u8)],
        "Only offset 1 should have been processed"
    );

    partition_manager.shutdown().await;
}

/// Waits for a specific number of messages to be processed or times out.
async fn wait_for_processed_offsets<H>(
    handler: &H,
    expected_count: usize,
    timeout: Duration,
) -> color_eyre::Result<()>
where
    H: HasProcessedOffsets + ?Sized,
{
    let deadline = Instant::now() + timeout;
    loop {
        {
            let processed = handler.processed_offsets().lock().await;
            if processed.len() >= expected_count {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(eyre!("Timeout waiting for {expected_count} messages"));
        }
        let notified = handler.notify().notified();
        tokio::select! {
            () = notified => {},
            () = sleep_until(deadline) => {
                return Err(eyre!("Timeout waiting for {expected_count} messages"));
            }
        }
    }
}

/// Waits for partition stall state to match `expected` or times out.
///
/// Awaits the offset tracker's stall-transition signal
/// (`OffsetTracker::wait_for_stall_state`) rather than polling: the background
/// watermark task flips the offset stall flag at two explicit points (the
/// oldest uncommitted offset exceeding the threshold, and a watermark advance
/// clearing it). The composite `PartitionManager::is_stalled` also folds in
/// heartbeat staleness, but the keyed processing loop beats its heartbeat every
/// `stall_threshold / HEARTBEAT_MARGIN`, so under normal dispatch only the
/// offset half ever transitions — this asserts the composite once the edge
/// fires to confirm it matches.
async fn wait_for_partition_stalled<P>(
    partition_manager: &PartitionManager<P>,
    expected: bool,
    timeout: Duration,
) -> color_eyre::Result<()>
where
    P: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    tokio::select! {
        result = partition_manager.offsets.wait_for_stall_state(expected) => result?,
        () = sleep_until(deadline) => {
            return Err(eyre!(
                "Timeout waiting for partition stalled state {expected}; last state was {}",
                partition_manager.is_stalled()
            ));
        }
    }
    let actual = partition_manager.is_stalled();
    if actual == expected {
        Ok(())
    } else {
        Err(eyre!(
            "partition stalled state {actual} did not match expected {expected} after the offset \
             stall signal fired"
        ))
    }
}

/// A test handler that records processed offsets and detects concurrent
/// processing.
#[derive(Clone)]
struct TestHandler {
    processed_offsets: Arc<Mutex<Vec<Offset>>>,
    has_concurrent_processing: Arc<Mutex<bool>>,
    keys_in_processing: Arc<Mutex<Vec<Key>>>,
    notify: Arc<Notify>,
    delay: Duration,
}

impl TestHandler {
    fn new() -> Self {
        Self::with_delay(Duration::ZERO)
    }

    /// A handler that holds each key "in processing" for `delay` (simulated
    /// processing time), widening the window in which a second same-key
    /// dispatch would overlap and trip the concurrency flag.
    fn with_delay(delay: Duration) -> Self {
        Self {
            processed_offsets: Arc::new(Mutex::new(Vec::new())),
            has_concurrent_processing: Arc::new(Mutex::new(false)),
            keys_in_processing: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            delay,
        }
    }
}

impl HasProcessedOffsets for TestHandler {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>> {
        &self.processed_offsets
    }

    fn notify(&self) -> &Arc<Notify> {
        &self.notify
    }
}

impl EventHandler for TestHandler {
    type Payload = serde_json::Value;

    fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let key = message.key().clone();
        let offset = message.offset();
        let processed = self.processed_offsets.clone();
        let concurrent_flag = self.has_concurrent_processing.clone();
        let keys_proc = self.keys_in_processing.clone();
        let notify = self.notify.clone();
        let delay = self.delay;
        async move {
            {
                let mut keys = keys_proc.lock().await;
                if keys.contains(&key) {
                    let mut flag = concurrent_flag.lock().await;
                    *flag = true;
                } else {
                    keys.push(key.clone());
                }
            }
            if !delay.is_zero() {
                sleep(delay).await;
            }
            {
                let mut list = processed.lock().await;
                list.push(offset);
            };
            {
                let mut keys = keys_proc.lock().await;
                keys.retain(|k| k != &key);
            };
            notify.notify_waiters();
            message.commit().await;
        }
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
        // todo: add timer test
    }

    async fn shutdown(self) {}
}

/// Helper functions to create test messages.
fn create_test_message(offset: Offset, key: &str) -> ConsumerMessage<serde_json::Value> {
    let semaphore = Arc::new(Semaphore::new(10));
    ConsumerMessage::new(
        ConsumerMessageValue {
            offset,
            key: key.into(),
            ..Default::default()
        },
        Span::current(),
        semaphore
            .try_acquire_owned()
            .expect("Failed to acquire permit"),
    )
}

/// Timer and processing heartbeats stay integrated into partition stall
/// detection: a registered-but-never-beaten heartbeat trips stall detection
/// once its last beat is older than the threshold, so remaining un-stalled
/// across a window several thresholds wide proves every registered heartbeat
/// is actively beaten.
#[tokio::test]
async fn test_partition_manager_timer_heartbeat_integration() {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    config.stall_threshold = Duration::from_millis(200);
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Initially, the partition should not be stalled
    assert!(
        !partition_manager.is_stalled(),
        "Partition should not be stalled initially"
    );

    // Send a message to spin up the keyed processing loop and timer manager,
    // registering their heartbeats
    let message = create_test_message(1, "test-key");
    assert!(
        partition_manager.try_send(message).is_ok(),
        "Message send should succeed"
    );
    wait_for_processed_offsets(&handler, 1, Duration::from_secs(1))
        .await
        .expect("Message should be processed");

    // Negative-invariant observation window (~3× the stall threshold): by its
    // end every registered heartbeat is past the threshold unless actively
    // beaten, so this fails if the heartbeats stop being beaten or integrated
    sleep(Duration::from_millis(600)).await;
    assert!(
        !partition_manager.is_stalled(),
        "Partition must not stall while its heartbeats are actively beaten"
    );

    // Shutdown drains the in-flight commit, so the watermark reflects it
    let watermark = partition_manager.shutdown().await;
    assert_eq!(watermark, Some(1), "Shutdown should drain the commit");
}
