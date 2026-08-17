//! Unit tests for the `PartitionManager` in the consumer partition module.
#![allow(clippy::expect_used, reason = "Test module uses expect for test setup")]

use super::*;
use crate::Key;
use crate::consumer::message::{
    ConsumerMessage, ConsumerMessageValue, ConsumerRecord, UncommittedMessage,
};
use crate::consumer::{DemandType, EventContext, EventHandler, Keyed, Uncommitted};
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
use runtime::{
    TestHandler, create_test_message, wait_for_partition_stalled, wait_for_processed_offsets,
};
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
        state_provider: memory_state_provider(CollectionDefRegistry::default()),
        timer_slab_size: CompactDuration::new(30),
        timer_semaphores: Arc::new(from_fn(|_| Arc::new(Semaphore::new(10)))),
        telemetry_sender: Telemetry::new().sender(),
        timer_spans: SpanRelation::default(),
        _payload: PhantomData,
    }
}

#[tokio::test]
async fn test_partition_manager_capacity() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    config.buffer_size = 5;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages up to buffer capacity
    for i in 0..5u8 {
        let message = create_test_message(Offset::from(i), "key")?;
        assert!(
            partition_manager
                .try_send_record(ConsumerRecord::Message(message))
                .is_ok(),
            "Message send should succeed"
        );
    }

    // Send one more message; it should be rejected because the buffer is full
    let message = create_test_message(5, "key")?;
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Message(message))
            .is_err(),
        "Message send should fail when buffer is full"
    );

    partition_manager.shutdown().await;
    Ok(())
}

/// Same-key events are strictly serialized, in order: each handler holds its
/// key "in processing" for a real delay, so any second same-key dispatch
/// before the first completes would trip the concurrency flag.
#[tokio::test]
async fn test_partition_manager_ordering() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::with_delay(Duration::from_millis(20));
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages with the same key and increasing offsets
    let offsets = vec![0, 1, 2, 3];
    for &offset in &offsets {
        let message = create_test_message(offset, "key")?;
        assert!(
            partition_manager
                .try_send_record(ConsumerRecord::Message(message))
                .is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for all messages to be processed
    wait_for_processed_offsets(&handler, offsets.len(), Duration::from_secs(1)).await?;

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
    Ok(())
}

#[tokio::test]
async fn test_partition_manager_watermark() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::new();
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send sequential messages
    for i in 0..5 {
        let message = create_test_message(i, "key")?;
        assert!(
            partition_manager
                .try_send_record(ConsumerRecord::Message(message))
                .is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for all messages to be processed
    wait_for_processed_offsets(&handler, 5, Duration::from_secs(1)).await?;

    // Verify that watermark was updated correctly
    let watermark = partition_manager.watermark();
    assert_eq!(watermark, Some(4), "Watermark should be updated to 4");

    partition_manager.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_partition_manager_max_uncommitted() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::new();
    let max_uncommitted = 5;
    let mut config = default_config();
    config.max_uncommitted = max_uncommitted;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send more messages than max_uncommitted
    for i in 0..(max_uncommitted + 5) {
        let message = create_test_message(i as Offset, "key")?;
        assert!(
            partition_manager
                .try_send_record(ConsumerRecord::Message(message))
                .is_ok(),
            "Message send should succeed"
        );
    }

    // Verify that only max_uncommitted messages are processed before backpressure
    wait_for_processed_offsets(&handler, max_uncommitted, Duration::from_secs(1)).await?;

    partition_manager.shutdown().await;
    Ok(())
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

        async fn on_excise<C>(
            &self,
            _context: C,
            message: UncommittedMessage<()>,
            _demand_type: DemandType,
        ) where
            C: EventContext<Payload = Self::Payload>,
        {
            message.commit().await;
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
    let message = create_test_message(0, "key")?;
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Message(message))
            .is_ok(),
        "Message send should succeed"
    );

    wait_for_partition_stalled(&partition_manager, true, Duration::from_secs(2)).await?;

    wait_for_processed_offsets(&handler, 1, Duration::from_secs(3)).await?;

    wait_for_partition_stalled(&partition_manager, false, Duration::from_secs(2)).await?;

    partition_manager.shutdown().await;
    Ok(())
}

mod runtime;
mod unwind;
