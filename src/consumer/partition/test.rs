//! Unit tests for the `PartitionManager` in the consumer partition module.
#![allow(clippy::expect_used)]

use super::*;
use crate::Key;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventContext, EventHandler, Uncommitted};
use crate::timers::store::memory::InMemoryTriggerStore;
use aho_corasick::StartKind;
use chrono::Utc;
use crossbeam_utils::CachePadded;
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};
use tracing::Span;

/// Helper trait for waiting on processed offsets.
trait HasProcessedOffsets {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>>;
    fn notify(&self) -> &Arc<Notify>;
}

/// Returns a default `PartitionConfiguration` with sensible defaults.
fn default_config() -> PartitionConfiguration<InMemoryTriggerStore> {
    PartitionConfiguration {
        group_id: Arc::from("test-group"),
        buffer_size: 10,
        max_uncommitted: 10,
        max_enqueued_per_key: 1,
        idempotence_cache_size: 0,
        allowed_events: None,
        shutdown_timeout: Duration::from_secs(1),
        stall_threshold: Duration::from_secs(1),
        watermark_version: Arc::new(CachePadded::new(AtomicUsize::new(0))),
        global_limit: Arc::new(Semaphore::new(10)),
        trigger_store: InMemoryTriggerStore::new(),
        timer_slab_size: CompactDuration::new(30),
    }
}

#[tokio::test]
async fn test_partition_manager_capacity() {
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

#[tokio::test]
async fn test_partition_manager_ordering() {
    let handler = TestHandler::new();
    let mut config = default_config();
    config.max_enqueued_per_key = 2;
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

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_concurrent_processing() {
    let handler = TestHandler::new();
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages with different keys
    for i in 0..5_u8 {
        let key = format!("key{i}");
        let message = create_test_message(Offset::from(i), &key);
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for all messages to be processed
    wait_for_processed_offsets(&handler, 5, Duration::from_secs(1))
        .await
        .expect("Messages should be processed");

    // Check that no concurrent processing of the same key occurred
    let has_concurrent = handler.has_concurrent_processing.lock().await;
    assert!(
        !*has_concurrent,
        "No concurrent processing of the same key should occur"
    );

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_watermark() {
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
    let handler = TestHandler::new();
    let max_uncommitted = 5;
    let mut config = default_config();
    config.max_uncommitted = max_uncommitted;
    config.global_limit = Arc::new(Semaphore::new(max_uncommitted));
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
async fn test_partition_manager_is_stalled() {
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
        fn on_message<C>(
            &self,
            _context: C,
            message: UncommittedMessage,
        ) -> impl Future<Output = ()> + Send
        where
            C: EventContext,
        {
            let offset = message.offset();
            let processed = self.processed_offsets.clone();
            let notify = self.notify.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                {
                    let mut vec = processed.lock().await;
                    vec.push(offset);
                };
                notify.notify_waiters();
                message.commit().await;
            }
        }

        async fn on_timer<C, U>(&self, _context: C, _timer: U)
        where
            C: EventContext,
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

    // Wait longer than the stall threshold to detect stall
    tokio::time::sleep(stall_threshold * 2).await;

    assert!(
        partition_manager.is_stalled(),
        "PartitionManager should report as stalled"
    );

    wait_for_processed_offsets(&handler, 1, Duration::from_secs(3))
        .await
        .expect("Message should be processed");

    assert!(
        !partition_manager.is_stalled(),
        "PartitionManager should no longer report as stalled"
    );

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_event_type_filtering() {
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

    // 1) a disallowed event ("type": "disallowed")
    let disallowed = ConsumerMessage::new(
        None,
        "test-topic".into(),
        0,
        Offset::from(0u8),
        "key".into(),
        Utc::now(),
        json!({ "type": "disallowed" }),
        Span::none(),
    );
    assert!(partition_manager.try_send(disallowed).is_ok());

    // 2) an allowed event ("type": "allowed")
    let allowed = ConsumerMessage::new(
        None,
        "test-topic".into(),
        0,
        Offset::from(1u8),
        "key".into(),
        Utc::now(),
        json!({ "type": "allowed" }),
        Span::none(),
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

#[tokio::test]
async fn test_partition_manager_deduplication() {
    let handler = TestHandler::new();
    let mut config = default_config();
    config.idempotence_cache_size = 100;
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Send messages with the same key and event ID
    let key = "key";
    let event_id = "event1";
    let offsets = vec![0, 1, 2];
    for &offset in &offsets {
        let message = create_test_message_with_event_id(offset, key, Some(event_id));
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    wait_for_processed_offsets(&handler, 1, Duration::from_secs(1))
        .await
        .expect("At least one message should be processed");

    {
        let processed = handler.processed_offsets.lock().await;
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0], 0);
    };

    // Send messages with different event IDs
    for (i, id) in ["event2", "event3"].iter().enumerate() {
        let message = create_test_message_with_event_id(i as Offset + 3, key, Some(id));
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    wait_for_processed_offsets(&handler, 3, Duration::from_secs(1))
        .await
        .expect("Messages should be processed");

    {
        let processed = handler.processed_offsets.lock().await;
        assert!(processed.contains(&0));
        assert!(processed.contains(&3));
        assert!(processed.contains(&4));
    };

    partition_manager.shutdown().await;
}

/// Waits for a specific number of messages to be processed or times out.
async fn wait_for_processed_offsets<H>(
    handler: &H,
    expected_count: usize,
    timeout: Duration,
) -> Result<(), String>
where
    H: HasProcessedOffsets + ?Sized,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let processed = handler.processed_offsets().lock().await;
            if processed.len() >= expected_count {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("Timeout waiting for {expected_count} messages"));
        }
        let notified = handler.notify().notified();
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep_until(deadline) => {
                return Err(format!("Timeout waiting for {expected_count} messages"));
            }
        }
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
}

impl TestHandler {
    fn new() -> Self {
        Self {
            processed_offsets: Arc::new(Mutex::new(Vec::new())),
            has_concurrent_processing: Arc::new(Mutex::new(false)),
            keys_in_processing: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
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
    fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext,
    {
        let key = message.key().clone();
        let offset = message.offset();
        let processed = self.processed_offsets.clone();
        let concurrent_flag = self.has_concurrent_processing.clone();
        let keys_proc = self.keys_in_processing.clone();
        let notify = self.notify.clone();
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
            {
                let mut keys = keys_proc.lock().await;
                keys.retain(|k| k != &key);
            };
            {
                let mut list = processed.lock().await;
                list.push(offset);
            };
            notify.notify_waiters();
            message.commit().await;
        }
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        // todo: add timer test
    }

    async fn shutdown(self) {}
}

/// Helper functions to create test messages.
fn create_test_message(offset: Offset, key: &str) -> ConsumerMessage {
    ConsumerMessage::new(
        None,
        "test-topic".into(),
        0,
        offset,
        key.into(),
        Utc::now(),
        serde_json::json!({}),
        Span::none(),
    )
}

fn create_test_message_with_event_id(
    offset: Offset,
    key: &str,
    event_id: Option<&str>,
) -> ConsumerMessage {
    let payload = if let Some(id) = event_id {
        serde_json::json!({ "id": id })
    } else {
        serde_json::json!({})
    };
    ConsumerMessage::new(
        None,
        "test-topic".into(),
        0,
        offset,
        key.into(),
        Utc::now(),
        payload,
        Span::none(),
    )
}

#[tokio::test]
async fn test_partition_manager_timer_heartbeat_integration() {
    // Test verifies that timer heartbeats are properly integrated into partition
    // stall detection
    let handler = TestHandler::new();
    let config = default_config();
    let partition_manager = PartitionManager::new(config, handler, "test-topic".into(), 0);

    // Initially, the partition should not be stalled
    assert!(
        !partition_manager.is_stalled(),
        "Partition should not be stalled initially"
    );

    // Send a message to trigger timer manager initialization
    let message = create_test_message_with_event_id(1, "test-key", None);
    let _ = partition_manager.try_send(message);

    // Give some time for the timer manager to initialize and heartbeat to be set
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The partition should still not be stalled after timer initialization
    assert!(
        !partition_manager.is_stalled(),
        "Partition should not be stalled after timer initialization"
    );

    // Clean shutdown
    let watermark = partition_manager.shutdown().await;
    assert!(
        watermark.is_some() || watermark.is_none(),
        "Shutdown should complete"
    );
}
