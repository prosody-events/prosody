//! Unit tests for the `PartitionManager` in the consumer partition module.
//!
//! This module contains tests to verify the correct functioning of the
//! `PartitionManager`. It covers various aspects, such as message queuing,
//! concurrency, ordering, watermarking, stalling, and deduplication.

#![allow(clippy::expect_used)]

use super::*;
use crate::Key;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::{EventHandler, Keyed, MessageContext};
use chrono::Utc;
use crossbeam_utils::CachePadded;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tracing::Span;

/// This trait is used to indicate that a handler has processed offsets
/// and provides a notification mechanism. It is primarily utilized in test
/// handlers to facilitate tracking of processed offsets and sending
/// notifications.
trait HasProcessedOffsets {
    /// Returns a shared reference to the processed offsets.
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>>;

    /// Returns a shared reference to the notify handle.
    fn notify(&self) -> &Arc<Notify>;
}

/// Tests the `PartitionManager`'s ability to handle message queue capacity.
/// Ensures that the `PartitionManager` respects the buffer size limit and
/// rejects additional messages when the queue is full.
#[tokio::test]
async fn test_partition_manager_capacity() {
    let handler = TestHandler::new();
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let buffer_size = 5_usize;
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        buffer_size,
        10, // max_uncommitted
        1,  // max_enqueued_per_key
        0,  // idempotence_cache_size
        Duration::from_secs(1),
        watermark_version,
    );

    // Send messages up to buffer capacity
    for i in 0..buffer_size {
        let message = create_test_message(i as Offset, "key");
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Send one more message; it should be rejected because the buffer is full
    let message = create_test_message(buffer_size as Offset, "key");
    assert!(
        partition_manager.try_send(message).is_err(),
        "Message send should fail when buffer is full"
    );

    partition_manager.shutdown().await;
}

/// Tests that the `PartitionManager` preserves message ordering for messages
/// with the same key. Ensures each key maintains its own message order.
#[tokio::test]
async fn test_partition_manager_ordering() {
    let handler = TestHandler::new();
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10, // buffer_size
        10, // max_uncommitted
        2,  // max_enqueued_per_key
        0,  // idempotence_cache_size
        Duration::from_secs(1),
        watermark_version,
    );

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

/// Tests concurrent processing across different keys in the `PartitionManager`.
/// Ensures that messages with different keys can be processed simultaneously
/// without interference.
#[tokio::test]
async fn test_partition_manager_concurrent_processing() {
    let handler = TestHandler::new();
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10, // buffer_size
        10, // max_uncommitted
        1,  // max_enqueued_per_key
        0,  // idempotence_cache_size
        Duration::from_secs(1),
        watermark_version,
    );

    // Send messages with different keys
    for i in 0_usize..5 {
        let key = format!("key{i}");
        let message = create_test_message(i as Offset, &key);
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
    let has_concurrent_processing = handler.has_concurrent_processing.lock().await;
    assert!(
        !*has_concurrent_processing,
        "No concurrent processing of the same key should occur"
    );

    partition_manager.shutdown().await;
}

/// Tests that the watermark reflects the highest contiguous offset processed.
/// Verifies the `PartitionManager`'s ability to track message offset
/// advancement correctly.
#[tokio::test]
async fn test_partition_manager_watermark() {
    let handler = TestHandler::new();
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10, // buffer_size
        10, // max_uncommitted
        1,  // max_enqueued_per_key
        0,  // idempotence_cache_size
        Duration::from_secs(1),
        watermark_version.clone(),
    );

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

/// Tests the handling of max uncommitted messages in `PartitionManager`.
/// Ensures that the backpressure is correctly applied when the max
/// uncommitted message limit is reached.
#[tokio::test]
async fn test_partition_manager_max_uncommitted() {
    let handler = TestHandler::new();
    let max_uncommitted = 5;
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10, // buffer_size
        max_uncommitted,
        1, // max_enqueued_per_key
        0, // idempotence_cache_size
        Duration::from_secs(1),
        watermark_version,
    );

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

/// Tests the detection of stalled message processing.
/// Simulates a delay in processing to trigger the stall detection mechanism.
#[tokio::test]
async fn test_partition_manager_is_stalled() {
    // Handler that introduces a delay to simulate a stall
    #[derive(Clone)]
    struct StallTestHandler {
        processed_offsets: Arc<Mutex<Vec<Offset>>>,
        notify: Arc<Notify>,
    }

    impl StallTestHandler {
        /// Creates a new `StallTestHandler`.
        fn new() -> Self {
            Self {
                processed_offsets: Arc::new(Mutex::new(Vec::new())),
                notify: Arc::new(Notify::new()),
            }
        }
    }

    impl EventHandler for StallTestHandler {
        fn on_message(
            &self,
            _context: MessageContext,
            message: UncommittedMessage,
        ) -> impl Future<Output = ()> + Send {
            let offset = message.offset();
            let processed_offsets = Arc::clone(&self.processed_offsets);
            let notify = Arc::clone(&self.notify);

            async move {
                // Simulate processing delay longer than stall threshold
                tokio::time::sleep(Duration::from_secs(2)).await;
                {
                    let mut offsets = processed_offsets.lock().await;
                    offsets.push(offset);
                };
                notify.notify_waiters();
                message.commit();
            }
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
    let stall_threshold = Duration::from_millis(100);
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10,              // buffer_size
        10,              // max_uncommitted
        1,               // max_enqueued_per_key
        0,               // idempotence_cache_size
        stall_threshold, // stall_threshold
        watermark_version,
    );

    // Send a message that is delayed in processing
    let message = create_test_message(0, "key");
    assert!(
        partition_manager.try_send(message).is_ok(),
        "Message send should succeed"
    );

    // Wait longer than the stall threshold to detect stall
    tokio::time::sleep(stall_threshold * 2).await;

    // Verify that `is_stalled` reports true
    assert!(
        partition_manager.is_stalled(),
        "PartitionManager should report as stalled"
    );

    // Wait for the message to be processed
    wait_for_processed_offsets(&handler, 1, Duration::from_secs(3))
        .await
        .expect("Message should be processed");

    // After processing is complete, ensure `is_stalled` is false
    assert!(
        !partition_manager.is_stalled(),
        "PartitionManager should no longer report as stalled"
    );

    partition_manager.shutdown().await;
}

/// Tests that messages with the same key and event ID are deduplicated.
/// Avoids reprocessing of identical messages, leveraging the idempotence cache.
#[tokio::test]
async fn test_partition_manager_deduplication() {
    let handler = TestHandler::new();
    let watermark_version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    let idempotence_cache_size = 100; // Enable deduplication cache
    let partition_manager = PartitionManager::new(
        "test-topic".into(),
        0,
        handler.clone(),
        10, // buffer_size
        10, // max_uncommitted
        1,  // max_enqueued_per_key
        idempotence_cache_size,
        Duration::from_secs(1),
        watermark_version,
    );

    // Send messages with the same key and event ID
    let key = "key";
    let event_id = "event1"; // Same event ID for deduplication
    let offsets = vec![0, 1, 2];
    for &offset in &offsets {
        let message = create_test_message_with_event_id(offset, key, Some(event_id));
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Wait until one message is processed
    wait_for_processed_offsets(&handler, 1, Duration::from_secs(1))
        .await
        .expect("At least one message should be processed");

    {
        // Verify only one message was processed
        let processed_offsets = handler.processed_offsets.lock().await;
        assert_eq!(
            processed_offsets.len(),
            1,
            "Only one message should be processed due to deduplication"
        );
        assert_eq!(processed_offsets[0], 0, "Processed offset should be 0");
    };

    // Send messages with different event IDs
    let event_ids = ["event2", "event3"];
    for (i, &event_id) in event_ids.iter().enumerate() {
        let offset = i as Offset + 3;
        let message = create_test_message_with_event_id(offset, key, Some(event_id));
        assert!(
            partition_manager.try_send(message).is_ok(),
            "Message send should succeed"
        );
    }

    // Wait for three messages to be processed in total
    wait_for_processed_offsets(&handler, 3, Duration::from_secs(1))
        .await
        .expect("Messages should be processed");

    {
        // Verify messages with different event IDs are processed
        let processed_offsets = handler.processed_offsets.lock().await;
        assert_eq!(
            processed_offsets.len(),
            3,
            "Three messages should be processed in total"
        );
        assert!(
            processed_offsets.contains(&0),
            "Processed offsets should contain 0"
        );
        assert!(
            processed_offsets.contains(&3),
            "Processed offsets should contain 3"
        );
        assert!(
            processed_offsets.contains(&4),
            "Processed offsets should contain 4"
        );
    };

    partition_manager.shutdown().await;
}

/// Waits for a specific number of messages to be processed or times out.
/// This function uses a notification mechanism to wait efficiently.
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
            let processed_offsets = handler.processed_offsets().lock().await;
            if processed_offsets.len() >= expected_count {
                return Ok(());
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "Timeout waiting for {expected_count} messages to be processed.",
            ));
        }

        // Wait for notification or timeout
        let notified = handler.notify().notified();
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep_until(deadline) => {
                return Err(format!(
                    "Timeout waiting for {expected_count} messages to be processed.",
                ));
            }
        }
    }
}

/// A test handler that records processed offsets and detects concurrent
/// processing. Used for testing the `PartitionManager`.
#[derive(Clone)]
struct TestHandler {
    /// Stores the order of processed message offsets.
    processed_offsets: Arc<Mutex<Vec<Offset>>>,
    /// Indicates if concurrent processing of the same key occurred.
    has_concurrent_processing: Arc<Mutex<bool>>,
    /// Tracks keys currently being processed.
    keys_in_processing: Arc<Mutex<Vec<Key>>>,
    /// Notifies when a message has been processed.
    notify: Arc<Notify>,
}

impl TestHandler {
    /// Creates a new `TestHandler`.
    fn new() -> Self {
        Self {
            processed_offsets: Arc::new(Mutex::new(Vec::new())),
            has_concurrent_processing: Arc::new(Mutex::new(false)),
            keys_in_processing: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
        }
    }
}

impl EventHandler for TestHandler {
    fn on_message(
        &self,
        _context: MessageContext,
        message: UncommittedMessage,
    ) -> impl Future<Output = ()> + Send {
        let key = message.key().clone();
        let offset = message.offset();
        let processed_offsets = Arc::clone(&self.processed_offsets);
        let has_concurrent_processing = Arc::clone(&self.has_concurrent_processing);
        let keys_in_processing = Arc::clone(&self.keys_in_processing);
        let notify = Arc::clone(&self.notify);

        async move {
            {
                let mut keys = keys_in_processing.lock().await;
                if keys.contains(&key) {
                    // Concurrent processing of the same key detected
                    let mut flag = has_concurrent_processing.lock().await;
                    *flag = true;
                } else {
                    keys.push(key.clone());
                }
            }

            // Processing simulated here

            {
                let mut keys = keys_in_processing.lock().await;
                keys.retain(|k| k != &key);
            };

            {
                let mut offsets = processed_offsets.lock().await;
                offsets.push(offset);
            };

            // Notify that a message has been processed
            notify.notify_waiters();

            message.commit();
        }
    }

    async fn shutdown(self) {}
}

impl HasProcessedOffsets for TestHandler {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>> {
        &self.processed_offsets
    }

    fn notify(&self) -> &Arc<Notify> {
        &self.notify
    }
}

/// Helper function to create a `ConsumerMessage` for testing purposes.
/// It simplifies the creation of test messages with given offsets and keys.
fn create_test_message(offset: Offset, key: &str) -> ConsumerMessage {
    ConsumerMessage::new(
        "test-topic".into(),
        0,
        offset,
        key.into(),
        Utc::now(),
        serde_json::json!({}),
        Span::none(),
    )
}

/// Helper function to create a `ConsumerMessage` with an event ID for testing
/// deduplication. This allows testing if messages with the same event ID are
/// avoided.
fn create_test_message_with_event_id(
    offset: Offset,
    key: &str,
    event_id: Option<&str>,
) -> ConsumerMessage {
    use serde_json::json;
    let payload = if let Some(eid) = event_id {
        json!({ "id": eid })
    } else {
        json!({})
    };
    ConsumerMessage::new(
        "test-topic".into(),
        0,
        offset,
        key.into(),
        Utc::now(),
        payload,
        Span::none(),
    )
}
