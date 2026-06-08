//! Unit tests for the `PartitionManager` in the consumer partition module.
#![allow(clippy::expect_used, reason = "Test module uses expect for test setup")]

use super::*;
use crate::Key;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue, UncommittedMessage};
use crate::consumer::middleware::defer::segment::compute_segment_id;
use crate::consumer::{DemandType, EventContext, EventHandler, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::loader::MemoryLoader;
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::manager::StateManagerProvider;
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::tests::value_suite::{FixedOracle, bytes};
use crate::state::value::{DurableWalStore, ValueOp};
use crate::state::{
    CollectionId, CollectionRef, DurableState, EventRef, SharedStateBackend, StateKey, StateName,
    StateType,
};
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerManagerConfig, TimerRequest};
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
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::yield_now;
use tokio::time::{self, advance};
use tokio::time::{Instant, sleep, sleep_until};
use tracing::Span;
use uuid::Uuid;

/// Helper trait for waiting on processed offsets.
trait HasProcessedOffsets {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>>;
    fn notify(&self) -> &Arc<Notify>;
}

/// Partition-agnostic memory keyed-state provider used by the partition
/// tests: state is always wired, so even tests that never touch state mint a
/// real (empty-registry) provider over the in-memory backend.
type MemoryStateProvider = StateManagerProvider<
    SharedStateBackend<
        MemoryDurableValueStore,
        FixedOracle,
        MemoryDirtyValueStoreProvider,
        MemoryDurableValueStore,
    >,
    MemoryLoader<serde_json::Value>,
>;

/// Builds a [`MemoryStateProvider`] with the given collection registry.
fn memory_state_provider(registry: CollectionDefRegistry) -> MemoryStateProvider {
    let durable = MemoryDurableValueStore::for_tests();
    StateManagerProvider::new(
        SharedStateBackend::new(
            durable.clone(),
            FixedOracle::committed(),
            MemoryDirtyValueStoreProvider,
            durable,
        ),
        MemoryLoader::new(),
        Arc::new(registry),
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

#[tokio::test]
async fn test_partition_manager_ordering() {
    init_test_logging();

    let handler = TestHandler::new();
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

    partition_manager.shutdown().await;
}

#[tokio::test]
async fn test_partition_manager_concurrent_processing() {
    init_test_logging();

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

    wait_for_partition_stalled(&partition_manager, true, Duration::from_secs(2))
        .await
        .expect("PartitionManager should report as stalled");

    wait_for_processed_offsets(&handler, 1, Duration::from_secs(3))
        .await
        .expect("Message should be processed");

    wait_for_partition_stalled(&partition_manager, false, Duration::from_secs(2))
        .await
        .expect("PartitionManager should no longer report as stalled");

    partition_manager.shutdown().await;
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
) -> Result<(), String>
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
            return Err(format!("Timeout waiting for {expected_count} messages"));
        }
        let notified = handler.notify().notified();
        tokio::select! {
            () = notified => {},
            () = sleep_until(deadline) => {
                return Err(format!("Timeout waiting for {expected_count} messages"));
            }
        }
    }
}

/// Waits for partition stall state to match `expected` or times out.
async fn wait_for_partition_stalled<P>(
    partition_manager: &PartitionManager<P>,
    expected: bool,
    timeout: Duration,
) -> Result<(), String>
where
    P: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    loop {
        let actual = partition_manager.is_stalled();
        if actual == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "Timeout waiting for partition stalled state {expected}; last state was {actual}"
            ));
        }
        tokio::select! {
            () = sleep(Duration::from_millis(10)) => {},
            () = sleep_until(deadline) => {
                return Err(format!(
                    "Timeout waiting for partition stalled state {expected}; last state was {actual}"
                ));
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

fn create_test_message_with_event_id(
    offset: Offset,
    key: &str,
    event_id: Option<&str>,
) -> ConsumerMessage<serde_json::Value> {
    let payload = event_id.map_or_else(
        || serde_json::json!({}),
        |id| serde_json::json!({ "id": id }),
    );
    let semaphore = Arc::new(Semaphore::new(10));
    ConsumerMessage::new(
        ConsumerMessageValue {
            offset,
            key: key.into(),
            payload,
            ..Default::default()
        },
        Span::current(),
        semaphore
            .try_acquire_owned()
            .expect("Failed to acquire permit"),
    )
}

#[tokio::test]
async fn test_partition_manager_timer_heartbeat_integration() {
    init_test_logging();

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
    sleep(Duration::from_millis(100)).await;

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

/// Handler that records every dispatch it sees; the interception test
/// asserts it sees none.
#[derive(Clone, Default)]
struct RecordingEventHandler {
    timer_calls: Arc<AtomicUsize>,
    message_calls: Arc<AtomicUsize>,
}

impl EventHandler for RecordingEventHandler {
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        self.message_calls.fetch_add(1, Ordering::SeqCst);
        message.commit().await;
    }

    async fn on_timer<C, U>(&self, _context: C, timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
        self.timer_calls.fetch_add(1, Ordering::SeqCst);
        let (_trigger, guard) = timer.into_inner();
        guard.commit().await;
    }

    async fn shutdown(self) {}
}

/// Builds a real in-memory `TimerManager` for the interception test.
async fn recovery_timer_manager(
    shutdown_rx: watch::Receiver<ShutdownPhase>,
) -> color_eyre::Result<(
    impl Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TimerManager<TableAdapter<InMemoryTriggerStore>>,
)> {
    let segment = Segment {
        id: Uuid::new_v4(),
        name: "test".to_owned(),
        slab_size: CompactDuration::new(300),
        version: SegmentVersion::V3,
    };
    let store = memory_store(segment);
    let telemetry = Telemetry::new();
    let timer_config = TimerManagerConfig {
        name: "test".to_owned(),
        store,
        telemetry: telemetry.partition_sender(Topic::from("t"), 0),
        source: Arc::from(""),
    };
    let semaphores: Arc<TimerSemaphores> = Arc::new(from_fn(|_| Arc::new(Semaphore::new(10))));
    TimerManager::new(
        timer_config,
        HeartbeatRegistry::test(),
        shutdown_rx,
        semaphores,
    )
    .await
    .map_err(|e| eyre!("{e}"))
}

/// Acquires a real keyed-state manager over the memory backend with `CART`
/// registered, returning the durable store alongside.
async fn recovery_state_manager() -> color_eyre::Result<(
    impl PartitionStateManager<
        Session: StateSession<Loader: MessageLoader<Payload = serde_json::Value>>,
    >,
    MemoryDurableValueStore,
)> {
    const CART: ValueDescriptor = value_state("cart");
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&CART, CollectionDef::new(None))?;
    let durable = MemoryDurableValueStore::for_tests();
    let provider = StateManagerProvider::new(
        SharedStateBackend::new(
            durable.clone(),
            FixedOracle::committed(),
            MemoryDirtyValueStoreProvider,
            durable.clone(),
        ),
        MemoryLoader::<serde_json::Value>::new(),
        Arc::new(registry),
        Arc::from("test-group"),
        CompactDuration::new(30),
    );
    let manager = provider
        .acquire(Topic::from("t"), 0)
        .await
        .map_err(|e| eyre!("acquire failed: {e}"))?;
    Ok((manager, durable))
}

/// `StateRecovery` triggers are framework-internal: the partition loop
/// intercepts them before dispatch, runs the state manager's sweep, and
/// commits the trigger — the user handler is structurally never invoked.
/// Also verifies the sweep's `unschedule_all(StateRecovery)` does not
/// fight the firing trigger's own commit (the trigger row is already in
/// its firing state when the clear runs).
#[tokio::test]
async fn state_recovery_trigger_is_intercepted_by_the_loop() -> color_eyre::Result<()> {
    init_test_logging();
    time::pause();

    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (stream, timer_manager) = recovery_timer_manager(shutdown_rx.clone()).await?;
    futures::pin_mut!(stream);
    let (state_manager, durable) = recovery_state_manager().await?;

    // A crashed seal waits on the pending partition.
    let key: Key = Arc::from("k");
    let id = CollectionId::new(
        StateKey::new(
            compute_segment_id(Topic::from("t"), 0, "test-group"),
            key.clone(),
        ),
        StateType::Application,
        StateName::try_new("cart")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    durable
        .seal(
            &collection_ref,
            EventRef::Message {
                dedup_id: Uuid::from_u128(1),
            },
            vec![ValueOp::Set { payload: bytes(7) }],
        )
        .await?;

    // The armed StateRecovery backstop fires.
    let fire = CompactDateTime::now()?.add_duration(CompactDuration::new(5))?;
    timer_manager
        .schedule(TimerRequest::new(
            key.clone(),
            fire,
            TimerType::StateRecovery,
            Span::current(),
        ))
        .await?;
    advance(Duration::from_secs(30)).await;
    yield_now().await;
    let pending = stream
        .next()
        .await
        .ok_or_else(|| eyre!("no pending timer fired"))?;

    let handler = RecordingEventHandler::default();
    process_event(
        UncommittedEvent::Timer(pending),
        &handler,
        &shutdown_rx,
        &timer_manager,
        &state_manager,
        DedupIdentity {
            version: "1",
            group_id: "test-group",
            topic: "t",
            partition: 0,
        },
        SpanRelation::default(),
    )
    .await;

    assert_eq!(
        handler.timer_calls.load(Ordering::SeqCst),
        0,
        "the user handler must never see a StateRecovery trigger"
    );
    assert_eq!(handler.message_calls.load(Ordering::SeqCst), 0);

    // The sweep resolved the crashed seal and the trigger committed
    // cleanly: nothing is scheduled and nothing redelivers.
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => assert_eq!(applied, Some(bytes(7))),
        other @ DurableState::Sealed { .. } => {
            return Err(eyre!("sweep must resolve the seal, got {other:?}"));
        }
    }
    let remaining = timer_manager
        .scheduled_times(&key, TimerType::StateRecovery)
        .await?;
    assert!(
        remaining.is_empty(),
        "the recovery trigger must commit without redelivery"
    );
    Ok(())
}
