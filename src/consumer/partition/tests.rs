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
        state_provider: memory_state_provider(CollectionDefRegistry::default()),
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

/// Abnormal-exit fencing through [`guarded_dispatch`] — the single
/// panic-unwind catch site, which `process_event` wraps every dispatch in
/// (above `RetryHandler`'s own `EventHandler` impl). On an unwind the catch
/// runs the gate-held terminal transition (close, discard, terminate — no epoch
/// write) then resumes; on a dropped dispatch future the scope's `Drop` flips
/// termination. Either way a handle the dispatch leaked past its attempt is
/// fenced on the op's next effect, with zero orchestration by any caller.
mod unwind {
    use super::*;
    use crate::codec::{JsonCodec, JsonCodecError};
    use crate::consumer::middleware::NextAttempt;
    use crate::consumer::middleware::tests::test_support::MockEventContext;
    use crate::state::access::StateAccessError;
    use crate::state::descriptor::tests::TestSession;
    use crate::state::descriptor::{CellStateError, Registered, ValueHandle, value_state};
    use crate::state::dirty::DirtyStore;
    use crate::state::manager::EventStateScope;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::session::{CellRead, KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::tests::cell_suite::value_cell;
    use crate::state::{EventRef, StateKey, StateName, StateType};
    use crate::timers::duration::CompactDuration;
    use bytes::Bytes;
    use color_eyre::eyre::{Result, bail, eyre};
    use parking_lot::Mutex as SyncMutex;
    use serde_json::{Value, json};
    use tokio::sync::{oneshot, watch};
    use tokio::time::timeout;
    use uuid::Uuid;

    const NAME: &str = "c";
    type Ctx = MockEventContext<Value, TestSession>;
    type Handle = ValueHandle<TestSession, JsonCodec>;

    /// Shared durable + dirty state so the event session and a fresh observer
    /// session read the same overlay — the residue probe.
    struct Fixture {
        cell: MemoryCellStore<FixedOracle>,
        dirty: Arc<DirtyStore>,
        state_key: StateKey,
        registry: Arc<CollectionDefRegistry>,
    }

    impl Fixture {
        fn new() -> Result<Self> {
            let mut registry = CollectionDefRegistry::default();
            registry.register(&value_state::<JsonCodec>(NAME), CollectionDef::new(None))?;
            let registry = Arc::new(registry);
            let cell = MemoryCellStore::new(
                MemoryCells::new(),
                FixedOracle::committed(),
                registry.clone(),
            );
            Ok(Self {
                cell,
                dirty: Arc::new(DirtyStore::new()),
                state_key: StateKey::new(Uuid::from_u128(0xE), Arc::from("user-1")),
                registry,
            })
        }

        /// A fresh session (new event epoch) over the shared durable + dirty
        /// state.
        fn session(&self) -> TestSession {
            let (_s, shutdown_rx) = watch::channel(ShutdownPhase::default());
            let (_c, cancel_rx) = watch::channel(false);
            KeyedStateSession::new(SessionParts {
                cell: self.cell.clone(),
                dirty: self.dirty.clone(),
                oracle: FixedOracle::committed(),
                loader: MemoryLoader::new(),
                registry: self.registry.clone(),
                state_key: self.state_key.clone(),
                event: EventRef::Message {
                    dedup_id: Uuid::new_v4(),
                },
                recovery_delay: CompactDuration::new(30),
                armed: Arc::default(),
                termination: TerminationWatch::new(shutdown_rx, cancel_rx),
                publisher: None,
            })
        }

        /// The buffered dirty bytes of `NAME` observed through a fresh,
        /// non-terminated session over the same overlay — the residue probe.
        async fn residue(&self) -> Result<Option<Bytes>> {
            self.session()
                .get(
                    StateType::Application,
                    &StateName::try_new(NAME)?,
                    &value_cell(),
                )
                .await
                .map_err(|e| eyre!("residue read: {e}"))
        }
    }

    fn handle(context: &Ctx) -> Result<Handle> {
        context
            .state(Registered::new(value_state::<JsonCodec>(NAME)))
            .map_err(|e| eyre!("bind: {e}"))
    }

    fn tag(error: &CellStateError<JsonCodecError>) -> String {
        match error {
            CellStateError::Access(StateAccessError::SessionClosed) => "SessionClosed".into(),
            CellStateError::Access(StateAccessError::Terminated) => "Terminated".into(),
            other => format!("other: {other}"),
        }
    }

    /// Runs `dispatch` through the production catch on a spawned task and
    /// returns `Ok(())` once the resumed panic is observed at the join.
    async fn expect_unwind<F>(scope: EventStateScope<TestSession>, dispatch: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let joined = spawn(async move {
            guarded_dispatch(&scope, dispatch).await;
        })
        .await;
        match joined {
            Err(e) if e.is_panic() => Ok(()),
            other => bail!("guarded_dispatch must resume the unwind, got {other:?}"),
        }
    }

    /// Arm A/C — a handler (or final apply hook) panics with no attempt bump,
    /// so a handle leaked past it keeps a CURRENT pin. After the catch resumes
    /// the panic: a leaked read errors `Terminated`, a leaked `commit()` errors
    /// `SessionClosed` (current pin, gate Closed — Closed is checked before
    /// termination), and the dirty overlay is empty.
    ///
    /// Falsify: drop the `close_gate()` acquire from the catch arm — the gate
    /// stays Open, so the current-pin `commit()` falls through to the
    /// termination check and errors `Terminated`, not `SessionClosed`. Closing
    /// the gate under the panic is the catch's uniquely-pinned contribution
    /// here. The read-`Terminated` and empty-overlay postconditions are *also*
    /// produced by [`EventStateScope`]'s `Drop` re-running `terminate` +
    /// `discard_dirty` (ungated) as the still-live scope unwinds, so deleting
    /// those from the catch alone is masked — Drop's `terminate` is pinned by
    /// [`dropped_dispatch_future_terminates_the_session`]. The catch's
    /// gate-held discard uniquely defeats residue from a mutator admitted
    /// *before* closure (the Arm D no-residue guarantee below), which the
    /// ungated Drop cannot.
    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "the unwind pins drive a deliberate panic through the catch"
    )]
    async fn handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared() -> Result<()> {
        let fx = Fixture::new()?;
        let session = fx.session();
        let scope = EventStateScope::new(session.clone());
        let context = MockEventContext::new().with_session(session);
        // Leak a current-pin handle and buffer a write, both at attempt 1.
        let leaked = handle(&context)?;
        leaked.set(json!("leaked")).await?;

        expect_unwind(scope, async move {
            let _hold = context;
            panic!("handler boom");
        })
        .await?;

        match leaked.get().await {
            Err(CellStateError::Access(StateAccessError::Terminated)) => {}
            other => bail!("leaked read must be Terminated, got {other:?}"),
        }
        match leaked.commit().await {
            Err(e) if tag(&e) == "SessionClosed" => {}
            other => bail!("current-pin leaked commit must be SessionClosed, got {other:?}"),
        }
        assert_eq!(
            fx.residue().await?,
            None,
            "the catch discarded the handler's buffered overlay",
        );
        Ok(())
    }

    /// Arm B — an intermediate `after_abort` panics mid retry loop, AFTER a
    /// `next_attempt` bump, so the leaked handle predates the bump and is
    /// STALE. Its `commit()` then errors `Terminated` (the pin compare fires
    /// before the closed-gate check), distinguishing it from the current-pin
    /// arms. Falsify: swap the pin/closed order in `mutate_permit` → this flips
    /// to `SessionClosed`.
    #[tokio::test]
    #[expect(
        clippy::panic,
        reason = "the unwind pins drive a deliberate panic through the catch"
    )]
    async fn intermediate_bump_makes_leaked_commit_terminated() -> Result<()> {
        let fx = Fixture::new()?;
        let session = fx.session();
        let scope = EventStateScope::new(session.clone());
        let context = MockEventContext::new().with_session(session);
        // Leak a handle at attempt 1 BEFORE the bump.
        let leaked = handle(&context)?;

        expect_unwind(scope, async move {
            // Advance the attempt boundary via the real verb — exactly what
            // retry runs between attempts — then panic mid-loop.
            let context = context.next_attempt().await;
            let _hold = context;
            panic!("intermediate after_abort boom");
        })
        .await?;

        match leaked.commit().await {
            Err(e) if tag(&e) == "Terminated" => Ok(()),
            other => bail!("stale-pin leaked commit must be Terminated, got {other:?}"),
        }
    }

    /// Arm E — the dispatch future is DROPPED mid-flight (task cancellation),
    /// which no catch ever sees. The scope's `Drop` flips termination
    /// synchronously, so a handle leaked past it errors `Terminated`. (No
    /// no-residue claim: the ungated drop cannot revoke an already-admitted
    /// parked mutator — the documented drop-path residual.) Falsify: remove
    /// `terminate()` from `EventStateScope::Drop` → the leaked read returns
    /// `Ok`.
    #[tokio::test]
    async fn dropped_dispatch_future_terminates_the_session() -> Result<()> {
        let fx = Fixture::new()?;
        let session = fx.session();
        let context = MockEventContext::new().with_session(session.clone());
        let leaked = handle(&context)?;

        let (parked_tx, parked_rx) = oneshot::channel::<()>();
        let (_never_tx, never_rx) = oneshot::channel::<()>();
        // A dispatch future owning the scope that parks forever; aborting the
        // task drops the future, running the scope's Drop.
        let task = spawn(async move {
            let scope = EventStateScope::new(session);
            guarded_dispatch(&scope, async move {
                parked_tx.send(()).ok();
                let _ = never_rx.await;
            })
            .await;
        });
        parked_rx
            .await
            .map_err(|_| eyre!("dispatch never parked"))?;
        task.abort();
        let _ = task.await;

        match leaked.get().await {
            Err(CellStateError::Access(StateAccessError::Terminated)) => Ok(()),
            other => bail!("a dropped-future leak must error Terminated, got {other:?}"),
        }
    }

    /// A handler that leaks a bound keyed-state handle into a detached task,
    /// then panics — the same current-pin leak as
    /// [`handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared`], but
    /// exercised through the production `process_event` wiring rather than a
    /// direct [`guarded_dispatch`] call. The leaked task parks until the test
    /// releases it (strictly after the catch has run), then reports its
    /// `commit()` error tag.
    struct PanicLeakHandler {
        /// Fires with the in-attempt `set` outcome the instant before the
        /// panic, so the test knows the message was dispatched (ruling out a
        /// `Draining` drop race) and the leaked handle was admitted while the
        /// attempt was live.
        reached: SyncMutex<Option<oneshot::Sender<Result<(), String>>>>,
        /// Releases the leaked task's `commit()` — the test sends this only
        /// after `shutdown()` has joined the panicked partition task.
        go: SyncMutex<Option<oneshot::Receiver<()>>>,
        /// Reports the leaked `commit()`'s error tag back to the test.
        tag: SyncMutex<Option<oneshot::Sender<String>>>,
    }

    impl EventHandler for PanicLeakHandler {
        type Payload = Value;

        #[expect(
            clippy::panic,
            reason = "the unwind pin drives a deliberate panic through the catch"
        )]
        fn on_message<C>(
            &self,
            context: C,
            _message: UncommittedMessage<Value>,
            _demand_type: DemandType,
        ) -> impl Future<Output = ()> + Send
        where
            C: EventContext<Payload = Self::Payload>,
        {
            let reached = self.reached.lock().take();
            let go = self.go.lock().take();
            let tag_tx = self.tag.lock().take();
            async move {
                let (Some(reached), Some(go), Some(tag_tx)) = (reached, go, tag_tx) else {
                    return;
                };
                let handle = match context.state(Registered::new(value_state::<JsonCodec>(NAME))) {
                    Ok(handle) => handle,
                    Err(e) => {
                        reached.send(Err(format!("bind: {e}"))).ok();
                        return;
                    }
                };
                let set_outcome = handle.set(json!("leaked")).await.map_err(|e| tag(&e));
                // Leak the CURRENT-pin handle into a detached task that outlives
                // the attempt; it commits only once the test releases `go`,
                // which it does strictly after the catch has closed the gate.
                spawn(async move {
                    if go.await.is_err() {
                        return;
                    }
                    let outcome = match handle.commit().await {
                        Ok(_) => "Ok".to_owned(),
                        Err(e) => tag(&e),
                    };
                    tag_tx.send(outcome).ok();
                });
                reached.send(set_outcome).ok();
                panic!("handler boom");
            }
        }

        async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
        where
            C: EventContext<Payload = Self::Payload>,
            U: UncommittedTimer,
        {
        }

        async fn shutdown(self) {}
    }

    /// Abnormal-exit fencing through the PRODUCTION entry — the same
    /// current-pin leak as
    /// [`handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared`], but
    /// driven through `process_event` (which wraps every dispatch in
    /// [`guarded_dispatch`]) instead of calling `guarded_dispatch` directly, so
    /// the zero-orchestration production wiring is what fences the leak. A
    /// handler leaks a live-attempt handle into a detached task and panics;
    /// after `shutdown()` joins the panicked partition task — which completes
    /// only once the catch has run `close_gate` → `resume_unwind` — the leaked
    /// `commit()` errors `SessionClosed`: current pin, closed gate, no epoch
    /// bump.
    ///
    /// Falsify: in `process_event`'s message arm, replace
    /// `guarded_dispatch(&scope, …).await` with `….await` (keeping the trailing
    /// `cloned_context.invalidate();`). With no catch, only
    /// [`EventStateScope`]'s `Drop` runs during the unwind (terminate +
    /// discard, gate left OPEN), so the leaked `commit()` falls through to the
    /// termination check and errors `Terminated`, not `SessionClosed`. This is
    /// the half of the abnormal-exit fencing the direct-`guarded_dispatch` unit
    /// arms above cannot reach; the stale-pin-through-`RetryHandler` half lives
    /// in `retry::tests`.
    #[tokio::test]
    async fn process_event_wires_the_catch_for_a_panicking_handler() -> Result<()> {
        init_test_logging();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (go_tx, go_rx) = oneshot::channel();
        let (tag_tx, tag_rx) = oneshot::channel();
        let handler = PanicLeakHandler {
            reached: SyncMutex::new(Some(reached_tx)),
            go: SyncMutex::new(Some(go_rx)),
            tag: SyncMutex::new(Some(tag_tx)),
        };

        let mut registry = CollectionDefRegistry::default();
        registry
            .register(&value_state::<JsonCodec>(NAME), CollectionDef::new(None))
            .map_err(|e| eyre!("register: {e}"))?;
        let mut config = default_config();
        config.state_provider = memory_state_provider(registry);
        let partition_manager = PartitionManager::new(config, handler, "test-topic".into(), 0);

        partition_manager
            .try_send(create_test_message(0, "key"))
            .map_err(|_| eyre!("message send rejected"))?;

        // The handler ran and buffered a set on the live attempt and is about
        // to panic; awaiting this before shutting down rules out any `Draining`
        // message-drop race. The deadline is only a hang-guard.
        let set_outcome = timeout(Duration::from_secs(5), reached_rx)
            .await
            .map_err(|_| eyre!("handler never reached the leak point"))?
            .map_err(|_| eyre!("reached sender dropped"))?;
        if let Err(t) = set_outcome {
            bail!("in-attempt set must succeed, got {t}");
        }

        // Joins the panicked partition task: `guarded_dispatch`'s catch (close
        // gate, discard, terminate, resume) has fully run once shutdown returns.
        partition_manager.shutdown().await;

        // Release the leaked handle's commit, now strictly after the catch.
        if go_tx.send(()).is_err() {
            bail!("leaked task dropped its release channel before committing");
        }
        let tag = timeout(Duration::from_secs(5), tag_rx)
            .await
            .map_err(|_| eyre!("leaked commit never reported"))?
            .map_err(|_| eyre!("tag sender dropped"))?;
        if tag != "SessionClosed" {
            bail!(
                "leaked current-pin commit through the production catch must be SessionClosed, \
                 got {tag}"
            );
        }
        Ok(())
    }

    // Arm D (the paused-time admitted-mutator FIFO race — a detached `set`
    // parked mid-op while the catch's `close_gate()` waits behind its permit)
    // is not a unit-level pin: forcing the park *between* `mutate_permit` and
    // the buffer write needs store instrumentation the memory backend does not
    // expose. Its no-residue outcome is covered by
    // `handler_panic_current_pin_leaks_are_fenced_and_overlay_cleared` (the
    // catch discards the handler's buffered write); the FIFO ordering itself —
    // `close_gate().await` strictly before the discard in `guarded_dispatch` —
    // is a code invariant backed by the gate-serialization pins in
    // `state::tests::gate_suite`.
}
