//! Shared scaffolding for timer tests: segment/trigger factories and the
//! in-memory [`TimerManager`] harness used across the timer test modules.

use crate::Key;
use crate::Topic;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{
    PendingTimer, TimerManager, TimerManagerConfig, TimerSemaphores, TimerType, Trigger,
};
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt, stream};
use std::array::from_fn;
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};
use tracing::Span;
use uuid::Uuid;

const TEST_TIMER_SEMAPHORE_SIZE: usize = 64;

/// Per-type timer semaphores sized generously for tests.
pub(crate) fn test_semaphores() -> Arc<TimerSemaphores> {
    Arc::new(from_fn(|_| {
        Arc::new(Semaphore::new(TEST_TIMER_SEMAPHORE_SIZE))
    }))
}

/// Fresh V3 [`Segment`] with a random id.
pub(crate) fn test_segment(name: &str, slab_size: impl Into<CompactDuration>) -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        slab_size: slab_size.into(),
        version: SegmentVersion::V3,
    }
}

/// Trigger `seconds_offset` seconds in the future with a fresh random tag
/// (tag-agreement tests rely on tags being distinct per trigger).
pub(crate) fn create_test_trigger(
    key: &str,
    seconds_offset: u32,
    timer_type: TimerType,
) -> Result<Trigger> {
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(seconds_offset))?;

    Ok(Trigger::new(
        Key::from(key),
        time,
        timer_type,
        Span::current(),
    ))
}

/// Sets up a timer manager over an in-memory store.
///
/// Returns `(stream, manager, shutdown_tx)`. The caller holds
/// `shutdown_tx` and can send `ShutdownPhase::Cancelling` to stop the
/// background scheduler actor.
pub(crate) async fn setup_timer_manager() -> Result<(
    impl Stream<Item = PendingTimer<InMemoryTriggerStore>>,
    TimerManager<InMemoryTriggerStore>,
    watch::Sender<ShutdownPhase>,
)> {
    setup_timer_manager_at(ShutdownPhase::default()).await
}

/// Like [`setup_timer_manager`] but seeds the shutdown watch channel at
/// `initial` before the scheduler actor spawns.
///
/// Seeding the phase up front is what makes drain-window tests
/// deterministic: the actor observes `initial` on its very first loop
/// iteration, with no send-then-observe race against the first command.
pub(crate) async fn setup_timer_manager_at(
    initial: ShutdownPhase,
) -> Result<(
    impl Stream<Item = PendingTimer<InMemoryTriggerStore>>,
    TimerManager<InMemoryTriggerStore>,
    watch::Sender<ShutdownPhase>,
)> {
    let store = memory_store(test_segment("test-segment", 300_u32));
    setup_timer_manager_over_at(store, initial).await
}

/// Builds a timer manager over an existing in-memory store.
pub(crate) async fn setup_timer_manager_over(
    store: InMemoryTriggerStore,
) -> Result<(
    impl Stream<Item = PendingTimer<InMemoryTriggerStore>>,
    TimerManager<InMemoryTriggerStore>,
    watch::Sender<ShutdownPhase>,
)> {
    setup_timer_manager_over_at(store, ShutdownPhase::default()).await
}

async fn setup_timer_manager_over_at(
    store: InMemoryTriggerStore,
    initial: ShutdownPhase,
) -> Result<(
    impl Stream<Item = PendingTimer<InMemoryTriggerStore>>,
    TimerManager<InMemoryTriggerStore>,
    watch::Sender<ShutdownPhase>,
)> {
    let (shutdown_tx, shutdown_rx) = watch::channel(initial);
    let telemetry = Telemetry::new();

    let config = TimerManagerConfig {
        store,
        telemetry: telemetry.partition_sender(Topic::from("test"), 0),
        source: Arc::from(""),
    };

    let (stream, manager) = TimerManager::new(
        config,
        HeartbeatRegistry::test(),
        shutdown_rx,
        test_semaphores(),
    )
    .await
    .map_err(|e| eyre!("Failed to create timer manager: {}", e))?;
    let stream = stream::iter(stream.await).flatten();
    Ok((stream, manager, shutdown_tx))
}
