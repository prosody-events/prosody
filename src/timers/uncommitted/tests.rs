use super::*;
use crate::Topic;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::TimerSemaphores;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::{TimerManager, TimerManagerConfig};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::{Segment, SegmentVersion};
use color_eyre::eyre::{Result, eyre};
use futures::{StreamExt, pin_mut};
use std::array::from_fn;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};
use tracing::dispatcher::set_default;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const TEST_TIMER_SEMAPHORE_SIZE: usize = 64;
const DROPPED_UNCOMMITTED_WARNING: &str = "timer was dropped without committing or aborting";

#[derive(Clone, Default)]
struct CapturedWarnings(Arc<parking_lot::Mutex<Vec<String>>>);

impl CapturedWarnings {
    fn contains(&self, needle: &str) -> bool {
        self.0.lock().iter().any(|event| event.contains(needle))
    }
}

struct WarningCaptureLayer {
    warnings: CapturedWarnings,
}

impl<S> Layer<S> for WarningCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = WarningVisitor::default();
        event.record(&mut visitor);
        self.warnings.0.lock().push(visitor.output);
    }

    fn on_new_span(&self, _attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {}

    fn on_record(&self, _span: &Id, _values: &Record<'_>, _ctx: Context<'_, S>) {}
}

#[derive(Default)]
struct WarningVisitor {
    output: String,
}

impl Visit for WarningVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        use std::fmt::Write;

        let _ = write!(&mut self.output, "{}={value:?};", field.name());
    }
}

fn test_semaphores() -> Arc<TimerSemaphores> {
    Arc::new(from_fn(|_| {
        Arc::new(Semaphore::new(TEST_TIMER_SEMAPHORE_SIZE))
    }))
}

use tokio::task;
use tokio::time::{self, advance};
use uuid::Uuid;

fn test_segment() -> Segment {
    Segment {
        id: Uuid::new_v4(),
        name: "test-segment".to_owned(),
        slab_size: CompactDuration::new(300),
        version: SegmentVersion::V3,
    }
}

/// Helper function to set up a timer manager for testing.
///
/// Returns `(stream, manager, shutdown_tx)`. The caller holds
/// `shutdown_tx` and can send `ShutdownPhase::Draining` to stop the
/// background scheduler actor.
async fn setup_timer_manager() -> Result<(
    impl futures::Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
    TimerManager<TableAdapter<InMemoryTriggerStore>>,
    watch::Sender<ShutdownPhase>,
)> {
    let store = memory_store(test_segment());
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let telemetry = Telemetry::new();

    let config = TimerManagerConfig {
        name: "test-manager".to_owned(),
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
    Ok((stream, manager, shutdown_tx))
}

/// Helper function to create a test trigger
fn create_test_trigger(key: &str, seconds_offset: u32, timer_type: TimerType) -> Result<Trigger> {
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(seconds_offset))?;

    Ok(Trigger::new(
        Key::from(key),
        time,
        timer_type,
        Span::current(),
    ))
}

#[tokio::test]
async fn test_pending_timer_fire_consumes() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("fire-test", 1, TimerType::Application)?;

    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time to trigger emission
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get the pending timer
    let pending_timer = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;

    // Verify fire() consumes the PendingTimer and returns FiringTimer
    let firing_timer = pending_timer
        .fire()
        .await
        .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

    // Verify the FiringTimer has correct metadata
    assert_eq!(firing_timer.time(), trigger.time);
    assert_eq!(firing_timer.timer_type(), TimerType::Application);
    assert_eq!(firing_timer.key(), &trigger.key);

    // Clean up by committing
    firing_timer.commit().await;

    Ok(())
}

#[tokio::test]
async fn test_cancelled_pending_timer_fire_completes_without_drop_warning() -> Result<()> {
    time::pause();

    let warnings = CapturedWarnings::default();
    let subscriber = tracing_subscriber::registry().with(WarningCaptureLayer {
        warnings: warnings.clone(),
    });
    let dispatcher = tracing::Dispatch::new(subscriber);
    let _guard = set_default(&dispatcher);

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("cancelled-fire-test", 1, TimerType::Application)?;

    manager.schedule_trigger(trigger.clone()).await?;

    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending_timer = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;

    manager
        .unschedule(&trigger.key, trigger.time, trigger.timer_type)
        .await?;

    assert!(
        pending_timer.fire().await.is_none(),
        "cancelled pending timer should not transition to firing"
    );
    assert!(
        !warnings.contains(DROPPED_UNCOMMITTED_WARNING),
        "cancelled pending timer should be marked completed before drop"
    );

    Ok(())
}

#[tokio::test]
async fn test_firing_timer_commit() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("commit-test", 1, TimerType::Application)?;

    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time to trigger emission
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get and fire the timer
    let pending_timer = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;
    let firing_timer = pending_timer
        .fire()
        .await
        .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

    // Commit the timer
    firing_timer.commit().await;

    // Verify the timer was removed from storage
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert!(times.is_empty(), "Timer should be removed after commit");

    Ok(())
}

#[tokio::test]
async fn test_firing_timer_abort() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let trigger = create_test_trigger("abort-test", 1, TimerType::Application)?;

    manager.schedule_trigger(trigger.clone()).await?;

    // Advance time to trigger emission
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    // Get and fire the timer
    let pending_timer = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;
    let firing_timer = pending_timer
        .fire()
        .await
        .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

    // Abort the timer
    firing_timer.abort().await;

    // Verify the timer is still in storage (abort preserves DB state)
    let times = manager
        .scheduled_times(&trigger.key, TimerType::Application)
        .await?;
    assert_eq!(times.len(), 1, "Timer should remain in storage after abort");
    assert!(times.contains(&trigger.time));

    Ok(())
}
