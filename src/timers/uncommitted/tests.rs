use super::*;
use crate::consumer::Uncommitted;
use crate::timers::manager::TimerManager;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::test_support::{create_test_trigger, setup_timer_manager};
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt, pin_mut};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time::{self, advance};
use tracing::dispatcher::set_default;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

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

type TestStore = TableAdapter<InMemoryTriggerStore>;

/// Schedules a 1s trigger for `key`, advances past it, and pops the
/// resulting pending timer from the stream.
async fn schedule_and_pop(
    manager: &TimerManager<TestStore>,
    stream: &mut (impl Stream<Item = PendingTimer<TestStore>> + Unpin),
    key: &str,
) -> Result<(Trigger, PendingTimer<TestStore>)> {
    let trigger = create_test_trigger(key, 1, TimerType::Application)?;
    manager.schedule_trigger(trigger.clone()).await?;

    advance(Duration::from_secs(2)).await;
    task::yield_now().await;

    let pending_timer = stream
        .next()
        .await
        .ok_or_else(|| eyre!("Expected a pending timer"))?;
    Ok((trigger, pending_timer))
}

#[tokio::test]
async fn test_pending_timer_fire_consumes() -> Result<()> {
    time::pause();

    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let (trigger, pending_timer) = schedule_and_pop(&manager, &mut stream, "fire-test").await?;

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
    let (trigger, pending_timer) =
        schedule_and_pop(&manager, &mut stream, "cancelled-fire-test").await?;

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
    let (trigger, pending_timer) = schedule_and_pop(&manager, &mut stream, "commit-test").await?;
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
    let (trigger, pending_timer) = schedule_and_pop(&manager, &mut stream, "abort-test").await?;
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
