use super::*;
use crate::consumer::{Receipted, Redelivery, Uncommitted};
use crate::related_span;
use crate::test_util::{
    assert_span_relation, capture_events, captured_spans, captured_spans_filtered,
    sampled_remote_context,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::TimerManager;
use crate::timers::slab::Slab;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::test_support::{create_test_trigger, setup_timer_manager};
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use opentelemetry::trace::TraceContextExt as _;
use std::thread;
use std::time::Duration;
use tokio::task;
use tokio::time::{self, advance};
use tracing_subscriber::filter::LevelFilter;

const DROPPED_UNCOMMITTED_WARNING: &str = "timer was dropped without committing or aborting";

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

    let (warnings, _guard) = capture_events(Level::WARN);

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
    let slab = Slab::from_time(manager.test_store().slab_size(), trigger.time);
    assert!(
        manager
            .test_store()
            .get_slab_triggers_all_types(slab.id())
            .try_collect::<Vec<_>>()
            .await?
            .is_empty(),
        "commit without receipt removes the slab row",
    );

    Ok(())
}

/// A receipt removes the key row. Retirement then removes the slab row.
#[tokio::test]
async fn receipt_then_commit_splits_timer_removal() -> Result<()> {
    time::pause();
    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);
    let (trigger, pending) = schedule_and_pop(&manager, &mut stream, "split-commit").await?;
    let firing = pending
        .fire()
        .await
        .ok_or_else(|| eyre!("expected firing timer"))?;
    let (_trigger, mut guard) = firing.into_inner();

    assert_eq!(guard.redelivery().await, Redelivery::Sweeps);
    guard.receipt().await;
    assert!(
        manager
            .test_store()
            .current_tag(&trigger.key, trigger.time, trigger.timer_type)
            .await?
            .is_none()
    );
    let slab = Slab::from_time(manager.test_store().slab_size(), trigger.time);
    assert_eq!(
        manager
            .test_store()
            .get_slab_triggers_all_types(slab.id())
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        1
    );

    guard.commit().await;
    assert!(
        manager
            .test_store()
            .get_slab_triggers_all_types(slab.id())
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    Ok(())
}

/// Defer timers and rescheduled application timers rerun their handlers.
#[tokio::test]
async fn redelivery_posture_follows_type_and_state() -> Result<()> {
    time::pause();
    let (stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    pin_mut!(stream);

    let deferred = create_test_trigger("deferred", 1, TimerType::DeferredMessage)?;
    let firing = schedule_and_fire(&manager, &mut stream, deferred).await?;
    assert_eq!(firing.redelivery().await, Redelivery::Reruns);
    firing.commit().await;

    let application = create_test_trigger("rescheduled", 3, TimerType::Application)?;
    let firing = schedule_and_fire(&manager, &mut stream, application.clone()).await?;
    manager.schedule_trigger(application).await?;
    assert_eq!(firing.redelivery().await, Redelivery::Reruns);
    firing.commit().await;
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

/// A trigger scheduled one second out, mirroring [`schedule_and_pop`]'s timing.
fn sched_time() -> Result<CompactDateTime> {
    Ok(CompactDateTime::now()?.add_duration(CompactDuration::new(1))?)
}

/// Builds a `Trigger::new` whose scheduling span is created under an `OTel`
/// subscriber, so the context it captures is valid and can be asserted against.
fn new_trigger_under_span(key: &str) -> Result<Trigger> {
    let time = sched_time()?;
    let mut built = None;
    let _ = captured_spans(|| {
        let scheduling = related_span!(
            SpanRelation::Child,
            opentelemetry::Context::current(),
            "scheduling"
        );
        built = Some(Trigger::new(
            Key::from(key),
            time,
            TimerType::Application,
            scheduling,
        ));
    });
    built.ok_or_else(|| eyre!("scheduling trigger was not built"))
}

/// Schedules `trigger` and fires it once the paused clock passes its time.
async fn schedule_and_fire(
    manager: &TimerManager<TestStore>,
    stream: &mut (impl Stream<Item = PendingTimer<TestStore>> + Unpin),
    trigger: Trigger,
) -> Result<FiringTimer<TestStore>> {
    manager.schedule_trigger(trigger).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected a pending timer"))?
        .fire()
        .await
        .ok_or_else(|| eyre!("expected fire() to return Some"))
}

/// [`schedule_and_fire`] for a `Trigger::restored` of `timer_type` carrying
/// the fixed sampled remote scheduling context.
async fn fire_restored(
    manager: &TimerManager<TestStore>,
    stream: &mut (impl Stream<Item = PendingTimer<TestStore>> + Unpin),
    timer_type: TimerType,
    key: &str,
) -> Result<FiringTimer<TestStore>> {
    let trigger = Trigger::restored(
        Key::from(key),
        sched_time()?,
        timer_type,
        0,
        sampled_remote_context(),
    );
    schedule_and_fire(manager, stream, trigger).await
}

/// Schedules `trigger`, fires it, then asserts that
/// [`FiringTimer::set_dispatch_span`] connects the `"trigger"` dispatch span to
/// the trigger's scheduling context per `relation`, by span-id equality.
async fn assert_dispatch_targets(
    manager: &TimerManager<TestStore>,
    stream: &mut (impl Stream<Item = PendingTimer<TestStore>> + Unpin),
    relation: SpanRelation,
    trigger: Trigger,
) -> Result<()> {
    let firing = schedule_and_fire(manager, stream, trigger).await?;

    let scheduling = firing.trigger().context().span().span_context().clone();
    assert!(
        scheduling.is_valid(),
        "scheduling context must be valid for a meaningful id assertion"
    );

    let spans = captured_spans(|| {
        firing.set_dispatch_span(relation);
        // Replace the dispatch span so it is dropped — and thus flushed by the
        // exporter — before the capture ends.
        firing.trigger().set_span(Span::none());
    });
    assert_span_relation(&spans, "trigger", relation, &scheduling)?;
    firing.commit().await;
    Ok(())
}

/// The dispatch span binds to the trigger's scheduling context under both span
/// relations and both trigger constructions (`new` from a scheduling span,
/// `restored` from a persisted context).
#[tokio::test]
async fn dispatch_span_targets_scheduling_context() -> Result<()> {
    time::pause();
    let (stream, manager, _shutdown) = setup_timer_manager().await?;
    pin_mut!(stream);

    for relation in [SpanRelation::Child, SpanRelation::FollowsFrom] {
        let restored = Trigger::restored(
            Key::from(format!("restored-{relation:?}").as_str()),
            sched_time()?,
            TimerType::Application,
            0,
            sampled_remote_context(),
        );
        assert_dispatch_targets(&manager, &mut stream, relation, restored).await?;

        let new_trigger = new_trigger_under_span(&format!("new-{relation:?}"))?;
        assert_dispatch_targets(&manager, &mut stream, relation, new_trigger).await?;
    }

    Ok(())
}

/// The dispatch span's level follows the fired timer's type
/// ([`TimerType::is_application`]): an INFO-filtered subscriber exports the
/// `"trigger"` span for an application timer but not for a
/// framework-internal one, which still exports — relation intact — at DEBUG.
#[tokio::test]
async fn dispatch_span_level_follows_timer_type() -> Result<()> {
    time::pause();
    let (stream, manager, _shutdown) = setup_timer_manager().await?;
    pin_mut!(stream);

    let application =
        fire_restored(&manager, &mut stream, TimerType::Application, "level-app").await?;
    let scheduling = application
        .trigger()
        .context()
        .span()
        .span_context()
        .clone();
    let spans = captured_spans_filtered(LevelFilter::INFO, || {
        application.set_dispatch_span(SpanRelation::Child);
        application.trigger().set_span(Span::none());
    });
    assert_span_relation(&spans, "trigger", SpanRelation::Child, &scheduling)?;
    application.commit().await;

    let internal = fire_restored(
        &manager,
        &mut stream,
        TimerType::StateRecovery,
        "level-info",
    )
    .await?;
    let spans = captured_spans_filtered(LevelFilter::INFO, || {
        internal.set_dispatch_span(SpanRelation::Child);
        internal.trigger().set_span(Span::none());
    });
    assert!(
        spans.iter().all(|span| span.name != "trigger"),
        "internal-timer dispatch span must not export at INFO"
    );
    internal.commit().await;

    let internal = fire_restored(
        &manager,
        &mut stream,
        TimerType::StateRecovery,
        "level-debug",
    )
    .await?;
    let scheduling = internal.trigger().context().span().span_context().clone();
    let spans = captured_spans_filtered(LevelFilter::DEBUG, || {
        internal.set_dispatch_span(SpanRelation::Child);
        internal.trigger().set_span(Span::none());
    });
    assert_span_relation(&spans, "trigger", SpanRelation::Child, &scheduling)?;
    internal.commit().await;

    Ok(())
}

/// The scheduling context travels with the trigger, not with a thread-local:
/// dispatching on a fresh thread with no ambient span still parents the
/// dispatch span on the carried scheduling context.
#[tokio::test]
async fn dispatch_span_context_survives_thread_hop() -> Result<()> {
    time::pause();
    let (stream, manager, _shutdown) = setup_timer_manager().await?;
    pin_mut!(stream);

    let trigger = Trigger::restored(
        Key::from("thread-hop"),
        sched_time()?,
        TimerType::Application,
        0,
        sampled_remote_context(),
    );
    manager.schedule_trigger(trigger).await?;
    advance(Duration::from_secs(2)).await;
    task::yield_now().await;
    let firing = stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected a pending timer"))?
        .fire()
        .await
        .ok_or_else(|| eyre!("expected fire() to return Some"))?;

    let scheduling = firing.trigger().context().span().span_context().clone();
    assert!(scheduling.is_valid(), "scheduling context must be valid");

    let (spans, firing) = thread::spawn(move || {
        let spans = captured_spans(|| {
            firing.set_dispatch_span(SpanRelation::Child);
            firing.trigger().set_span(Span::none());
        });
        (spans, firing)
    })
    .join()
    .map_err(|_| eyre!("thread-hop worker panicked"))?;

    assert_span_relation(&spans, "trigger", SpanRelation::Child, &scheduling)?;
    firing.commit().await;
    Ok(())
}
