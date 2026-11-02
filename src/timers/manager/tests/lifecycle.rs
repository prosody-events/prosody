//! The lifecycle of one timer, as its owner sees it.

use super::{Store, listed, next_pending, past, seeded_store};
use crate::Key;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, TelemetryEvent, TimerEventType};
use crate::timers::datetime::CompactDateTime;
use crate::timers::test_support::{Memory, Step, after, setup_timer_manager_with_store};
use crate::timers::uncommitted::UncommittedTriggerGuard;
use crate::timers::{PendingTimer, TimerRequest, TimerType, UncommittedTimer};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::{Stream, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use strum::VariantArray;
use tokio::runtime::Builder;
use tokio::sync::broadcast;
use tracing::Span;

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::VARIANTS[usize::from(u8::arbitrary(g)) % Self::VARIANTS.len()]
    }
}

/// After every step, memory lists the timer and `scheduled_times` reports it
/// as the model predicts. A schedule reports a timer that was not scheduled,
/// and an unschedule reports a timer whose delivery does not run. A fire
/// happens only while an attempt waits, and a commit or abort only during a
/// delivery.
#[test]
fn prop_lifecycle_follows_the_model() {
    fn property(steps: Vec<Step>) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(steps))
    }
    QuickCheck::new().quickcheck(property as fn(Vec<Step>) -> Result<()>);
}

async fn run(steps: Vec<Step>) -> Result<()> {
    let (store, now) = seeded_store(true).await?;
    let telemetry = Telemetry::new();
    let mut events = telemetry.subscribe();
    let (stream, manager, _shutdown) =
        setup_timer_manager_with_store(store, ShutdownPhase::default(), &telemetry).await?;
    pin_mut!(stream);
    let key = Key::from("lifecycle");
    let (time, timer_type) = (past(now), TimerType::Application);
    let mut model = None;
    let mut delivery = None;

    for step in steps {
        match (step, model) {
            (Step::Schedule, _) => {
                let request = TimerRequest::new(key.clone(), time, timer_type, Span::current());
                manager.schedule(request).await?;
            }
            (Step::Unschedule, _) => manager.unschedule(&key, time, timer_type).await?,
            (Step::Fire, Some(Memory::Waits)) => {
                delivery = Some(fire(&mut stream, &key, time).await?);
            }
            (Step::Commit | Step::Abort, Some(Memory::Firing { .. })) => {
                let guard = delivery.take().ok_or_eyre("no delivery to settle")?;
                if matches!(step, Step::Commit) {
                    guard.commit().await;
                } else {
                    guard.abort().await;
                }
            }
            _ => continue,
        }
        let reports = match step {
            Step::Schedule => (usize::from(matches!(model, None | Some(Memory::Kept))), 0),
            Step::Unschedule => (
                0,
                usize::from(!matches!(model, Some(Memory::Firing { .. }))),
            ),
            Step::Fire | Step::Commit | Step::Abort => (0, 0),
        };
        ensure!(
            reported(&mut events) == reports,
            "{step:?} in {model:?} did not report {reports:?}"
        );
        model = after(model, step);

        let listed = listed(&manager, &key, time, timer_type).await?;
        ensure!(
            listed == model.and_then(Memory::listed),
            "{step:?} listed {listed:?}, not {model:?}"
        );
        let entries = manager.snapshot().await?.active;
        ensure!(
            entries == u32::from(model.is_some()),
            "{step:?} left {entries} entries"
        );

        let scheduled = manager.scheduled_times(&key, timer_type).await?;
        let reported = !matches!(model, None | Some(Memory::Firing { again: false }));
        ensure!(
            scheduled == Vec::from_iter(reported.then_some(time)),
            "{step:?} in {model:?} reported {scheduled:?}"
        );
    }

    if let Some(guard) = delivery {
        guard.abort().await;
    }
    Ok(())
}

/// Counts the scheduled and the cancelled events in `events`.
fn reported(events: &mut broadcast::Receiver<TelemetryEvent>) -> (usize, usize) {
    let mut counts = (0, 0);
    while let Ok(event) = events.try_recv() {
        if let Data::Timer(timer) = &*event.data {
            match timer.event_type {
                TimerEventType::Scheduled => counts.0 += 1,
                TimerEventType::Cancelled => counts.1 += 1,
                _ => {}
            }
        }
    }
    counts
}

/// Takes timers from the stream until one starts, and checks that it is the
/// timer at `time`. A timer cancelled after it left the queue does not start.
async fn fire(
    stream: &mut (impl Stream<Item = PendingTimer<Store>> + Unpin),
    key: &Key,
    time: CompactDateTime,
) -> Result<UncommittedTriggerGuard<Store>> {
    loop {
        let pending = next_pending(stream).await?;
        ensure!(
            pending.trigger().key == *key && pending.trigger().time == time,
            "another timer fired"
        );
        if let Some(firing) = pending.fire().await {
            return Ok(firing.into_inner().1);
        }
    }
}
