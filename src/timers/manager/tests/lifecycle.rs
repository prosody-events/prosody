//! The lifecycle of one timer, as its owner sees it.

use super::{Store, listed, next_pending, past, seeded_store};
use crate::Key;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::datetime::CompactDateTime;
use crate::timers::test_support::{Memory, setup_timer_manager_with_store};
use crate::timers::uncommitted::UncommittedTriggerGuard;
use crate::timers::{PendingTimer, TimerRequest, TimerType, UncommittedTimer};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::{Stream, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use strum::VariantArray;
use tokio::runtime::Builder;
use tracing::Span;

/// One step that a timer's owner can take.
#[derive(Clone, Copy, Debug, VariantArray)]
enum Step {
    Schedule,
    Unschedule,
    /// Takes the due timer from the stream and starts its delivery.
    Fire,
    Commit,
    Abort,
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::VARIANTS[usize::from(u8::arbitrary(g)) % Self::VARIANTS.len()]
    }
}

/// After every step, memory lists the timer and `scheduled_times` reports it
/// as the model predicts. A fire happens only while an attempt waits, and a
/// commit or abort only during a delivery.
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
    let (stream, manager, _shutdown) =
        setup_timer_manager_with_store(store, ShutdownPhase::default()).await?;
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
        model = next(model, step);

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

/// The model of one timer's memory, as its owner sees it.
fn next(memory: Option<Memory>, step: Step) -> Option<Memory> {
    use Memory::{Firing, Kept, Waits};

    match (step, memory) {
        // A delivery keeps its row. A schedule during it queues an attempt.
        (Step::Schedule, Some(Firing { .. })) => Some(Firing { again: true }),
        (Step::Unschedule, Some(Firing { .. })) | (Step::Fire, _) => Some(Firing { again: false }),
        // The end of a delivery leaves the attempt that waits.
        (Step::Schedule, _) | (Step::Commit | Step::Abort, Some(Firing { again: true })) => {
            Some(Waits)
        }
        (Step::Unschedule | Step::Commit, _) => None,
        (Step::Abort, _) => Some(Kept),
    }
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
