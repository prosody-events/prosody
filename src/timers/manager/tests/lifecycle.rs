//! The lifecycle of one timer follows the state table in `timers::active`.

use super::{Store, past, seeded_store};
use crate::Key;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::active::TimerState;
use crate::timers::datetime::CompactDateTime;
use crate::timers::test_support::setup_timer_manager_with_store;
use crate::timers::uncommitted::UncommittedTriggerGuard;
use crate::timers::{PendingTimer, TimerRequest, TimerType, UncommittedTimer};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::{Stream, StreamExt, pin_mut};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::time::Duration;
use strum::VariantArray;
use tokio::runtime::Builder;
use tokio::time::timeout;
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

/// Every step leaves the timer's registry state and its `scheduled_times` as
/// the state table predicts. A fire happens only from `Scheduled`, and a
/// commit or abort only during a delivery.
#[test]
fn prop_lifecycle_follows_the_state_table() {
    fn property(steps: Vec<Step>) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(steps))
    }
    QuickCheck::new().quickcheck(property as fn(Vec<Step>) -> Result<()>);
}

async fn run(steps: Vec<Step>) -> Result<()> {
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};

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
            (Step::Fire, Some(Scheduled)) => delivery = Some(fire(&mut stream, &key, time).await?),
            (Step::Commit | Step::Abort, Some(Firing | FiringRescheduled)) => {
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

        let state = manager
            .0
            .scheduler
            .active_triggers()
            .get(&key, time, timer_type)
            .await
            .map(|entry| entry.state);
        ensure!(state == model, "{step:?} left {state:?}, not {model:?}");
        let listed = manager.scheduled_times(&key, timer_type).await?;
        let stored = matches!(model, Some(Scheduled | FiringRescheduled | Aborted));
        ensure!(
            listed == Vec::from_iter(stored.then_some(time)),
            "{step:?} in {model:?} listed {listed:?}"
        );
    }

    if let Some(guard) = delivery {
        guard.abort().await;
    }
    Ok(())
}

/// The state table, as its owner sees one timer.
fn next(state: Option<TimerState>, step: Step) -> Option<TimerState> {
    use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};

    match (step, state) {
        (Step::Schedule, None | Some(Aborted))
        | (Step::Commit | Step::Abort, Some(FiringRescheduled)) => Some(Scheduled),
        (Step::Schedule, Some(Firing)) => Some(FiringRescheduled),
        (Step::Schedule, queued) => queued,
        (Step::Unschedule, Some(Firing | FiringRescheduled)) | (Step::Fire, _) => Some(Firing),
        (Step::Unschedule | Step::Commit, _) => None,
        (Step::Abort, _) => Some(Aborted),
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
        let pending = timeout(Duration::from_secs(10), stream.next())
            .await?
            .ok_or_eyre("timer stream ended")?;
        ensure!(
            pending.trigger().key == *key && pending.trigger().time == time,
            "another timer fired"
        );
        if let Some(firing) = pending.fire().await {
            return Ok(firing.into_inner().1);
        }
    }
}
