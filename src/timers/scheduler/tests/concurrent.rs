//! Calls on one key and timer type that run at the same time.

use super::support::{Actor, TestStore};
use super::{SLAB_SIZE_SECS, fresh_state};
use crate::Key;
use crate::consumer::Uncommitted;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::scheduler::actor::load::load_step;
use crate::timers::store::TriggerStore;
use crate::timers::store::memory::memory_store;
use crate::timers::test_support::test_segment;
use crate::timers::uncommitted::PendingTimer;
use crate::timers::{TimerManager, TimerRequest, TimerType, Trigger};
use color_eyre::eyre::{Result, ensure};
use futures::future::try_join_all;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::cell::RefCell;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::Semaphore;
use tracing::Span;

/// An operation on one key and timer type. The flag picks the later of two
/// times. A delivery takes the attempt that left the queue at its time, if no
/// delivery before it took that attempt.
#[derive(Clone, Copy, Debug)]
enum Call {
    Schedule(bool),
    Unschedule(bool),
    Clear(bool),
    UnscheduleAll,
    /// Fires the attempt and commits it.
    Commit(bool),
    /// Fires the attempt and aborts it.
    Abort(bool),
    /// Aborts the attempt before it fires, as admission does on shutdown.
    Abandon(bool),
}

/// What the calls of [`concurrent`] share.
struct Shared {
    manager: TimerManager<TestStore>,
    key: Key,
    times: [CompactDateTime; 2],
    /// The attempts that fired.
    fired: RefCell<Vec<(CompactDateTime, i32)>>,
    /// The times of the deliveries that did not commit.
    aborted: RefCell<Vec<CompactDateTime>>,
}

impl Call {
    /// Runs the call. A delivery takes the attempt in `pending`.
    async fn run(self, shared: &Shared, pending: Option<PendingTimer<TestStore>>) -> Result<()> {
        let Shared {
            manager,
            key,
            times,
            fired,
            aborted,
        } = shared;
        let ty = TimerType::Application;
        let request = |later: bool| {
            TimerRequest::new(key.clone(), times[usize::from(later)], ty, Span::current())
        };
        match self {
            Self::Schedule(later) => manager.schedule(request(later)).await?,
            Self::Unschedule(later) => {
                manager
                    .unschedule(key, times[usize::from(later)], ty)
                    .await?;
            }
            Self::Clear(later) => manager.clear_and_schedule(request(later)).await?,
            Self::UnscheduleAll => manager.unschedule_all(key, ty).await?,
            Self::Abandon(_) => pending.into_iter().for_each(PendingTimer::abandon),
            Self::Commit(_) | Self::Abort(_) => {
                let Some(pending) = pending else {
                    return Ok(());
                };
                let Some(firing) = pending.fire().await else {
                    return Ok(());
                };
                let (time, tag) = (firing.trigger().time, firing.trigger().tag);
                fired.borrow_mut().push((time, tag));
                if matches!(self, Self::Abort(_)) {
                    firing.abort().await;
                } else {
                    firing.commit().await;
                    aborted.borrow_mut().retain(|&aborted| aborted != time);
                }
            }
        }
        Ok(())
    }
}

impl Arbitrary for Call {
    fn arbitrary(g: &mut Gen) -> Self {
        let later = bool::arbitrary(g);
        match u8::arbitrary(g) % 7 {
            0 => Self::Schedule(later),
            1 => Self::Unschedule(later),
            2 => Self::Clear(later),
            3 => Self::UnscheduleAll,
            4 => Self::Commit(later),
            5 => Self::Abort(later),
            _ => Self::Abandon(later),
        }
    }
}

/// Calls that run at the same time on one key and type keep memory exact, in
/// any order of their commands and when some of them stop early. Each stored
/// row has a waiting attempt with the tag of the row, unless a delivery at its
/// time aborted: an abort leaves the row to a restart. Each waiting attempt
/// has its row, and no attempt fires twice.
#[test]
fn prop_concurrent_operations_keep_memory_exact() {
    fn property(calls: Vec<(Call, Option<u8>)>, order: Vec<u8>) -> Result<()> {
        RuntimeBuilder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()?
            .block_on(concurrent(calls, order))
    }
    QuickCheck::new().quickcheck(property as fn(Vec<(Call, Option<u8>)>, Vec<u8>) -> Result<()>);
}

/// Seeds a row at each of two times and takes their attempts from the queue.
/// Then it runs `calls` at the same time. The actor takes the waiting
/// commands in an order from `order`. A call with a stop drops after that
/// many polls, modulo 4.
async fn concurrent(calls: Vec<(Call, Option<u8>)>, order: Vec<u8>) -> Result<()> {
    let segment = test_segment("concurrent", SLAB_SIZE_SECS);
    let store = memory_store(segment.clone());
    store.insert_segment().await?;
    let now = CompactDateTime::now()?.epoch_seconds();
    let times = [601, 600].map(|ago| CompactDateTime::from(now.saturating_sub(ago)));
    let mut actor = Actor::new(fresh_state(store.clone(), segment));
    load_step(&mut actor.state, &mut actor.timers).await;

    let shared = Shared {
        manager: actor.manager(&store, &Telemetry::new()),
        key: Key::from("concurrent"),
        times,
        fired: RefCell::default(),
        aborted: RefCell::default(),
    };
    let seeds = [Call::Schedule(false), Call::Schedule(true)].map(|call| call.run(&shared, None));
    actor.serve(try_join_all(seeds)).await?;
    let mut popped = [actor.timers.next().await, actor.timers.next().await];
    popped.sort_by_key(|trigger| trigger.as_ref().map(|trigger| trigger.time));
    ensure!(
        popped.iter().all(Option::is_some),
        "a seeded attempt is not queued"
    );

    let permits = Arc::new(Semaphore::new(popped.len()));
    let mut runs = Vec::new();
    for (call, stop) in calls {
        let pending = match call {
            Call::Commit(later) | Call::Abort(later) | Call::Abandon(later) => {
                popped[usize::from(later)].take()
            }
            _ => None,
        };
        let pending = match pending {
            Some(trigger) => {
                shared.aborted.borrow_mut().push(trigger.time);
                let permit = permits.clone().try_acquire_owned()?;
                Some(PendingTimer::new(trigger, shared.manager.clone(), permit))
            }
            None => None,
        };
        runs.push(stop_after(
            stop.map(|polls| polls % 4),
            call.run(&shared, pending),
        ));
    }
    let mut order = order.into_iter().cycle();
    let pick = |_| order.next().map_or(0, usize::from);
    actor.serve_in(try_join_all(runs), pick).await?;

    let Shared {
        key,
        fired,
        aborted,
        ..
    } = shared;
    let ty = TimerType::Application;
    let mut stored = Vec::new();
    for time in times {
        let row = store.current_trigger(&key, time, ty).await?;
        stored.extend(row.map(|trigger| (time, trigger.tag)));
    }
    let held = fire_held(&mut actor, popped).await?;
    let mut fired = fired.into_inner();
    fired.extend(&held);
    fired.sort_unstable();
    ensure!(
        fired.windows(2).all(|pair| pair[0] != pair[1]),
        "an attempt fired twice: {fired:?}"
    );
    let aborted = aborted.into_inner();
    stored.retain(|row| !held.contains(row) && !aborted.contains(&row.0));
    ensure!(
        stored.is_empty(),
        "memory holds no attempt for the stored rows {stored:?}"
    );
    for (time, tag) in held {
        let row = store.current_trigger(&key, time, ty).await?;
        let stored = row.map(|row| row.tag);
        ensure!(
            stored == Some(tag),
            "memory fires {tag} at {time}, but the store holds {stored:?}"
        );
    }
    Ok(())
}

/// Fires each attempt that memory holds, after every delivery ended. These are
/// the queued attempts and the attempts in `popped` that still wait.
async fn fire_held(
    actor: &mut Actor,
    popped: [Option<Trigger>; 2],
) -> Result<Vec<(CompactDateTime, i32)>> {
    let timers = &mut actor.timers;
    ensure!(
        timers.snapshot(CompactDateTime::MIN).in_flight == 0,
        "a delivery never ended"
    );
    let mut held = Vec::new();
    while let Some(trigger) = timers.next().await {
        ensure!(
            timers.fire(&trigger.id(), trigger.tag),
            "the queued attempt {trigger:?} does not fire"
        );
        held.push((trigger.time, trigger.tag));
    }
    for trigger in popped.into_iter().flatten() {
        if timers.fire(&trigger.id(), trigger.tag) {
            held.push((trigger.time, trigger.tag));
        }
    }
    Ok(held)
}

/// Runs `call`, but drops it after `stop` polls.
async fn stop_after(stop: Option<u8>, call: impl Future<Output = Result<()>>) -> Result<()> {
    let mut call = pin!(call);
    let mut left = stop;
    poll_fn(|cx| {
        if left == Some(0) {
            return Poll::Ready(Ok(()));
        }
        let polled = call.as_mut().poll(cx);
        left = left.map(|left| left.saturating_sub(1));
        if polled.is_pending() && left == Some(0) {
            return Poll::Ready(Ok(()));
        }
        polled
    })
    .await
}
