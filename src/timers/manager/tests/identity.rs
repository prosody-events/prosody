use super::*;
use crate::timers::slab::Slab;
use crate::timers::store::SegmentVersion;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::test_support::setup_timer_manager_with_store;
use crate::timers::uncommitted::FiringTimer;
use IdentityOp::{Abort, Clear, Commit, Fire, Schedule, Unschedule};
use TimerState::{Aborted, Firing, FiringRescheduled, Scheduled};
use tokio::sync::Semaphore;

type MemoryStore = TableAdapter<InMemoryTriggerStore>;
type MemoryTimers = TimerManager<MemoryStore>;

struct IdentityHarness<S> {
    manager: MemoryTimers,
    stream: S,
    key: Key,
    times: [CompactDateTime; 2],
    models: [IdentityModel; 2],
    seen: BTreeSet<i32>,
    /// The newest delivery for each coordinate that no dispatch has consumed.
    inbox: [Option<PendingTimer<MemoryStore>>; 2],
    delivered: Option<(usize, FiringTimer<MemoryStore>)>,
    permits: Arc<Semaphore>,
}

#[derive(Clone, Copy, Debug, Default)]
struct IdentityModel {
    state: Option<TimerState>,
    tag: Option<i32>,
    row: Option<i32>,
}

#[derive(Clone, Debug)]
struct IdentityTrace(Vec<IdentityOp>, i32);

#[derive(Clone, Copy, Debug)]
enum IdentityOp {
    Schedule(usize),
    Fire(usize),
    Clear(usize),
    Unschedule(usize),
    Commit,
    Abort,
}

impl Arbitrary for IdentityTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::from(u8::arbitrary(g) % 48);
        let ops = (0..len)
            .map(|_| {
                let index = usize::from(bool::arbitrary(g));
                match u8::arbitrary(g) % 6 {
                    0 => Schedule(index),
                    1 => Fire(index),
                    2 => Clear(index),
                    3 => Unschedule(index),
                    4 => Commit,
                    _ => Abort,
                }
            })
            .collect();
        Self(ops, i32::arbitrary(g))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ops = self.0.clone();
        let tag = self.1;
        Box::new(
            (0..ops.len())
                .rev()
                .map(move |len| Self(ops[..len].to_vec(), tag)),
        )
    }
}

impl<S> IdentityHarness<S>
where
    S: Stream<Item = PendingTimer<MemoryStore>> + Unpin,
{
    async fn apply(&mut self, op: IdentityOp) -> Result<()> {
        match op {
            Schedule(index) | Clear(index) => self.schedule(index, op).await?,
            Fire(index) if self.delivered.is_none() => self.fire(index).await?,
            Commit | Abort => self.finish(op).await?,
            Unschedule(index) => {
                self.manager
                    .unschedule(&self.key, self.times[index], TimerType::Application)
                    .await?;
                let model = &mut self.models[index];
                match model.state {
                    Some(FiringRescheduled) => model.state = Some(Firing),
                    Some(Firing) => {}
                    _ => *model = IdentityModel::default(),
                }
            }
            Fire(_) => {}
        }
        self.check(op).await
    }

    async fn schedule(&mut self, index: usize, op: IdentityOp) -> Result<()> {
        let request = TimerRequest::new(
            self.key.clone(),
            self.times[index],
            TimerType::Application,
            Span::current(),
        );
        if matches!(op, Clear(_)) {
            self.manager.clear_and_schedule(request).await?;
            let other = &mut self.models[1 - index];
            if other.row.take().is_some() {
                if matches!(other.state, Some(Firing | FiringRescheduled)) {
                    other.state = Some(Firing);
                } else {
                    *other = IdentityModel::default();
                }
            }
        } else {
            self.manager.schedule(request).await?;
        }
        let model = &mut self.models[index];
        let written = self
            .manager
            .0
            .store
            .current_trigger(&self.key, self.times[index], TimerType::Application)
            .await?
            .ok_or_else(|| eyre!("schedule has no durable row"))?;
        if matches!(model.state, Some(Scheduled | FiringRescheduled)) {
            assert_eq!(
                Some(written.tag),
                model.tag,
                "queued identity changed: {op:?}"
            );
        } else {
            assert!(
                self.seen.insert(written.tag),
                "a new attempt reused tag {}: {op:?}",
                written.tag
            );
            model.tag = Some(written.tag);
            model.state = Some(if model.state == Some(Firing) {
                FiringRescheduled
            } else {
                Scheduled
            });
        }
        model.row = Some(written.tag);
        Ok(())
    }

    async fn fire(&mut self, index: usize) -> Result<()> {
        let model = self.models[index];
        let pending = if model.state == Some(Scheduled) {
            let tag = model
                .tag
                .ok_or_else(|| eyre!("a queued attempt has a tag"))?;
            self.deliver(index, tag).await?
        } else {
            // Nothing is queued. A stale trigger must not start an attempt.
            let trigger = Trigger::with_tag(
                self.key.clone(),
                self.times[index],
                TimerType::Application,
                model.tag.unwrap_or(0_i32),
                Span::current(),
            );
            PendingTimer::new(
                trigger,
                self.manager.clone(),
                self.permits.clone().acquire_owned().await?,
            )
        };
        let firing = pending.fire().await;
        assert_eq!(firing.is_some(), model.state == Some(Scheduled));
        if let Some(firing) = firing {
            assert_eq!(Some(firing.trigger().tag), model.tag);
            self.models[index].state = Some(Firing);
            self.delivered = Some((index, firing));
        }
        Ok(())
    }

    /// Waits for the stream to deliver attempt `tag` of coordinate `index`.
    /// The actor pops the queue entry before this dispatch, as in production.
    /// Paused time advances to a pending coordinate. A delivery for the other
    /// coordinate waits in the inbox. An older delivery for the same
    /// coordinate is a dead attempt and is dropped.
    async fn deliver(&mut self, index: usize, tag: i32) -> Result<PendingTimer<MemoryStore>> {
        loop {
            if let Some(pending) = self.inbox[index].take_if(|pending| pending.trigger().tag == tag)
            {
                return Ok(pending);
            }
            let pending = timeout(Duration::from_secs(5), self.stream.next())
                .await?
                .ok_or_else(|| eyre!("timer stream ended"))?;
            let slot = self
                .times
                .iter()
                .position(|&time| time == pending.trigger().time)
                .ok_or_else(|| eyre!("delivery for an unknown time"))?;
            self.inbox[slot] = Some(pending);
        }
    }

    async fn finish(&mut self, op: IdentityOp) -> Result<()> {
        if let Some((index, firing)) = self.delivered.take() {
            let model = &mut self.models[index];
            let old = firing.trigger().clone();
            if matches!(op, Commit) {
                firing.commit().await;
            } else {
                firing.abort().await;
            }
            if model.state == Some(FiringRescheduled) {
                model.state = Some(Scheduled);
                // A stale delivery must not dispatch the queued replacement.
                let pending = PendingTimer::new(
                    old,
                    self.manager.clone(),
                    self.permits.clone().acquire_owned().await?,
                );
                assert!(pending.fire().await.is_none());
            } else if matches!(op, Commit) {
                *model = IdentityModel::default();
            } else {
                model.state = Some(Aborted);
            }
        }
        Ok(())
    }

    async fn check(&self, op: IdentityOp) -> Result<()> {
        for (index, model) in self.models.iter().enumerate() {
            let active = self
                .manager
                .0
                .scheduler
                .active_triggers()
                .get(&self.key, self.times[index], TimerType::Application)
                .await;
            assert_eq!(
                active.map(|entry| entry.state),
                model.state,
                "registry state after {op:?}"
            );
            assert_eq!(
                active.map(|entry| entry.tag),
                model.tag,
                "registry identity after {op:?}"
            );
            let row = self
                .manager
                .0
                .store
                .current_trigger(&self.key, self.times[index], TimerType::Application)
                .await?;
            assert_eq!(
                row.map(|trigger| trigger.tag),
                model.row,
                "key row after {op:?}"
            );
            let slab = Slab::from_time(self.manager.0.store.slab_size(), self.times[index]);
            let rows: Vec<Trigger> = self
                .manager
                .0
                .store
                .get_slab_triggers_all_types(slab.id())
                .try_collect()
                .await?;
            let row = rows.iter().find(|trigger| {
                trigger.key == self.key
                    && trigger.time == self.times[index]
                    && trigger.timer_type == TimerType::Application
            });
            assert_eq!(
                row.map(|trigger| trigger.tag),
                model.row,
                "slab row after {op:?}"
            );
        }
        Ok(())
    }
}

/// A queued attempt keeps one identity through dispatch and completion.
/// New schedules use a different identity in the registry and both indexes.
#[test]
fn prop_queued_attempt_identity() {
    QuickCheck::new()
        .quickcheck(prop_queued_attempt_identity_inner as fn(IdentityTrace) -> TestResult);
}

fn prop_queued_attempt_identity_inner(trace: IdentityTrace) -> TestResult {
    let runtime = match Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("runtime build failed: {error}")),
    };
    runtime.block_on(async {
        match run_identity_trace(trace).await {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    })
}

async fn run_identity_trace(trace: IdentityTrace) -> Result<()> {
    // Seed the old layout directly. The loader must preserve its stored identity.
    let mut segment = test_segment("identity", 300_u32);
    segment.version = SegmentVersion::V3;
    let store = memory_store(segment);
    let key = Key::from("identity");
    let timer_type = TimerType::Application;
    let loaded = Trigger::with_tag(
        key.clone(),
        CompactDateTime::now()?,
        timer_type,
        trace.1,
        Span::current(),
    );
    store.add_trigger(loaded.clone()).await?;
    store
        .insert_slab(Slab::from_time(store.slab_size(), loaded.time))
        .await?;
    let (stream, manager, _shutdown_tx) =
        setup_timer_manager_with_store(store, ShutdownPhase::default()).await?;
    pin_mut!(stream);
    check_loaded_identity(&manager, &mut stream, &loaded).await?;

    // Index 0 starts the loaded slab and is due at insert. Index 1 sits two
    // seconds ahead: `now()` rounds to the nearest second, so its queue entry
    // waits until a dispatch advances paused time. The first load reaches at
    // least sixty seconds ahead, so the actor owns both coordinates.
    let base = Slab::from_time(manager.0.store.slab_size(), loaded.time)
        .range()
        .start;
    let times = [base, loaded.time.add_duration(CompactDuration::new(2))?];
    let mut harness = IdentityHarness {
        manager,
        stream,
        key,
        times,
        models: [IdentityModel::default(); 2],
        seen: BTreeSet::from([loaded.tag]),
        inbox: [None, None],
        delivered: None,
        permits: Arc::new(Semaphore::new(2)),
    };

    // Exercise every identity transition before the random interleavings.
    let prefix = [
        Schedule(0),
        Schedule(0),
        Clear(0),
        Fire(0),
        Schedule(0),
        Schedule(0),
        Clear(0),
        Commit,
        Fire(0),
        Clear(0),
        Commit,
        Fire(0),
        Abort,
        Schedule(0),
        Fire(0),
        Abort,
        Clear(0),
        Fire(0),
        Schedule(0),
        Abort,
        Fire(0),
        Schedule(0),
        Unschedule(0),
        Schedule(0),
        Commit,
        Fire(0),
        Commit,
        Schedule(0),
        Fire(0),
        Clear(1),
        Commit,
        Fire(1),
        Commit,
    ];
    for op in prefix.into_iter().chain(trace.0) {
        harness.apply(op).await?;
    }
    if let Some((_, firing)) = harness.delivered {
        firing.abort().await;
    }
    Ok(())
}

async fn check_loaded_identity<S>(
    manager: &MemoryTimers,
    stream: &mut S,
    loaded: &Trigger,
) -> Result<()>
where
    S: Stream<Item = PendingTimer<MemoryStore>> + Unpin,
{
    let key = &loaded.key;
    let timer_type = loaded.timer_type;
    let (mut observed, mut guard) = wait_and_fire(stream, "loaded identity").await?;
    assert_eq!(observed.tag, loaded.tag);
    for clear in [false, true] {
        if clear {
            manager.clear_and_schedule(timer_request(loaded)).await?;
        } else {
            manager.schedule(timer_request(loaded)).await?;
        }
        let written = manager
            .0
            .store
            .current_trigger(key, loaded.time, timer_type)
            .await?
            .ok_or_else(|| eyre!("replacement row missing"))?;
        assert_ne!(written.tag, observed.tag);
        guard.commit().await;
        (observed, guard) = timeout(
            Duration::from_secs(5),
            wait_and_fire(stream, "replacement identity"),
        )
        .await??;
        assert_eq!(observed.tag, written.tag);
    }
    guard.commit().await;
    assert!(
        manager
            .0
            .store
            .current_trigger(key, loaded.time, timer_type)
            .await?
            .is_none()
    );

    Ok(())
}
