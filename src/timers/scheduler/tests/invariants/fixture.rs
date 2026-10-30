//! The fixture that applies each [`Op`] to the actor and to the model.

use super::super::support::{Actor, TestStore};
use super::super::{SLAB_SIZE_SECS, fresh_state, start_state};
use super::checks::{assert_past_watermark_pull, check_cleanup_effects};
use super::{Op, Stop, TriggerModel, TriggerModels, TriggerSpec, build_universe};
use crate::Key;
use crate::timers::active::Insert;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::scheduler::actor::load::load_step;
use crate::timers::scheduler::actor::owns_slab;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::memory::memory_store;
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::test_support::Memory;
use crate::timers::test_support::test_segment;
use crate::timers::{TimerType, Trigger, TriggerId};
use color_eyre::eyre::{OptionExt, Result, WrapErr, bail, ensure};
use tracing::Span;

/// The actor under test and the model that it must match.
pub(super) struct Fixture {
    pub(super) store: TestStore,
    pub(super) segment: Segment,
    pub(super) actor: Actor,
    /// Authoritative per-trigger model.
    pub(super) expected: TriggerModels,
    /// Anchored at construction so all ops within one iteration use a
    /// stable `now_slab_id` regardless of wall-clock drift.
    now_slab: SlabId,
    /// All `(key, time, type)` triples the generator can produce, used
    /// for the universe-iteration check after every op.
    pub(super) universe: Vec<(Key, CompactDateTime, TimerType)>,
}

impl Fixture {
    pub(super) async fn new() -> Result<Self> {
        let segment = test_segment("scheduler-test", SLAB_SIZE_SECS);
        let store = memory_store(segment.clone());
        store.insert_segment().await.wrap_err("insert_segment")?;

        let now = CompactDateTime::now().wrap_err("wall-clock now")?;
        let now_slab = Slab::from_time(segment.slab_size, now).id();
        let actor = Actor::new(fresh_state(store.clone(), segment.clone()));
        Ok(Self {
            store,
            segment,
            actor,
            expected: TriggerModels::default(),
            now_slab,
            universe: build_universe(now_slab),
        })
    }

    pub(super) async fn apply(&mut self, op: Op) -> Result<()> {
        match op {
            Op::Schedule(spec, stop) => self.apply_schedule(spec, stop).await,
            Op::Unschedule(spec) => self.apply_unschedule(spec).await,
            Op::Fire => self.apply_fire().await,
            Op::Abort(spec) => self.apply_abort(spec).await,
            Op::LoadStep => {
                load_step(&mut self.actor.state, &mut self.actor.timers).await;
                self.reconcile_loaded_models();
                Ok(())
            }
            Op::CleanupStep => check_cleanup_effects(self).await,
            Op::Restart { stale_watermark } => self.apply_restart(stale_watermark).await,
        }
    }

    async fn apply_schedule(&mut self, spec: TriggerSpec, stop: Stop) -> Result<()> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        let mut model = self.model_for(&key, time, ty);
        let slab_id = Slab::from_time(self.segment.slab_size, time).id();
        let state = &self.actor.state;
        let past_unregistered = state.last_persisted_watermark.is_some_and(|w| slab_id <= w)
            && !state.known_slab_ids.contains(&slab_id);
        let old_watermark = state.last_persisted_watermark;

        let trigger = Trigger::new(key.clone(), time, ty, Span::current());
        let scheduler = self.actor.scheduler();
        let (trigger, ticket) = self
            .actor
            .serve(scheduler.insert(trigger, Insert::Schedule))
            .await
            .wrap_err("insert")?;
        if past_unregistered {
            assert_past_watermark_pull(self, slab_id, old_watermark)?;
        }
        match stop {
            Stop::BeforeWrite => drop(ticket),
            Stop::Never | Stop::AfterWrite | Stop::FailedWrite => {
                let write = async {
                    if stop != Stop::FailedWrite {
                        let Ok(()) = self.store.add_trigger(trigger).await;
                    }
                    if stop == Stop::Never {
                        Ok(())
                    } else {
                        bail!("the write of a {stop:?} schedule stops")
                    }
                };
                // The error of a stopped write is the point of the op.
                let _ = ticket.run(write).await;
                model.in_store |= stop != Stop::FailedWrite;
                let owned = owns_slab(&self.actor.state, slab_id);
                model.memory = match model.memory {
                    Some(Memory::Firing { .. }) => Some(Memory::Firing { again: true }),
                    None if !owned => None,
                    Some(Memory::Waits | Memory::Kept) | None => Some(Memory::Waits),
                };
            }
        }
        self.actor.drain().await;

        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_unschedule(&mut self, spec: TriggerSpec) -> Result<()> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        let mut model = self.model_for(&key, time, ty);
        let scheduler = self.actor.scheduler();
        let started = self
            .actor
            .serve(scheduler.unschedule(TriggerId::new(&key, time, ty)))
            .await
            .wrap_err("unschedule")?;
        if let Some(ticket) = started {
            ticket
                .run(self.store.remove_trigger(&key, time, ty))
                .await
                .wrap_err("remove_trigger")?;
            model = TriggerModel::default();
        } else {
            model.memory = Some(Memory::Firing { again: false });
        }
        self.actor.drain().await;
        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_fire(&mut self) -> Result<()> {
        let Some(trigger) = self.actor.timers.next().await else {
            return Ok(());
        };
        let fired = self.actor.timers.fire(&trigger.id(), trigger.tag);
        let (key, time, ty) = (trigger.key, trigger.time, trigger.timer_type);
        let mut model = self.model_for(&key, time, ty);
        // A second delivery of a timer never starts while the first runs.
        if model.delivery.is_some() {
            ensure!(!fired, "a second delivery of {key} at {time} started");
            return Ok(());
        }
        ensure!(
            fired,
            "the attempt at {time} that left the queue did not fire"
        );
        model.memory = Some(Memory::Firing { again: false });
        model.delivery = Some(trigger.tag);
        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_abort(&mut self, spec: TriggerSpec) -> Result<()> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        let mut model = self.model_for(&key, time, ty);
        // No attempt here waits outside the queue, so only a delivery aborts.
        // With no delivery, the abort changes nothing.
        let tag = model.delivery.take().unwrap_or_default();
        self.actor
            .scheduler()
            .abort(TriggerId::new(&key, time, ty), tag);
        self.actor.drain().await;
        model.memory = match model.memory {
            Some(Memory::Firing { again: true }) => Some(Memory::Waits),
            Some(Memory::Firing { again: false }) => Some(Memory::Kept),
            memory => memory,
        };
        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_restart(&mut self, stale_watermark: bool) -> Result<()> {
        let now = CompactDateTime::now().wrap_err("wall-clock now")?;
        if stale_watermark {
            let old_slab = Slab::from_time(CompactDuration::new(60), now);
            let Ok(()) = self
                .store
                .set_slab_watermark(old_slab.id().checked_sub(1))
                .await;
        }
        let Ok(segment) = self.store.get_segment().await;
        let segment = segment.ok_or_eyre("restart: segment missing")?;
        self.actor = Actor::new(start_state(self.store.clone(), segment, now));
        for model in self.expected.values_mut() {
            model.memory = None;
            model.delivery = None;
        }
        Ok(())
    }

    /// The slab of the wall clock, as cleanup and actor start compute it.
    pub(super) fn current_slab(&self) -> SlabId {
        match CompactDateTime::now() {
            Ok(t) => Slab::from_time(self.segment.slab_size, t).id(),
            Err(_) => self.now_slab,
        }
    }

    pub(super) fn model_for(
        &self,
        key: &Key,
        time: CompactDateTime,
        ty: TimerType,
    ) -> TriggerModel {
        self.expected
            .get(&(key.clone(), time, ty))
            .copied()
            .unwrap_or_default()
    }

    fn set_model(&mut self, key: Key, time: CompactDateTime, ty: TimerType, model: TriggerModel) {
        let triple = (key, time, ty);
        if model.in_store || model.memory.is_some() {
            self.expected.insert(triple, model);
        } else {
            self.expected.remove(&triple);
        }
    }

    fn reconcile_loaded_models(&mut self) {
        for (key, time, ty) in &self.universe {
            let slab_id = Slab::from_time(self.segment.slab_size, *time).id();
            if !owns_slab(&self.actor.state, slab_id) {
                continue;
            }
            if let Some(model) = self.expected.get_mut(&(key.clone(), *time, *ty))
                && model.in_store
                && model.memory.is_none()
            {
                model.memory = Some(Memory::Waits);
            }
        }
    }
}
