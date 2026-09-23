//! Applies resolved timer transitions to the store and the scheduler.

use super::{TimerManagerError, TimerManagerInner};
use crate::error::ClassifyError;
use crate::timers::Trigger;
use crate::timers::active::{
    Announce, MemoryEffects, QueueEffect, StoreEffect, TimerOp, Transition, transition,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::store::TriggerStore;
use std::error::Error;
use std::fmt::Debug;
use tracing::{Span, debug};

impl<T: TriggerStore> TimerManagerInner<T> {
    /// Emits one `timer_cancelled` event per replaced time (excluding the
    /// new time) and one `timer_scheduled` event for the new trigger.
    pub(super) fn emit_clear_telemetry(
        &self,
        trigger: &Trigger,
        existing_times: &[CompactDateTime],
    ) {
        for &old_time in existing_times {
            if old_time != trigger.time {
                self.telemetry.timer_cancelled(
                    trigger.key.clone(),
                    old_time,
                    trigger.timer_type,
                    self.source.clone(),
                );
            }
        }
        self.telemetry.timer_scheduled(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            self.source.clone(),
        );
    }

    /// Resolves the state-machine transition for `trigger` and applies it.
    pub(super) async fn drive(
        &self,
        trigger: &Trigger,
        op: TimerOp,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let prior = self
            .scheduler
            .active_triggers()
            .get_state(&trigger.key, trigger.time, trigger.timer_type)
            .await;
        self.apply(trigger, transition(prior, op)).await
    }

    /// Applies a resolved [`Transition`]: pre-persist in-memory effects, the
    /// durable write, post-persist in-memory effects, then telemetry.
    async fn apply(
        &self,
        trigger: &Trigger,
        t: Transition,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let (pre, post) = t.phases();
        apply_memory(&self.scheduler, trigger, pre).await?;

        match t.store() {
            StoreEffect::None => {}
            StoreEffect::Insert => self
                .store
                .add_trigger(trigger.clone())
                .await
                .map_err(TimerManagerError::Store)?,
            StoreEffect::Delete => self
                .store
                .remove_trigger(&trigger.key, trigger.time, trigger.timer_type)
                .await
                .map_err(TimerManagerError::Store)?,
        }

        apply_memory(&self.scheduler, trigger, post).await?;

        match t.announce() {
            Some(Announce::Scheduled) => self.telemetry.timer_scheduled(
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                self.source.clone(),
            ),
            Some(Announce::Cancelled) => self.telemetry.timer_cancelled(
                trigger.key.clone(),
                trigger.time,
                trigger.timer_type,
                self.source.clone(),
            ),
            None => {}
        }
        Ok(())
    }
}

/// Applies the queue effect, then sets the resulting registry state.
pub(super) async fn apply_memory<E>(
    scheduler: &TriggerScheduler<E>,
    trigger: &Trigger,
    effects: MemoryEffects,
) -> Result<(), TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    match effects.queue {
        QueueEffect::None => {}
        QueueEffect::Dequeue => scheduler.remove_from_queue(trigger.clone()).await?,
        QueueEffect::Insert => scheduler.schedule(trigger.clone()).await?,
        QueueEffect::Remove => scheduler.unschedule(trigger.clone()).await?,
        QueueEffect::Deactivate => {
            scheduler
                .deactivate(&trigger.key, trigger.time, trigger.timer_type)
                .await;
        }
    }
    if let Some(state) = effects.next_state {
        scheduler
            .active_triggers()
            .set_state(&trigger.key, trigger.time, trigger.timer_type, state)
            .await;
    }
    Ok(())
}

/// Unschedules every replaced (old-time) timer during a `clear_and_schedule`
/// operation, resolving each through the state machine's `ClearReplaced` op.
///
/// All effects are in-memory: the caller's atomic store write subsumes the
/// per-row deletes. The scheduler removal is idempotent — the actor finds
/// nothing when the slab isn't loaded.
pub(super) async fn unschedule_replaced_timers<E>(
    scheduler: &TriggerScheduler<E>,
    new_trigger: &Trigger,
    existing_times: &[CompactDateTime],
) -> Result<(), TimerManagerError<E>>
where
    E: ClassifyError + Error + Debug + Send + Sync + 'static,
{
    for &old_time in existing_times {
        if old_time == new_trigger.time {
            continue; // Same time as new — resolved by the caller's ClearSchedule op.
        }

        let old = Trigger::new(
            new_trigger.key.clone(),
            old_time,
            new_trigger.timer_type,
            Span::current(),
        );
        let prior = scheduler
            .active_triggers()
            .get_state(&old.key, old.time, old.timer_type)
            .await;
        let (pre, post) = transition(prior, TimerOp::ClearReplaced).phases();
        debug!(
            key = %old.key,
            timer_type = ?old.timer_type,
            old_time = ?old.time,
            prior_state = ?prior,
            "clear_and_schedule: unscheduling replaced timer"
        );
        apply_memory(scheduler, &old, pre).await?;
        apply_memory(scheduler, &old, post).await?;
    }
    Ok(())
}
