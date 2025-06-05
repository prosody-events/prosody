use crate::Key;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::TimerManager;
use crate::timers::store::TriggerStore;
use crate::timers::trigger::Trigger;
use educe::Educe;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{Span, warn};

const RETRY_DURATION: Duration = Duration::from_secs(1);

#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct UncommittedTimer<T>
where
    T: TriggerStore,
{
    trigger: Trigger,

    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

pub struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    key: Key,
    time: CompactDateTime,
    manager: TimerManager<T>,
    completed: bool,
}

impl<T> UncommittedTimer<T>
where
    T: TriggerStore,
{
    pub fn new(trigger: Trigger, manager: TimerManager<T>) -> Self {
        let key = trigger.key.clone();
        let time = trigger.time;
        let completed = false;

        Self {
            trigger,
            uncommitted: UncommittedTrigger {
                key,
                time,
                manager,
                completed,
            },
        }
    }

    pub fn into_inner(self) -> (Trigger, UncommittedTrigger<T>) {
        (self.trigger, self.uncommitted)
    }

    pub fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    pub fn span(&self) -> &Span {
        &self.trigger.span
    }

    pub async fn is_active(&self) -> bool {
        self.uncommitted.is_active().await
    }
}

impl<T> Uncommitted for UncommittedTimer<T>
where
    T: TriggerStore,
{
    async fn commit(mut self) {
        self.uncommitted.commit().await;
    }

    async fn abort(mut self) {
        self.uncommitted.abort().await;
    }
}

impl<T> Keyed for UncommittedTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    pub async fn is_active(&self) -> bool {
        self.manager.is_active(&self.key, self.time).await
    }

    pub async fn commit(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        loop {
            let Err(error) = self.manager.complete(&self.key, self.time).await else {
                break;
            };

            tracing::error!("failed to commit timer: {error:#}; retrying");
            sleep(RETRY_DURATION).await;
        }
    }

    pub async fn abort(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager.abort(&self.key, self.time).await;
    }
}

impl<T> Drop for UncommittedTrigger<T>
where
    T: TriggerStore,
{
    fn drop(&mut self) {
        if !self.completed {
            warn!("trigger was dropped without committing");
        }
    }
}