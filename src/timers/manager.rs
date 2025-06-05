use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

pub use crate::timers::error::TimerManagerError;
use crate::timers::loader::{get_or_create_segment, slab_loader, State};
use crate::timers::slab_lock::SlabLock;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::Slab;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::timers::Trigger;
use crate::timers::uncommitted::UncommittedTimer;
use educe::Educe;
use futures::stream::iter;
use futures::{Stream, StreamExt, TryStreamExt};
use std::sync::Arc;

use tokio::spawn;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span};

pub const DELETE_CONCURRENCY: usize = 16;

#[derive(Educe)]
#[educe(Debug(bound = ""), Clone(bound()))]
pub struct TimerManager<T>(#[educe(Debug(ignore))] Arc<TimerManagerInner<T>>);

pub struct TimerManagerInner<T> {
    segment: Segment,
    state: SlabLock<State<T>>,
}

impl<T> TimerManager<T>
where
    T: TriggerStore,
{
    pub async fn new(
        segment_id: SegmentId,
        slab_size: CompactDuration,
        name: &str,
        store: T,
    ) -> Result<(impl Stream<Item = UncommittedTimer<T>>, Self), TimerManagerError<T::Error>> {
        let segment = get_or_create_segment(&store, segment_id, slab_size, name).await?;
        let (trigger_rx, scheduler) = TriggerScheduler::new();
        let state = SlabLock::new(State::new(store, scheduler));

        spawn(slab_loader(segment.clone(), state.clone()));

        let manager = Self(Arc::new(TimerManagerInner { segment, state }));
        let cloned_manager = manager.clone();
        let stream = ReceiverStream::new(trigger_rx)
            .map(move |trigger| UncommittedTimer::new(trigger, cloned_manager.clone()));

        Ok((stream, manager))
    }

    pub async fn scheduled_times(
        &self,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        self.0
            .state
            .trigger_lock()
            .await
            .store
            .get_key_triggers(&self.0.segment.id, key)
            .map_err(TimerManagerError::Store)
            .try_collect()
            .await
    }

    pub async fn schedule(&self, trigger: Trigger) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, trigger.time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        state
            .store
            .add_trigger(&self.0.segment, slab, trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;

        if state.is_owned(slab_id) {
            state.scheduler.schedule(trigger).await?;
        }

        Ok(())
    }

    pub async fn unschedule(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            let trigger = Trigger {
                key: key.clone(),
                time,
                span: Span::current(),
            };

            state.scheduler.unschedule(trigger).await?;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)
    }

    pub async fn unschedule_all(&self, key: &Key) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();
        let times = self.scheduled_times(key).await?;

        let futures = times
            .into_iter()
            .map(|time| self.unschedule(key, time).instrument(span.clone()));

        iter(futures)
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    pub async fn is_active(&self, key: &Key, time: CompactDateTime) -> bool {
        self.0
            .state
            .trigger_lock()
            .await
            .scheduler
            .is_active(key, time)
            .await
    }

    pub async fn complete(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }

        state
            .store
            .remove_trigger(&self.0.segment, &slab, key, time)
            .await
            .map_err(TimerManagerError::Store)?;

        Ok(())
    }

    pub async fn abort(&self, key: &Key, time: CompactDateTime) {
        let slab = Slab::from_time(self.0.segment.id, self.0.segment.slab_size, time);
        let slab_id = slab.id();
        let state = self.0.state.trigger_lock().await;

        if state.is_owned(slab_id) {
            state.scheduler.deactivate(key, time).await;
        }
    }
}