#![allow(dead_code, clippy::unused_async)]

use crate::Key;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::range::LocalRange;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::store::SegmentId;
use chrono::OutOfRangeError;
use educe::Educe;
use futures::TryFutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::{Span, error};

mod active;
mod datetime;
mod duration;
mod range;
mod scheduler;
mod slab;
mod store;
mod triggers;

#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,

    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}

#[derive(Clone, Debug)]
pub struct TimerManager<T> {
    segment: SegmentId,
    range_rx: watch::Receiver<LocalRange>,
    scheduler: TriggerScheduler,
    store: T,
}

impl<T> TimerManager<T> {
    fn new(segment: SegmentId, store: T) -> (mpsc::Receiver<Trigger>, Self) {
        let (_range_tx, range_rx) = watch::channel(LocalRange::default());
        let (trigger_rx, scheduler) = TriggerScheduler::new();

        let manager = Self {
            segment,
            range_rx,
            scheduler,
            store,
        };

        (trigger_rx, manager)
    }

    async fn in_range(&self, time: CompactDateTime) -> Result<bool, TimerManagerError> {
        let mut range_rx = self.range_rx.clone();

        if !range_rx.borrow().contains(time) {
            return Ok(false);
        }

        range_rx
            .wait_for(|range| !range.is_loading(time))
            .map_err(|_| TimerManagerError::Shutdown)
            .await?;

        Ok(true)
    }
}

#[derive(Clone, Debug, Error)]
pub enum TimerManagerError {
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),

    #[error("Time must be in the future: {0:#}")]
    PastTime(#[from] OutOfRangeError),

    #[error("Time not found")]
    NotFound,

    #[error("Timer has been shutdown")]
    Shutdown,
}
