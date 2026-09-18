//! The memory catalog.
//!
//! It reads the shared substrates the memory providers write, so a memory-mode
//! maintenance run observes exactly what the memory handlers stored.

use super::Catalog;
use crate::Key;
use crate::consumer::middleware::defer::message::store::MemoryMessageDeferStoreProvider;
use crate::consumer::middleware::defer::segment::MemorySegmentStore;
use crate::consumer::middleware::defer::timer::store::MemoryTimerDeferStoreProvider;
use crate::maintenance::identity::{DeferSegmentId, GroupId, Segment, TimerSegmentId};
use crate::timers::store::Segment as TimerSegment;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use async_stream::stream;
use futures::Stream;
use std::convert::Infallible;
use std::future::ready;
use tokio::task::coop::cooperative;

/// Reads the shared memory substrates the memory providers write.
///
/// Each scan takes its snapshot on the first poll. Dropping the stream after
/// that stops no work.
///
/// Do not swap in fresh providers here. The catalog sees only what the
/// providers it holds wrote, so four independently defaulted substrates would
/// give a catalog that reports nothing.
#[derive(Clone, Debug)]
pub struct MemoryCatalog {
    segments: MemorySegmentStore,
    messages: MemoryMessageDeferStoreProvider,
    timers: MemoryTimerDeferStoreProvider,
    triggers: InMemoryTriggerStoreProvider,
}

impl MemoryCatalog {
    /// Builds a catalog over the substrates these providers share.
    ///
    /// Crate-private, because no caller outside the crate can name the
    /// parameter types. The tests are its only caller until a memory-mode
    /// maintenance client calls it.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(
        segments: MemorySegmentStore,
        messages: MemoryMessageDeferStoreProvider,
        timers: MemoryTimerDeferStoreProvider,
        triggers: InMemoryTriggerStoreProvider,
    ) -> Self {
        Self {
            segments,
            messages,
            timers,
            triggers,
        }
    }
}

impl Catalog for MemoryCatalog {
    type Error = Infallible;

    fn segments(&self) -> impl Stream<Item = Result<Segment, Self::Error>> + Send + 'static {
        let registry = self.segments.clone();
        stream! {
            for segment in registry.segments().await {
                // The snapshot holds no tokio leaf await, so `cooperative` is
                // the only per-item budget checkpoint in these three scans.
                yield cooperative(ready(Ok(Segment::new(
                    GroupId::new(segment.consumer_group()),
                    *segment.topic(),
                    segment.partition(),
                ))))
                .await;
            }
        }
    }

    async fn timer_segment(&self, id: TimerSegmentId) -> Result<Option<TimerSegment>, Self::Error> {
        Ok(self.triggers.segment(id.as_uuid()).await)
    }

    // The two key scans are not folded: the two providers are different types,
    // so a fold needs a trait or a macro, and plain arms read better.
    fn message_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static {
        let messages = self.messages.clone();
        stream! {
            for key in messages.keys(id.as_uuid()).await {
                yield cooperative(ready(Ok(key))).await;
            }
        }
    }

    fn timer_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static {
        let timers = self.timers.clone();
        stream! {
            for key in timers.keys(id.as_uuid()).await {
                yield cooperative(ready(Ok(key))).await;
            }
        }
    }
}
