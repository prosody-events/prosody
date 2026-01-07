//! In-memory implementation of [`DeferStoreProvider`] for testing.

use super::DeferStoreProvider;
use crate::consumer::middleware::defer::message::store::memory::MemoryMessageDeferStore;
use crate::consumer::middleware::defer::segment::{LazySegment, MemorySegmentStore};
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;

/// In-memory defer store provider for testing and development.
///
/// Creates [`MemoryMessageDeferStore`] and [`MemoryTimerDeferStore`] instances
/// that store data in memory. All data is volatile and lost on process exit.
///
/// # Usage
///
/// ```rust,no_run
/// use prosody::consumer::middleware::defer::MemoryDeferStoreProvider;
///
/// let provider = MemoryDeferStoreProvider::new();
/// // Pass to MessageDeferMiddleware
/// ```
#[derive(Clone, Debug)]
pub struct MemoryDeferStoreProvider {
    segment: MemorySegmentStore,
}

impl MemoryDeferStoreProvider {
    /// Creates a new in-memory defer store provider.
    #[must_use]
    pub fn new() -> Self {
        Self {
            segment: MemorySegmentStore::new(),
        }
    }
}

impl Default for MemoryDeferStoreProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DeferStoreProvider for MemoryDeferStoreProvider {
    type MessageStore = MemoryMessageDeferStore;
    type Segment = MemorySegmentStore;
    type TimerStore = MemoryTimerDeferStore;

    fn segment(&self) -> &Self::Segment {
        &self.segment
    }

    fn create_message_store(&self, _segment: LazySegment<Self::Segment>) -> Self::MessageStore {
        MemoryMessageDeferStore::new()
    }

    fn create_timer_store(&self, _segment: LazySegment<Self::Segment>) -> Self::TimerStore {
        MemoryTimerDeferStore::new()
    }
}
