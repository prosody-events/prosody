//! In-memory storage implementation for timer data.
//!
//! This module provides [`InMemoryTriggerStore`], a memory-based implementation
//! of the [`TriggerStore`] trait. This implementation is designed for testing,
//! development, and scenarios where persistence is not required.
//!
//! ## Features
//!
//! - **Thread-Safe**: Uses concurrent data structures for safe multi-threaded
//!   access
//! - **Fast Operations**: All operations are in-memory with O(1) or O(log n)
//!   complexity
//! - **Complete Implementation**: Supports all [`TriggerStore`] operations
//! - **Zero Dependencies**: No external storage systems required
//!
//! ## Performance Characteristics
//!
//! - **Memory Usage**: Proportional to the number of stored triggers
//! - **Lookup Speed**: O(1) for key-based lookups, O(log n) for range queries
//! - **Concurrency**: High concurrency with minimal lock contention
//! - **Durability**: Data is lost when the process terminates
//!
//! ## Use Cases
//!
//! - **Testing**: Unit and integration tests that need timer functionality
//! - **Development**: Local development without external dependencies
//! - **Ephemeral Workloads**: Short-lived processes where persistence isn't
//!   needed
//! - **Prototyping**: Quick validation of timer-based application logic
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use prosody::timers::store::TriggerStore;
//! use prosody::timers::store::memory::InMemoryTriggerStore;
//!
//! let store = InMemoryTriggerStore::new();
//! // Use store with any TriggerStore operations...
//! ```

use crate::Key;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use async_stream::try_stream;
use futures::TryStreamExt;
use futures::stream::Stream;
use scc::HashMap;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::join;
use tracing::Span;

/// An in-memory implementation of [`TriggerStore`] for testing and development.
///
/// [`InMemoryTriggerStore`] provides a complete implementation of the
/// [`TriggerStore`] trait using concurrent in-memory data structures. All data
/// is stored in memory and will be lost when the process terminates.
///
/// ## Thread Safety
///
/// This implementation uses [`scc::HashMap`] for concurrent access, allowing
/// multiple threads to safely read and write timer data simultaneously with
/// minimal lock contention.
///
/// ## Data Organization
///
/// The store maintains several concurrent hash maps to support efficient
/// operations:
/// - **Segments**: Metadata about timer segments
/// - **Slab Index**: Which slabs exist for each segment
/// - **Time Index**: Triggers organized by time ranges (slabs)
/// - **Key Index**: Triggers organized by entity keys
///
/// ## Memory Usage
///
/// Memory usage scales linearly with the number of:
/// - Active segments
/// - Active slabs
/// - Stored triggers
/// - Unique keys
///
/// For large-scale production deployments, consider using a persistent
/// storage implementation instead.
#[derive(Clone, Debug, Default)]
pub struct InMemoryTriggerStore(Arc<Inner>);

/// Internal storage structure for the in-memory trigger store.
///
/// This structure contains all the concurrent data structures needed to
/// implement the dual-indexing strategy required by [`TriggerStore`].
///
/// ## Index Organization
///
/// - `segments`: Maps segment IDs to their configuration (name and slab size)
/// - `segment_slabs`: Maps segment IDs to sets of active slab IDs
/// - `slab_triggers`: Maps slab specifications to sets of triggers (time-based
///   index)
/// - `key_triggers`: Maps (segment, key) pairs to sets of scheduled times
///   (key-based index)
///
/// ## Concurrency
///
/// All maps use [`scc::HashMap`] which provides lock-free concurrent access
/// with strong consistency guarantees. The [`BTreeSet`] values maintain
/// ordering for efficient range operations.
#[derive(Debug, Default)]
struct Inner {
    /// Segment metadata: segment ID -> (name, `slab_size`)
    segments: HashMap<SegmentId, (String, CompactDuration)>,

    /// Slab registry: segment ID -> set of active slab IDs
    segment_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,

    /// Time-based index: slab -> set of triggers in that time range
    slab_triggers: HashMap<Slab, BTreeSet<Trigger>>,

    /// Key-based index: (segment ID, key) -> set of scheduled times
    key_triggers: HashMap<(SegmentId, Key), BTreeSet<Trigger>>,
}

impl InMemoryTriggerStore {
    /// Creates a new empty in-memory trigger store.
    ///
    /// The store starts with no segments, slabs, or triggers. All data
    /// structures are initialized as empty and ready for use.
    ///
    /// # Returns
    ///
    /// A new [`InMemoryTriggerStore`] instance ready for operation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::store::memory::InMemoryTriggerStore;
    ///
    /// let store = InMemoryTriggerStore::new();
    /// // Store is now ready for TriggerStore operations
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TriggerStore for InMemoryTriggerStore {
    /// Error type for in-memory operations.
    ///
    /// Since this is an in-memory implementation, operations cannot fail
    /// due to I/O or network issues. [`Infallible`] indicates that all
    /// operations will complete successfully.
    type Error = Infallible;

    // Segment management operations
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        self.0
            .segments
            .upsert_async(segment.id, (segment.name, segment.slab_size))
            .await;

        Ok(())
    }

    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        Ok(self.0.segments.get_async(segment_id).await.map(|e| {
            let (name, slab_size) = e.get();
            Segment {
                id: *segment_id,
                name: name.clone(),
                slab_size: *slab_size,
            }
        }))
    }

    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        join!(
            self.0.segments.remove_async(segment_id),
            self.0.segment_slabs.remove_async(segment_id)
        );

        Ok(())
    }

    // Segment slabs
    fn get_slabs(&self, segment_id: &SegmentId) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        try_stream! {
            let Some(entry) = self.0.segment_slabs.get_async(segment_id).await else {
                return;
            };

            for &slab_id in entry.iter() {
                yield slab_id;
            }
        }
    }

    fn get_slab_range<B>(
        &self,
        segment_id: &SegmentId,
        range: B,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>>
    where
        B: RangeBounds<SlabId>,
    {
        try_stream! {
            let Some(entry) = self.0.segment_slabs.get_async(segment_id).await else {
                return;
            };

            for &slab_id in entry.range(range) {
                yield slab_id;
            }
        }
    }

    async fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.0
            .segment_slabs
            .entry_async(*segment_id)
            .await
            .or_default()
            .get_mut()
            .insert(slab_id);

        Ok(())
    }

    async fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        let Some(mut entry) = self.0.segment_slabs.get_async(segment_id).await else {
            return Ok(());
        };

        entry.get_mut().remove(&slab_id);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    // Slab triggers
    fn get_slab_triggers(&self, slab: &Slab) -> impl Stream<Item = Result<Trigger, Self::Error>> {
        try_stream! {
            let Some(set) = self.0.slab_triggers.get_async(slab).await else {
                return;
            };

            for trigger in set.iter() {
                yield trigger.clone();
            }
        }
    }

    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        self.0
            .slab_triggers
            .entry_async(slab)
            .await
            .or_default()
            .get_mut()
            .insert(trigger);

        Ok(())
    }

    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let Some(mut entry) = self.0.slab_triggers.get_async(slab).await else {
            return Ok(());
        };

        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::current(),
        };

        entry.get_mut().remove(&trigger);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        self.0.slab_triggers.remove_async(slab).await;
        Ok(())
    }

    // Key triggers
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        self.get_key_triggers(segment_id, key)
            .map_ok(|trigger| trigger.time)
    }

    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let map_key = (*segment_id, key.clone());
            let Some(entry) = self.0.key_triggers.get_async(&map_key).await else {
                return;
            };

            for trigger in entry.iter() {
                yield trigger.clone();
            }
        }
    }

    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, trigger.key.clone());
        self.0
            .key_triggers
            .entry_async(map_key)
            .await
            .or_default()
            .get_mut()
            .insert(trigger);

        Ok(())
    }

    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let map_key = (*segment_id, key.clone());
        let Some(mut entry) = self.0.key_triggers.get_async(&map_key).await else {
            return Ok(());
        };

        let trigger = Trigger {
            key: key.clone(),
            time,
            span: Span::none(),
        };

        entry.get_mut().remove(&trigger);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    async fn clear_key_triggers(&self, segment: &SegmentId, key: &Key) -> Result<(), Self::Error> {
        let map_key = (*segment, key.clone());
        self.0.key_triggers.remove_async(&map_key).await;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::timers::store::memory::InMemoryTriggerStore;
    use crate::trigger_store_tests;

    trigger_store_tests!(InMemoryTriggerStore, InMemoryTriggerStore::new());
}
