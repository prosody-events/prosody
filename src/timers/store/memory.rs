//! In-memory implementation of the `TriggerStore` trait.
//!
//! This module provides `InMemoryTriggerStore`, a lock-free, concurrent,
//! in-memory timer data store. It maintains dual indices for time-based and
//! key-based lookups, enabling efficient insertion, deletion, and queries
//! without external dependencies.
//!
//! # Data Organization
//!
//! - **Segments**: Metadata mapping a segment ID to its name and slab size.
//! - **Slab Index**: Tracks which slab IDs are registered under each segment.
//! - **Time Index**: Maps each `Slab` to the set of `Trigger`s scheduled within
//!   it.
//! - **Key Index**: Maps each (segment, key) pair to the set of `Trigger`s for
//!   that key.
//!
//! All maps use [`scc::HashMap`] for concurrent access, and values are stored
//! in [`BTreeSet`] to maintain sorted order where needed.

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::TryStreamExt;
use futures::stream::Stream;
use scc::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::join;

/// In-memory, concurrent implementation of
/// [`TriggerStore`](super::TriggerStore) for testing and development.
///
/// All data is held in memory; timers and segments are lost when the process
/// exits. This store supports the full `TriggerStore` trait API with
/// low-latency, operations.
///
/// # Examples
///
/// ```rust,no_run
/// use prosody::timers::store::memory::memory_store;
/// use prosody::timers::store::{Segment, SegmentVersion};
/// use prosody::timers::duration::CompactDuration;
/// use uuid::Uuid;
///
/// let segment = Segment {
///     id: Uuid::new_v4(),
///     name: "example".to_string(),
///     slab_size: CompactDuration::new(600),
///     version: SegmentVersion::V2,
/// };
/// let store = memory_store(segment);
/// // Now you can call TriggerStore methods on `store`
/// ```
///
/// For tests that need the concrete type, you can use
/// `InMemoryTriggerStore::new()` directly.
#[derive(Clone, Debug)]
pub struct InMemoryTriggerStore {
    segment: Segment,
    inner: Arc<Inner>,
}

/// Partition key for slab triggers: (`segment_id`, `slab_size`, `slab_id`)
type SlabPartitionKey = (SegmentId, CompactDuration, SlabId);

/// Clustering key for slab triggers: (`timer_type`, `key`, `time`)
type SlabClusteringKey = (TimerType, Key, CompactDateTime);

/// Partition key for key triggers: (`segment_id`, `key`)
type KeyPartitionKey = (SegmentId, Key);

/// Clustering key for key triggers: (`timer_type`, `time`)
type KeyClusteringKey = (TimerType, CompactDateTime);

/// Internal state for `InMemoryTriggerStore`.
///
/// Maintains concurrent maps to support dual indexing and segment management.
#[derive(Debug, Default)]
struct Inner {
    segments: HashMap<SegmentId, (String, CompactDuration, SegmentVersion)>,

    /// Maps each segment ID to its active set of slab IDs.
    segment_slabs: HashMap<SegmentId, BTreeSet<SlabId>>,

    /// V2 time-based index: maps (`segment_id`, `slab_size`, `slab_id`) to a
    /// map of triggers organized by (`timer_type`, `key`, `time`). Matches v2
    /// `timer_typed_slabs` table structure: partition key + clustering key.
    /// This allows efficient queries for all timer types within a slab.
    slab_triggers: HashMap<SlabPartitionKey, BTreeMap<SlabClusteringKey, Trigger>>,

    /// Key-based index: maps (`segment_id`, `key`) to a map of triggers
    /// organized by (`timer_type`, `time`). Matches v2 `timer_typed_keys`
    /// table structure. This allows efficient queries for all timer types for a
    /// key.
    key_triggers: HashMap<KeyPartitionKey, BTreeMap<KeyClusteringKey, Trigger>>,
}

impl InMemoryTriggerStore {
    /// Create a new, empty in-memory trigger store scoped to the given segment.
    ///
    /// # Returns
    ///
    /// A ready-to-use `InMemoryTriggerStore`.
    #[must_use]
    pub fn new(segment: Segment) -> Self {
        Self {
            segment,
            inner: Arc::new(Inner::default()),
        }
    }
}

impl TriggerOperations for InMemoryTriggerStore {
    type Error = Infallible;

    fn segment(&self) -> &Segment {
        &self.segment
    }

    // -- Segment management operations --

    async fn insert_segment(&self) -> Result<(), Self::Error> {
        let segment = &self.segment;
        self.inner
            .segments
            .upsert_async(
                segment.id,
                (segment.name.clone(), segment.slab_size, segment.version),
            )
            .await;

        Ok(())
    }

    async fn get_segment(&self) -> Result<Option<Segment>, Self::Error> {
        let segment_id = self.segment.id;
        Ok(self.inner.segments.get_async(&segment_id).await.map(|e| {
            let (name, slab_size, version) = e.get();
            Segment {
                id: segment_id,
                name: name.clone(),
                slab_size: *slab_size,
                version: *version,
            }
        }))
    }

    async fn delete_segment(&self) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        // Remove from both the segments map and the segment_slabs map.
        join!(
            self.inner.segments.remove_async(&segment_id),
            self.inner.segment_slabs.remove_async(&segment_id)
        );

        Ok(())
    }

    // -- Slab management operations --

    fn get_slabs(&self) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        let segment_id = self.segment.id;
        try_stream! {
            let Some(entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
                return;
            };

            for &slab_id in entry.iter() {
                yield slab_id;
            }
        }
    }

    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> {
        let segment_id = self.segment.id;
        try_stream! {
            let Some(entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
                return;
            };

            for &slab_id in entry.range(range) {
                yield slab_id;
            }
        }
    }

    async fn insert_slab(&self, slab: Slab) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        self.inner
            .segment_slabs
            .entry_async(segment_id)
            .await
            .or_default()
            .get_mut()
            .insert(slab.id());

        Ok(())
    }

    async fn delete_slab(&self, slab_id: SlabId) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let Some(mut entry) = self.inner.segment_slabs.get_async(&segment_id).await else {
            return Ok(());
        };

        entry.get_mut().remove(&slab_id);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    // -- Slab trigger operations (time index) --

    /// Stream all triggers of a specific type within a given slab.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab specification to query.
    /// * `timer_type` - The timer type to query (Application or
    ///   `DeferredMessage`).
    ///
    /// # Returns
    ///
    /// A stream of `Trigger` for the specified type. Never yields an error.
    fn get_slab_triggers(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> {
        let segment_id = self.segment.id;
        let slab_size = slab.size();
        let slab_id = slab.id();

        try_stream! {
            let partition_key = (segment_id, slab_size, slab_id);
            let Some(triggers_map) = self.inner.slab_triggers.get_async(&partition_key).await else {
                return;
            };

            // Filter triggers by timer_type using range query on BTreeMap
            for ((t_type, _key, _time), trigger) in triggers_map.iter() {
                if *t_type == timer_type {
                    yield trigger.clone();
                }
            }
        }
    }

    /// Stream ALL triggers within a slab across all timer types.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab specification to query.
    ///
    /// # Returns
    ///
    /// A stream of all `Trigger`s in the slab, regardless of timer type.
    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> {
        let segment_id = self.segment.id;
        let slab_size = slab.size();
        let slab_id = slab.id();

        try_stream! {
            let partition_key = (segment_id, slab_size, slab_id);
            let Some(triggers_map) = self.inner.slab_triggers.get_async(&partition_key).await else {
                return;
            };

            // Stream all triggers from the partition
            for (_clustering_key, trigger) in triggers_map.iter() {
                yield trigger.clone();
            }
        }
    }

    /// Insert a trigger into a slab's time index.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to receive the trigger.
    /// * `trigger` - The `Trigger` to add.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let partition_key = (self.segment.id, slab.size(), slab.id());
        let clustering_key = (trigger.timer_type, trigger.key.clone(), trigger.time);

        self.inner
            .slab_triggers
            .entry_async(partition_key)
            .await
            .or_default()
            .get_mut()
            .insert(clustering_key, trigger);

        Ok(())
    }

    /// Delete a specific trigger from a slab's time index.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to modify.
    /// * `timer_type` - The timer type (Application or `DeferredMessage`).
    /// * `key` - Trigger's key.
    /// * `time` - Trigger's scheduled time.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let partition_key = (self.segment.id, slab.size(), slab.id());
        let clustering_key = (timer_type, key.clone(), time);

        let Some(mut entry) = self.inner.slab_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        entry.get_mut().remove(&clustering_key);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers from a slab's time index across ALL timer types.
    ///
    /// This clears both Application and `DeferredMessage` timers. Used for
    /// `slab_size` migration and cleanup operations.
    ///
    /// # Arguments
    ///
    /// * `slab` - Slab to clear.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        // Clear the entire partition (all timer types)
        let partition_key = (self.segment.id, slab.size(), slab.id());
        self.inner.slab_triggers.remove_async(&partition_key).await;
        Ok(())
    }

    // -- Key trigger operations (entity index) --

    /// Stream all scheduled times for a given key and timer type.
    ///
    /// # Returns
    ///
    /// A stream of `CompactDateTime`. Never yields an error.
    fn get_key_times(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        self.get_key_triggers(timer_type, key)
            .map_ok(|trigger| trigger.time)
    }

    /// Stream all triggers for a given key and timer type.
    ///
    /// # Returns
    ///
    /// A stream of `Trigger`. Never yields an error.
    fn get_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = self.segment.id;
        try_stream! {
            let partition_key = (segment_id, key.clone());
            let Some(triggers_map) = self.inner.key_triggers.get_async(&partition_key).await else {
                return;
            };

            // Filter triggers by timer_type
            for ((t_type, _time), trigger) in triggers_map.iter() {
                if *t_type == timer_type {
                    yield trigger.clone();
                }
            }
        }
    }

    /// Stream ALL triggers for a given key across all timer types.
    ///
    /// # Returns
    ///
    /// A stream of all `Trigger`s for the key, regardless of timer type.
    fn get_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = self.segment.id;
        try_stream! {
            let partition_key = (segment_id, key.clone());
            let Some(triggers_map) = self.inner.key_triggers.get_async(&partition_key).await else {
                return;
            };

            // Stream all triggers from the partition
            for (_clustering_key, trigger) in triggers_map.iter() {
                yield trigger.clone();
            }
        }
    }

    /// Insert a trigger into the key-based index.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn insert_key_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, trigger.key.clone());
        let clustering_key = (trigger.timer_type, trigger.time);

        self.inner
            .key_triggers
            .entry_async(partition_key)
            .await
            .or_default()
            .get_mut()
            .insert(clustering_key, trigger);

        Ok(())
    }

    /// Delete a specific trigger from the key-based index.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn delete_key_trigger(
        &self,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        let clustering_key = (timer_type, time);

        let Some(mut entry) = self.inner.key_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        entry.get_mut().remove(&clustering_key);
        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Remove all triggers for a key and timer type from the key index.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        let Some(mut entry) = self.inner.key_triggers.get_async(&partition_key).await else {
            return Ok(());
        };

        // Remove all triggers matching the timer_type
        entry
            .get_mut()
            .retain(|(t_type, _time), _trigger| *t_type != timer_type);

        if entry.is_empty() {
            let _ = entry.remove();
        }

        Ok(())
    }

    /// Atomically clears existing timers and schedules a new one in the key
    /// index.
    ///
    /// For in-memory store, this simply clears and inserts.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_and_schedule_key(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, trigger.key.clone());
        let clustering_key = (trigger.timer_type, trigger.time);

        // Get or create the partition entry
        let mut entry = self
            .inner
            .key_triggers
            .entry_async(partition_key)
            .await
            .or_default();

        // Clear all existing triggers for this timer_type
        entry
            .get_mut()
            .retain(|(t_type, _time), _| *t_type != trigger.timer_type);

        // Insert the new trigger
        entry.get_mut().insert(clustering_key, trigger);

        Ok(())
    }

    /// Remove all triggers for a key across ALL timer types from the key index.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn clear_key_triggers_all_types(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let partition_key = (segment_id, key.clone());
        self.inner.key_triggers.remove_async(&partition_key).await;
        Ok(())
    }

    // -- V1 migration methods --

    /// Update segment metadata including version and slab size.
    ///
    /// # Errors
    ///
    /// Never returns an error.
    async fn update_segment_version(
        &self,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        if let Some(entry) = self.inner.segments.get_async(&segment_id).await {
            let (name, ..) = entry.get();
            let name = name.clone();
            drop(entry);
            self.inner
                .segments
                .upsert_async(segment_id, (name, new_slab_size, new_version))
                .await;
        }
        Ok(())
    }
}

/// Creates a new in-memory trigger store scoped to the given segment.
///
/// Returns an implementation of `TriggerStore` backed by in-memory data
/// structures. This is the recommended way to create an in-memory store.
///
/// # Example
///
/// ```rust,ignore
/// let store = memory_store(segment);
/// let manager = TimerManager::new(..., store);
/// ```
#[must_use]
pub fn memory_store(segment: Segment) -> TableAdapter<InMemoryTriggerStore> {
    TableAdapter::new(InMemoryTriggerStore::new(segment))
}

/// Trivial provider for tests that creates a fresh `InMemoryTriggerStore` per
/// partition.
#[derive(Clone, Debug)]
pub struct InMemoryTriggerStoreProvider;

impl InMemoryTriggerStoreProvider {
    /// Creates a new provider.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for InMemoryTriggerStoreProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl TriggerStoreProvider for InMemoryTriggerStoreProvider {
    type Store = TableAdapter<InMemoryTriggerStore>;

    fn create_store(&self, segment: Segment) -> Self::Store {
        memory_store(segment)
    }
}

#[cfg(test)]
mod test {
    use super::{InMemoryTriggerStore, memory_store};
    use crate::timers::store::{Segment, SegmentVersion};
    use crate::trigger_store_tests;
    use std::convert::Infallible;
    use uuid::Uuid;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    // Low-level tests use InMemoryTriggerStore directly
    // High-level tests use TableAdapter<InMemoryTriggerStore>
    // Uses QuickCheck's default test count (no external systems involved)
    trigger_store_tests!(
        InMemoryTriggerStore,
        |slab_size| async move {
            let segment = Segment {
                id: Uuid::new_v4(),
                name: String::new(),
                slab_size,
                version: SegmentVersion::V3,
            };
            Result::<_, Infallible>::Ok(InMemoryTriggerStore::new(segment))
        },
        crate::timers::store::adapter::TableAdapter<InMemoryTriggerStore>,
        |slab_size| async move {
            let segment = Segment {
                id: Uuid::new_v4(),
                name: String::new(),
                slab_size,
                version: SegmentVersion::V3,
            };
            Result::<_, Infallible>::Ok(memory_store(segment))
        }
    );
}
