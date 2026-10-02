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
use crate::timers::slab::SlabId;
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::{Segment, SegmentId, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use scc::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod operations;

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
/// use prosody::timers::duration::CompactDuration;
/// use prosody::timers::store::memory::memory_store;
/// use prosody::timers::store::{Segment, SegmentVersion};
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

    /// Persisted `slab_watermark` per segment. Mirrors the static column on
    /// the Cassandra `timer_segments` table. `None` (or absent) means no
    /// watermark has been set yet → callers scan from slab 0.
    slab_watermarks: HashMap<SegmentId, SlabId>,

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
    #[must_use]
    pub fn new(segment: Segment) -> Self {
        Self {
            segment,
            inner: Arc::new(Inner::default()),
        }
    }
}

/// Creates a new in-memory trigger store scoped to the given segment.
///
/// Returns an implementation of `TriggerStore` backed by in-memory data
/// structures. This is the recommended way to create an in-memory store.
#[must_use]
pub fn memory_store(segment: Segment) -> TableAdapter<InMemoryTriggerStore> {
    TableAdapter::new(InMemoryTriggerStore::new(segment))
}

/// Hands out per-segment views of one shared in-memory trigger store.
///
/// The shared maps are memory mode's **durable substrate**: every
/// `create_store` call returns a store over the same maps, so stores minted
/// across partition (re)acquisitions observe the same rows — mirroring
/// [`MemoryDeduplicationStoreProvider`]. A fresh store per call would make
/// every "durable" row vanish with the store that wrote it. All maps are
/// keyed by [`SegmentId`], so sharing across segments cannot collide. The
/// state manager does not create stores here. It receives a clone of the
/// partition's store handle.
///
/// [`MemoryDeduplicationStoreProvider`]:
///     crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStoreProvider
#[derive(Clone, Debug, Default)]
pub struct InMemoryTriggerStoreProvider {
    inner: Arc<Inner>,
}

impl InMemoryTriggerStoreProvider {
    /// Creates a new provider backed by one fresh shared store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TriggerStoreProvider for InMemoryTriggerStoreProvider {
    type Store = TableAdapter<InMemoryTriggerStore>;

    fn create_store(&self, segment: Segment) -> Self::Store {
        TableAdapter::new(InMemoryTriggerStore {
            segment,
            inner: Arc::clone(&self.inner),
        })
    }
}

#[cfg(test)]
mod test {
    use super::{InMemoryTriggerStore, memory_store};
    use crate::timers::test_support::test_segment;
    use crate::trigger_store_tests;
    use std::convert::Infallible;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    // Low-level tests use InMemoryTriggerStore directly
    // High-level tests use TableAdapter<InMemoryTriggerStore>
    // Uses QuickCheck's default test count (no external systems involved)
    trigger_store_tests!(
        InMemoryTriggerStore,
        |slab_size| async move {
            Result::<_, Infallible>::Ok(InMemoryTriggerStore::new(test_segment("", slab_size)))
        },
        crate::timers::store::adapter::TableAdapter<InMemoryTriggerStore>,
        |slab_size| async move {
            Result::<_, Infallible>::Ok(memory_store(test_segment("", slab_size)))
        }
    );
}
