//! Memory mode's message defer store. All data is volatile, and the stores one
//! provider mints share its substrate.
//!
//! Uses [`scc::HashMap`] for lock-free concurrent access.

use super::MessageDeferStore;
use super::provider::MessageDeferStoreProvider;
use crate::consumer::middleware::defer::segment::{LazySegment, MemorySegmentStore};
use crate::{Key, Offset, Partition, SegmentId, Topic};

#[cfg(test)]
use crate::defer_store_tests;
use ahash::RandomState;
use scc::HashMap;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::Arc;

/// Topic, partition, and group a standalone [`MemoryMessageDeferStore`] is
/// scoped to.
const STANDALONE_SEGMENT: &str = "memory";

/// In-memory message defer store.
///
/// Lock-free via [`scc::HashMap`]. Each key maps to a `BTreeSet<Offset>`
/// (sorted queue) plus a shared retry counter. Thread-safe and cheap to clone.
///
/// The store reads and writes one segment of the substrate its provider owns.
/// Partition isolation comes from the segment id in every map key.
#[derive(Clone, Debug)]
pub struct MemoryMessageDeferStore {
    segment: LazySegment<MemorySegmentStore>,
    inner: Arc<Inner>,
}

impl MemoryMessageDeferStore {
    /// A standalone store over its own substrate and one fixed segment.
    ///
    /// Callers that need several segments to share rows mint their stores from
    /// one [`MemoryMessageDeferStoreProvider`] instead.
    #[must_use]
    pub fn new() -> Self {
        MemoryMessageDeferStoreProvider::default().create_store(
            Topic::from(STANDALONE_SEGMENT),
            0,
            STANDALONE_SEGMENT,
            0,
        )
    }

    /// The segment this store addresses, registering it on first call.
    async fn segment_id(&self) -> Result<SegmentId, Infallible> {
        Ok(self.segment.get().await?.id())
    }
}

impl Default for MemoryMessageDeferStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Storage: (`segment`, `key`) → (`sorted offsets`, `retry_count`).
#[derive(Debug)]
struct Inner {
    deferred: HashMap<(SegmentId, Key), (BTreeSet<Offset>, u32), RandomState>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            deferred: HashMap::with_hasher(RandomState::new()),
        }
    }
}

impl MessageDeferStore for MemoryMessageDeferStore {
    type Error = Infallible;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        self.inner
            .deferred
            .entry_async((segment, Arc::clone(key)))
            .await
            .and_modify(|(offsets, retry_count)| {
                offsets.insert(offset);
                *retry_count = 0;
            })
            .or_insert_with(|| {
                let mut offsets = BTreeSet::new();
                offsets.insert(offset);
                (offsets, 0)
            });

        Ok(())
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        let segment = self.segment_id().await?;
        let result = self
            .inner
            .deferred
            .get_async(&(segment, Arc::clone(key)))
            .await
            .and_then(|entry| {
                let (offsets, retry_count) = entry.get();
                offsets.first().map(|&offset| (offset, *retry_count))
            });

        Ok(result)
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        self.inner
            .deferred
            .entry_async((segment, Arc::clone(key)))
            .await
            .and_modify(|(offsets, _)| {
                offsets.insert(offset);
            })
            .or_insert_with(|| {
                // Shouldn't happen (should use defer_first_message first)
                // but handle gracefully with retry_count=0
                let mut offsets = BTreeSet::new();
                offsets.insert(offset);
                (offsets, 0)
            });

        Ok(())
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        // Drop the entry when its last offset is removed. Once all deferred
        // messages for a key are processed, the entry is dead state
        // (retry_count = 0 ≡ retry_count absent), matching Cassandra's
        // delete_key on min-only-row removal. Atomic via remove_if_async.
        let segment = self.segment_id().await?;
        let _ = self
            .inner
            .deferred
            .remove_if_async(&(segment, Arc::clone(key)), |(offsets, _)| {
                offsets.remove(&offset);
                offsets.is_empty()
            })
            .await;

        Ok(())
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        // No-op on a key with no offsets. Production only calls this with an
        // active deferred message present; creating an entry here would leave
        // an orphan, violating "no entry after all messages are processed."
        let segment = self.segment_id().await?;
        let _ = self
            .inner
            .deferred
            .entry_async((segment, Arc::clone(key)))
            .await
            .and_modify(|(_, current)| {
                *current = retry_count;
            });

        Ok(())
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        let segment = self.segment_id().await?;
        self.inner
            .deferred
            .remove_async(&(segment, Arc::clone(key)))
            .await;
        Ok(())
    }
}

/// Hands out per-segment views of one shared in-memory message defer store.
///
/// The shared map is memory mode's **durable substrate**: two stores minted
/// for the same segment observe each other's rows, exactly as two Cassandra
/// stores over one partition do. A fresh map per `create_store` would make
/// every durable row vanish with the store that wrote it. The map is keyed by
/// segment id and key, so segments cannot collide.
///
/// A row leaves the map when its queue drains or `delete_key` runs. A queue a
/// revoked partition leaves behind stays until the provider drops, because
/// memory mode has no TTL. Cassandra reclaims the same rows through the base
/// TTL every deferred write binds.
#[derive(Clone, Debug, Default)]
pub struct MemoryMessageDeferStoreProvider {
    segments: MemorySegmentStore,
    inner: Arc<Inner>,
}

impl MemoryMessageDeferStoreProvider {
    /// Creates a provider that registers its segments in `segments`.
    #[must_use]
    pub(crate) fn new(segments: MemorySegmentStore) -> Self {
        Self {
            segments,
            inner: Arc::new(Inner::default()),
        }
    }

    /// Keys with a deferred queue in one segment. A snapshot of the whole
    /// shared map; it drops with the caller's stream.
    pub(crate) async fn keys(&self, segment: SegmentId) -> Vec<Key> {
        let mut out = Vec::new();
        self.inner
            .deferred
            .iter_async(|(id, key), _| {
                if *id == segment {
                    out.push(Arc::clone(key));
                }
                true
            })
            .await;
        out
    }
}

impl MessageDeferStoreProvider for MemoryMessageDeferStoreProvider {
    type Store = MemoryMessageDeferStore;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
        _cache_size: usize,
    ) -> Self::Store {
        MemoryMessageDeferStore {
            segment: LazySegment::new(
                self.segments.clone(),
                topic,
                partition,
                Arc::from(consumer_group),
            ),
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests;
