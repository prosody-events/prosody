//! In-memory dirty Value workspace.
//!
//! [`DirtyValueStore`] is the per-event write buffer: handler `set`/`clear`
//! ops land here, and the durability boundary reads them back through
//! [`PendingOpSource`] when it stages provisional cells. There is exactly one
//! dirty store per event, owned uniquely by the session — never shared, never
//! cloned — so it is a bare lock-free [`scc::HashMap`], not an `Arc<Mutex<…>>`.
//!
//! Value collections are last-writer-wins, so the store keeps only the compact
//! final op per collection. A collection absent from the map is untouched, so
//! its read returns [`Read::Unknown`] and higher layers fall through to the
//! committed value.
//!
//! The dirty store is volatile and rebuilt per event — it is never a
//! durability or recovery source. Crash recovery runs off the Cassandra
//! provisional cells and the commit oracle.

use super::value::{PendingOpSource, ValueKind, ValueOp, ValueStore};
use super::{CollectionId, PendingOps, Read};
use ahash::RandomState;
use bytes::Bytes;
use std::convert::Infallible;
use std::option::IntoIter as OptionIntoIter;

/// In-memory dirty Value store: one compacted op per touched collection.
#[derive(Debug, Default)]
pub struct DirtyValueStore {
    entries: scc::HashMap<CollectionId<ValueKind>, ValueOp, RandomState>,
}

impl DirtyValueStore {
    /// Creates an empty dirty Value store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Discards every buffered op in place, reusing the allocation. The
    /// session calls this from `reset` at an attempt boundary; per-key
    /// serialization guarantees no handler op is in flight when it runs.
    pub(crate) fn clear_all(&self) {
        self.entries.clear_sync();
    }
}

impl ValueStore for DirtyValueStore {
    type Error = Infallible;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        Ok(self
            .entries
            .read_async(collection, |_, op| match op {
                // Cheap `Bytes` refcount bump, not a payload copy.
                ValueOp::Set { payload } => Read::Present(payload.clone()),
                ValueOp::Clear => Read::Absent,
            })
            .await
            .unwrap_or(Read::Unknown))
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: &'a [u8],
    ) -> Result<(), Self::Error> {
        // The buffered op owns its payload, so the borrowed slice is copied
        // once into an owned `Bytes` here.
        self.entries
            .upsert_async(
                collection.clone(),
                ValueOp::Set {
                    payload: Bytes::copy_from_slice(payload),
                },
            )
            .await;
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.entries
            .upsert_async(collection.clone(), ValueOp::Clear)
            .await;
        Ok(())
    }
}

impl PendingOpSource<ValueKind> for DirtyValueStore {
    type Error = Infallible;
    type Ops<'a> = OptionIntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        Ok(self
            .entries
            .read_sync(collection, |_, op| op.clone())
            .map(PendingOps::single))
    }

    fn clear_pending_ops(&self, collection: &CollectionId<ValueKind>) -> Result<(), Self::Error> {
        self.entries.remove_sync(collection);
        Ok(())
    }
}
