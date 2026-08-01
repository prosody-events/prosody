//! Assignment-local provisional-coordinate indexing for recovery.

use crate::state::CollectionId;
use crate::state::cell_key::CellKey;
use ahash::RandomState;

/// The memory backend's bounded-lifetime provisional-coordinate index.
///
/// A fresh index is created per assignment and drains entries when cells
/// settle. `seeded` means every provisional coordinate for that collection is
/// present in `coords`.
#[derive(Debug, Default)]
pub(super) struct WarmIndex {
    seeded: scc::HashSet<CollectionId, RandomState>,
    coords: scc::HashSet<(CollectionId, CellKey), RandomState>,
}

impl WarmIndex {
    pub(super) async fn is_seeded(&self, collection: &CollectionId) -> bool {
        self.seeded.contains_async(collection).await
    }

    pub(super) async fn mark_seeded(&self, collection: &CollectionId) {
        let _ = self.seeded.insert_async(collection.clone()).await;
    }

    pub(super) async fn record(&self, collection: &CollectionId, cell: &CellKey) {
        let _ = self
            .coords
            .insert_async((collection.clone(), cell.clone()))
            .await;
    }

    pub(super) async fn clear(&self, collection: &CollectionId, cell: &CellKey) {
        self.coords
            .remove_async(&(collection.clone(), cell.clone()))
            .await;
    }

    pub(super) fn snapshot(&self, collection: &CollectionId) -> Vec<CellKey> {
        let mut out = Vec::new();
        self.coords.iter_sync(|(id, cell)| {
            if id == collection {
                out.push(cell.clone());
            }
            true
        });
        out
    }
}
