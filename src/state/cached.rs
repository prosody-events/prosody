//! Fjall write-through cache over the durable lower store.
//!
//! [`Cached`] fronts a lower [`CellStore`] (the durable
//! [`CassandraStore`](crate::state::cassandra::CassandraStore)) with a
//! [`FjallCellCache`] of committed cell values. It is **transitional**
//! (decision D4): `get` serves a fjall **point** hit and otherwise falls
//! through to the lower store and populates fjall; `scan_cells` and
//! `provisional_cells` always reach the lower store. The coverage model — fjall
//! serving complete *scans* under a sealed `Covered` witness — is a later,
//! pure optimization, so until it lands there is no incomplete-cache risk.
//!
//! The cache is a **hint**: every fjall failure is logged and degraded (a miss
//! / a skipped patch), never surfaced — correctness rests on the lower store,
//! so [`Cached::Error`](CellStore::Error) is just the lower store's error.
//!
//! # Coherence
//!
//! The committed-value cache must never serve a stale value. A stage write
//! leaves the committed value unchanged, but a `write_resolved` /
//! `mark_resolved` changes it, and `mark_resolved` (promote) carries no data
//! here (it only nulls `prev`/`event` on the durable row). So every mutator
//! **invalidates** the touched fjall cells; the next read misses and
//! repopulates from the now-current lower store. Simple and correct for a
//! read-through cache.

use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Scan};
use super::event_ref::EventRef;
use super::fjall::FjallCellCache;
use super::identity::{CollectionId, CollectionRef};
use super::store::CellStore;
use bytes::Bytes;
use futures::Stream;
use tracing::warn;

/// A fjall write-through cache over a lower committed [`CellStore`].
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
}

impl<L> Cached<L> {
    /// Composes a cache over `lower`, serving committed-value hits from
    /// `fjall`.
    #[must_use]
    pub fn new(fjall: FjallCellCache, lower: L) -> Self {
        Self { fjall, lower }
    }

    /// Best-effort fjall invalidation: a failure leaves a possibly-stale entry,
    /// but the next read still reaches the lower store, so log and degrade.
    async fn invalidate(&self, collection: &CollectionId, cell: &CellKey) {
        if let Err(error) = self.fjall.invalidate(collection, cell).await {
            warn!(error = %error, "committed-value cache invalidation failed; may be stale");
        }
    }
}

impl<L> CellStore for Cached<L>
where
    L: CellStore,
{
    type Error = L::Error;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        match self.fjall.get(collection, cell).await {
            Ok(Some(committed)) => return Ok(committed),
            Ok(None) => {}
            Err(error) => warn_skip("read", &error),
        }
        let committed = self.lower.get(collection, cell, own).await?;
        if let Err(error) = self.fjall.put(collection, cell, &committed).await {
            warn_skip("populate", &error);
        }
        Ok(committed)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Transitional: scans always reach the lower store (no coverage yet).
        self.lower.scan_cells(collection, scan, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.lower.provisional_cells(collection)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.lower.write_provisional(collection, writes).await?;
        for (cell, _) in writes {
            self.invalidate(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.lower.write_resolved(collection, cells).await?;
        for (cell, _) in cells {
            self.invalidate(collection.id(), cell).await;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.lower.mark_resolved(collection, cells).await?;
        for cell in cells {
            self.invalidate(collection.id(), cell).await;
        }
        Ok(())
    }
}

/// Logs a degraded fjall cache operation (the cache is a hint).
fn warn_skip(op: &str, error: &super::fjall::FjallValueStoreError) {
    warn!(error = %error, "committed-value cache {op} failed; degrading");
}
