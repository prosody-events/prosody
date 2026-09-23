//! Durability operations that keep the cell cache coherent.

use super::{Cached, PromoteCacheGuard, retry_delete, warn_skip};
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::marker::{EventMarker, MarkerState, ProvisionalStage, SectionClear};
use crate::state::store::{CellBuffer, CellStore, CoordinateBatch};
use bytes::Bytes;
use std::future::Future;

impl<L: CellStore> CellStore for Cached<L> {
    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // A pure lower read — no fjall step, so no cache-disabled branch is needed.
        self.lower.provisional_cell_at(collection, cell).await
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>>
    + Send
    + use<'a, L> {
        // A raw provisional read the committed-value cache cannot answer, so
        // delegate straight to the lower store — no fjall step, no cache-disabled
        // branch (like `provisional_cell_at`). Nothing is published into the
        // cache.
        self.lower.provisional_many(collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        stage: ProvisionalStage<'a>,
    ) -> Result<(), Self::Error> {
        // A disabled cache delegates the write to the lower store.
        if self.fjall.is_disabled() {
            return self.lower.write_provisional(collection, stage).await;
        }
        let stamped_at = self.fjall.clock().now_ms();
        self.lower.write_provisional(collection, stage).await?;
        // The committed value stays `prev` while the cell is provisional
        // (commit/abort republishes), so publish `prev` — never the in-flight
        // `data`.
        self.publish_written(collection, stage.writes(), stamped_at, |write| {
            Committed::new(write.prev().cloned())
        })
        .await;
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        if self.fjall.is_disabled() {
            return self.lower.write_resolved(collection, cells, clears).await;
        }
        // Remove each cleared section before the lower write.
        // A failed lower write leaves the section uncached.
        for clear in clears {
            retry_delete(&self.fjall, "clear section", || {
                self.fjall
                    .delete_section(collection.id(), clear.section(), &[])
            })
            .await;
        }
        // Remove old entries before the durable write.
        // Cancellation can then leave entries absent, but never stale.
        // Publish the new values only after the durable write succeeds.
        let cell_keys: CellBuffer<CellKey> = cells.iter().map(|(cell, _)| cell.clone()).collect();
        retry_delete(&self.fjall, "resolved cells", || {
            self.fjall.delete_batch(collection.id(), &cell_keys)
        })
        .await;
        // Pre-write anchor, establish-first — see `write_provisional`.
        let stamped_at = self.fjall.clock().now_ms();
        self.lower.write_resolved(collection, cells, clears).await?;
        self.publish_written(collection, cells, stamped_at, |data| {
            Committed::new(data.clone())
        })
        .await;
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        if self.fjall.is_disabled() {
            return self.lower.mark_resolved(collection, cells).await;
        }
        // The keys do not contain the new committed values.
        // Remove their old entries before the durable promotion.
        retry_delete(&self.fjall, "promote", || {
            self.fjall.delete_batch(collection.id(), cells)
        })
        .await;
        self.lower.mark_resolved(collection, cells).await?;
        Ok(())
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        let clears = marker.clears();
        if self.fjall.is_disabled() {
            return self
                .lower
                .commit_provisional(collection, marker, writes)
                .await;
        }
        let cache_guard = PromoteCacheGuard(Some(&self.fjall));
        if let Err(error) = self
            .lower
            .commit_provisional(collection, marker, writes)
            .await
        {
            self.evict_marker_cache_entries(collection.id(), marker)
                .await;
            cache_guard.complete();
            return Err(error);
        }
        // Publish only after the lower promote succeeds.
        // The event result is final before this function starts.
        // Remove the entries if this cache update fails.
        // Ruling: keep this transform. Delete-and-refill would leave staged
        // cells cold after each commit and cost one durable point read per hot
        // cell per event.
        if let Err(error) = self.fjall.commit_batch(collection.id(), writes).await {
            warn_skip("commit transform", &error);
            let cells: CellBuffer<CellKey> = writes.iter().map(|(cell, _)| cell.clone()).collect();
            retry_delete(&self.fjall, "commit transform fallback", || {
                self.fjall.delete_batch(collection.id(), &cells)
            })
            .await;
        }
        // Remove other entries from each cleared section.
        // Keep the staged entries that this settlement just published.
        if !clears.is_empty() {
            let staged: CellBuffer<CellKey> = writes.iter().map(|(cell, _)| cell.clone()).collect();
            for clear in clears {
                retry_delete(&self.fjall, "commit clear section", || {
                    self.fjall
                        .delete_section(collection.id(), clear.section(), &staged)
                })
                .await;
            }
        }
        cache_guard.complete();
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        if self.fjall.is_disabled() {
            return self.lower.abort_provisional(collection, writes).await;
        }
        let cells: CellBuffer<(CellKey, Option<Bytes>)> = writes
            .iter()
            .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
            .collect();
        // No pre-call action exists for the abort: the cached `prev` IS the
        // committed projection while an aborted marker stands, so on a lower
        // Err the result returns verbatim with the cache already correct. And
        // no section delete — an uncommitted clear never invalidates anything
        // (the cached pre-clear values are still the committed truth the
        // rollback restores).
        //
        // Pre-write anchor: the rollback re-writes `prev` with a fresh
        // `USING TTL`, so it co-expires from this instant. Forward to the
        // lower `abort_provisional` (not a bare `write_resolved`) so the lower
        // store's marker delete runs — the cache owns only the fjall
        // re-publish of the rolled-back `prev`, layered over the lower settle.
        let stamped_at = self.fjall.clock().now_ms();
        let result = self.lower.abort_provisional(collection, writes).await;
        if result.is_ok() {
            self.publish_written(collection, &cells, stamped_at, |data| {
                Committed::new(data.clone())
            })
            .await;
        }
        result
    }

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        self.lower.marker_state(collection).await
    }
}
