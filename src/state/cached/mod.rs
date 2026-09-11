//! A write-through cache of committed cells over durable storage.
//!
//! The cache follows five invariants:
//!
//! - **KV1 — a hit is current.** Each live entry equals the committed cell. A
//!   failed required removal disables the cache.
//! - **KV2 — a miss is unknown.** A miss reads durable storage and caches its
//!   result, including absence.
//! - **KV3 — scans bypass the cache.** A scan uses durable storage and does not
//!   change the cache.
//! - **KV4 — a fill cannot overwrite a newer write.** Per-key dispatch and the
//!   session operation gate serialize reads and writes. Admission and
//!   settlement do not overlap handler operations.
//! - **KV5 — a successful update retains warmth.** Expiry, reassignment,
//!   clears, and cache errors can force a durable read.
//!
//! Mutators publish values after the durable write succeeds. Direct writes
//! remove old entries before the write. Promotion retains the expiry from the
//! stage. Cancellation during promotion disables the cache until the assignment
//! ends. A cache failure does not change the durable operation's result.
//! Required removals retry within a fixed budget.
//!
//! All clones share the disabled state. Each operation checks that state once
//! and completes work already accepted. A disabled cache sends reads to durable
//! storage. The admission check set also stops its disk operations.
//!
//! An entry must not outlive its durable cell. Direct writes use the time
//! before the write plus the collection TTL. Promotion preserves that expiry.
//! Read fills use the remaining durable TTL. All stamps round down to whole
//! seconds.

pub(crate) mod metrics;

use self::metrics::{CacheResult, CellMetrics, Source};
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::fjall::{CacheRead, FjallCellCache, FjallCellCacheError};
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, MarkerState, SectionClear};
use super::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, PresenceBatch,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use futures::Stream;
use quanta::Instant;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Delay between attempts of a must-succeed repair delete ([`retry_delete`])
/// while fjall is transiently failing. Zero under test: the bounded-retry
/// tests assert completes-or-disables, never pacing.
#[cfg(not(test))]
const DELETE_RETRY_DELAY: Duration = Duration::from_millis(100);
#[cfg(test)]
const DELETE_RETRY_DELAY: Duration = Duration::ZERO;

/// Maximum cache removal attempts before cache disablement.
pub(crate) const DELETE_RETRY_BUDGET: usize = 5;

/// A write-through fjall K/V cache over a lower committed `CellStore`.
///
/// A shared cache handle for one partition assignment.
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
    metrics: CellMetrics,
}

impl<L> Cached<L> {
    /// Composes a committed-value cache over `lower`.
    #[must_use]
    pub fn new(fjall: FjallCellCache, lower: L) -> Self {
        Self {
            fjall,
            lower,
            metrics: CellMetrics::default(),
        }
    }

    /// Replaces the metric instruments for a test-local meter.
    #[cfg(test)]
    pub(crate) fn with_metrics(mut self, metrics: CellMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// The fjall expiry for a cell **read back** from the lower store now: the
    /// clock is read at fill time and `remaining` is the already-decremented
    /// `TTL(data)`, so [`expiry_at`] stamps `floor(now) + remaining` (see the
    /// module's TTL co-expiry doc).
    fn expiry_for(&self, remaining: Option<CompactDuration>) -> u64 {
        expiry_at(self.fjall.clock().now_ms(), remaining)
    }

    /// The absolute expiry stamped on a cell's current fjall entry (`None` if
    /// absent) — the co-expiry-anchor property asserts this equals the modeled
    /// durable death after every mutation.
    #[cfg(test)]
    pub(crate) async fn stored_expiry(
        &self,
        collection: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<u64>, FjallCellCacheError> {
        self.fjall.stored_expiry(collection, cell).await
    }

    /// Reads the lower batch and publishes every position with its co-expiry
    /// stamp. Value reads and presence reads share this fill.
    async fn fill_batch(
        &self,
        collection: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
        op: &'static str,
    ) -> Result<CommittedBatch, L::Error>
    where
        L: CellStore,
    {
        // Sample time before the lower read so cache entries cannot outlive durable
        // rows.
        let stamped_at = self.fjall.clock().now_ms();
        // A failed lower read publishes no cache entries.
        let filled: CacheBatch = self
            .lower
            .get_many_for_cache(collection, section, batch)
            .await?;
        // Publish present and absent cells atomically. A failed fill preserves existing
        // entries.
        let projected =
            batch
                .iter()
                .zip(filled.iter())
                .map(|(coordinate, (committed, remaining))| {
                    (
                        CellKey {
                            section,
                            coordinate: coordinate.clone(),
                        },
                        committed.clone(),
                        expiry_at(stamped_at, *remaining),
                    )
                });
        if let Err(error) = self.fjall.put_batch(collection, projected).await {
            warn_skip("populate batch", &error);
            self.metrics.cache_error(op, "fill");
        }
        Ok(filled.into_iter().map(|(committed, _)| committed).collect())
    }

    /// Removes cache entries that a Staged payload can change.
    ///
    /// A failed removal disables the cache.
    async fn evict_marker_cache_entries(&self, collection: &CollectionId, marker: &EventMarker) {
        retry_delete(&self.fjall, "marker staged", || {
            self.fjall.delete_batch(collection, marker.staged())
        })
        .await;
        for clear in marker.clears() {
            retry_delete(&self.fjall, "marker section", || {
                self.fjall.delete_section(collection, clear.section(), &[])
            })
            .await;
        }
    }

    /// Publishes each touched cell's `projection` after a successful
    /// `lower.write` (establish-then-publish) in **one** atomic fjall batch
    /// ([`FjallCellCache::put_batch`]). `stamped_at` is a clock reading taken
    /// **before** the lower write; [`expiry_at`] floors it to match
    /// Cassandra's TTL resolution (see the module's TTL co-expiry doc). The
    /// collection's write TTL is the full TTL (the value was just written).
    /// `project` computes each cell's committed projection from its batch
    /// entry.
    ///
    /// A failed cache update removes all old entries for these cells. The
    /// durable value has moved, so an old entry would serve the pre-write
    /// value.
    async fn publish_written<T>(
        &self,
        collection: &CollectionRef,
        cells: &[(CellKey, T)],
        stamped_at: u64,
        project: impl Fn(&T) -> Committed,
    ) {
        let expiry = expiry_at(stamped_at, collection.ttl());
        // Project each touched cell and publish atomically, streaming the batch
        // input straight into `put_batch` (no intermediate collect): a multi-cell
        // update is never torn, and the whole settle is one blocking thread-hop
        // instead of N.
        let projected = cells
            .iter()
            .map(|(cell, value)| (cell.clone(), project(value), expiry));
        if let Err(error) = self.fjall.put_batch(collection.id(), projected).await {
            warn_skip("publish", &error);
            // failed-publish cache guard repair: rebuild the delete keys from the `cells`
            // param.
            let keys: CellBuffer<CellKey> = cells.iter().map(|(cell, _)| cell.clone()).collect();
            retry_delete(&self.fjall, "publish repair", || {
                self.fjall.delete_batch(collection.id(), &keys)
            })
            .await;
        }
    }
}

/// Disables stale cache entries if a promote stops before publication
/// completes.
struct PromoteCacheGuard<'a>(Option<&'a FjallCellCache>);

impl PromoteCacheGuard<'_> {
    fn complete(mut self) {
        self.0.take();
    }
}

impl Drop for PromoteCacheGuard<'_> {
    fn drop(&mut self) {
        if let Some(cache) = self.0 {
            cache.disable();
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
    ) -> Result<Committed, Self::Error> {
        let started = Instant::now();
        // Send the read to durable storage when the cache is disabled.
        if self.fjall.is_disabled() {
            let loaded = self.lower.get(collection, cell).await;
            self.metrics
                .point(started, Source::Store, CacheResult::Disabled, &loaded);
            return loaded;
        }
        let cache_result = match self.fjall.get(collection, cell).await {
            // A hit (Present value or Absent tag) is the current committed
            // projection (KV1); serve it verbatim with zero lower reads.
            Ok(CacheRead::Hit(committed)) => {
                let loaded = Ok(committed);
                self.metrics
                    .point(started, Source::Cache, CacheResult::Hit, &loaded);
                return loaded;
            }
            // A Miss asserts nothing and an Expired entry is a co-expiry gap
            // (KV2): fall through and re-publish.
            Ok(CacheRead::Miss) => CacheResult::Miss,
            Ok(CacheRead::Expired) => CacheResult::Expired,
            // A fjall read failure degrades this one read to a durable one.
            Err(error) => {
                warn_skip("read", &error);
                self.metrics.cache_error("get", "lookup");
                CacheResult::Error
            }
        };
        let loaded = async {
            let (committed, remaining) = self.lower.get_for_cache(collection, cell).await?;
            // Cache the durable result with its remaining lifetime.
            // A failed update keeps an equal live entry or no entry.
            let expiry = self.expiry_for(remaining);
            if let Err(error) = self.fjall.put(collection, cell, &committed, expiry).await {
                warn_skip("populate", &error);
                self.metrics.cache_error("get", "fill");
            }
            Ok(committed)
        }
        .await;
        self.metrics
            .point(started, Source::Store, cache_result, &loaded);
        loaded
    }

    /// Reads a batch from the cache only when every entry is current.
    ///
    /// One missing or expired entry reloads the complete batch.
    /// A batch of hits consults no marker. This is sound for three reasons. The
    /// settle transform installs committed values after the durable promote.
    /// Per-key dispatch serializes events on a key. Every assignment starts
    /// with a cold cache.
    ///
    /// Ruling: partial refetch (keep the hits, load only the misses) stays
    /// deferred until a benchmark shows a material Cassandra gain. Such a
    /// design must pin the committed-but-unpromoted window with a property
    /// test.
    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CommittedBatch, Self::Error> {
        let started = Instant::now();
        // Check the disabled state once when this operation starts.
        // Complete accepted cache work if another operation disables the cache.
        if self.fjall.is_disabled() {
            let loaded = self.lower.get_many(collection, section, batch).await;
            self.metrics.batch(
                batch.len(),
                started,
                Source::Store,
                CacheResult::Disabled,
                &loaded,
            );
            return loaded;
        }
        // Probe: ONE blocking hop, exhaustive.
        let cache_result = match self.fjall.get_batch(collection, section, batch).await {
            // Every position is a hit (Present value or Absent tag), the current
            // committed projection (KV1): serve it without a lower read.
            Ok(Some(hits)) => {
                let loaded = Ok(hits);
                self.metrics.batch(
                    batch.len(),
                    started,
                    Source::Cache,
                    CacheResult::Hit,
                    &loaded,
                );
                return loaded;
            }
            // Any miss/expired (KV2): fall through and refetch the complete batch.
            Ok(None) => CacheResult::NotAllHit,
            // A fjall probe failure degrades this read to a durable one.
            Err(error) => {
                warn_skip("read batch", &error);
                self.metrics.cache_error("get_many", "lookup");
                CacheResult::Error
            }
        };
        let loaded = self
            .fill_batch(collection, section, batch, "get_many")
            .await;
        self.metrics
            .batch(batch.len(), started, Source::Store, cache_result, &loaded);
        loaded
    }

    /// A third presence frame adds codec and commit logic to save only one cold
    /// payload transfer.
    async fn contains_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<PresenceBatch, Self::Error> {
        let started = Instant::now();
        if self.fjall.is_disabled() {
            let loaded = self.lower.contains_many(collection, section, batch).await;
            self.metrics.presence(
                batch.len(),
                started,
                Source::Store,
                CacheResult::Disabled,
                &loaded,
            );
            return loaded;
        }
        let cache_result = match self
            .fjall
            .get_presence_batch(collection, section, batch)
            .await
        {
            Ok(Some(hits)) => {
                let loaded = Ok(hits);
                self.metrics.presence(
                    batch.len(),
                    started,
                    Source::Cache,
                    CacheResult::Hit,
                    &loaded,
                );
                return loaded;
            }
            Ok(None) => CacheResult::NotAllHit,
            Err(error) => {
                warn_skip("read presence batch", &error);
                self.metrics.cache_error("contains_many", "lookup");
                CacheResult::Error
            }
        };
        let loaded = self
            .fill_batch(collection, section, batch, "contains_many")
            .await
            .map(|cells| cells.into_iter().map(|c| c.get().is_some()).collect());
        self.metrics
            .presence(batch.len(), started, Source::Store, cache_result, &loaded);
        loaded
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.lower.scan_cells(collection, scan)
    }

    fn scan_keys<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        self.lower.scan_keys(collection, scan)
    }

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
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        // A raw provisional read the committed-value cache cannot answer, so
        // delegate straight to the lower store — no fjall step, no cache-disabled
        // branch (like `provisional_cell_at`). Nothing is published into the
        // cache.
        self.lower.provisional_many(collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // A disabled cache delegates the write to the lower store.
        if self.fjall.is_disabled() {
            return self
                .lower
                .write_provisional(collection, writes, marker)
                .await;
        }
        let stamped_at = self.fjall.clock().now_ms();
        self.lower
            .write_provisional(collection, writes, marker)
            .await?;
        // The committed value stays `prev` while the cell is provisional
        // (commit/abort republishes), so publish `prev` — never the in-flight
        // `data`.
        self.publish_written(collection, writes, stamped_at, |write| {
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

/// The absolute fjall expiry (millis; `0` = never) for a cell whose durable row
/// was (or is about to be) written at `stamped_at` with `remaining` whole
/// seconds of TTL. A `None` TTL means the durable row never expires, so the
/// entry never does either.
///
/// Cassandra anchors a row's death on the **coordinator wall clock at
/// whole-second resolution** and ignores the write timestamp for TTL. We floor
/// `stamped_at` DOWN to the same second resolution so the mirrored fjall stamp
/// sheds the 0–999 ms sub-second remainder that would otherwise
/// deterministically overhang the row (rounding to *nearest* would round up
/// half the time and still overhang by ≤500 ms). Flooring is the safe direction
/// — an early fjall expiry falls through and self-heals. Two residuals remain,
/// bounded and accepted. Cross-node clock skew (the coordinator's wall clock
/// differs from this node's) no client-side arithmetic can remove; a *forward*
/// skew or step only shortens the fjall life, so it too falls through and
/// self-heals. A **backward** local clock step after publication is the one
/// direction the fall-through does not cover: the entry never reads as expired,
/// so it stays a hit past the durable row's death for the size of the step,
/// until the next write-through or marker eviction heals it — bounded by the
/// NTP step magnitude. A monotonic-clock floor would remove it; it is not
/// applied here.
fn expiry_at(stamped_at: u64, remaining: Option<CompactDuration>) -> u64 {
    match remaining {
        Some(remaining) => {
            let anchor = stamped_at - stamped_at % 1_000;
            anchor.saturating_add(u64::from(remaining.seconds()).saturating_mul(1_000))
        }
        None => 0,
    }
}

/// Runs a must-succeed repair delete: up to [`DELETE_RETRY_BUDGET`] attempts
/// with [`DELETE_RETRY_DELAY`] between them, warning per failure; on
/// exhaustion it **disables the cache** and returns. Completes-or-disables: it
/// never fails upward and never stalls settlement — see the module's cache
/// disablement section for why every failure class (there is no Permanent
/// escape hatch) lands in the same bounded place.
///
/// A dropped **boundary-owned** settle/admission future abandons the retry
/// harmlessly: the drop coincides with assignment revocation (the workspace —
/// and any stale entry — dies with it) or with an idempotent admission that
/// re-attempts the repair. The one **user-droppable** caller — mid-handler
/// `commit()` / `ReadUncommitted` finalize via [`Cached::write_resolved`] — is
/// not covered by that argument (nothing re-runs a marker-free direct write);
/// it is made drop-safe instead by `write_resolved`'s pre-call delete of the
/// written cells, so a drop leaves them cold rather than stale.
async fn retry_delete<F, Fut>(fjall: &FjallCellCache, op: &str, mut delete: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), FjallCellCacheError>>,
{
    for attempt in 1..=DELETE_RETRY_BUDGET {
        match delete().await {
            Ok(()) => return,
            Err(error) => {
                warn!(error = %error, attempt, "committed-value cache {op} delete failed");
                if attempt < DELETE_RETRY_BUDGET {
                    sleep(DELETE_RETRY_DELAY).await;
                }
            }
        }
    }
    fjall.disable();
}

/// Logs a degraded fjall cache operation (the cache is a hint; correctness
/// rests on the lower store).
fn warn_skip(op: &str, error: &FjallCellCacheError) {
    warn!(error = %error, "committed-value cache {op} failed; degrading");
}
