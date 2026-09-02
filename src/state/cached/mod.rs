//! Write-through fjall K/V cache over the durable lower store.
//!
//! [`Cached`] fronts a lower `CellStore` (the durable
//! [`CassandraStore`](crate::state::cassandra::CassandraStore)) with a
//! [`FjallCellCache`] of committed cell projections. The performance contract
//! fits one sentence: **point reads are cached, scans are durable.** As the
//! single writer of an owned partition we observe every committed change we
//! make, so the cache is **write-through** — each mutator runs `lower.write`
//! first, then publishes the committed projection of every touched cell into
//! fjall.
//!
//! # The five invariants
//!
//! - **KV1 — a hit is current.** Each unexpired entry equals the committed
//!   cell. Every state change updates or removes the entry. A required removal
//!   failure disables the cache.
//! - **KV2 — a miss is unknown.** A fjall miss (or expired entry) asserts
//!   nothing. The reader falls through to the lower store's resolving read and
//!   publishes what it finds — **including absence** (the Absent tag), so
//!   repeated reads of a genuinely absent cell pay one durable read, not many.
//!   Negative caching is sound for the same reason write-through is: absence
//!   can only become presence through this partition's single writer, whose
//!   write-through updates the entry. The read-back publish is sound because of
//!   **`GetNeverReadsOwnStaged`**: `get` is never called on a cell the current
//!   event already staged (staging is at `finalize`/settle, which resolves via
//!   `commit_provisional`/`abort_provisional`, never `get`), so the lower read
//!   is always a settled committed projection.
//! - **KV3 — scans bypass the cache.** `scan_cells` never reads or writes
//!   fjall; its only cache interaction is the pre-scan prior-clear invalidation
//!   (prior-clear cache guard). There is no way to serve a range from the
//!   cache, so there is no completeness fact to maintain. A scan's cost is
//!   always exactly one lower-store scan.
//! - **KV4 — a read-back fill can never overwrite a newer write-through.**
//!   Enforced, not argued, by three legs: per-key event dispatch serializes
//!   whole events on a key; the per-event **session operation gate**
//!   (`SessionGate` in [`crate::state::session`]) serializes in-handler ops so
//!   a suspended fill cannot straddle a `commit()`'s durable write; and the
//!   sweep/settle boundary never overlaps a handler-issued fill.
//! - **KV5 — first-touch permanence.** A successful update keeps later point
//!   reads in the cache. Reassignment, scans, expiry, and clears can remove an
//!   entry. A cache error can also cause one durable read.
//!
//! # The must-succeed repair sites
//!
//! Update or remove affected entries before a lower operation can make them
//! stale. Remove old entries when an update fails after a durable write.
//! Use the expiry stamp for time-based removal.
//!
//! A required removal retries for a bounded period.
//! A final failure disables the cache for the assignment.
//! The failure does not stop durable state settlement.
//!
//! A durable write always occurs before its cache update.
//! A failed durable write cannot publish a new cache value.
//!
//! **The Incomplete trap.**
//! `commit_provisional`
//! / `abort_provisional` return the **lower**
//! `Result` verbatim and never fold a fjall failure into it — else a transient
//! fjall failure would fold into
//! [`ApplyOutcome::Incomplete`](crate::state::session) and arm `StateRecovery`
//! forever for a perfectly healthy durable store.
//!
//! # Cache disablement
//!
//! A failed required removal disables the cache after bounded retries.
//! All clones share this state for the assignment.
//! Each operation reads this state once when it starts.
//! An accepted operation completes its cache work.
//!
//! A disabled cache sends committed cell operations to durable storage.
//! It also bypasses the provisional index.
//! [`MarkerCheckSet`](crate::state::fjall) stops its disk operations.
//!
//! # TTL co-expiry
//!
//! Cassandra cells expire (`USING TTL`); fjall has no native per-entry TTL, so
//! the cache mirrors the expiry. The one invariant underneath every stamp: **a
//! published entry's expiry never overhangs the durable row's death** — dying
//! early is safe (a fall-through), outliving is a stale hit. Every path
//! anchors a clock read and floors it to Cassandra's whole-second TTL
//! resolution (see `expiry_at`), each with its own anchor:
//!
//! * A direct write (`write_resolved` / `write_provisional` /
//!   `abort_provisional`) anchors on a clock read taken **before** the lower
//!   write and stamps `floor(stamped_at) + ttl` ([`CollectionRef::ttl`]).
//! * Settlement keeps the expiry that the stage assigned. Durable promotion
//!   does not change the durable expiry.
//! * A fill reads the cell's *remaining* TTL from the lower store and stamps
//!   `floor(now) + remaining`: the point fill (`CellStore::get_for_cache`)
//!   reads the clock after the lower read, while the batch fill
//!   (`CellStore::get_many_for_cache`) anchors its clock read before the
//!   durable read, so a wide resolution can only stamp entries early.
//!
//! The cache read path stays a hint: a fjall read error is logged and degrades
//! that read to a durable one, and a failed fill publish degrades with **no**
//! delete (a Miss/Expired prior state already fell through; a live entry
//! surviving a fjall read error equals what the next read resolves — see
//! `Cached::get`) — correctness rests on the lower store, so
//! `Cached::Error` is just the lower store's error.

pub(crate) mod metrics;

use self::metrics::{CacheResult, CellMetrics, Source};
use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::event_ref::EventRef;
use super::fjall::{CacheRead, FjallCellCache, FjallCellCacheError};
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, SectionClear};
use super::resolve::PriorEventClear;
use super::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, section_batches,
};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
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
    /// Composes a cache over `lower`, serving committed-value point hits from
    /// `fjall`. The warm provisional-coordinate index rides `fjall`'s `index`
    /// keyspace.
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

    /// Test-only: force-deletes the cells' fjall entries — the settlement cache
    /// update tests' cold arm (the transform's delete-fallback shape,
    /// reproduced directly).
    #[cfg(test)]
    pub(crate) async fn evict_for_tests(
        &self,
        collection: &CollectionId,
        cells: &[CellKey],
    ) -> Result<(), FjallCellCacheError> {
        self.fjall.delete_batch(collection, cells).await
    }

    /// Removes cache entries that an event marker can change.
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
    /// A failed cache update removes all old entries for these cells.
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

impl<L> Cached<L>
where
    L: CellStore,
{
    /// Removes entries before a read resolves a prior section clear.
    ///
    /// The clear can change staged cells and other cells in its sections.
    /// Remove all affected entries before the lower read.
    async fn evict_prior_clear_before_read(
        &self,
        collection: &CollectionId,
        own: EventRef,
    ) -> Result<(), L::Error> {
        if let Some(marker) = self.lower.unsettled_marker(collection).await?
            && let Some(clear) = PriorEventClear::new(&marker, own)
        {
            self.evict_marker_cache_entries(collection, clear.marker())
                .await;
        }
        Ok(())
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
        let started = Instant::now();
        // Send the read to durable storage when the cache is disabled.
        if self.fjall.is_disabled() {
            let loaded = self.lower.get(collection, cell, own).await;
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
            self.evict_prior_clear_before_read(collection, own).await?;
            let (committed, remaining) = self.lower.get_for_cache(collection, cell, own).await?;
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
    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> Result<CommittedBatch, Self::Error> {
        let started = Instant::now();
        // Check the disabled state once when this operation starts.
        // Complete accepted cache work if another operation disables the cache.
        if self.fjall.is_disabled() {
            let loaded = self.lower.get_many(collection, section, batch, own).await;
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
            // committed projection (KV1): serve verbatim, zero lower reads, no prior-clear cache
            // guard.
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
        let loaded = async {
            // All-hits-or-refetch: any non-hit discards every sampled value and
            // re-reads the whole batch from durable truth after the prior-clear guard.
            self.evict_prior_clear_before_read(collection, own).await?;
            // Anchor the co-expiry on a clock read taken before the durable read
            // (see the module's TTL co-expiry doc): a wide batch resolution can only
            // stamp entries EARLY, never past their durable row death.
            let stamped_at = self.fjall.clock().now_ms();
            // On Err: publish NOTHING (a negative/Absent entry is published only from
            // a fully successful batch).
            let filled: CacheBatch = self
                .lower
                .get_many_for_cache(collection, section, batch, own)
                .await?;
            // Publish every cell (present AND absent), one atomic batch, NO delete on
            // failure (the read-fill no-delete degrade — distinct from the mutator
            // failed-publish cache guard delete-on-failure). Each `CellKey` is
            // built inline — no scratch buffer.
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
                self.metrics.cache_error("get_many", "fill");
            }
            Ok(filled.into_iter().map(|(committed, _)| committed).collect())
        }
        .await;
        self.metrics
            .batch(batch.len(), started, Source::Store, cache_result, &loaded);
        loaded
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Scans bypass the cache (KV3): prior-clear cache guard, then the lower
        // scan — the cache-disabled check happens on first poll, inside the
        // generator.
        try_stream! {
            if !self.fjall.is_disabled() {
                self.evict_prior_clear_before_read(collection, own).await?;
            }
            let inner = self.lower.scan_cells(collection, scan, own);
            pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        // The disk-backed warm provisional-coordinate cache gates the recovery
        // sweep. Warm (seeded): the local fjall snapshot answers with ZERO
        // Cassandra queries (the zero-query-on-quiescence goal); an empty
        // snapshot yields nothing. Cold (a fresh assignment after
        // crash/rebalance mints an empty `index` keyspace): the lower store's
        // bounded seed runs — the event-marker point read and its per-section
        // batch reads (one raw `IN` read per `<=CELL_BATCH` chunk; cost ∝
        // #provisional, never #cells) —
        // each coordinate is recorded into fjall as it streams, and the
        // collection is marked seeded.
        //
        // A warm read/write failure degrades toward the cold path (re-seed
        // from durable truth), never toward trusting a possibly-incomplete
        // warm set — the fjall index is a hint over the authoritative durable
        // event marker.
        try_stream! {
            // Bypass the provisional index when the cache is disabled.
            // An older index can be incomplete after disablement.
            if self.fjall.is_disabled() {
                let inner = self.lower.provisional_cells(collection);
                pin_mut!(inner);
                while let Some(item) = inner.next().await {
                    yield item?;
                }
                return;
            }
            // Resolve the warm coordinate list, or fall through to the cold seed.
            // A warm read failure — `is_seeded` OR `snapshot` — must degrade to
            // the cold durable re-seed (`None`), NEVER to an empty set: an empty
            // set is a terminal "clean" answer that would unschedule the backstop
            // and strand real provisional cells (F2). Only a genuinely-seeded,
            // successfully-read snapshot short-circuits.
            let warm_coords = match self.fjall.index_seeded(collection).await {
                Ok(true) => match self.fjall.index_snapshot(collection).await {
                    Ok(coords) => Some(coords),
                    Err(error) => {
                        warn_skip("snapshot", &error);
                        None
                    }
                },
                Ok(false) => None,
                Err(error) => {
                    warn_skip("is_seeded", &error);
                    None
                }
            };
            if let Some(coords) = warm_coords {
                // Rebuild each warm coordinate through one lower batch per
                // per-section `<=CELL_BATCH` chunk (the section is reattached to
                // each survivor, since coordinates repeat across sections). A
                // concurrently-resolved or absent coordinate is dropped by
                // `provisional_many` (over-report-safe, matching the cold path's
                // filter). Sub-batches run sequentially: real lower-store I/O
                // leaves drive the coop budget.
                for (section, batch) in section_batches(&coords) {
                    // `Box::pin` keeps the large per-chunk batch-read future off
                    // this generator's state so it stays small across the yield
                    // (bounded per-chunk alloc on a warm recovery path).
                    let survivors =
                        Box::pin(self.lower.provisional_many(collection, section, &batch)).await?;
                    for (coordinate, provisional) in survivors {
                        yield (CellKey { section, coordinate }, provisional);
                    }
                }
            } else {
                // Cold path taken (fresh assignment, or a warm read failed above).
                let inner = self.lower.provisional_cells(collection);
                pin_mut!(inner);
                let mut all_recorded = true;
                while let Some(item) = inner.next().await {
                    let (cell, provisional) = item?;
                    if let Err(error) = self.fjall.index_record(collection, &cell).await {
                        warn_skip("record", &error);
                        all_recorded = false;
                    }
                    yield (cell, provisional);
                }
                // Latch `seeded` only if the whole coords set landed on disk. If
                // any record failed, leave it unseeded so the next sweep re-seeds
                // cold from the durable event marker rather than
                // short-circuiting on an incomplete snapshot and stranding a
                // provisional cell — symmetric with `write_provisional`. A
                // failed latch write is safe to lose (warn-and-continue): the
                // next sweep merely re-seeds.
                if all_recorded
                    && let Err(error) = self.fjall.index_mark_seeded(collection).await
                {
                    warn_skip("mark_seeded", &error);
                }
            }
        }
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
        // Disabled cache: lower call only — no boundary repair (entries are
        // unreachable), no index recording, no publish. On Err return
        // verbatim; the index is bypassed wholesale, so there is no unseed.
        if self.fjall.is_disabled() {
            return self
                .lower
                .write_provisional(collection, writes, marker)
                .await;
        }
        // The lower store can resolve a prior event marker during this stage.
        // Remove each affected cache entry before that resolution.
        if let Some(marker) = marker
            && let Some(unsettled) = self.lower.unsettled_marker(collection.id()).await?
            && unsettled.event() != marker.event()
        {
            self.evict_marker_cache_entries(collection.id(), &unsettled)
                .await;
        }
        // Anchor the co-expiry on a clock read taken before the lower write
        // (see the module's TTL co-expiry doc). Establish first: a failed
        // lower write returns the error — but a PARTIAL durable stage may have
        // landed cells the warm set now misses, so the seeded latch must drop
        // (must-succeed: an unseed WRITE failure would leave the latch true
        // over an incomplete snapshot, short-circuiting every later sweep on
        // it and stranding the cell). The marker lifecycle lives entirely in
        // the lower store; the cache never caches markers.
        let stamped_at = self.fjall.clock().now_ms();
        if let Err(error) = self
            .lower
            .write_provisional(collection, writes, marker)
            .await
        {
            retry_delete(&self.fjall, "unseed", || {
                self.fjall.index_unseed(collection.id())
            })
            .await;
            return Err(error);
        }
        // Record the staged coordinates into the warm provisional-coordinate
        // cache after the durable ack, as one atomic batch. A warm write
        // failure drops the seeded latch — must-succeed, same strand argument
        // as above — so the next sweep re-seeds from the durable event marker,
        // never leaving the latch true with an unaccounted coordinate.
        if let Err(error) = self
            .fjall
            .index_record_batch(collection.id(), writes.iter().map(|(cell, _)| cell))
            .await
        {
            warn_skip("record", &error);
            retry_delete(&self.fjall, "unseed", || {
                self.fjall.index_unseed(collection.id())
            })
            .await;
        }
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
        // Remove entries that an unsettled section clear can change.
        // Do this before the lower store resolves the clear.
        if let Some(unsettled) = self.lower.unsettled_marker(collection.id()).await?
            && !unsettled.clears().is_empty()
        {
            self.evict_marker_cache_entries(collection.id(), &unsettled)
                .await;
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
        // Rollback/committed-write resolved the cells; drop their warm
        // provisional coordinates in one batch (a no-op for a never-staged
        // direct write). A failed clear is a harmless over-report the sweep's
        // point-read filter drops (warn-and-continue).
        if let Err(error) = self
            .fjall
            .index_clear_batch(collection.id(), cells.iter().map(|(cell, _)| cell))
            .await
        {
            warn_skip("clear", &error);
        }
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
        // Promote resolved the cells; drop their warm provisional coordinates
        // (warn-and-continue: over-report-safe).
        if let Err(error) = self
            .fjall
            .index_clear_batch(collection.id(), cells.iter())
            .await
        {
            warn_skip("clear", &error);
        }
        Ok(())
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        if self.fjall.is_disabled() {
            return self
                .lower
                .commit_provisional(collection, writes, clears)
                .await;
        }
        // Publish the committed values before durable promotion.
        // The event result is final before this function starts.
        // Remove the entries if this cache update fails.
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
        // (3) Settle in the lower store (the authoritative settle) — the lower
        // store owns the promote-vs-delete routing and the marker delete. The
        // result is returned VERBATIM (the Incomplete trap, module doc): the
        // cache already holds the committed projection either way — on Err or
        // a dropped future the entries hold `data`, correct because the
        // verdict was fixed before the call.
        let result = self
            .lower
            .commit_provisional(collection, writes, clears)
            .await;
        // (4) Every write is resolved; drop the warm provisional coordinates
        // (warn-and-continue: over-report-safe).
        if result.is_ok()
            && let Err(error) = self
                .fjall
                .index_clear_batch(collection.id(), writes.iter().map(|(cell, _)| cell))
                .await
        {
            warn_skip("clear", &error);
        }
        result
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
            // The rollback resolved the cells; drop their warm provisional
            // coordinates in one batch, then publish the rolled-back `prev`
            // (the TTL refresh).
            if let Err(error) = self
                .fjall
                .index_clear_batch(collection.id(), cells.iter().map(|(cell, _)| cell))
                .await
            {
                warn_skip("clear", &error);
            }
            self.publish_written(collection, &cells, stamped_at, |data| {
                Committed::new(data.clone())
            })
            .await;
        }
        result
    }

    async fn unsettled_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        // A pure lower read — the cache never caches markers (the marker
        // lifecycle lives in the lower store), so no cache-disabled branch is needed.
        self.lower.unsettled_marker(collection).await
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
/// until the next write-through or D-site eviction heals it — bounded by the
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
/// never fails upward and never stalls settlement — see the module's retry
/// posture for why every failure class (there is no Permanent escape hatch)
/// lands in the same bounded place.
///
/// A dropped **boundary-owned** settle/sweep future abandons the retry
/// harmlessly: the drop coincides with assignment revocation (the workspace —
/// and any stale entry — dies with it) or with an idempotent sweep re-run that
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
