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
//! - **KV1 — a hit is current.** Every fjall entry (Present bytes or Absent
//!   tag) under an unexpired stamp equals that cell's current committed
//!   projection. Maintained by write-through on every mutator, TTL co-expiry
//!   (below), and the must-succeed repair sites (D1–D5). A hit is served
//!   verbatim — there is no read-side mismatch detection — so a lost delete is
//!   a wrong answer, not a slow one; that is why the repair deletes are
//!   must-succeed, and why one that cannot land blows the **cache fuse** (see
//!   the retry posture): a fused cache serves no hits at all, so KV1 holds
//!   vacuously.
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
//!   fjall; its only cache interaction is the pre-scan read-window invalidation
//!   (D3). There is no way to serve a range from the cache, so there is no
//!   completeness fact to maintain. A scan's cost is always exactly one
//!   lower-store scan.
//! - **KV4 — a read-back fill can never overwrite a newer write-through.**
//!   Enforced, not argued, by three legs: per-key event dispatch serializes
//!   whole events on a key; the per-event **session operation gate**
//!   (`SessionGate` in [`crate::state::session`]) serializes in-handler ops so
//!   a suspended fill cannot straddle a `commit()`'s durable write; and the
//!   sweep/settle boundary never overlaps a handler-issued fill.
//! - **KV5 — first-touch permanence.** Once a cell's projection has been
//!   successfully published — by any mutator's write-through (present and
//!   absent projections, including the staged `prev` and the settle transform's
//!   `data`) or by a fill's read-back — every later point read of it is a fjall
//!   hit with zero durable reads, until: partition loss/rebalance (the
//!   workspace is assignment-scoped, born cold); a scan (durable by design, not
//!   a point read); TTL expiry (the co-expired entry falls through once and
//!   re-caches); an explicit clear covering the cell (D4 — the next touch
//!   re-reads once); or a D-site eviction (re-warmed by the next read). Under
//!   fjall faults the contract is eventual, not one-shot: a failed publish
//!   leaves the cell cold for one more durable read, and a failed fjall *read*
//!   degrades that one read to a durable one.
//!
//! # The must-succeed repair sites
//!
//! Two rules generate every site — the review criterion for any future verb:
//! **(1)** before invoking any lower operation that can change or resolve the
//! logical committed projection without atomically installing the matching
//! cache projection, install that projection (D5's transform) or delete every
//! affected entry (D2–D4); **(2)** after authoritative success, a failed cache
//! publication deletes the entries it failed to replace (D1). TTL co-expiry is
//! the one staleness source neither rule reaches — it is time-based, handled by
//! the expiry stamp.
//!
//! | # | Site | Deleted / rewritten | Ordering |
//! |---|---|---|---|
//! | D1 | Failed fjall publish after a successful lower write | the written cells' entries | after the lower ack — the durable value moved |
//! | D2 | Raw `mark_resolved` promote (the value is not carried) | the promoted cells' entries | **before** the lower call — deleting early costs a fall-through; deleting after leaves a promoted-but-cached `prev` on a mid-way cancel, a window the sweep never repairs |
//! | D3 | Standing clears-bearing marker resolved beneath the cache (fall-through read, `scan_cells`, the stage boundary, or a blind `write_resolved`) | the marker's staged coordinates **and** cleared sections | before the lower call, verdict-blind; rides the lower store's marker memo |
//! | D4 | Committed section clears | the cleared sections' entries — at the commit site, **excluding** the staged coordinates the D5 transform just installed | before the lower call — delete-first leaves the sections merely cold on a failed/cancelled lower write |
//! | D5 | The **settle transform** (`commit_provisional` only) | staged entries rewritten `prev → data` at their stage-anchored expiry, atomically, **before** the lower promote; a failed transform falls back to must-succeed deletion of the same entries | the verdict is already fixed when the verb runs, so `data` *is* the committed projection — installing it pre-call is correct even if the promote fails or the future is dropped, and the staged cells stay **warm** through the settle |
//!
//! "Must-succeed" means: the delete either lands (bounded retries) or blows
//! the cache fuse — it never fails upward, never stalls settlement, and never
//! leaves a stale entry reachable.
//!
//! Establish-then-publish, in one line: `lower.write` precedes every fjall
//! publish, so a failed lower write returns the error and **never publishes**
//! the new value. It may still leave the touched cells cold: `write_resolved`'s
//! pre-call deletes (delete-first D4 section clears, and the drop-safe delete
//! of the written cells) run before the lower write, so a failed apply degrades
//! to a correct slow fall-through, never a wrong warm hit.
//!
//! **The Incomplete trap.**
//! `commit_provisional`
//! / `abort_provisional` return the **lower**
//! `Result` verbatim and never fold a fjall failure into it — else a transient
//! fjall failure would fold into
//! [`ApplyOutcome::Incomplete`](crate::state::session) and arm `StateRecovery`
//! forever for a perfectly healthy durable store.
//!
//! # The retry posture and the cache fuse
//!
//! A failed must-succeed delete retries up to `DELETE_RETRY_BUDGET` times
//! (the budget absorbs transients), then blows the workspace's one-way
//! **cache fuse** — the sick-disk arm: local disk health can never stall the
//! data path. The fuse is one shared `AtomicBool` in the fjall workspace's
//! inner state, so every `Cached` clone of one assignment observes the same
//! bit; it is loud (warn + metric), permanent for the assignment, and dies
//! with the workspace. Each verb snapshots it **once at entry** — one
//! admission decision governs every internal fjall step, never a no-op/live
//! mix — and an admitted verb finishes against fjall even if the fuse blows
//! mid-verb (safe: per-key dispatch excludes same-key readers, and the fuse
//! never resets, so partial state is bypassed forever). Blown, fjall's three
//! consumers partition by their contracts:
//!
//! 1. **Committed cell entries** — every publish/delete no-ops and every read
//!    falls through to the lower resolving read, so an undeleted stale entry is
//!    unreachable and KV1 holds vacuously.
//! 2. **The warm provisional index and its seeded latch** — bypassed wholesale:
//!    `provisional_cells` delegates the lower stream verbatim (no seed check,
//!    no recording, no latch).
//! 3. **[`MarkerPresence`](crate::state::fjall)** — fused for uniformity; its
//!    own contract is over-report-safe.
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
//! * The settle transform (D5) **reuses** the stage-anchored expiry already on
//!   the cell's entry — the promote does not re-stamp the durable TTL, so
//!   `data` keeps the death set at stage time; a fresh `now + ttl` would
//!   overhang it by the whole stage→commit gap.
//! * A fill reads the cell's *remaining* TTL from the lower store and stamps
//!   `floor(now) + remaining`: the point fill (`CellStore::get_for_cache`)
//!   reads the clock AFTER the lower read, while the batch fill
//!   (`CellStore::get_many_for_cache`) anchors its clock read BEFORE the
//!   durable read, so a wide resolution can only stamp entries early.
//!
//! The cache read path stays a hint: a fjall read error is logged and degrades
//! that read to a durable one, and a failed fill publish degrades with **no**
//! delete (a Miss/Expired prior state already fell through; a live entry
//! surviving a fjall read error equals what the next read resolves — see
//! `Cached::get`) — correctness rests on the lower store, so
//! `Cached::Error` is just the lower store's error.

use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Coordinate, Scan, Section};
use super::event_ref::EventRef;
use super::fjall::{CacheRead, FjallCellCache, FjallCellCacheError};
use super::identity::{CollectionId, CollectionRef};
use super::marker::{EventMarker, SectionClear};
use super::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, section_batches,
};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Delay between attempts of a must-succeed repair delete ([`retry_delete`])
/// while fjall is transiently failing. Zero under test: the bounded-retry
/// pins assert lands-or-fuses, never pacing.
#[cfg(not(test))]
const DELETE_RETRY_DELAY: Duration = Duration::from_millis(100);
#[cfg(test)]
const DELETE_RETRY_DELAY: Duration = Duration::ZERO;

/// Attempts a must-succeed repair delete makes before blowing the cache fuse:
/// the budget absorbs transient I/O hiccups (one must not cost a weeks-long
/// assignment its cache); the fuse handles the sick disk.
pub(crate) const DELETE_RETRY_BUDGET: usize = 5;

/// A write-through fjall K/V cache over a lower committed `CellStore`.
///
/// A cheap `Arc`-backed handle, cloned with the stack per event; the fjall
/// workspace (and its shared cache fuse) is per-assignment — cold at a fresh
/// assignment, dropped at revocation.
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
}

impl<L> Cached<L> {
    /// Composes a cache over `lower`, serving committed-value point hits from
    /// `fjall`. The warm provisional-coordinate index rides `fjall`'s `index`
    /// keyspace.
    #[must_use]
    pub fn new(fjall: FjallCellCache, lower: L) -> Self {
        Self { fjall, lower }
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

    /// Test-only: force-deletes the cells' fjall entries — the D5 pins' cold
    /// arm (the transform's delete-fallback shape, reproduced directly).
    #[cfg(test)]
    pub(crate) async fn evict_for_tests(
        &self,
        collection: &CollectionId,
        cells: &[CellKey],
    ) -> Result<(), FjallCellCacheError> {
        self.fjall.delete_batch(collection, cells).await
    }

    /// Deletes a foreign marker's staged coordinates and cleared sections —
    /// the shared D3 body behind the fall-through-read guard and the stage
    /// boundary. Must-succeed (lands-or-fuses): a stale entry left behind a
    /// beneath-cache resolution would be served verbatim forever.
    async fn delete_marker_window(&self, collection: &CollectionId, standing: &EventMarker) {
        retry_delete(&self.fjall, "marker staged", || {
            self.fjall.delete_batch(collection, standing.staged())
        })
        .await;
        for clear in standing.clears() {
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
    /// A failed publish runs D1: after the lower ack the durable value moved,
    /// so a still-present old entry would serve the pre-write value verbatim —
    /// the written cells' entries are deleted, must-succeed.
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
            // D1 repair: rebuild the delete keys from the `cells` param.
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
    /// The fall-through read's D3 guard: a lower read can read-help-resolve a
    /// standing foreign clears-bearing event marker **beneath** this cache
    /// (the bottom store's committed-unapplied read window), settling the
    /// marker WHOLE — gap tombstones land and staged cells promote while
    /// sibling entries may still hold pre-settle values — a settle no cache
    /// verb observes, so no repair would ever follow. Delete the marker's
    /// staged coordinates and cleared sections BEFORE issuing the lower read
    /// (mirroring the boundary guard in
    /// [`write_provisional`](CellStore::write_provisional)): belt-and-braces
    /// in production — a committed clears-bearing marker's deletes already ran
    /// at the settle attempt, or the workspace died with the assignment — but
    /// load-bearing under the fault alphabet's skipped-settle window.
    /// Verdict-blind (rare path; correctness beats eviction precision). The
    /// consult rides the lower store's marker memo (presence latch + standing
    /// map), so the fast path adds no durable read.
    async fn delete_read_window(
        &self,
        collection: &CollectionId,
        own: EventRef,
    ) -> Result<(), L::Error> {
        if let Some(standing) = self.lower.standing_marker(collection).await?
            && standing.event() != own
            && !standing.clears().is_empty()
        {
            self.delete_marker_window(collection, &standing).await;
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
        // Blown fuse: pure passthrough — no D3 (its only purpose is
        // fjall-entry repair), no publish.
        if self.fjall.fuse_blown() {
            return self.lower.get(collection, cell, own).await;
        }
        match self.fjall.get(collection, cell).await {
            // A hit (Present value or Absent tag) is the current committed
            // projection (KV1); serve it verbatim with zero lower reads.
            Ok(CacheRead::Hit(committed)) => return Ok(committed),
            // A Miss asserts nothing and an Expired entry is a co-expiry gap
            // (KV2): fall through and re-publish.
            Ok(CacheRead::Miss | CacheRead::Expired) => {}
            // A fjall read failure degrades this one read to a durable one.
            Err(error) => warn_skip("read", &error),
        }
        self.delete_read_window(collection, own).await?;
        let (committed, remaining) = self.lower.get_for_cache(collection, cell, own).await?;
        // Best-effort fill publish — present or absent (negative caching),
        // stamped with the remaining-TTL co-expiry. Sound because of
        // GetNeverReadsOwnStaged (module doc, KV2). A failed publish degrades
        // with NO delete. On the Miss/Expired arms the prior state already
        // fell through, so nothing stale survives. On the `Err(fjall read)`
        // arm the prior state can be a live, unexpired entry that stays — safe
        // because every schedule reaching here re-reads a value EQUAL to that
        // surviving entry (the D5 pre-call transform installs the committed
        // projection; per-key dispatch serializes whole events), so the entry
        // is never stale on its own — a stale hit here would already be a KV1
        // break from another site.
        let expiry = self.expiry_for(remaining);
        if let Err(error) = self.fjall.put(collection, cell, &committed, expiry).await {
            warn_skip("populate", &error);
        }
        Ok(committed)
    }

    /// Batch point read with the **all-hits-or-refetch** contract: one blocking
    /// probe of the whole `CoordinateBatch`; every position a hit serves the
    /// batch verbatim (KV1, zero lower reads), and any miss or expired entry
    /// discards all sampled values and refetches every position from durable
    /// truth.
    ///
    /// Serving the pure-hit batch with zero marker consults rests on the
    /// cache-coherence invariant underpinning KV1: no cache entry ever coexists
    /// with an oracle-committed-but-unpromoted provisional cell a same-key
    /// event could read. It holds by construction — the D5 settle transform
    /// installs the committed `data` projection into the cache before the
    /// promote, so any entry standing through the committed-but-unpromoted
    /// window already IS the current projection; per-key dispatch serializes
    /// whole events, so no other same-key event races that settle; and every
    /// ownership change mints a born-cold workspace (KV5), so no stale entry
    /// survives a crash or rebalance. Ruling: selective partial-refetch
    /// (retaining sampled hits beside durably-fetched misses) is sound
    /// under this same invariant but stays deferred — held as
    /// all-hits-or-refetch pending a benchmark showing material Cassandra
    /// bytes/latency improvement; a partial-refetch design must pin the
    /// committed-but-unpromoted window with a property.
    async fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> Result<CommittedBatch, Self::Error> {
        // Fuse snapshot ONCE at entry. Blown: pure passthrough — no D3 (its only
        // purpose is fjall-entry repair), no publish. Admitted: the whole verb
        // runs admitted even if the fuse blows mid-call — no second fuse check
        // below (the snapshot-once admission contract).
        if self.fjall.fuse_blown() {
            return self.lower.get_many(collection, section, batch, own).await;
        }
        // Probe: ONE blocking hop, exhaustive.
        match self.fjall.get_batch(collection, section, batch).await {
            // Every position is a hit (Present value or Absent tag), the current
            // committed projection (KV1): serve verbatim, zero lower reads, no D3.
            Ok(Some(hits)) => return Ok(hits),
            // Any miss/expired (KV2): fall through and refetch the WHOLE batch.
            Ok(None) => {}
            // A fjall probe failure degrades this read to a durable one.
            Err(error) => warn_skip("read batch", &error),
        }
        // All-hits-or-refetch: any non-hit discards every sampled value and
        // re-reads the whole batch from durable truth AFTER the read-window guard.
        self.delete_read_window(collection, own).await?;
        // Anchor the co-expiry on a clock read taken BEFORE the durable read
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
        // failure (the read-fill no-delete degrade — distinct from the mutator D1
        // delete-on-failure). Each `CellKey` is built inline — no scratch buffer.
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
        }
        Ok(filled.into_iter().map(|(committed, _)| committed).collect())
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Scans bypass the cache (KV3): D3 guard, then the lower scan — the
        // fuse snapshot happens on first poll, inside the generator.
        try_stream! {
            if !self.fjall.fuse_blown() {
                self.delete_read_window(collection, own).await?;
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
            // Blown fuse: the warm index AND its seeded latch are bypassed
            // WHOLESALE — no seed check, no recording, no latch. Post-fuse,
            // write_provisional's index recording no-ops, so a snapshot seeded
            // pre-fuse is silently incomplete; consulting it would
            // short-circuit the sweep and strand a provisional cell (the F2
            // class).
            if self.fjall.fuse_blown() {
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
        // A pure lower read — no fjall step, so no fuse arm is needed.
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
        // delegate straight to the lower store — no fjall step, no fuse arm
        // (like `provisional_cell_at`). Nothing is published into the cache.
        self.lower.provisional_many(collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // Blown fuse: lower call only — no boundary repair (entries are
        // unreachable), no index recording, no publish. On Err return
        // verbatim; the index is bypassed wholesale, so there is no unseed.
        if self.fjall.fuse_blown() {
            return self
                .lower
                .write_provisional(collection, writes, marker)
                .await;
        }
        // Boundary D3: the lower store's stage boundary resolves any standing
        // FOREIGN event marker *beneath* this cache — a settle no wrapper verb
        // observes, so no publish or delete would ever follow it. With warm
        // entries still holding those cells' stage-time `prev`, a committed
        // foreign marker resolved down there would promote durably while fjall
        // keeps serving the stale `prev` — an unbounded stale window, because
        // the boundary (unlike the sweep) never routes through this type's
        // settle verbs. So delete the marker's staged coordinates — and its
        // cleared sections, whose gap tombstones the boundary resolve lands —
        // BEFORE forwarding down. Deleting early only costs a fall-through
        // (the lower read is oracle-resolving), and the uncommitted arm needs
        // no special casing — the delete is verdict-blind (rare path;
        // correctness beats eviction precision). Warm via the lower store's
        // marker memo, so the fast path adds no durable read.
        if let Some(marker) = marker
            && let Some(standing) = self.lower.standing_marker(collection.id()).await?
            && standing.event() != marker.event()
        {
            self.delete_marker_window(collection.id(), &standing).await;
        }
        // Anchor the co-expiry on a clock read taken BEFORE the lower write
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
        if self.fjall.fuse_blown() {
            return self.lower.write_resolved(collection, cells, clears).await;
        }
        // D3: the lower `write_resolved` now runs the write-side boundary
        // (`help_write_window`) — a blind write resolves a standing
        // clears-bearing marker BENEATH this cache (staged cells promote, gap
        // tombstones land), a settle no cache verb observes, so no publish
        // would ever follow. Delete the marker's staged coordinates and cleared
        // sections first, as the fall-through read guard does. No own event to
        // compare (`write_resolved` carries no `EventRef`) — deleting an own
        // marker's window costs a fall-through, never staleness. Rides the
        // lower store's marker memo, so the fast path adds no durable read.
        if let Some(standing) = self.lower.standing_marker(collection.id()).await?
            && !standing.clears().is_empty()
        {
            self.delete_marker_window(collection.id(), &standing).await;
        }
        // D4, whole-section, BEFORE the lower call: this is the marker-free
        // direct apply (ReadUncommitted finalize / mid-handler `commit()`) — a
        // stale entry in a cleared section has NO later repair (no marker
        // exists for read-help to resolve), so delete-first is mandatory. A
        // failed or cancelled lower write then leaves the sections merely cold
        // (a slow read, never a wrong one). Whole-section is correct here —
        // unlike the commit site — because the written cells are deleted
        // pre-call (drop-safety, below) and re-warmed by `publish_written(data)`
        // AFTER the lower call.
        for clear in clears {
            retry_delete(&self.fjall, "clear section", || {
                self.fjall
                    .delete_section(collection.id(), clear.section(), &[])
            })
            .await;
        }
        // Drop-safety: `write_resolved` is the one user-droppable write path —
        // mid-handler `commit()` / ReadUncommitted finalize run in a
        // handler-owned future the `SessionGate` blesses dropping. The written
        // cells' OLD entries go stale the instant the durable write lands, and
        // the re-warming `publish_written` runs only AFTER the lower ack — so a
        // drop (or a publish + D1 failure) in that window would leave them
        // served verbatim forever (KV1). Delete the written cells' entries
        // pre-call too, must-succeed: a drop then leaves them cold (a
        // fall-through), never stale, and the success path re-warms them. A
        // pre-call INSTALL would be wrong here — unlike the D5 transform the
        // write's success is not yet fixed, so caching `data` early could cache
        // a value that never lands. (Not a per-message hot path: this verb runs
        // only on `commit()`/finalize, and the keys are one bounded batch off
        // the `cells` param — the same key-clone shape D1 already uses.)
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
        if self.fjall.fuse_blown() {
            return self.lower.mark_resolved(collection, cells).await;
        }
        // D2: the raw promote keeps `data` as the committed value but does not
        // carry it here, so the cache cannot publish the new committed
        // projection from the keys alone (`commit_provisional` is the promote
        // path that does carry the staged writes; this raw promote is only
        // reached by the recovery sweep / `resolve_cell`). Because a hit is
        // served verbatim, a stale `prev` left cached would be served forever
        // — so delete the touched entries, must-succeed, BEFORE the durable
        // promote: deleting early only costs a fall-through (the lower read
        // resolves through the oracle), while deleting after would leave a
        // promoted-but-cached `prev` if the caller is cancelled mid-way — a
        // window the sweep could never repair, since a resolved cell is never
        // re-promoted. Delete-first, a crash or cancellation leaves the cell
        // provisional and the sweep retries whole.
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
        if self.fjall.fuse_blown() {
            return self
                .lower
                .commit_provisional(collection, writes, clears)
                .await;
        }
        // (1) The D5 settle transform, strictly BEFORE the lower promote: the
        // commit verdict is already fixed when this verb runs (the settle
        // boundary records the marker and commits the offset first; the sweep
        // calls it only on an oracle `committed` verdict), so `data` IS the
        // logical committed projection — installing it pre-call is correct
        // even if the promote then fails or this future is dropped, and the
        // staged cells stay WARM through the settle. A failed transform batch
        // falls back to must-succeed deletion of the same entries (cold, never
        // stale).
        //
        // Recorded ruling — commit warmth pays rent; do not replace the
        // transform with delete-and-refill: leaving staged cells cold after
        // commit would charge one durable point read per hot cell per event on
        // the dominant read-modify-write workload.
        if let Err(error) = self.fjall.commit_batch(collection.id(), writes).await {
            warn_skip("commit transform", &error);
            let cells: CellBuffer<CellKey> = writes.iter().map(|(cell, _)| cell.clone()).collect();
            retry_delete(&self.fjall, "commit transform fallback", || {
                self.fjall.delete_batch(collection.id(), &cells)
            })
            .await;
        }
        // (2) D4 scoped to the cleared sections MINUS the staged coordinates —
        // the set equation: S (the staged coordinates) holds `data` warm, C∖S
        // (every other cached entry in the cleared sections) is deleted,
        // everything else untouched. A whole-section delete here would evict
        // the entries the transform just installed — cold, not wrong, but it
        // would silently void commit warmth exactly on clear-and-repopulate
        // events; and running D4 first instead would destroy the stage frames
        // whose expiries the transform reads. Order: transform → scoped D4 →
        // lower promote.
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
        if self.fjall.fuse_blown() {
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

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        // A pure lower read — the cache never caches markers (the marker
        // lifecycle lives in the lower store), so no fuse arm is needed.
        self.lower.standing_marker(collection).await
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
/// self-heals. A **backward** local clock step AFTER publication is the one
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
/// exhaustion it **blows the cache fuse** and returns. Lands-or-fuses: it
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
    fjall.blow_fuse();
}

/// Logs a degraded fjall cache operation (the cache is a hint; correctness
/// rests on the lower store).
fn warn_skip(op: &str, error: &FjallCellCacheError) {
    warn!(error = %error, "committed-value cache {op} failed; degrading");
}
