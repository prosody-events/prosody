//! A write-through cache of committed cell projections over durable storage.
//!
//! [`CellRead`] uses one projection for the cache probe, durable read, and
//! fill. A presence fill stores no payload. Value frames can also answer
//! presence reads.
//!
//! The cache follows five invariants:
//!
//! - **KV1 — a hit is current.** Each hit equals the requested committed
//!   projection. A failed required removal disables the cache.
//! - **KV2 — a miss is unknown.** A miss reads durable storage and caches its
//!   result, including absence.
//! - **KV3 — scans bypass the cache.** A scan uses durable storage and does not
//!   change the cache.
//! - **KV4 — a fill cannot overwrite a newer write.** Per-key dispatch and the
//!   session operation gate serialize reads and writes. Admission and
//!   settlement do not overlap handler operations. The gate is an exclusive
//!   hold, so two fills of one cell never overlap either. This is what keeps a
//!   presence fill from replacing a concurrent value fill.
//! - **KV5 — a successful update retains warmth.** Expiry, reassignment,
//!   clears, and cache errors can force a durable read.
//!
//! A probe error publishes nothing. A corrupt frame is overwritten as a repair.
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
mod read;
mod store;

use self::metrics::CellMetrics;
use super::cell::{Committed, Values};
use super::cell_key::CellKey;
use super::fjall::{FjallCellCache, FjallCellCacheError};
use super::identity::{CollectionId, CollectionRef};
use super::marker::EventMarker;
use super::store::{CellBackend, CellBuffer};
use crate::timers::duration::CompactDuration;
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

/// A shared cache over a durable store for one partition assignment.
#[derive(Clone)]
pub struct Cached<L> {
    fjall: FjallCellCache,
    lower: L,
    metrics: CellMetrics,
}

impl<L> Cached<L> {
    /// Constructs a cache for committed projections over `lower`.
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
            .map(|(cell, value)| (cell.as_ref(), project(value), expiry));
        if let Err(error) = self
            .fjall
            .put_batch::<Values>(collection.id(), projected)
            .await
        {
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

impl<L: CellBackend> CellBackend for Cached<L> {
    type Error = L::Error;
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
