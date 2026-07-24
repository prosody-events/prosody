//! The reader's read-through, byte-budgeted, TTL cache.
//!
//! `quick_cache` has no native TTL, so this module supplies one over it. Two
//! rules make it race-free (see [`ReaderCache`]):
//!
//! * **Unique [`Stamp`]** `(issued_ms, seq)`. Bare milliseconds collide when
//!   two fills land in one millisecond, or when the test clock is frozen. A
//!   colliding stamp would let expiry evict a newer fill. The per-bundle `seq`
//!   counter makes every issued store read's stamp unique. See [`Stamp`] for
//!   how the two fields stay separate.
//! * **Stamp taken at issue.** The stamp is taken when the store read is
//!   issued, not when it completes. TTL therefore bounds the age since the read
//!   began. A slow fill enters already-aged, so it cannot pass an old value off
//!   as fresh to a future reader.
//!
//! The `age >= ttl` gate applies to cache hits only. A fill always serves the
//! store result it just read. That result reflects committed state as of the
//! fill's completion, so it is fresh no matter how long the fill took. Taking
//! the stamp at issue only makes the cached entry expire conservatively for
//! later readers. There is no fill-retry loop, which would risk livelock.

use crate::Key;
use crate::error::ErrorCategory;
use crate::state::StateName;
use crate::state::access::StateAccessError;
use crate::state::cell_key::CellKey;
use crate::state::store::CellBuffer;
use crate::state_reader::source::SourceId;
use bytes::Bytes;
use quick_cache::Weighter;
use quick_cache::sync::{Cache, DefaultLifecycle, EntryAction, EntryResult};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Fixed per-entry accounting overhead added to every entry's declared weight,
/// so a zero-byte negative entry still costs the budget something.
const READER_CACHE_ENTRY_OVERHEAD: u64 = 64;

/// A cache entry's issue token. The two fields answer two different questions,
/// so they are never combined into one ordering:
///
/// * `issued_ms` governs TTL age (see [`ReaderCache::fresh`]): the wall-time
///   since observation began.
/// * `seq` governs issue order (see [`ReaderCache::write_through`]'s
///   newer-wins). The per-bundle atomic counter is the only total order of when
///   reads were issued.
///
/// The clock is read before the separate `fetch_add` of `seq`. So a fill that
/// read an earlier millisecond can still get a higher `seq` than one that read
/// a later millisecond. Newer-wins therefore compares `seq` alone. Do not add
/// an `Ord` derive and compare `(issued_ms, seq)`: an older-issued fill could
/// then overwrite a newer one, losing the newer write. Expiry compares the
/// whole stamp for equality. The unique `seq` means `remove_if` never evicts a
/// racing newer fill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    /// Milliseconds since the epoch when the backing store read was issued.
    issued_ms: u64,
    /// Per-bundle monotonic issue counter, unique and totally ordered across
    /// all issued reads — the newer-wins key.
    seq: u64,
}

/// The cache key: the stable [`SourceId`], the collection name, the partition
/// key, and the cell. The [`SourceId`] is stable, never an ordinal, so an entry
/// never aliases another source across a snapshot reorder. The [`StateName`]
/// keeps two collections that share the cache from aliasing each other.
pub(crate) type CacheKey = (SourceId, StateName, Key, CellKey);

/// The cached value: the issue stamp and the committed bytes (or cached
/// known-absence).
type CacheVal = (Stamp, Option<Bytes>);

/// The concrete `quick_cache` instance the reader shares, byte-weighted and
/// `ahash`-hashed.
type ReaderCacheInner = Cache<CacheKey, CacheVal, ReaderWeighter, ahash::RandomState>;

/// A wall clock, injectable in tests. The reader defines its own clock type
/// rather than importing one from the owner-side cache.
#[derive(Clone)]
pub(crate) enum ReaderClock {
    /// Wall clock: milliseconds since the epoch.
    Wall,
    /// A test clock advanced explicitly, never by sleeping.
    #[cfg(test)]
    Fixed(Arc<AtomicU64>),
}

impl ReaderClock {
    /// Milliseconds since the epoch.
    pub(crate) fn now_ms(&self) -> u64 {
        match self {
            Self::Wall => match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(elapsed) => elapsed.as_millis() as u64,
                Err(_) => 0,
            },
            #[cfg(test)]
            Self::Fixed(now) => now.load(Ordering::Relaxed),
        }
    }
}

/// Byte weigher: key byte length + value byte length + a fixed overhead. The
/// budget bounds **declared weight** (bytes + overhead), never process RSS.
#[derive(Clone)]
pub(crate) struct ReaderWeighter;

impl Weighter<CacheKey, CacheVal> for ReaderWeighter {
    fn weight(&self, key: &CacheKey, val: &CacheVal) -> u64 {
        let (source, name, partition_key, cell) = key;
        let key_bytes = source.group_id.len()
            + source.topic.len()
            + name.as_str().len()
            + partition_key.len()
            // section discriminator (i8) + coordinate bytes
            + 1
            + cell.coordinate.as_bytes().len();
        let val_bytes = val.1.as_ref().map_or(0, Bytes::len);
        key_bytes as u64 + val_bytes as u64 + READER_CACHE_ENTRY_OVERHEAD
    }
}

/// One read-through, TTL-bounded, byte-budgeted cache, shared by every reader
/// drawing from a bundle. Clone shares the underlying `Arc`s.
///
/// One allocation is accepted downstream of a cache hit. `CellView`'s decode
/// falls back to copying its input when that input is shared, and a cache hit's
/// `Bytes` is always shared because the cache retains a reference. The uncached
/// store path stays zero-copy. The cache itself is zero-copy: every value is a
/// `Bytes` refcount bump.
#[derive(Clone)]
pub(crate) struct ReaderCache {
    inner: Arc<ReaderCacheInner>,
    clock: ReaderClock,
    seq: Arc<AtomicU64>,
}

impl ReaderCache {
    /// A wall-clock cache holding up to `budget` declared bytes.
    #[must_use]
    pub(crate) fn with_budget(budget: u64) -> Self {
        Self::build(budget, ReaderClock::Wall)
    }

    /// A cache with an injected clock, for deterministic TTL tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_clock(budget: u64, clock: ReaderClock) -> Self {
        Self::build(budget, clock)
    }

    fn build(budget: u64, clock: ReaderClock) -> Self {
        // Estimate item count from the budget and the fixed overhead, so
        // quick_cache sizes its shards sensibly; the byte budget is the real
        // bound.
        let estimated = (budget / READER_CACHE_ENTRY_OVERHEAD).max(1) as usize;
        let inner = Cache::with(
            estimated,
            budget,
            ReaderWeighter,
            ahash::RandomState::default(),
            DefaultLifecycle::default(),
        );
        Self {
            inner: Arc::new(inner),
            clock,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Current declared weight held by the cache — the byte budget bounds this.
    #[cfg(test)]
    pub(crate) fn weight(&self) -> u64 {
        self.inner.weight()
    }

    /// A clone of the cache's clock, so the reader's snapshot-refresh cadence
    /// and the cache's TTL age observe the same (possibly injected) time.
    pub(crate) fn clock(&self) -> ReaderClock {
        self.clock.clone()
    }

    /// A fresh, unique issue stamp (`seq` monotonic, `issued_ms` from the
    /// clock).
    fn issue_stamp(&self) -> Stamp {
        Stamp {
            issued_ms: self.clock.now_ms(),
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Whether an entry stamped `stamp` is still fresh under `ttl_ms`.
    fn fresh(&self, stamp: Stamp, ttl_ms: u64) -> bool {
        self.clock.now_ms().saturating_sub(stamp.issued_ms) < ttl_ms
    }

    /// The read-through point read. Serve a fresh hit. Expire a stale hit,
    /// removing it only if it still carries the observed stamp, so a newer
    /// racing fill is never evicted. Then refill single-flight through `fill`.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_cached<F, Fut>(
        &self,
        key: CacheKey,
        ttl_ms: u64,
        fill: F,
    ) -> Result<Option<Bytes>, StateAccessError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, StateAccessError>>,
    {
        loop {
            match self.inner.get_value_or_guard_async(&key).await {
                Ok((stamp, value)) => {
                    if self.fresh(stamp, ttl_ms) {
                        return Ok(value);
                    }
                    // Stale for this reader: evict iff still this exact stamp,
                    // then retry (a newer racing fill survives and is re-read).
                    self.inner
                        .remove_if(&key, |(observed, _)| *observed == stamp);
                }
                Err(guard) => {
                    // Single-flight: we own the fill. Stamp at issue.
                    let stamp = self.issue_stamp();
                    let value = fill().await?;
                    let _ = guard.insert((stamp, value.clone()));
                    return Ok(value);
                }
            }
        }
    }

    /// The read-through batch read, index-aligned to `keys`. Serves the batch
    /// entirely from the cache when every key is a fresh hit. Otherwise it
    /// issues one batch store read through `fill`, writes each key back
    /// newer-wins, and returns the store answers.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_many_cached<F, Fut>(
        &self,
        keys: &[CacheKey],
        ttl_ms: u64,
        fill: F,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CellBuffer<Option<Bytes>>, StateAccessError>>,
    {
        let mut hits: CellBuffer<Option<Bytes>> = CellBuffer::with_capacity(keys.len());
        for key in keys {
            match self.inner.get(key) {
                Some((stamp, value)) if self.fresh(stamp, ttl_ms) => hits.push(value),
                // A single miss refetches the whole batch, so probing the
                // remaining keys is wasted work — stop at the first.
                _ => break,
            }
        }
        if hits.len() == keys.len() {
            return Ok(hits);
        }
        // One shared issue stamp for the whole batch fill.
        let stamp = self.issue_stamp();
        let fresh = fill().await?;
        // The store's batch read returns values index-aligned to `keys`. Check
        // that alignment here in every build, not just a debug assert. A shorter
        // fill would `zip` to a truncated, misaligned result. An overlong one
        // would cache only a prefix. Surface either as a store error instead.
        if fresh.len() != keys.len() {
            return Err(StateAccessError::Store {
                message: format!(
                    "batch fill returned {} values for {} keys",
                    fresh.len(),
                    keys.len()
                ),
                category: ErrorCategory::Permanent,
            });
        }
        for (key, value) in keys.iter().zip(fresh.iter()) {
            self.write_through(key, stamp, value.clone()).await;
        }
        Ok(fresh)
    }

    /// Writes `value` for `key` newer-wins. Under the shard lock an occupied
    /// entry keeps the higher-stamped value, so an older-issued fill that
    /// completes late loses. An absent entry is inserted through the guard.
    async fn write_through(&self, key: &CacheKey, stamp: Stamp, value: Option<Bytes>) {
        let outcome = self
            .inner
            .entry_async(key, |_, existing: &mut CacheVal| {
                // Newer-wins compares `seq` alone, never `(issued_ms, seq)`.
                // See [`Stamp`].
                if stamp.seq > existing.0.seq {
                    *existing = (stamp, value.clone());
                }
                EntryAction::Retain(())
            })
            .await;
        if let EntryResult::Vacant(guard) = outcome {
            let _ = guard.insert((stamp, value));
        }
    }
}
