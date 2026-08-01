//! The reader's read-through, byte-budgeted, TTL cache.
//!
//! `quick_cache` has no native TTL, so this module supplies one over it.
//!
//! The cache records when each store read begins, not when it completes. TTL
//! therefore bounds the age since the read began. A slow fill enters
//! already-aged, so it cannot pass an old value off as fresh to a future
//! reader.
//!
//! The `age >= ttl` gate applies to cache hits only. A fill always serves the
//! store result it just read. That result reflects committed state as of the
//! fill's completion, so it is fresh no matter how long the fill took. Using
//! the issue time only makes the cached entry expire conservatively for later
//! readers. A completed fill is never re-checked and never re-run. The
//! retry in [`ReaderCache::get_cached`] re-reads the key only after evicting
//! the stale entry it just observed, so each pass either drops an entry or
//! takes the fill guard.

use crate::Key;
use crate::error::ErrorCategory;
use crate::state::access::StateAccessError;
use crate::state::cell_key::CellKey;
use crate::state::store::CellBuffer;
use crate::state::{StateName, StateType};
use crate::state_reader::source::SourceId;
use bytes::Bytes;
use quanta::{Clock, Instant};
use quick_cache::Weighter;
use quick_cache::sync::{Cache, DefaultLifecycle, EntryAction, EntryResult};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::coop::cooperative;

/// Fixed per-entry accounting overhead added to every entry's declared weight,
/// so a zero-byte negative entry still costs the budget something.
const READER_CACHE_ENTRY_OVERHEAD: u64 = 176;

/// The cache key: the stable [`SourceId`], state namespace, collection name,
/// partition key, and cell. The [`SourceId`] is stable, never an ordinal, so
/// an entry never aliases another source across a snapshot reorder.
pub(crate) type CacheKey = (SourceId, StateType, StateName, Key, CellKey);

/// The cached value: the issue time and the committed bytes (or cached
/// known-absence).
type CacheVal = (Instant, Option<Bytes>);

/// The concrete `quick_cache` instance the reader shares, byte-weighted and
/// `ahash`-hashed.
type ReaderCacheInner = Cache<CacheKey, CacheVal, ReaderWeighter, ahash::RandomState>;

/// Byte weigher: key byte length + value byte length + a fixed overhead. The
/// budget bounds **declared weight** (bytes + overhead), never process RSS.
#[derive(Clone)]
pub(crate) struct ReaderWeighter;

impl Weighter<CacheKey, CacheVal> for ReaderWeighter {
    fn weight(&self, key: &CacheKey, val: &CacheVal) -> u64 {
        let (source, _state_type, name, partition_key, cell) = key;
        let key_bytes = source.group_id.len()
            + source.topic.len()
            + name.as_str().len()
            + partition_key.len()
            // state type + section discriminator + coordinate bytes
            + 1
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
    clock: Clock,
}

impl ReaderCache {
    /// A cache holding up to `budget` declared bytes, aged on the process
    /// monotonic clock.
    #[must_use]
    pub(crate) fn with_budget(budget: u64) -> Self {
        Self::build(budget, Clock::new())
    }

    /// A cache with an injected clock, for deterministic TTL tests. Pair it
    /// with [`quanta::Clock::mock`] and advance the returned handle.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_clock(budget: u64, clock: Clock) -> Self {
        Self::build(budget, clock)
    }

    fn build(budget: u64, clock: Clock) -> Self {
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
        }
    }

    /// Current declared weight held by the cache — the byte budget bounds this.
    #[cfg(test)]
    pub(crate) fn weight(&self) -> u64 {
        self.inner.weight()
    }

    /// A clone of the cache's clock, so the reader's snapshot-refresh cadence
    /// and the cache's TTL age observe the same (possibly mocked) time.
    pub(crate) fn clock(&self) -> Clock {
        self.clock.clone()
    }

    /// Whether an entry issued at `issued` is still fresh under `ttl`.
    fn fresh(&self, issued: Instant, ttl: Duration) -> bool {
        self.clock.now().duration_since(issued) < ttl
    }

    /// The read-through point read. Serve a fresh hit. Expire a stale hit,
    /// removing it only if it still carries the observed issue time. Then
    /// refill single-flight through `fill`.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_cached<F, Fut>(
        &self,
        key: CacheKey,
        ttl: Duration,
        fill: F,
    ) -> Result<Option<Bytes>, StateAccessError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, StateAccessError>>,
    {
        loop {
            match self.inner.get_value_or_guard_async(&key).await {
                Ok((issued, value)) => {
                    if self.fresh(issued, ttl) {
                        return Ok(value);
                    }
                    // Equal clock readings are interchangeable cache
                    // observations. Removing either can only cause a refill.
                    self.inner
                        .remove_if(&key, |(observed, _)| *observed == issued);
                }
                Err(guard) => {
                    // Single-flight: we own the fill. Record its issue time.
                    let issued = self.clock.now();
                    let value = fill().await?;
                    let _ = guard.insert((issued, value.clone()));
                    return Ok(value);
                }
            }
        }
    }

    /// The read-through batch read, index-aligned to `keys`. Serves the batch
    /// entirely from the cache when every key is a fresh hit. Otherwise it
    /// issues one batch store read through `fill`, writes each key back, and
    /// returns the store answers.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_many_cached<F, Fut>(
        &self,
        keys: &[CacheKey],
        ttl: Duration,
        fill: F,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CellBuffer<Option<Bytes>>, StateAccessError>>,
    {
        let mut hits: CellBuffer<Option<Bytes>> = CellBuffer::with_capacity(keys.len());
        for key in keys {
            match self.inner.get(key) {
                Some((issued, value)) if self.fresh(issued, ttl) => hits.push(value),
                // A single miss refetches the whole batch, so probing the
                // remaining keys is wasted work — stop at the first.
                _ => break,
            }
        }
        if hits.len() == keys.len() {
            return Ok(hits);
        }
        // One shared issue time for the whole batch fill.
        let issued = self.clock.now();
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
            cooperative(self.write_through(key, issued, value.clone())).await;
        }
        Ok(fresh)
    }

    /// Writes `value` for `key`. A fill replaces an observation issued earlier.
    /// Equal instants are interchangeable cache observations.
    async fn write_through(&self, key: &CacheKey, issued: Instant, value: Option<Bytes>) {
        let outcome = self
            .inner
            .entry_async(key, |_, existing: &mut CacheVal| {
                if issued > existing.0 {
                    *existing = (issued, value.clone());
                }
                EntryAction::Retain(())
            })
            .await;
        if let EntryResult::Vacant(guard) = outcome {
            let _ = guard.insert((issued, value));
        }
    }
}
