//! A bounded cache for committed reader projections.
//!
//! Each entry records the read issue time and [`CacheEntry`] knowledge.
//! [`Projection`] selects the answer. Presence reads do not copy cached
//! payloads. The cache follows the lattice contract in [`crate::state::cell`].
//!
//! TTL bounds age from the start of the source read.
//! A slow fill therefore enters the cache with its elapsed age.
//! Expired entries cannot answer later reads. A completed fill returns its
//! source result without another age check or read.
//!
//! Point reads share one fill for each missing key.
//! Batch reads probe every key and write every filled position through the
//! lattice. Cache admission does not change a successful source result into an
//! error.

use crate::Key;
use crate::state::access::StateAccessError;
use crate::state::cell::{CacheEntry, Projection, Read};
use crate::state::cell_key::CellKey;
use crate::state::store::CellBuffer;
use crate::state::{StateName, StateType};
use crate::state_reader::source::SourceId;
use bytes::Bytes;
use quanta::{Clock, Instant};
use quick_cache::Weighter;
use quick_cache::sync::{Cache, DefaultLifecycle, EntryAction, EntryResult};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::coop::cooperative;

/// Inline key and value storage. Rust computes the target-specific layout,
/// including padding and alignment. Quick Cache's private allocations are not
/// part of the declared weight.
const READER_CACHE_ENTRY_INLINE_BYTES: u64 = (size_of::<CacheKey>() + size_of::<CacheVal>()) as u64;

/// The cache key: the stable [`SourceId`], state namespace, collection name,
/// partition key, and cell. The [`SourceId`] is stable, never an ordinal, so
/// an entry never aliases another source across a snapshot reorder.
pub(crate) type CacheKey = (SourceId, StateType, StateName, Key, CellKey);

/// The issue time and the committed cache knowledge.
type CacheVal = (Instant, CacheEntry<Bytes>);

/// The concrete `quick_cache` instance the reader shares, byte-weighted and
/// `ahash`-hashed.
type ReaderCacheInner = Cache<CacheKey, CacheVal, ReaderWeighter, ahash::RandomState>;

/// Byte weigher: target-specific inline layout plus owned key and value bytes.
/// The budget bounds **declared weight**, never process RSS.
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
        let val_bytes = match &val.1 {
            CacheEntry::Value(bytes) => bytes.len(),
            CacheEntry::Absent | CacheEntry::Exists => 0,
        };
        key_bytes as u64 + val_bytes as u64 + READER_CACHE_ENTRY_INLINE_BYTES
    }
}

/// One cache with a byte budget and TTL, shared across collection readers.
/// Clones share the cache and clock. Value hits clone the `Bytes` handle.
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
        // Estimate item count from the budget and the inline layout, so
        // quick_cache sizes its shards sensibly; the byte budget is the real
        // bound.
        let estimated = (budget / READER_CACHE_ENTRY_INLINE_BYTES).max(1) as usize;
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

    /// Serves a fresh answer. Retains a fresh entry that cannot answer this
    /// projection, then fills and writes through the result. Removes a stale
    /// entry only if its issue time is unchanged, then refills single-flight.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_cached<P: Projection, F, Fut>(
        &self,
        key: CacheKey,
        ttl: Duration,
        fill: F,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<P::Payload>, StateAccessError>>,
    {
        loop {
            match self.inner.get_value_or_guard_async(&key).await {
                Ok((issued, value)) => {
                    if self.fresh(issued, ttl) {
                        match P::from_cached(value) {
                            Read::Present(value) => return Ok(Some(value)),
                            Read::Absent => return Ok(None),
                            Read::Unknown => {
                                let issued = self.clock.now();
                                let value = fill().await?;
                                self.write_through(&key, issued, P::into_cached(value.clone()))
                                    .await;
                                return Ok(value);
                            }
                        }
                    }
                    self.inner.remove_if(&key, |(observed, _)| {
                        *observed == issued && !self.fresh(*observed, ttl)
                    });
                }
                Err(guard) => {
                    // Single-flight: we own the fill. Record its issue time.
                    let issued = self.clock.now();
                    let value = fill().await?;
                    // The store answer remains valid if admission loses a race.
                    drop(guard.insert((issued, P::into_cached(value.clone()))));
                    return Ok(value);
                }
            }
        }
    }

    /// The read-through batch read, index-aligned to `keys`. Serves the batch
    /// entirely from the cache when every key is a fresh hit. Otherwise it
    /// issues one batch store read through `fill`, writes every position, and
    /// returns the store answers.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_many_cached<P: Projection, F, Fut>(
        &self,
        keys: &[CacheKey],
        ttl: Duration,
        fill: F,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CellBuffer<Option<P::Payload>>, StateAccessError>>,
    {
        let mut hits = CellBuffer::with_capacity(keys.len());
        for key in keys {
            let hit = match self.inner.get(key) {
                Some((issued, entry)) if self.fresh(issued, ttl) => P::from_cached(entry),
                Some((issued, _)) => {
                    self.inner
                        .remove_if(key, |(observed, _)| *observed == issued);
                    Read::Unknown
                }
                None => Read::Unknown,
            };
            hits.push(hit);
        }
        if hits.iter().all(|hit| !matches!(hit, Read::Unknown)) {
            return Ok(hits
                .into_iter()
                .map(|hit| match hit {
                    Read::Present(value) => Some(value),
                    Read::Absent | Read::Unknown => None,
                })
                .collect());
        }
        // One shared issue time for the whole batch fill.
        let issued = self.clock.now();
        let fresh = fill().await?;
        // Check the fill alignment in every build, not just a debug assert:
        // a misaligned fill would cache values under the wrong keys.
        if fresh.len() != keys.len() {
            return Err(StateAccessError::misaligned_batch(fresh.len(), keys.len()));
        }
        for (key, value) in keys.iter().zip(&fresh) {
            cooperative(self.write_through(key, issued, P::into_cached(value.clone()))).await;
        }
        Ok(fresh)
    }

    /// Writes `value` for `key`. A fill replaces an observation issued earlier.
    /// Replacements follow the lattice contract in [`crate::state::cell`].
    async fn write_through(&self, key: &CacheKey, issued: Instant, value: CacheEntry<Bytes>) {
        let outcome = self
            .inner
            .entry_async(key, |_, existing: &mut CacheVal| {
                // Equal issue times permit a value to refine presence.
                let refines_presence = issued == existing.0 && existing.1.downgrades(&value);
                if !value.downgrades(&existing.1) && (issued > existing.0 || refines_presence) {
                    *existing = (issued, value.clone());
                }
                EntryAction::Retain(())
            })
            .await;
        if let EntryResult::Vacant(guard) = outcome {
            // The store answer remains valid if admission loses a race.
            drop(guard.insert((issued, value)));
        }
    }
}
