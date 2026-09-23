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
//! Batch reads stop the cache probe at the first miss and write each filled
//! position through the lattice. Cache admission does not change a successful
//! source result into an error.

use crate::Key;
use crate::state::access::StateAccessError;
use crate::state::cell::{CacheEntry, Projection, Read};
use crate::state::cell_key::{CellKey, CellRef};
use crate::state::store::{CellBuffer, ensure_aligned};
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

/// A cache lookup that borrows its address. Fresh hits do not copy coordinates.
#[derive(Clone, Copy, Hash)]
pub(crate) struct CacheLookup<'a>(
    pub(crate) (&'a SourceId, StateType, &'a StateName, &'a Key, CellRef<'a>),
);

impl CacheLookup<'_> {
    fn into_owned(self) -> CacheKey {
        let (source, state_type, name, key, cell) = self.0;
        (
            source.clone(),
            state_type,
            name.clone(),
            key.clone(),
            cell.into_owned(),
        )
    }
}

impl quick_cache::Equivalent<CacheKey> for CacheLookup<'_> {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == (&key.0, key.1, &key.2, &key.3, key.4.as_ref())
    }
}

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

    /// Returns a fresh answer.
    /// A placeholder replaces a stale entry or a fresh entry that cannot answer
    /// this projection, so concurrent readers share one fill.
    /// A failed fill restores the fresh entry.
    ///
    /// # Errors
    ///
    /// Propagates the store error from `fill`.
    pub(crate) async fn get_cached<P: Projection, F, Fut>(
        &self,
        key: CacheLookup<'_>,
        ttl: Duration,
        fill: F,
    ) -> Result<Option<P::Payload>, StateAccessError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<P::Payload>, StateAccessError>>,
    {
        if let Some((issued, entry)) = self.inner.get(&key)
            && self.fresh(issued, ttl)
        {
            match P::from_cached(entry) {
                Read::Present(value) => return Ok(Some(value)),
                Read::Absent => return Ok(None),
                Read::Unknown => {}
            }
        }
        let key = key.into_owned();
        let outcome = self
            .inner
            .entry_async(&key, |_, (issued, entry): &mut CacheVal| {
                if !self.fresh(*issued, ttl) {
                    return EntryAction::ReplaceWithGuard;
                }
                match P::from_cached(entry.clone()) {
                    Read::Present(value) => EntryAction::Retain(Some(value)),
                    Read::Absent => EntryAction::Retain(None),
                    Read::Unknown => EntryAction::ReplaceWithGuard,
                }
            })
            .await;
        let (guard, restore) = match outcome {
            EntryResult::Retained(answer) => return Ok(answer),
            // Restore fresh knowledge if the fill fails.
            EntryResult::Replaced(guard, old) => (guard, self.fresh(old.0, ttl).then_some(old)),
            EntryResult::Vacant(guard) => (guard, None),
            // The callback never removes, and the async entry never times out.
            // Answer from the store.
            EntryResult::Removed(..) | EntryResult::Timeout => return fill().await,
        };
        let issued = self.clock.now();
        match fill().await {
            Ok(value) => {
                drop(guard.insert((issued, P::into_cached(value.clone()))));
                Ok(value)
            }
            Err(error) => {
                if let Some(old) = restore {
                    drop(guard.insert(old));
                }
                Err(error)
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
    pub(crate) async fn get_many_cached<'a, P: Projection, F, Fut>(
        &self,
        keys: impl ExactSizeIterator<Item = CacheLookup<'a>> + Clone,
        ttl: Duration,
        fill: F,
    ) -> Result<CellBuffer<Option<P::Payload>>, StateAccessError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CellBuffer<Option<P::Payload>>, StateAccessError>>,
    {
        let hits = keys
            .clone()
            .try_fold(CellBuffer::with_capacity(keys.len()), |mut hits, key| {
                let value = match self.inner.get(&key) {
                    Some((issued, entry)) if self.fresh(issued, ttl) => match P::from_cached(entry)
                    {
                        Read::Present(value) => Some(Some(value)),
                        Read::Absent => Some(None),
                        Read::Unknown => None,
                    },
                    Some((issued, _)) => {
                        self.inner
                            .remove_if(&key, |(observed, _)| *observed == issued);
                        None
                    }
                    None => None,
                }?;
                hits.push(value);
                Some(hits)
            });
        if let Some(hits) = hits {
            return Ok(hits);
        }
        // One shared issue time for the whole batch fill.
        let issued = self.clock.now();
        let fresh = fill().await?;
        // A misaligned fill would cache values under the wrong keys.
        ensure_aligned(fresh.len(), keys.len())?;
        for (key, value) in keys.zip(&fresh) {
            cooperative(self.write_through(&key, issued, P::into_cached(value.clone()))).await;
        }
        Ok(fresh)
    }

    /// Writes `value` for `key`. A fill replaces an observation issued earlier.
    /// Replacements follow the lattice contract in [`crate::state::cell`].
    async fn write_through(
        &self,
        key: &CacheLookup<'_>,
        issued: Instant,
        value: CacheEntry<Bytes>,
    ) {
        let key = (*key).into_owned();
        let outcome = self
            .inner
            .entry_async(&key, |_, existing: &mut CacheVal| {
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
