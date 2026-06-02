//! Layered Value store combinator: process-local cache + authoritative backing.
//!
//! `LayeredValueStore<Cache, Backing>` fronts an arbitrary [`ValueStore`]
//! cache with an arbitrary durable backing store. The cache is a hint;
//! correctness depends on the backing store.
//!
//! # Error surface
//!
//! The combinator's [`ValueStore`], [`DurableWalStore`], and
//! [`DirectApplyStore`] impls all return the **backing** store's error
//! type. Cache failures are logged at WARN and either degrade to a
//! backing fall-through (read path) or to invalidation (write path).
//! They never become the outer error. This keeps the error type aligned
//! with `<Backing as DurableWalStore<ValueKind>>::Error`, which the
//! shared `value_test_suite::DurableBundle` requires.
//!
//! ## Patch rules (load-bearing invariants)
//!
//! 1. **`get` cache-first, miss or cache error falls through and populates.**
//!    Cache `Read::Unknown` *or* a cache `Err` triggers exactly one backing
//!    `get`. The result is written back to the cache as a best-effort hint,
//!    automatically repairing a corrupt cell.
//! 2. **`seal` does not change applied state → cache untouched.** The cache
//!    mirrors `applied`; the WAL columns are backing-only.
//! 3. **`apply_sealed` Applied → read backing's new applied and patch.** The
//!    backing's authoritative answer wins; the cache is patched to match.
//!    `NoOp` leaves the cache alone.
//! 4. **`rollback_sealed` does not change applied state → cache untouched.**
//!    Same reasoning as `seal`.
//! 5. **`direct_apply` Applied → patch by inspecting the last op.** Value's
//!    fold is last-writer-wins, so the last op determines the new applied
//!    state; no extra backing read needed.
//! 6. **Cache failure after backing success → log + invalidate; return durable
//!    success.** Backing is authoritative.

use super::value::{
    DirectApplyStore, DurableWalStore, StoredPayload, ValueKind, ValueOp, ValueStore,
};
use super::{
    CollectionId, CollectionKind, CollectionRef, DurableState, EventRef, Read, SealedCollection,
    StoreOutcome,
};
use crate::state::pending::PendingIndexStore;
use tracing::warn;

/// A two-layer Value store: cache + backing.
///
/// See module documentation for the patch rules each method enforces.
#[derive(Clone, Debug)]
pub struct LayeredValueStore<Cache, Backing> {
    cache: Cache,
    backing: Backing,
}

impl<Cache, Backing> LayeredValueStore<Cache, Backing> {
    /// Composes a cache and a backing store.
    #[must_use]
    pub fn new(cache: Cache, backing: Backing) -> Self {
        Self { cache, backing }
    }

    // TODO: audit `cache()`/`backing()` against the public interface once the
    // composition stabilizes; drop them if no consumer materializes
    // (re-addable non-breakingly).

    /// Returns a reference to the cache store.
    #[must_use]
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Returns a reference to the backing store.
    #[must_use]
    pub fn backing(&self) -> &Backing {
        &self.backing
    }
}

impl<Cache, Backing> LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore,
    Backing: ValueStore,
{
    /// Patches the cache best-effort: writes `Present`/`Absent`,
    /// invalidates on write failure.
    ///
    /// Invariant 6: a cache failure after a backing-side success never
    /// surfaces as `Err`. If the patch fails we attempt a clear; if even
    /// that fails the cache may serve a stale value on the next read,
    /// which the next miss-then-populate cycle corrects.
    async fn patch_cache_or_invalidate(
        &self,
        id: &CollectionId<ValueKind>,
        new_applied: Option<StoredPayload>,
    ) {
        let write_result = match new_applied {
            Some(payload) => self.cache.set(id, payload).await,
            None => self.cache.clear(id).await,
        };
        if let Err(error) = write_result {
            warn!(error = %error, "cache patch failed; attempting invalidation");
            if let Err(clear_error) = self.cache.clear(id).await {
                warn!(
                    error = %clear_error,
                    "cache invalidation also failed; entry may be stale until next miss"
                );
            }
        }
    }
}

impl<Cache, Backing> ValueStore for LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore,
    Backing: ValueStore,
{
    type Error = Backing::Error;

    /// Invariant 1: cache-first; on `Unknown` *or* cache error consult
    /// backing and patch. Cache read errors are logged at WARN.
    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        let cache_read = match self.cache.get(collection).await {
            Ok(read) => read,
            Err(error) => {
                warn!(error = %error, "cache read failed; falling through to backing");
                Read::Unknown
            }
        };
        match cache_read {
            hit @ (Read::Present(_) | Read::Absent) => Ok(hit),
            Read::Unknown => {
                let backing_read = self.backing.get(collection).await?;
                let to_patch = match &backing_read {
                    Read::Present(payload) => Some(payload.clone()),
                    Read::Absent => None,
                    // Backing should not return Unknown for Value; if it
                    // does, treat it as "do not patch" rather than synthesize.
                    Read::Unknown => return Ok(Read::Unknown),
                };
                self.patch_cache_or_invalidate(collection, to_patch).await;
                Ok(backing_read)
            }
        }
    }

    /// Writes through to backing, then patches the cache.
    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        self.backing.set(collection, payload.clone()).await?;
        self.patch_cache_or_invalidate(collection, Some(payload))
            .await;
        Ok(())
    }

    /// Writes through to backing, then patches the cache.
    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.backing.clear(collection).await?;
        self.patch_cache_or_invalidate(collection, None).await;
        Ok(())
    }
}

impl<Cache, Backing> DurableWalStore<ValueKind> for LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore + Clone,
    Backing: DurableWalStore<ValueKind>
        + ValueStore<Error = <Backing as DurableWalStore<ValueKind>>::Error>,
{
    type Error = <Backing as DurableWalStore<ValueKind>>::Error;

    /// Cache cannot answer WAL state; delegate unchanged.
    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        DurableWalStore::read_partition(&self.backing, collection).await
    }

    /// Invariant 2: applied state unchanged → cache untouched.
    async fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        event: EventRef,
        ops: I,
    ) -> Result<SealedCollection<ValueKind>, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        self.backing.seal(collection, event, ops).await
    }

    /// Invariant 3: on Applied, read backing's new applied via `get` and
    /// patch the cache. `NoOp` leaves the cache alone.
    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        let outcome = self
            .backing
            .apply_sealed(collection, expected_event)
            .await?;
        if outcome == StoreOutcome::Applied {
            let new_applied = match ValueStore::get(&self.backing, collection.id()).await? {
                Read::Present(payload) => Some(payload),
                Read::Absent | Read::Unknown => None,
            };
            self.patch_cache_or_invalidate(collection.id(), new_applied)
                .await;
        }
        Ok(outcome)
    }

    /// Invariant 4: applied state unchanged → cache untouched.
    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.backing
            .rollback_sealed(collection, expected_event)
            .await
    }
}

impl<Cache, Backing> DirectApplyStore<ValueKind> for LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore + Clone,
    Backing: DirectApplyStore<ValueKind>
        + ValueStore<Error = <Backing as DirectApplyStore<ValueKind>>::Error>,
{
    type Error = <Backing as DirectApplyStore<ValueKind>>::Error;

    /// Invariant 5: on Applied, derive the new applied from the last op
    /// (Value fold is last-writer-wins) and patch the cache. `NoOp`
    /// leaves the cache alone.
    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let ops: Vec<ValueOp> = ops.into_iter().collect();
        let last_op = ops.last().cloned();
        let outcome = self.backing.direct_apply(collection, ops).await?;
        if outcome == StoreOutcome::Applied {
            let new_applied = match last_op {
                Some(ValueOp::Set { payload }) => Some(payload),
                Some(ValueOp::Clear) | None => None,
            };
            self.patch_cache_or_invalidate(collection.id(), new_applied)
                .await;
        }
        Ok(outcome)
    }
}

/// Pending-index pass-through.
///
/// The cache holds no pending index — pending rows are authoritative state
/// owned by the backing store — so both methods delegate straight to
/// `backing`. `Error` is spelled as the backing's [`DurableWalStore`] error
/// (aligned with the [`DurableWalStore`] impl above) so
/// `Layered<Cache, Recovering<Backing>>` satisfies the middleware's
/// `PendingIndexStore<Error = DurableWalStore::Error>` bound.
impl<Cache, Backing> PendingIndexStore for LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore + Clone,
    Backing: DurableWalStore<ValueKind>
        + PendingIndexStore<Error = <Backing as DurableWalStore<ValueKind>>::Error>,
{
    type Error = <Backing as DurableWalStore<ValueKind>>::Error;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.backing.insert_pending(id).await
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.backing.delete_pending(id).await
    }
}
