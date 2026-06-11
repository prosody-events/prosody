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
//! shared `value_suite::DurableBundle` requires.
//!
//! ## Patch rules (load-bearing invariants)
//!
//! 1. **`get` cache-first, miss or cache error falls through and populates.**
//!    Cache `Read::Unknown` *or* a cache `Err` triggers exactly one backing
//!    `get`. The result is written back to the cache as a best-effort hint,
//!    automatically repairing a corrupt cell.
//! 2. **`seal` re-syncs the cache from the backing's post-seal applied.** A
//!    plain backing leaves `applied` unchanged across a seal, but a recovering
//!    backing may resolve a prior crashed-but-sealed WAL during seal
//!    (recover-before-seal), folding it into `applied` beneath the cache.
//!    Reading the post-seal applied via `read_partition` — which never triggers
//!    recovery, unlike `get` on the partition we just sealed — and patching
//!    keeps the cache coherent in both cases.
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
    DirectApplyStore, DurableWalStore, ValueKind, ValueOp, ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionKind, CollectionRef, DurableState, EventRef, Read, SealedCollection,
    StoreOutcome,
};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::pending::PendingIndexStore;
use crate::timers::store::SegmentId;
use bytes::Bytes;
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
        new_applied: Option<Bytes>,
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
    ) -> Result<Read<Bytes>, Self::Error> {
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
        payload: Bytes,
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

    /// Invariant 2: re-sync the cache from the backing's post-seal applied — a
    /// recovering backing may have changed it via recover-before-seal.
    async fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        event: EventRef,
        ops: I,
    ) -> Result<SealedCollection<ValueKind>, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let sealed = self.backing.seal(collection, event, ops).await?;
        // `read_partition` reports the applied state without resolving the WAL
        // we just sealed; `get` would recover it. The applied it returns already
        // reflects any recover-before-seal the backing performed.
        let new_applied = match DurableWalStore::read_partition(&self.backing, collection.id())
            .await
        {
            Ok(DurableState::Idle { applied } | DurableState::Sealed { applied, .. }) => applied,
            Err(error) => {
                // The seal committed, but we cannot read the post-seal
                // applied to re-sync the cache. Leaving the pre-seal entry
                // would be silently stale — and a Permanent classification
                // of this error strands it past recovery (the sweep rolls
                // back the WAL but never repatches the cache). Best-effort
                // invalidate so the next `get` re-reads the backing: seal's
                // contract is "cache re-synced or invalidated, never
                // silently stale" (mirrors `patch_cache_or_invalidate`).
                warn!(error = %error, "post-seal cache re-sync read failed; invalidating cache entry");
                if let Err(clear_error) = self.cache.clear(collection.id()).await {
                    warn!(
                        error = %clear_error,
                        "cache invalidation after failed re-sync also failed; entry may be stale until next miss"
                    );
                }
                return Err(error);
            }
        };
        self.patch_cache_or_invalidate(collection.id(), new_applied)
            .await;
        Ok(sealed)
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
        let new_applied = fold_value_ops(None, &ops);
        let outcome = self.backing.direct_apply(collection, ops).await?;
        if outcome == StoreOutcome::Applied {
            self.patch_cache_or_invalidate(collection.id(), new_applied)
                .await;
        }
        Ok(outcome)
    }
}

/// Descriptor-identity pass-through.
///
/// Identity rows are authoritative state owned by the backing store — the
/// cache holds none — so both methods delegate. `Error` is spelled as the
/// backing's [`DurableWalStore`] error (aligned with the impls above) so
/// the layered composition satisfies the middleware's
/// `DescriptorIdentityStore<Error = DurableWalStore::Error>` bound.
impl<Cache, Backing> DescriptorIdentityStore for LayeredValueStore<Cache, Backing>
where
    Cache: ValueStore,
    Backing: DurableWalStore<ValueKind>
        + DescriptorIdentityStore<Error = <Backing as DurableWalStore<ValueKind>>::Error>,
{
    type Error = <Backing as DurableWalStore<ValueKind>>::Error;

    async fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<DurableDescriptorIdentity>, Self::Error> {
        self.backing.read_descriptor_identities(segment_id).await
    }

    async fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> Result<(), Self::Error> {
        self.backing
            .write_descriptor_identities(segment_id, rows)
            .await
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

#[cfg(test)]
mod tests {
    use super::LayeredValueStore;
    use crate::Key;
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::memory::{MemoryDurableValueStore, MemoryStateError};
    use crate::state::value::{DurableWalStore, ValueKind, ValueOp, ValueStore};
    use crate::state::{
        CollectionId, CollectionRef, DurableState, EventRef, Read, SealedCollection, StateKey,
        StateName, StateType, StoreOutcome,
    };
    use bytes::Bytes;
    use color_eyre::eyre::Result;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use thiserror::Error;
    use uuid::Uuid;

    /// Backing that delegates to [`MemoryDurableValueStore`] but fails the
    /// `fail_at`-th `read_partition` (1-based). Used to simulate the post-seal
    /// re-sync read failing exactly once.
    #[derive(Clone)]
    struct FailNthReadPartition {
        inner: MemoryDurableValueStore,
        fail_at: usize,
        reads: Arc<AtomicUsize>,
    }

    impl FailNthReadPartition {
        fn new(fail_at: usize) -> Self {
            Self {
                inner: MemoryDurableValueStore::for_tests(),
                fail_at,
                reads: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl ValueStore for FailNthReadPartition {
        type Error = FailReadError;

        async fn get<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<Read<Bytes>, Self::Error> {
            self.inner.get(collection).await.map_err(Into::into)
        }

        async fn set<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
            payload: Bytes,
        ) -> Result<(), Self::Error> {
            self.inner
                .set(collection, payload)
                .await
                .map_err(Into::into)
        }

        async fn clear<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<(), Self::Error> {
            self.inner.clear(collection).await.map_err(Into::into)
        }
    }

    impl DurableWalStore<ValueKind> for FailNthReadPartition {
        type Error = FailReadError;

        async fn read_partition<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<DurableState<ValueKind>, Self::Error> {
            if self.reads.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
                return Err(FailReadError::Injected);
            }
            DurableWalStore::read_partition(&self.inner, collection)
                .await
                .map_err(Into::into)
        }

        async fn seal<'a, I>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            event: EventRef,
            ops: I,
        ) -> Result<SealedCollection<ValueKind>, Self::Error>
        where
            I: IntoIterator<Item = ValueOp> + Send + 'a,
        {
            self.inner
                .seal(collection, event, ops)
                .await
                .map_err(Into::into)
        }

        async fn apply_sealed<'a>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            expected_event: EventRef,
        ) -> Result<StoreOutcome, Self::Error> {
            self.inner
                .apply_sealed(collection, expected_event)
                .await
                .map_err(Into::into)
        }

        async fn rollback_sealed<'a>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            expected_event: EventRef,
        ) -> Result<StoreOutcome, Self::Error> {
            self.inner
                .rollback_sealed(collection, expected_event)
                .await
                .map_err(Into::into)
        }
    }

    #[derive(Debug, Error)]
    enum FailReadError {
        #[error("injected read_partition failure")]
        Injected,
        #[error(transparent)]
        Memory(#[from] MemoryStateError),
    }

    impl ClassifyError for FailReadError {
        fn classify_error(&self) -> ErrorCategory {
            ErrorCategory::Permanent
        }
    }

    fn collection_id(key: &Key) -> Result<CollectionId<ValueKind>> {
        Ok(CollectionId::new(
            StateKey::new(Uuid::from_u128(0x5E6), key.clone()),
            StateType::Application,
            StateName::try_new("cart")?,
        ))
    }

    /// Invariant 2 failure path: when the post-seal re-sync `read_partition`
    /// fails, `seal` must invalidate the cache before propagating the error —
    /// never leave the cache entry silently diverged from the backing.
    /// Otherwise a `Permanent` classification strands the stale entry past
    /// recovery, since the sweep rolls back the WAL but never repatches the
    /// cache.
    ///
    /// The cache is preloaded with a value the backing does not agree with
    /// (the recover-before-seal shape: a pre-recovery cache hint over a
    /// backing whose authoritative state has since moved). With the re-sync
    /// read failing once, `seal` returns the error, but the cache must be
    /// invalidated — so the next `get` falls through and *agrees with the
    /// backing* rather than serving the stale hit. Pre-fix, `?` propagated
    /// before any patch and the stale cache entry survived, so `get` would
    /// return `Present("old")` while the backing reads `Absent`.
    #[tokio::test]
    async fn failed_post_seal_resync_invalidates_cache_rather_than_leaving_it_stale() -> Result<()>
    {
        let key: Key = Arc::from("k");
        let id = collection_id(&key)?;
        let stale = Bytes::from_static(b"old");

        let cache = MemoryDurableValueStore::for_tests();
        // Fail the very first read_partition — the post-seal re-sync read.
        let backing = FailNthReadPartition::new(1);
        let layered = LayeredValueStore::new(cache.clone(), backing.clone());

        // The cache holds a value the backing does not agree with.
        cache.set(&id, stale.clone()).await?;

        let collection = CollectionRef::new(id.clone(), None);
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0xE0E0),
        };
        let result = layered
            .seal(
                &collection,
                event,
                [ValueOp::Set {
                    payload: stale.clone(),
                }],
            )
            .await;
        assert!(
            result.is_err(),
            "the failed re-sync read still surfaces as the seal's error"
        );

        // The backing disagrees with the stale cache entry; after the failed
        // re-sync the cache must be invalidated, so `get` agrees with the
        // backing instead of serving the stale hit.
        let backing_view = ValueStore::get(&backing, &id).await?;
        assert_ne!(
            backing_view,
            Read::Present(stale),
            "precondition: the backing does not hold the stale value"
        );
        assert_eq!(
            layered.get(&id).await?,
            backing_view,
            "post-seal get must reflect the backing, not the stale cache entry"
        );
        Ok(())
    }
}
