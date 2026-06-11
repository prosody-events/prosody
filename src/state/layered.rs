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

/// The cache role over Value cells: a [`ValueStore`] plus true entry
/// invalidation.
///
/// `invalidate` *removes* the entry, so the next [`ValueStore::get`]
/// returns [`Read::Unknown`] and the layered read falls through to the
/// backing. This is deliberately distinct from [`ValueStore::clear`], which
/// writes an authoritative **known-Absent** cell — using `clear` to
/// "invalidate" would assert "the value is absent" over a backing that may
/// hold one, serving wrong reads until the next successful patch. Keeping
/// the two operations as separate methods makes that confusion
/// unrepresentable at the call site.
pub trait ValueCache: ValueStore {
    /// Removes the cached entry for `id`, if any, so the next read misses.
    ///
    /// # Errors
    ///
    /// Returns the cache's store error when removal fails; callers treat
    /// invalidation as best-effort and log.
    fn invalidate(
        &self,
        id: &CollectionId<ValueKind>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

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
    Cache: ValueCache,
    Backing: ValueStore,
{
    /// Patches the cache best-effort: writes `Present`/`Absent`,
    /// invalidates (removes the entry) on write failure.
    ///
    /// Invariant 6: a cache failure after a backing-side success never
    /// surfaces as `Err`. If the patch fails we invalidate so the next read
    /// misses and re-populates from the backing.
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
            self.invalidate_cache_entry(id).await;
        }
    }

    /// Best-effort cache invalidation: removes the entry so the next read
    /// misses and falls through to the backing. A removal failure is logged
    /// — never propagated — after which the entry may serve stale until the
    /// next successful patch.
    async fn invalidate_cache_entry(&self, id: &CollectionId<ValueKind>) {
        if let Err(error) = self.cache.invalidate(id).await {
            warn!(
                error = %error,
                "cache invalidation failed; entry may be stale until the next successful patch"
            );
        }
    }
}

impl<Cache, Backing> ValueStore for LayeredValueStore<Cache, Backing>
where
    Cache: ValueCache,
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
    Cache: ValueCache + Clone,
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
                // silently stale".
                warn!(error = %error, "post-seal cache re-sync read failed; invalidating cache entry");
                self.invalidate_cache_entry(collection.id()).await;
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
            // The apply changed authoritative state beneath the cache. If
            // the re-read fails we cannot patch — invalidate before
            // propagating, or the cache serves the pre-apply value
            // indefinitely (a retried apply resolves `NoOp` and never
            // reaches this patch again). Same contract as `seal`:
            // re-synced or invalidated, never silently stale.
            let new_applied = match ValueStore::get(&self.backing, collection.id()).await {
                Ok(Read::Present(payload)) => Some(payload),
                Ok(Read::Absent | Read::Unknown) => None,
                Err(error) => {
                    warn!(error = %error, "post-apply cache re-sync read failed; invalidating cache entry");
                    self.invalidate_cache_entry(collection.id()).await;
                    return Err(error);
                }
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
    Cache: ValueCache + Clone,
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
    use super::{LayeredValueStore, ValueCache};
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
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use thiserror::Error;
    use uuid::Uuid;

    /// Purpose-built cache fixture with real miss semantics: a missing
    /// entry reads [`Read::Unknown`] (unlike the durable memory store,
    /// whose `get` answers `Absent`), `invalidate` *removes* the entry, and
    /// `set` can be scripted to fail — the seam the invalidation paths
    /// need.
    #[derive(Clone, Debug, Default)]
    struct TestCache {
        cells: Arc<Mutex<HashMap<CollectionId<ValueKind>, Option<Bytes>>>>,
        fail_sets: Arc<AtomicUsize>,
    }

    impl TestCache {
        fn new() -> Self {
            Self::default()
        }

        /// Fails the first `count` `set` calls.
        fn failing_sets(count: usize) -> Self {
            Self {
                cells: Arc::default(),
                fail_sets: Arc::new(AtomicUsize::new(count)),
            }
        }

        fn preload(&self, id: &CollectionId<ValueKind>, payload: Bytes) {
            self.cells.lock().insert(id.clone(), Some(payload));
        }
    }

    impl ValueStore for TestCache {
        type Error = FailReadError;

        async fn get<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<Read<Bytes>, Self::Error> {
            Ok(match self.cells.lock().get(collection) {
                None => Read::Unknown,
                Some(None) => Read::Absent,
                Some(Some(payload)) => Read::Present(payload.clone()),
            })
        }

        async fn set<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
            payload: Bytes,
        ) -> Result<(), Self::Error> {
            if self.fail_sets.load(Ordering::SeqCst) > 0 {
                self.fail_sets.fetch_sub(1, Ordering::SeqCst);
                return Err(FailReadError::Injected);
            }
            self.cells.lock().insert(collection.clone(), Some(payload));
            Ok(())
        }

        async fn clear<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<(), Self::Error> {
            self.cells.lock().insert(collection.clone(), None);
            Ok(())
        }
    }

    impl ValueCache for TestCache {
        async fn invalidate(
            &self,
            collection: &CollectionId<ValueKind>,
        ) -> Result<(), Self::Error> {
            self.cells.lock().remove(collection);
            Ok(())
        }
    }

    /// Backing that delegates to [`MemoryDurableValueStore`] but fails the
    /// `fail_at`-th `read_partition` (1-based) and the first `fail_gets`
    /// `get` calls. Used to simulate the post-seal / post-apply re-sync
    /// reads failing exactly once.
    #[derive(Clone)]
    struct FailNthReadPartition {
        inner: MemoryDurableValueStore,
        fail_at: usize,
        reads: Arc<AtomicUsize>,
        fail_gets: Arc<AtomicUsize>,
    }

    impl FailNthReadPartition {
        fn new(fail_at: usize) -> Self {
            Self {
                inner: MemoryDurableValueStore::for_tests(),
                fail_at,
                reads: Arc::new(AtomicUsize::new(0)),
                fail_gets: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Never fails `read_partition`; fails the first `count` `get`s.
        fn failing_gets(count: usize) -> Self {
            Self {
                inner: MemoryDurableValueStore::for_tests(),
                fail_at: 0,
                reads: Arc::new(AtomicUsize::new(0)),
                fail_gets: Arc::new(AtomicUsize::new(count)),
            }
        }
    }

    impl ValueStore for FailNthReadPartition {
        type Error = FailReadError;

        async fn get<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<Read<Bytes>, Self::Error> {
            if self.fail_gets.load(Ordering::SeqCst) > 0 {
                self.fail_gets.fetch_sub(1, Ordering::SeqCst);
                return Err(FailReadError::Injected);
            }
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
    /// fails, `seal` must invalidate the cache (remove the entry) before
    /// propagating the error — never leave the cache entry silently
    /// diverged from the backing. Otherwise a `Permanent` classification
    /// strands the stale entry past recovery, since the sweep rolls back
    /// the WAL but never repatches the cache.
    ///
    /// The cache is preloaded with a stale value while the backing's
    /// authoritative applied has moved (the recover-before-seal shape).
    /// After the failed re-sync the next `get` must *miss* and return the
    /// backing's value — pre-fix the stale `Present` hit survived
    /// indefinitely.
    #[tokio::test]
    async fn failed_post_seal_resync_invalidates_cache_rather_than_leaving_it_stale() -> Result<()>
    {
        let key: Key = Arc::from("k");
        let id = collection_id(&key)?;
        let stale = Bytes::from_static(b"old");
        let recovered = Bytes::from_static(b"recovered");

        let cache = TestCache::new();
        // Fail the very first read_partition — the post-seal re-sync read.
        let backing = FailNthReadPartition::new(1);
        let layered = LayeredValueStore::new(cache.clone(), backing.clone());

        // The backing's applied state moved beneath a stale cache hint.
        backing.inner.set(&id, recovered.clone()).await?;
        cache.preload(&id, stale.clone());

        let collection = CollectionRef::new(id.clone(), None);
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0xE0E0),
        };
        let result = layered
            .seal(&collection, event, [ValueOp::Set { payload: stale }])
            .await;
        assert!(
            result.is_err(),
            "the failed re-sync read still surfaces as the seal's error"
        );

        assert_eq!(
            layered.get(&id).await?,
            Read::Present(recovered),
            "post-seal get must miss the invalidated cache and serve the backing"
        );
        Ok(())
    }

    /// B1 pin: invalidation must *remove* the entry, never write a
    /// known-`Absent` cell. A durable `set` succeeds, the cache patch fails
    /// once, and the fallback invalidates — the next `get` must fall
    /// through and return the freshly written `Present` value. With
    /// clear-as-invalidate (the pre-fix behavior) the cache asserted
    /// `Absent` over the backing's `Present`, serving wrong reads until the
    /// next successful patch.
    #[tokio::test]
    async fn failed_patch_invalidation_never_asserts_absent() -> Result<()> {
        let key: Key = Arc::from("k");
        let id = collection_id(&key)?;
        let value = Bytes::from_static(b"v");

        let cache = TestCache::failing_sets(1);
        let backing = FailNthReadPartition::new(0);
        let layered = LayeredValueStore::new(cache, backing);

        // Backing write succeeds; the cache patch fails and falls back to
        // invalidation.
        layered.set(&id, value.clone()).await?;

        assert_eq!(
            layered.get(&id).await?,
            Read::Present(value),
            "after a failed patch the cache must miss, not assert Absent"
        );
        Ok(())
    }

    /// Invariant 3 failure path (the `apply_sealed` sibling of the seal
    /// test): the apply changed authoritative state beneath the cache; if
    /// the post-apply re-read fails, the cache must be invalidated before
    /// the error propagates. Pre-fix the `?` skipped the patch entirely and
    /// the stale entry survived — unhealable, because the sweep's retried
    /// apply resolves `NoOp` and never patches.
    #[tokio::test]
    async fn failed_post_apply_resync_invalidates_cache_rather_than_leaving_it_stale() -> Result<()>
    {
        let key: Key = Arc::from("k");
        let id = collection_id(&key)?;
        let stale = Bytes::from_static(b"old");
        let new = Bytes::from_static(b"new");

        let cache = TestCache::new();
        // Fail the first backing `get` — the post-apply re-sync read.
        let backing = FailNthReadPartition::failing_gets(1);
        let layered = LayeredValueStore::new(cache.clone(), backing.clone());

        // A sealed WAL waits while the cache holds the pre-apply value.
        let collection = CollectionRef::new(id.clone(), None);
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0xE0E1),
        };
        backing
            .inner
            .seal(
                &collection,
                event,
                vec![ValueOp::Set {
                    payload: new.clone(),
                }],
            )
            .await?;
        cache.preload(&id, stale);

        let result = layered.apply_sealed(&collection, event).await;
        assert!(
            result.is_err(),
            "the failed re-sync read still surfaces as the apply's error"
        );

        assert_eq!(
            layered.get(&id).await?,
            Read::Present(new),
            "post-apply get must miss the invalidated cache and serve the applied value"
        );
        Ok(())
    }
}
