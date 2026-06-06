//! First-touch recovery combinator for the Value collection kind.
//!
//! `RecoveringValueStore<Inner, Oracle>` wraps any durable Value store that
//! implements [`ValueStore`] + [`DurableWalStore<ValueKind>`] +
//! [`DirectApplyStore<ValueKind>`] together with a [`CommitOracle`]. Two
//! methods recover a crashed-but-sealed WAL before proceeding —
//! [`ValueStore::get`] (read-before-use) and [`DurableWalStore::seal`]
//! (recover-before-overwrite); every other method passes through unchanged.
//! Together they cover every durable-touching path: `get` is the only read
//! that reaches durable, and `seal` is the only write that overwrites the
//! WAL columns.
//!
//! # Recovery flow ([`ValueStore::get`])
//!
//! 1. `inner.read_partition(id)`.
//! 2. `DurableState::Idle` → return `applied` as
//!    [`Read::Present`]/[`Read::Absent`].
//! 3. `DurableState::Sealed` → resolve the sealed WAL through the shared
//!    `resolve_sealed` helper (oracle → apply / rollback), then return
//!    `inner.get(..)`.
//!
//! # Recovery flow ([`DurableWalStore::seal`])
//!
//! Before overwriting the WAL columns, `seal` reads the partition. A
//! `Sealed` state owning a **different** event is resolved through
//! `resolve_sealed` first; a `Sealed` state for the seal's own event is a
//! redelivery, so recovery is skipped and the idempotent reseal proceeds
//! (the same-event guard — the oracle is not consulted).
//!
//! Recovery writes bind their TTL through a [`CollectionTtl`] resolver `R`,
//! so first-touch recovery binds the **same per-collection TTL** the
//! middleware's timer-sweep recovery does. The default resolver
//! [`ConstTtl`] reproduces the historical single-TTL behavior; production
//! wiring injects the shared [`Arc<CollectionDefRegistry>`] as `R` so a
//! per-collection override applies identically on both recovery paths.
//! `None` means "do not bind a TTL" — passed straight through to the
//! [`crate::state::cassandra::CassandraValueStore`]'s `*_no_ttl` query
//! variants. Binding a TTL when the collection opted out would corrupt the
//! durable retention contract; the wrapper never invents a TTL.
//!
//! [`Arc<CollectionDefRegistry>`]: crate::state::registry::CollectionDefRegistry
//!
//! # Concurrency
//!
//! Single-writer-per-key (CLAUDE.md "Concurrency invariants") makes
//! concurrent recovery on the same partition unrepresentable; no LWTs or
//! distributed locks are needed. The combinator owns no shared state of
//! its own.
//!
//! # Method recovery contract
//!
//! | Method                       | Recovers? |
//! |------------------------------|-----------|
//! | [`ValueStore::get`]          | **Yes** — read-before-use |
//! | [`ValueStore::set`]          | No (pass) — never touches durable |
//! | [`ValueStore::clear`]        | No (pass) — never touches durable |
//! | `DurableWalStore::read_partition` | No (pass) — raw visibility for the recovery scanner |
//! | `DurableWalStore::seal`            | **Yes** — recover-before-overwrite |
//! | `DurableWalStore::apply_sealed`    | No (pass) — the resolution itself |
//! | `DurableWalStore::rollback_sealed` | No (pass) — the resolution itself |
//! | `DirectApplyStore::direct_apply`   | No (pass) — direct mode never produces a sealed state |
//! | `PendingIndexStore::insert_pending` | No (pass) — the index rows live in `inner` |
//! | `PendingIndexStore::delete_pending` | No (pass) — the index rows live in `inner` |

use super::oracle::CommitOracle;
use super::value::{DirectApplyStore, DurableWalStore, ValueKind, ValueOp, ValueStore};
use super::{
    CollectionId, CollectionKind, CollectionRef, DurableState, EventRef, Read, SealedCollection,
    StoreOutcome,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::manager::{ResolveSealedError, resolve_sealed};
use crate::state::pending::PendingIndexStore;
use crate::state::registry::CollectionDefRegistry;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use bytes::Bytes;
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;

#[cfg(test)]
mod tests;

/// Resolves the TTL bound onto a recovery write for a given collection.
///
/// First-touch recovery ([`RecoveringValueStore::get`]) and the middleware's
/// timer-sweep recovery must bind the **same** TTL for a collection, or the
/// two paths corrupt the retention contract in opposite directions (a write
/// that lives forever via one path and expires early via the other). Sharing
/// one resolver makes that divergence unrepresentable.
pub trait CollectionTtl: Clone + Send + Sync + 'static {
    /// Returns the TTL to bind for `id`, or `None` for indefinite retention
    /// / the Cassandra over-20-year overflow fallback.
    fn ttl_for(&self, id: &CollectionId<ValueKind>) -> Option<CompactDuration>;
}

/// A [`CollectionTtl`] that binds the same TTL for every collection.
///
/// The default resolver, preserving the wrapper's historical single-TTL
/// behavior. Use [`RecoveringValueStore::with_default_ttl`] to construct a
/// store backed by this resolver.
#[derive(Clone, Copy, Debug)]
pub struct ConstTtl(pub Option<CompactDuration>);

impl CollectionTtl for ConstTtl {
    fn ttl_for(&self, _id: &CollectionId<ValueKind>) -> Option<CompactDuration> {
        self.0
    }
}

impl CollectionTtl for Arc<CollectionDefRegistry> {
    fn ttl_for(&self, id: &CollectionId<ValueKind>) -> Option<CompactDuration> {
        CollectionDefRegistry::ttl_for(self, id.name())
    }
}

/// Bundle bound for the inner store the recovering wrapper drives.
///
/// A Value store whose [`ValueStore`] and [`DurableWalStore`] error types
/// coincide — the single constraint shared verbatim by the wrapper's
/// [`ValueStore`] and [`DurableWalStore`] impls. Mirrors
/// [`DurableValueBundle`](super::middleware::DurableValueBundle) but omits
/// the direct-apply / `Clone` / `Debug` requirements those two impls do not
/// need, keeping the bound local to what recovery reads require. The
/// [`DirectApplyStore`] impl adds [`DirectApplyStore`] on top.
pub(crate) trait RecoverableValueStore:
    ValueStore<Error = <Self as DurableWalStore<ValueKind>>::Error> + DurableWalStore<ValueKind>
{
}

impl<T> RecoverableValueStore for T where
    T: ValueStore<Error = <T as DurableWalStore<ValueKind>>::Error> + DurableWalStore<ValueKind>
{
}

/// First-touch recovery wrapper over an authoritative Value store.
///
/// See the [module docs][self] for the recovery contract and per-method
/// behavior. Production wiring composes
/// `Layered<Cache, Recovering<Backing, Oracle>>` so the cache populates with
/// post-recovery applied for free.
#[derive(Clone, Debug)]
pub struct RecoveringValueStore<Inner, Oracle, R = ConstTtl> {
    inner: Inner,
    oracle: Oracle,
    ttl: R,
}

impl<Inner, Oracle, R> RecoveringValueStore<Inner, Oracle, R> {
    /// Wraps `inner` with first-touch recovery driven by `oracle`, binding
    /// recovery-write TTLs through the [`CollectionTtl`] resolver `ttl`.
    ///
    /// Production wiring passes the shared
    /// [`Arc<CollectionDefRegistry>`](crate::state::registry::CollectionDefRegistry)
    /// here so first-touch recovery binds the same per-collection TTL the
    /// timer-sweep recovery does. For a single fixed TTL, prefer
    /// [`Self::with_default_ttl`].
    #[must_use]
    pub fn new(inner: Inner, oracle: Oracle, ttl: R) -> Self {
        Self { inner, oracle, ttl }
    }
}

impl<Inner, Oracle> RecoveringValueStore<Inner, Oracle, ConstTtl> {
    /// Wraps `inner` with first-touch recovery that binds a single fixed
    /// TTL on every recovery write, via a [`ConstTtl`] resolver.
    ///
    /// `default_ttl` is bound onto every [`CollectionRef`] this wrapper
    /// builds for recovery writes. `None` is a first-class choice:
    /// indefinite retention or the Cassandra over-20-year overflow
    /// fallback. The wrapper never silently substitutes a default; `None`
    /// propagates verbatim into the underlying `*_no_ttl` query path.
    #[must_use]
    pub fn with_default_ttl(
        inner: Inner,
        oracle: Oracle,
        default_ttl: Option<CompactDuration>,
    ) -> Self {
        Self::new(inner, oracle, ConstTtl(default_ttl))
    }
}

impl<Inner, Oracle, R> ValueStore for RecoveringValueStore<Inner, Oracle, R>
where
    Inner: RecoverableValueStore,
    Oracle: CommitOracle,
    R: CollectionTtl,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    /// Read with first-touch recovery for [`DurableState::Sealed`]
    /// partitions. See the [module docs][self] for the full flow.
    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        match DurableWalStore::read_partition(&self.inner, collection)
            .await
            .map_err(RecoveringValueStoreError::Inner)?
        {
            DurableState::Idle { applied } => Ok(applied.map_or(Read::Absent, Read::Present)),
            DurableState::Sealed { wal, .. } => {
                let recovery_ref =
                    CollectionRef::new(collection.clone(), self.ttl.ttl_for(collection));
                resolve_sealed(&self.inner, &self.oracle, &recovery_ref, wal.event()).await?;
                ValueStore::get(&self.inner, collection)
                    .await
                    .map_err(RecoveringValueStoreError::Inner)
            }
        }
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        self.inner
            .set(collection, payload)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.inner
            .clear(collection)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }
}

impl<Inner, Oracle, R> DurableWalStore<ValueKind> for RecoveringValueStore<Inner, Oracle, R>
where
    Inner: RecoverableValueStore,
    Oracle: CommitOracle,
    R: CollectionTtl,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        DurableWalStore::read_partition(&self.inner, collection)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    /// Recover-before-seal: resolve any prior crashed-but-sealed WAL on this
    /// partition before overwriting it.
    ///
    /// A WAL-mode seal overwrites the per-collection WAL columns. If a prior
    /// event sealed and crashed before its commit decision was resolved, that
    /// decision is recoverable only from the durable `Sealed` state — sealing
    /// over it loses the decision permanently. So before sealing we read the
    /// partition; on a `Sealed` state owning a **different** event we resolve
    /// it through the oracle (apply or rollback) first. A `Sealed` state for
    /// *our own* `event` is a redelivery: resealing it is idempotent and the
    /// oracle is not consulted (the same-event guard).
    ///
    /// Ordering is load-bearing and free from the sequential `await` plus
    /// single-writer-per-key (CLAUDE.md): recovery's `delete_pending`
    /// completes before the new seal's `insert_pending`, both keyed on the
    /// same per-collection pending row.
    async fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        event: EventRef,
        ops: I,
    ) -> Result<SealedCollection<ValueKind>, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        if let DurableState::Sealed { wal, .. } =
            DurableWalStore::read_partition(&self.inner, collection.id())
                .await
                .map_err(RecoveringValueStoreError::Inner)?
            && wal.event() != event
        {
            let recovery_ref =
                CollectionRef::new(collection.id().clone(), self.ttl.ttl_for(collection.id()));
            resolve_sealed(&self.inner, &self.oracle, &recovery_ref, wal.event()).await?;
        }
        self.inner
            .seal(collection, event, ops)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.inner
            .apply_sealed(collection, expected_event)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.inner
            .rollback_sealed(collection, expected_event)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }
}

impl<Inner, Oracle, R> DirectApplyStore<ValueKind> for RecoveringValueStore<Inner, Oracle, R>
where
    Inner: RecoverableValueStore
        + DirectApplyStore<ValueKind, Error = <Inner as DurableWalStore<ValueKind>>::Error>,
    Oracle: CommitOracle,
    R: CollectionTtl,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        self.inner
            .direct_apply(collection, ops)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }
}

/// Pending-index pass-through.
///
/// The wrapper owns no pending index of its own; recovery resolves the
/// inner store's sealed WALs, and the pending rows that index them live in
/// `inner`. Delegating keeps `Self::Error` equal to the wrapper's
/// [`DurableWalStore`] error so `Layered<Cache, Recovering<Backing>>`
/// satisfies the middleware's
/// `PendingIndexStore<Error = DurableWalStore::Error>` bound.
/// Descriptor-identity pass-through: identity validation happens before
/// any state op, so recovery never participates — delegate to `inner` and
/// lift its error into the wrapper's error type.
impl<Inner, Oracle, R> DescriptorIdentityStore for RecoveringValueStore<Inner, Oracle, R>
where
    Inner: RecoverableValueStore
        + DescriptorIdentityStore<Error = <Inner as DurableWalStore<ValueKind>>::Error>,
    Oracle: CommitOracle,
    R: CollectionTtl,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    async fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<DurableDescriptorIdentity>, Self::Error> {
        self.inner
            .read_descriptor_identities(segment_id)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    async fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> Result<(), Self::Error> {
        self.inner
            .write_descriptor_identities(segment_id, rows)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }
}

impl<Inner, Oracle, R> PendingIndexStore for RecoveringValueStore<Inner, Oracle, R>
where
    Inner: RecoverableValueStore
        + PendingIndexStore<Error = <Inner as DurableWalStore<ValueKind>>::Error>,
    Oracle: CommitOracle,
    R: CollectionTtl,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner
            .insert_pending(id)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner
            .delete_pending(id)
            .await
            .map_err(RecoveringValueStoreError::Inner)
    }
}

/// Error returned by [`RecoveringValueStore`].
///
/// The single enum is shared across [`ValueStore`],
/// [`DurableWalStore`], and [`DirectApplyStore`] so the
/// `DurableBundle::ValueStore::Error == DurableWalStore::Error` constraint
/// used by the shared value test suite is satisfied without alias
/// gymnastics.
#[derive(Debug, Error)]
pub enum RecoveringValueStoreError<InnerError, OracleError>
where
    InnerError: ClassifyError + Error + Send + Sync + 'static,
    OracleError: ClassifyError + Error + Send + Sync + 'static,
{
    /// Inner Value store failed.
    #[error("recovering value store inner failed")]
    Inner(#[source] InnerError),

    /// Commit oracle failed.
    #[error("recovering value store oracle failed")]
    Oracle(#[source] OracleError),
}

impl<InnerError, OracleError> ClassifyError for RecoveringValueStoreError<InnerError, OracleError>
where
    InnerError: ClassifyError + Error + Send + Sync + 'static,
    OracleError: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(error) => error.classify_error(),
            Self::Oracle(error) => error.classify_error(),
        }
    }
}

/// A [`resolve_sealed`] failure folds into the wrapper's error: a durable
/// failure becomes [`Inner`](RecoveringValueStoreError::Inner), an oracle
/// failure [`Oracle`](RecoveringValueStoreError::Oracle). Lets the get-side
/// and seal-side recovery callsites resolve with `?`.
impl<InnerError, OracleError> From<ResolveSealedError<InnerError, OracleError>>
    for RecoveringValueStoreError<InnerError, OracleError>
where
    InnerError: ClassifyError + Error + Send + Sync + 'static,
    OracleError: ClassifyError + Error + Send + Sync + 'static,
{
    fn from(error: ResolveSealedError<InnerError, OracleError>) -> Self {
        match error {
            ResolveSealedError::Durable(e) => Self::Inner(e),
            ResolveSealedError::Oracle(e) => Self::Oracle(e),
        }
    }
}
