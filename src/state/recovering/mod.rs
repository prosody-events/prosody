//! First-touch recovery combinator for the Value collection kind.
//!
//! `RecoveringValueStore<Inner, Oracle>` wraps any durable Value store that
//! implements [`ValueStore`] + [`DurableWalStore<ValueKind>`] +
//! [`DirectApplyStore<ValueKind>`] together with a [`CommitOracle`]. The
//! only recovery-triggering method is [`ValueStore::get`]; every other
//! method passes through unchanged.
//!
//! # Recovery flow ([`ValueStore::get`])
//!
//! 1. `inner.read_partition(id)`.
//! 2. [`DurableState::Idle { applied }`] → return `applied` as
//!    [`Read::Present`]/[`Read::Absent`].
//! 3. [`DurableState::Sealed { wal, .. }`] → consult the oracle:
//!    - [`CommitDecision::Committed`] → `inner.apply_sealed(..)` and return
//!      `inner.get(..)`.
//!    - [`CommitDecision::NotCommitted`] → `inner.rollback_sealed(..)` and
//!      return `inner.get(..)`.
//!
//! Recovery writes use a [`CollectionRef::new(id.clone(), self.default_ttl)`].
//! Production wiring (Slice 8) supplies the same `default_ttl` to both the
//! inner store and this wrapper so the two values agree. `None` means
//! "do not bind a TTL" — passed straight through to the
//! [`crate::state::cassandra::CassandraValueStore`]'s `*_no_ttl` query
//! variants. Binding a TTL when the collection opted out would corrupt the
//! durable retention contract; the wrapper never invents a TTL.
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
//! | [`ValueStore::get`]          | **Yes**   |
//! | [`ValueStore::set`]          | No (pass) |
//! | [`ValueStore::clear`]        | No (pass) |
//! | `DurableWalStore::read_partition` | No (pass) — raw visibility for the future Slice 8 scanner |
//! | `DurableWalStore::seal`            | No (pass) — recovery itself runs through this method |
//! | `DurableWalStore::apply_sealed`    | No (pass) |
//! | `DurableWalStore::rollback_sealed` | No (pass) |
//! | `DirectApplyStore::direct_apply`   | No (pass) — direct mode never produces a sealed state |

use super::oracle::CommitOracle;
use super::value::{
    DirectApplyStore, DurableWalStore, StoredPayload, ValueKind, ValueOp, ValueStore,
};
use super::{
    CollectionId, CollectionRef, CommitDecision, DurableState, EventRef, Read, SealedCollection,
    StoreOutcome,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDuration;
use std::error::Error;
use thiserror::Error;

#[cfg(test)]
mod tests;

/// First-touch recovery wrapper over an authoritative Value store.
///
/// See the [module docs][self] for the recovery contract and per-method
/// behavior. Production wiring (Slice 8) composes
/// `Layered<Cache, Recovering<Backing, Oracle>>` so the cache populates with
/// post-recovery applied for free.
#[derive(Clone, Debug)]
pub struct RecoveringValueStore<Inner, Oracle> {
    inner: Inner,
    oracle: Oracle,
    default_ttl: Option<CompactDuration>,
}

impl<Inner, Oracle> RecoveringValueStore<Inner, Oracle> {
    /// Wraps `inner` with first-touch recovery driven by `oracle`.
    ///
    /// `default_ttl` is the TTL bound onto every [`CollectionRef`] this
    /// wrapper builds for recovery writes (`apply_sealed` /
    /// `rollback_sealed`). Pass the same value supplied to the inner
    /// store's constructor so the two stores agree on retention. `None`
    /// is a first-class choice: indefinite retention or the Cassandra
    /// over-20-year overflow fallback. The wrapper never silently
    /// substitutes a default; `None` propagates verbatim into the
    /// underlying `*_no_ttl` query path.
    #[must_use]
    pub fn new(inner: Inner, oracle: Oracle, default_ttl: Option<CompactDuration>) -> Self {
        Self {
            inner,
            oracle,
            default_ttl,
        }
    }

    /// Returns a reference to the wrapped inner store.
    #[must_use]
    pub fn inner(&self) -> &Inner {
        &self.inner
    }

    /// Returns a reference to the wrapped oracle.
    #[must_use]
    pub fn oracle(&self) -> &Oracle {
        &self.oracle
    }
}

impl<Inner, Oracle> ValueStore for RecoveringValueStore<Inner, Oracle>
where
    Inner: ValueStore<Error = <Inner as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>,
    Oracle: CommitOracle,
{
    type Error =
        RecoveringValueStoreError<<Inner as DurableWalStore<ValueKind>>::Error, Oracle::Error>;

    /// Read with first-touch recovery for [`DurableState::Sealed`]
    /// partitions. See the [module docs][self] for the full flow.
    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        match DurableWalStore::read_partition(&self.inner, collection)
            .await
            .map_err(RecoveringValueStoreError::Inner)?
        {
            DurableState::Idle { applied } => Ok(applied.map_or(Read::Absent, Read::Present)),
            DurableState::Sealed { wal, .. } => {
                let decision = self
                    .oracle
                    .resolve(collection, wal.event())
                    .await
                    .map_err(RecoveringValueStoreError::Oracle)?;
                let recovery_ref = CollectionRef::new(collection.clone(), self.default_ttl);
                match decision {
                    CommitDecision::Committed => {
                        self.inner
                            .apply_sealed(&recovery_ref, wal.event())
                            .await
                            .map_err(RecoveringValueStoreError::Inner)?;
                    }
                    CommitDecision::NotCommitted => {
                        self.inner
                            .rollback_sealed(&recovery_ref, wal.event())
                            .await
                            .map_err(RecoveringValueStoreError::Inner)?;
                    }
                }
                ValueStore::get(&self.inner, collection)
                    .await
                    .map_err(RecoveringValueStoreError::Inner)
            }
        }
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
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

impl<Inner, Oracle> DurableWalStore<ValueKind> for RecoveringValueStore<Inner, Oracle>
where
    Inner: ValueStore<Error = <Inner as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>,
    Oracle: CommitOracle,
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

impl<Inner, Oracle> DirectApplyStore<ValueKind> for RecoveringValueStore<Inner, Oracle>
where
    Inner: DirectApplyStore<ValueKind>
        + ValueStore<Error = <Inner as DirectApplyStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind, Error = <Inner as DirectApplyStore<ValueKind>>::Error>,
    Oracle: CommitOracle,
{
    type Error =
        RecoveringValueStoreError<<Inner as DirectApplyStore<ValueKind>>::Error, Oracle::Error>;

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
