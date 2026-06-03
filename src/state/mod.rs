//! Keyed application state protocol types.
//!
//! This module defines the shared typed identity and transaction state shapes
//! used by keyed state stores. The first implemented collection kind is
//! [`ValueKind`], but collection identities carry the kind both statically and
//! at runtime so future collection families cannot share state by accident.
//!
//! The shapes themselves live in leaf-to-root submodules and are
//! re-exported flat below, so consumers keep importing `crate::state::X`:
//!
//! * [`identity`] — typed collection identity ([`CollectionId`],
//!   [`CollectionRef`], [`StateKey`], [`CollectionKind`], …).
//! * [`event_ref`] — event identity and verdicts ([`EventRef`],
//!   [`CommitDecision`], [`StoreOutcome`], …).
//! * [`encoding`] — payload/WAL encoding selectors ([`PayloadEncoding`],
//!   [`WalFormat`], [`EncodingError`]).
//! * [`wal`] — write-ahead-log payloads ([`WalEnvelope`], [`WalBlob`],
//!   [`SealedWal`], [`NonEmptyOps`], …).
//! * [`value`] — the Value collection kind ([`ValueKind`], [`ValueOp`],
//!   [`ValueOverlay`], …).
//! * [`descriptor`] — typed descriptors and handles bound over the raw byte
//!   cells the stores persist.
//! * [`transaction`] — transaction-side state ([`DurableState`], [`LocalTx`],
//!   [`CommitMode`], [`SealedCollection`], …).
//!
//! The cross-cutting factory traits ([`DirtyStoreProvider`],
//! [`DirtyStoreFactory`], [`StateBackendFactory`]) belong to no leaf and
//! stay here.

use crate::error::ClassifyError;
use crate::state::oracle::CommitOracle;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

pub mod cassandra;
pub mod descriptor;
pub mod encoding;
pub mod event_ref;
pub mod fjall;
pub mod identity;
pub mod layered;
pub mod memory;
pub mod middleware;
pub mod oracle;
pub mod pending;
pub mod production;
pub mod recovering;
pub mod transaction;
pub mod value;
pub mod wal;

#[cfg(test)]
mod dirty_value_test_suite;
#[cfg(test)]
mod encoding_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod value_test_suite;

pub use encoding::{EncodingError, PayloadEncoding, WalFormat};
pub use event_ref::{CommitDecision, EventRef, EventScopeId, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, StateKey, StateName,
    StateNameError, StateType,
};
pub use transaction::{
    CommitMode, DirtyCollection, DurableState, LocalTx, PendingOps, Read, SealedCollection,
};
pub use value::{ValueApplied, ValueKind, ValueOp, ValueOverlay};
pub use wal::{
    EmptyOperationsError, NonEmptyOps, NonEmptyOpsSlice, SealedWal, WalBlob, WalEnvelope,
};

/// The per-partition keyed-state backend: the durable Value bundle, the
/// commit oracle it recovers through, and the dirty-workspace provider.
///
/// Minted as one unit by [`StateBackendFactory::for_partition`] so the
/// oracle baked into the durable bundle's recovery wrapper and the oracle
/// the middleware's sweep consults are the *same* instance, and the
/// fjall workspace backing the dirty provider and the durable cache is
/// opened once.
pub struct StateBackend<D, O, P> {
    /// Durable Value bundle for this partition.
    pub durable: D,

    /// Commit oracle for this partition's recovery decisions.
    pub oracle: O,

    /// Per-event dirty-workspace provider for this partition.
    pub dirty: P,
}

/// The backend triple a [`StateBackendFactory`] mints for one partition.
pub type BackendOf<B> = StateBackend<
    <B as StateBackendFactory>::Durable,
    <B as StateBackendFactory>::Oracle,
    <B as StateBackendFactory>::DirtyProvider,
>;

/// Process-wide factory minting the per-partition keyed-state backend.
///
/// Both the commit oracle's timer-tag reads and the durable stores are
/// partition-scoped (timer tags live in segment-keyed tables; the fjall
/// workspace is per assignment), so the backend cannot be a single global
/// value — the keyed-state middleware calls [`Self::for_partition`] inside
/// `handler_for_partition` and surfaces failures on first dispatch, the
/// same shape as [`DirtyStoreFactory`].
pub trait StateBackendFactory: Clone + Send + Sync + 'static {
    /// Durable Value bundle minted per partition.
    type Durable;

    /// Commit oracle minted per partition.
    type Oracle: CommitOracle;

    /// Per-event dirty-workspace provider minted per partition.
    type DirtyProvider: DirtyStoreProvider<ValueKind>;

    /// Error returned when a partition's backend cannot be materialized.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Mints the backend for `(topic, partition)`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when partition-scoped state (e.g. the
    /// fjall workspace) cannot be opened.
    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<BackendOf<Self>, Self::Error>;
}

/// Partition-agnostic [`StateBackendFactory`]: clones the same durable
/// bundle, oracle, and dirty provider for every partition.
///
/// Suits compositions whose stores are not partition-scoped — memory-backed
/// tests and bespoke wiring; production uses the partition-scoped factories
/// in [`production`].
#[derive(Clone, Debug)]
pub struct SharedStateBackend<D, O, P> {
    durable: D,
    oracle: O,
    dirty: P,
}

impl<D, O, P> SharedStateBackend<D, O, P> {
    /// Creates a backend factory that hands out clones of the supplied
    /// parts.
    #[must_use]
    pub fn new(durable: D, oracle: O, dirty: P) -> Self {
        Self {
            durable,
            oracle,
            dirty,
        }
    }
}

impl<D, O, P> StateBackendFactory for SharedStateBackend<D, O, P>
where
    D: Clone + Send + Sync + 'static,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
{
    type DirtyProvider = P;
    type Durable = D;
    type Error = Infallible;
    type Oracle = O;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> Result<StateBackend<D, O, P>, Self::Error> {
        Ok(StateBackend {
            durable: self.durable.clone(),
            oracle: self.oracle.clone(),
            dirty: self.dirty.clone(),
        })
    }
}

/// Per-partition factory for dirty stores of one collection kind.
///
/// A [`DirtyStoreProvider`] is minted *per Kafka partition* — its
/// [`Self::Store`] type owns whichever partition-scoped state (e.g. Fjall
/// partition handles) the dirty workspace needs. The middleware calls
/// [`Self::for_scope`] once per event handler invocation to materialize
/// the per-event dirty workspace.
///
/// To create a partition-scoped provider from a process-wide factory,
/// use [`DirtyStoreFactory::for_partition`].
pub trait DirtyStoreProvider<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Per-event dirty workspace this provider mints.
    type Store: value::PendingOpSource<K> + fmt::Debug + 'static;

    /// Returns a fresh per-event dirty workspace bound to `scope`.
    fn for_scope(&self, scope: EventScopeId) -> Self::Store;
}

/// Process-wide factory that produces per-partition
/// [`DirtyStoreProvider`]s on demand.
///
/// The keyed-state middleware owns a single `F: DirtyStoreFactory<...>`
/// at the type level and calls [`Self::for_partition`] inside
/// `handler_for_partition` to mint a partition-scoped provider that
/// stays alive for the lifetime of the assignment.
///
/// `for_partition` is fallible (e.g. Fjall workspace open can fail on a
/// missing cache directory) but
/// [`crate::consumer::middleware::FallibleHandlerProvider::handler_for_partition`]
/// is not, so the keyed-state middleware captures the `Result` at
/// assignment time and surfaces failures through
/// [`crate::state::middleware::KeyedStateMiddlewareError::Factory`] on
/// the first event dispatch for that partition.
pub trait DirtyStoreFactory<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Per-partition provider produced by this factory.
    type Provider: DirtyStoreProvider<K>;

    /// Error returned when a partition's dirty workspace cannot be
    /// materialized.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Mints a partition-scoped provider for `(topic, partition)`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the per-partition workspace cannot
    /// be opened (e.g. Fjall partition open failure on a corrupted or
    /// missing cache directory).
    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Provider, Self::Error>;
}
