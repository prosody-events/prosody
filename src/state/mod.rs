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
//!   [`ValueApplied`], …).
//! * [`descriptor`] — typed descriptors and handles bound over the raw byte
//!   cells the stores persist.
//! * [`transaction`] — transaction-side state ([`DurableState`], [`LocalTx`],
//!   [`CommitMode`], [`SealedCollection`], …).
//!
//! The cross-cutting factory traits ([`DirtyStoreProvider`],
//! [`StateBackendFactory`]) belong to no leaf and stay here.

use crate::error::ClassifyError;
use crate::state::oracle::CommitOracle;
use crate::state::pending::PendingIndexScanner;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

pub mod cassandra;
pub mod config;
pub mod descriptor;
pub mod descriptor_identity;
pub mod encoding;
pub mod event_ref;
pub mod fjall;
pub mod identity;
pub mod layered;
pub mod manager;
pub mod memory;
pub mod oracle;
pub mod pending;
pub mod production;
pub mod recovering;
pub mod registry;
pub mod session;
pub mod transaction;
pub mod value;
pub mod wal;

#[cfg(test)]
pub(crate) mod tests;

pub use encoding::{EncodingError, PayloadEncoding, WalFormat};
pub use event_ref::{CommitDecision, EventRef, EventScopeId, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, StateKey, StateName,
    StateNameError, StateType,
};
pub use transaction::{CommitMode, DurableState, LocalTx, PendingOps, Read, SealedCollection};
pub use value::{ValueApplied, ValueKind, ValueOp};
pub use wal::{EmptyOperationsError, NonEmptyOps, SealedWal, WalBlob, WalEnvelope};

/// The per-partition keyed-state backend: the durable Value bundle, the
/// commit oracle it recovers through, the dirty-workspace provider, and the
/// pending-index scanner the recovery sweep streams.
///
/// Minted as one unit by [`StateBackendFactory::for_partition`] so the
/// oracle baked into the durable bundle's recovery wrapper and the oracle
/// the middleware's sweep consults are the *same* instance, and the
/// fjall workspace backing the dirty provider and the durable cache is
/// opened once.
///
/// `scanner` and `durable` play distinct recovery roles: the scanner
/// streams the pending-index rows (`scan_pending`), while the durable
/// bundle does the point reads and resolutions (`read_partition`,
/// `apply_sealed`, …). They are the *un-wrapped* inner store and the
/// recovery-wrapped bundle respectively — distinct types even when both
/// front the same backing store.
pub struct StateBackend<D, O, P, Sc> {
    /// Durable Value bundle for this partition.
    pub durable: D,

    /// Commit oracle for this partition's recovery decisions.
    pub oracle: O,

    /// Per-event dirty-workspace provider for this partition.
    pub dirty: P,

    /// Pending-index scanner for this partition's recovery sweep.
    pub scanner: Sc,
}

/// The backend bundle a [`StateBackendFactory`] mints for one partition.
pub type BackendOf<B> = StateBackend<
    <B as StateBackendFactory>::Durable,
    <B as StateBackendFactory>::Oracle,
    <B as StateBackendFactory>::DirtyProvider,
    <B as StateBackendFactory>::Scanner,
>;

/// Process-wide factory minting the per-partition keyed-state backend.
///
/// Both the commit oracle's timer-tag reads and the durable stores are
/// partition-scoped (timer tags live in segment-keyed tables; the fjall
/// workspace is per assignment), so the backend cannot be a single global
/// value — the keyed-state middleware calls [`Self::for_partition`] inside
/// `handler_for_partition` and surfaces failures on first dispatch.
pub trait StateBackendFactory: Clone + Send + Sync + 'static {
    /// Durable Value bundle minted per partition.
    type Durable;

    /// Commit oracle minted per partition.
    type Oracle: CommitOracle;

    /// Per-event dirty-workspace provider minted per partition.
    type DirtyProvider: DirtyStoreProvider<ValueKind>;

    /// Pending-index scanner minted per partition for the recovery sweep.
    type Scanner: PendingIndexScanner;

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
pub struct SharedStateBackend<D, O, P, Sc> {
    durable: D,
    oracle: O,
    dirty: P,
    scanner: Sc,
}

impl<D, O, P, Sc> SharedStateBackend<D, O, P, Sc> {
    /// Creates a backend factory that hands out clones of the supplied
    /// parts.
    #[must_use]
    pub fn new(durable: D, oracle: O, dirty: P, scanner: Sc) -> Self {
        Self {
            durable,
            oracle,
            dirty,
            scanner,
        }
    }
}

impl<D, O, P, Sc> StateBackendFactory for SharedStateBackend<D, O, P, Sc>
where
    D: Clone + Send + Sync + 'static,
    O: CommitOracle,
    P: DirtyStoreProvider<ValueKind>,
    Sc: PendingIndexScanner,
{
    type DirtyProvider = P;
    type Durable = D;
    type Error = Infallible;
    type Oracle = O;
    type Scanner = Sc;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> Result<BackendOf<Self>, Self::Error> {
        Ok(StateBackend {
            durable: self.durable.clone(),
            oracle: self.oracle.clone(),
            dirty: self.dirty.clone(),
            scanner: self.scanner.clone(),
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
pub trait DirtyStoreProvider<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Per-event dirty workspace this provider mints.
    type Store: value::PendingOpSource<K> + fmt::Debug + 'static;

    /// Returns a fresh per-event dirty workspace bound to `scope`.
    fn for_scope(&self, scope: EventScopeId) -> Self::Store;
}
