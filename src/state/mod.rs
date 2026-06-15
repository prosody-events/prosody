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
//! * [`encoding`] — payload encoding selectors ([`PayloadEncoding`],
//!   [`EncodingError`]).
//! * [`cell`] — the provisional-cell durability model ([`Cell`], [`Committed`],
//!   [`ProvisionalCell`], [`ProvisionalWrite`]).
//! * [`value`] — the Value collection kind ([`ValueKind`], [`ValueOp`], …).
//! * [`descriptor`] — typed descriptors and handles bound over the raw byte
//!   cells the stores persist.
//! * [`transaction`] — transaction-side state ([`CommitMode`], [`Read`], …).
//!
//! The cross-cutting factory trait [`StateBackendFactory`] belongs to no leaf
//! and stays here.
//!
//! [`Cell`]: cell::Cell
//! [`Committed`]: cell::Committed
//! [`ProvisionalCell`]: cell::ProvisionalCell
//! [`ProvisionalWrite`]: cell::ProvisionalWrite

use crate::error::ClassifyError;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::CommittedCache;
use crate::state::store::CellStore;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::error::Error;

pub mod cassandra;
pub mod cell;
pub mod config;
pub mod descriptor;
pub mod descriptor_identity;
pub mod dirty;
pub mod encoding;
pub mod event_ref;
pub mod fjall;
pub mod identity;
pub mod manager;
pub mod memory;
pub mod oracle;
pub mod partition_store;
pub mod production;
#[cfg(test)]
pub(crate) mod proof_kind;
pub mod registry;
pub mod resolve;
pub mod session;
pub mod store;
pub mod transaction;
pub mod value;

#[cfg(test)]
pub(crate) mod tests;

pub use dirty::DirtyValueStore;
pub use encoding::{EncodingError, PayloadEncoding};
pub use event_ref::{CommitDecision, EventRef, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, StateKey, StateName,
    StateNameError, StateType,
};
pub use transaction::{CommitMode, Read};
pub use value::{ValueKind, ValueOp};

/// Maximum concurrent per-collection durable operations in the keyed-state
/// Value lifecycle (finalize stage, commit promote, rollback, recovery sweep).
/// Each collection is its own Cassandra partition, so the fan-out is safe.
pub(crate) const STATE_FANOUT_CONCURRENCY: usize = 16;

/// The per-partition keyed-state backend: the durable cell store, the commit
/// oracle it resolves provisional cells through, and the committed-value
/// cache.
///
/// Minted as one unit by [`StateBackendFactory::for_partition`] so the oracle
/// the sessions stage against and the oracle the recovery sweep resolves
/// through are the *same* instance, and the fjall workspace backing the
/// committed-value cache is opened once. The per-event dirty workspace is not
/// part of the backend — it is a single in-memory [`DirtyValueStore`] the
/// session owns and rebuilds per event, never a durability or recovery source.
pub struct StateBackend<S, O, C> {
    /// Durable cell store for this partition.
    pub cell: S,

    /// Commit oracle for this partition's resolution decisions.
    pub oracle: O,

    /// Committed-value cache for this partition.
    pub cache: C,
}

/// The backend bundle a [`StateBackendFactory`] mints for one partition.
pub type BackendOf<B> = StateBackend<
    <B as StateBackendFactory>::Cell,
    <B as StateBackendFactory>::Oracle,
    <B as StateBackendFactory>::Cache,
>;

/// Process-wide factory minting the per-partition keyed-state backend.
///
/// Both the commit oracle's timer-tag reads and the cell store are
/// partition-scoped (timer tags live in segment-keyed tables; the fjall
/// workspace is per assignment), so the backend cannot be a single global
/// value — the keyed-state manager calls [`Self::for_partition`] at partition
/// acquisition and surfaces failures on the retry-until-shutdown loop.
pub trait StateBackendFactory: Clone + Send + Sync + 'static {
    /// Durable cell store minted per partition. It also persists the
    /// segment's descriptor-identity rows, validated eagerly at acquisition,
    /// so it carries [`DescriptorIdentityStore`] under the same error type.
    type Cell: CellStore<ValueKind>
        + DescriptorIdentityStore<Error = <Self::Cell as CellStore<ValueKind>>::Error>;

    /// Commit oracle minted per partition.
    type Oracle: CommitOracle;

    /// Committed-value cache minted per partition.
    type Cache: CommittedCache<ValueKind>;

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

/// Partition-agnostic [`StateBackendFactory`]: clones the same cell store,
/// oracle, and cache for every partition.
///
/// Suits compositions whose stores are not partition-scoped — memory-backed
/// tests and bespoke wiring; production uses the partition-scoped factories
/// in [`production`].
#[derive(Clone, Debug)]
pub struct SharedStateBackend<S, O, C> {
    cell: S,
    oracle: O,
    cache: C,
}

impl<S, O, C> SharedStateBackend<S, O, C> {
    /// Creates a backend factory that hands out clones of the supplied
    /// parts.
    #[must_use]
    pub fn new(cell: S, oracle: O, cache: C) -> Self {
        Self {
            cell,
            oracle,
            cache,
        }
    }
}

impl<S, O, C> StateBackendFactory for SharedStateBackend<S, O, C>
where
    S: CellStore<ValueKind>
        + DescriptorIdentityStore<Error = <S as CellStore<ValueKind>>::Error>
        + Clone,
    O: CommitOracle,
    C: CommittedCache<ValueKind>,
{
    type Cache = C;
    type Cell = S;
    type Error = Infallible;
    type Oracle = O;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> Result<BackendOf<Self>, Self::Error> {
        Ok(StateBackend {
            cell: self.cell.clone(),
            oracle: self.oracle.clone(),
            cache: self.cache.clone(),
        })
    }
}
