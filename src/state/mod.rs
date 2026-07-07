//! Keyed application state protocol types.
//!
//! This module defines the shared collection identity and cell shapes used by
//! the keyed-state cell store. The cell layer is **uniform and
//! untyped** — it addresses cells by [`CellKey`] and names no collection
//! family; typed collection handles (Value, Map, Deque) are built
//! atop it in [`descriptor`].
//!
//! The shapes themselves live in leaf-to-root submodules and are
//! re-exported flat below, so consumers keep importing `crate::state::X`:
//!
//! * [`identity`] — collection identity ([`CollectionId`], [`CollectionRef`],
//!   [`StateKey`], [`CollectionKindId`], …).
//! * [`cell_key`] — intra-collection cell addressing ([`CellKey`], [`Section`],
//!   [`Coordinate`], [`Scan`]).
//! * [`event_ref`] — event identity and verdicts ([`EventRef`],
//!   [`CommitDecision`], [`StoreOutcome`], …).
//! * [`encoding`] — payload encoding selectors ([`Encoding`],
//!   [`EncodingError`]).
//! * [`cell`] — the provisional-cell durability model ([`Cell`], [`Committed`],
//!   [`ProvisionalCell`], [`ProvisionalWrite`]).
//! * [`store`] — the uniform [`CellStore`] backend trait.
//! * [`descriptor`] — typed collection handles bound over the raw byte cells
//!   the stores persist.
//! * [`registry`] — per-collection operational settings ([`CommitMode`],
//!   [`registry::CollectionDef`], …).
//!
//! The cross-cutting backend abstraction — the [`StateBackend`] bundle trait,
//! its one concrete [`PartitionBackend`], and the [`StateBackendFactory`] that
//! mints it per partition — belongs to no leaf and stays here.
//!
//! [`Cell`]: cell::Cell
//! [`Committed`]: cell::Committed
//! [`ProvisionalCell`]: cell::ProvisionalCell
//! [`ProvisionalWrite`]: cell::ProvisionalWrite

use crate::error::ClassifyError;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::oracle::CommitOracle;
use crate::state::store::CellStore;
use crate::{Partition, Topic};
#[cfg(test)]
use std::convert::Infallible;
use std::error::Error;

pub mod access;
pub mod cached;
pub mod cassandra;
pub mod cell;
pub mod cell_key;
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
pub mod order_codec;
pub mod overlay;
pub mod production;
pub mod registry;
pub mod resolve;
pub mod session;
pub mod store;

#[cfg(test)]
pub(crate) mod tests;

pub use access::StateAccessError;
pub use cell_key::{CellKey, Coordinate, Direction, Scan, Section};
pub use encoding::{Encoding, EncodingError};
pub use event_ref::{CommitDecision, EventRef, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKindId, CollectionRef, StateKey, StateName, StateNameError, StateType,
};
pub use order_codec::{
    I64KeyCodec, KeyCodecError, OrderedKeyCodec, U64KeyCodec, Utf8KeyCodec, order_preserving_i64,
    order_preserving_i64_decode,
};
pub use registry::CommitMode;

/// Maximum concurrent per-collection durable operations in the keyed-state
/// lifecycle (finalize stage, commit promote, rollback, recovery sweep).
/// Each collection is its own Cassandra partition, so the fan-out is safe.
pub(crate) const STATE_FANOUT_CONCURRENCY: usize = 16;

/// Maximum concurrent in-flight requests within a *single* collection
/// (one Cassandra partition → one Scylla shard): the batch-chunk submission
/// of one durable write, the recovery sweep's per-cell resolution, and a
/// stage's committed-base reads. Same shard, so this bounds round-trip /
/// oracle-consult *overlap* (latency), not throughput; kept modest because it
/// nests inside the per-collection `STATE_FANOUT_CONCURRENCY` fan-out, so the
/// product is the per-shard in-flight depth.
pub(crate) const SHARD_FANOUT_CONCURRENCY: usize = 8;

/// The per-partition backend bundle: the one uniform durable cell store, the
/// shared commit oracle, and the shared descriptor-identity store — behind one
/// type parameter so the session and manager name only `B`.
///
/// Minted as one unit by [`StateBackendFactory::for_partition`] so the oracle
/// the sessions stage against (for the dedup marker) and the oracle the cell
/// store resolves provisional cells through are the *same* instance, and the
/// fjall workspace backing the committed-value cache is opened once. The
/// per-event dirty workspace is not part of the backend — it is the in-memory
/// [`DirtyStore`](dirty::DirtyStore) the session's
/// [`Overlay`](overlay::Overlay) owns and clears per event, never a durability
/// or recovery source.
pub trait StateBackend: Send + Sync + 'static {
    /// The commit oracle, shared with the cell store, so a provisional cell
    /// resolves against the exact commit record the one marker certifies.
    type Oracle: CommitOracle;

    /// The shared descriptor-identity control-plane store, validated eagerly
    /// at acquisition. It is decoupled from the cell data store — the cell
    /// store does **not** implement [`DescriptorIdentityStore`].
    type Identity: DescriptorIdentityStore;

    /// The one uniform durable cell store (`Cached<CassandraStore>` in
    /// production, `MemoryCellStore` in tests). The session wraps it in the
    /// per-event dirty [`Overlay`](overlay::Overlay).
    type Cell: CellStore;

    /// The shared commit oracle (the marker flush writes through it).
    fn oracle(&self) -> Self::Oracle;

    /// The shared descriptor-identity store.
    fn identity(&self) -> Self::Identity;

    /// The uniform durable cell store.
    fn cell(&self) -> Self::Cell;
}

/// The one concrete backend every factory mints; [`StateBackend`] projects its
/// store type so callers name only `B`.
#[derive(Clone, Debug)]
pub struct PartitionBackend<O, I, C> {
    oracle: O,
    identity: I,
    cell: C,
}

impl<O, I, C> PartitionBackend<O, I, C> {
    /// Bundles the per-partition backend parts: the shared oracle, the shared
    /// descriptor-identity store, and the uniform cell store.
    #[must_use]
    pub fn new(oracle: O, identity: I, cell: C) -> Self {
        Self {
            oracle,
            identity,
            cell,
        }
    }
}

impl<O, I, C> StateBackend for PartitionBackend<O, I, C>
where
    O: CommitOracle,
    I: DescriptorIdentityStore + Clone,
    C: CellStore,
{
    type Cell = C;
    type Identity = I;
    type Oracle = O;

    fn oracle(&self) -> O {
        self.oracle.clone()
    }

    fn identity(&self) -> I {
        self.identity.clone()
    }

    fn cell(&self) -> C {
        self.cell.clone()
    }
}

/// Process-wide factory minting the per-partition keyed-state [`StateBackend`].
///
/// Both the commit oracle's timer-tag reads and the cell store are
/// partition-scoped (timer tags live in segment-keyed tables; the fjall
/// workspace is per assignment), so the backend cannot be a single global
/// value — the keyed-state manager calls [`Self::for_partition`] at partition
/// acquisition and surfaces failures on the retry-until-shutdown loop.
///
/// `T` is the partition's trigger-store handle, passed down from the
/// partition loop so the backend's commit oracle reads timer tags **through
/// the same store instance the partition writes through** — one identity,
/// one value. Minting a sibling store from a provider is not equivalent: a
/// store may answer tag reads from a per-instance cache that only its own
/// writes keep current. Factories whose oracle is supplied whole (e.g. the
/// test-only `SharedStateBackend`) ignore the handle and accept any `T`.
pub trait StateBackendFactory<T>: Clone + Send + Sync + 'static {
    /// The per-partition backend bundle this factory mints.
    type Backend: StateBackend;

    /// Error returned when a partition's backend cannot be materialized.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Mints the backend for `(topic, partition)`, wiring `triggers` — the
    /// partition's trigger-store handle — into the commit oracle's timer
    /// half.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when partition-scoped state (e.g. the
    /// fjall workspace) cannot be opened.
    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> Result<Self::Backend, Self::Error>;
}

/// Partition-agnostic [`StateBackendFactory`]: clones the same cell store,
/// identity store, and oracle for every partition.
///
/// Test-only: suits memory-backed compositions whose stores are not
/// partition-scoped; production uses the partition-scoped factories in
/// [`production`]. The supplied `cell` must already embed `oracle` (it resolves
/// provisional cells through it), so the two are the same instance.
#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SharedStateBackend<S, I, O> {
    cell: S,
    identity: I,
    oracle: O,
}

#[cfg(test)]
impl<S, I, O> SharedStateBackend<S, I, O> {
    /// Creates a backend factory that hands out clones of the supplied parts.
    #[must_use]
    pub fn new(cell: S, identity: I, oracle: O) -> Self {
        Self {
            cell,
            identity,
            oracle,
        }
    }
}

/// The oracle is supplied whole at construction, so the partition's
/// trigger-store handle is ignored and any `T` is accepted.
#[cfg(test)]
impl<S, I, O, T> StateBackendFactory<T> for SharedStateBackend<S, I, O>
where
    S: CellStore + Clone,
    I: DescriptorIdentityStore + Clone,
    O: CommitOracle,
{
    type Backend = PartitionBackend<O, I, S>;
    type Error = Infallible;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
        _triggers: T,
    ) -> Result<Self::Backend, Self::Error> {
        Ok(PartitionBackend::new(
            self.oracle.clone(),
            self.identity.clone(),
            self.cell.clone(),
        ))
    }
}
