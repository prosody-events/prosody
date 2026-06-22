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
//! * [`encoding`] — payload encoding selectors ([`Encoding`],
//!   [`EncodingError`]).
//! * [`cell`] — the provisional-cell durability model ([`Cell`], [`Committed`],
//!   [`ProvisionalCell`], [`ProvisionalWrite`]).
//! * [`value`] — the Value collection kind ([`ValueKind`], [`ValueOp`], …).
//! * [`descriptor`] — typed descriptors and handles bound over the raw byte
//!   cells the stores persist.
//! * [`transaction`] — transaction-side state ([`CommitMode`], [`Read`], …).
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
use crate::state::partition_store::CommittedCache;
use crate::state::store::CellStore;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::error::Error;

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

pub use cell_key::{CellKey, Direction, OrderKey, Scan, Section};
pub use dirty::DirtyValueStore;
pub use encoding::{Encoding, EncodingError};
pub use event_ref::{CommitDecision, EventRef, StoreOutcome, TimerEventRef};
pub use identity::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, StateKey, StateName,
    StateNameError, StateType,
};
pub use order_codec::{
    I64KeyCodec, KeyCodecError, OrderedKeyCodec, U64KeyCodec, Utf8KeyCodec, order_preserving_i64,
    order_preserving_i64_decode,
};
pub use transaction::{CommitMode, Read};
pub use value::{ValueKind, ValueOp};

/// Maximum concurrent per-collection durable operations in the keyed-state
/// Value lifecycle (finalize stage, commit promote, rollback, recovery sweep).
/// Each collection is its own Cassandra partition, so the fan-out is safe.
pub(crate) const STATE_FANOUT_CONCURRENCY: usize = 16;

/// The per-partition backend bundle: every kind's durable cell store + cache,
/// the one shared commit oracle, and the shared descriptor-identity store —
/// behind one type parameter so the session and manager name only `B`.
///
/// Minted as one unit by [`StateBackendFactory::for_partition`] so the oracle
/// the sessions stage against and the oracle the recovery sweep resolves
/// through are the *same* instance, and the fjall workspace backing the
/// committed-value cache is opened once. The per-event dirty workspace is not
/// part of the backend — it is a single in-memory [`DirtyValueStore`] the
/// session owns and rebuilds per event, never a durability or recovery source.
///
/// Adding a kind grows this trait by two associated types + two accessors (a
/// `KindCell: CellStore<KindKind>` and a `KindCache:
/// CommittedCache<KindKind>`); no session or manager signature changes, because
/// they project these types behind the one `B`. The accessors return cheap
/// `Arc`-clones.
pub trait StateBackend: Send + Sync + 'static {
    /// The one commit oracle, shared by every lane, so a provisional cell of
    /// any kind resolves against the exact commit record the one marker
    /// certifies.
    type Oracle: CommitOracle;

    /// The shared descriptor-identity control-plane store, validated eagerly
    /// at acquisition. It is decoupled from any kind's data store — a kind's
    /// [`CellStore<K>`] does **not** implement [`DescriptorIdentityStore`], so
    /// "which kind owns identity?" is un-askable.
    type Identity: DescriptorIdentityStore;

    /// Durable cell store for the [`ValueKind`] lane.
    type ValueCell: CellStore<ValueKind>;

    /// Committed-value cache for the [`ValueKind`] lane.
    type ValueCache: CommittedCache<ValueKind>;
    // Adding Map: `type MapCell: CellStore<MapKind>; type MapCache:
    // CommittedCache<MapKind>;`

    /// The shared commit oracle.
    fn oracle(&self) -> Self::Oracle;

    /// The shared descriptor-identity store.
    fn identity(&self) -> Self::Identity;

    /// The Value lane's durable cell store.
    fn value_cell(&self) -> Self::ValueCell;

    /// The Value lane's committed-value cache.
    fn value_cache(&self) -> Self::ValueCache;
}

/// The one concrete backend every factory mints; [`StateBackend`] projects its
/// per-kind store types so callers name only `B`. Type-param growth is confined
/// to this struct and the factory's
/// [`for_partition`](StateBackendFactory::for_partition) — never the session or
/// manager.
#[derive(Clone, Debug)]
pub struct PartitionBackend<O, I, VCell, VCache> {
    oracle: O,
    identity: I,
    value_cell: VCell,
    value_cache: VCache,
}

impl<O, I, VCell, VCache> PartitionBackend<O, I, VCell, VCache> {
    /// Bundles the per-partition backend parts: the shared oracle, the shared
    /// descriptor-identity store, and the Value lane's cell store and cache.
    #[must_use]
    pub fn new(oracle: O, identity: I, value_cell: VCell, value_cache: VCache) -> Self {
        Self {
            oracle,
            identity,
            value_cell,
            value_cache,
        }
    }
}

impl<O, I, VCell, VCache> StateBackend for PartitionBackend<O, I, VCell, VCache>
where
    O: CommitOracle,
    I: DescriptorIdentityStore + Clone,
    VCell: CellStore<ValueKind>,
    VCache: CommittedCache<ValueKind>,
{
    type Identity = I;
    type Oracle = O;
    type ValueCache = VCache;
    type ValueCell = VCell;

    fn oracle(&self) -> O {
        self.oracle.clone()
    }

    fn identity(&self) -> I {
        self.identity.clone()
    }

    fn value_cell(&self) -> VCell {
        self.value_cell.clone()
    }

    fn value_cache(&self) -> VCache {
        self.value_cache.clone()
    }
}

/// Process-wide factory minting the per-partition keyed-state [`StateBackend`].
///
/// Both the commit oracle's timer-tag reads and the cell store are
/// partition-scoped (timer tags live in segment-keyed tables; the fjall
/// workspace is per assignment), so the backend cannot be a single global
/// value — the keyed-state manager calls [`Self::for_partition`] at partition
/// acquisition and surfaces failures on the retry-until-shutdown loop.
pub trait StateBackendFactory: Clone + Send + Sync + 'static {
    /// The per-partition backend bundle this factory mints.
    type Backend: StateBackend;

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
    ) -> Result<Self::Backend, Self::Error>;
}

/// Partition-agnostic [`StateBackendFactory`]: clones the same cell store,
/// oracle, and cache for every partition.
///
/// Suits compositions whose stores are not partition-scoped — memory-backed
/// tests and bespoke wiring; production uses the partition-scoped factories
/// in [`production`]. The cell store doubles as the shared
/// [`DescriptorIdentityStore`].
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
    S: CellStore<ValueKind> + DescriptorIdentityStore + Clone,
    O: CommitOracle,
    C: CommittedCache<ValueKind>,
{
    type Backend = PartitionBackend<O, S, S, C>;
    type Error = Infallible;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> Result<Self::Backend, Self::Error> {
        // Identity store = the cell store (it also persists identity rows).
        Ok(PartitionBackend::new(
            self.oracle.clone(),
            self.cell.clone(),
            self.cell.clone(),
            self.cache.clone(),
        ))
    }
}
