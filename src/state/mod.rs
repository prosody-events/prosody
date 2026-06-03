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
//! The two cross-cutting factory traits ([`DirtyStoreProvider`],
//! [`DirtyStoreFactory`]) belong to no leaf and stay here.

use crate::error::ClassifyError;
use crate::{Partition, Topic};
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
