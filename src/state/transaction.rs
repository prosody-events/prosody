//! Transaction-side state shapes.
//!
//! [`DurableState`] is what a backend reads back for a partition (idle or
//! sealed); [`LocalTx`] is the in-handler transaction state machine; the
//! remaining types are the proofs and payloads those two thread between
//! the dirty workspace and the durable store.

use super::event_ref::EventRef;
use super::identity::{CollectionKind, CollectionRef};
use super::wal::SealedWal;
use std::num::NonZeroU64;
use std::option::IntoIter as OptionIntoIter;

/// Persistence mode for local state changes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CommitMode {
    /// Seal dirty operations before the event commit oracle resolves them.
    Wal,

    /// Apply dirty operations directly with no sealed write-ahead state.
    Direct,
}

/// Three-valued read used by overlays.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Read<T> {
    /// Value is present.
    Present(T),

    /// Value is known absent.
    Absent,

    /// This layer has not observed the value.
    Unknown,
}

/// Durable collection state is either idle or sealed for one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableState<K>
where
    K: CollectionKind,
{
    /// No sealed operations are pending; `applied` is authoritative.
    Idle {
        /// Authoritative applied state.
        applied: K::Applied,
    },

    /// A non-empty WAL is sealed for recovery.
    Sealed {
        /// Authoritative applied state observed before the WAL was sealed.
        applied: K::Applied,

        /// Durable sealed WAL.
        wal: SealedWal<K>,
    },
}

/// Local transaction state for one collection and event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalTx<K>
where
    K: CollectionKind,
{
    /// No dirty operations are pending.
    Clean(CollectionRef<K>),

    /// Dirty operations are buffered in the local pending store.
    Dirty,

    /// Dirty operations have been sealed durably.
    Sealed(SealedCollection<K>),

    /// The transaction was resolved and must not transition again.
    Finished,
}

/// Local proof that dirty operations were sealed for an event.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SealedCollection<K>
where
    K: CollectionKind,
{
    collection: CollectionRef<K>,
    event: EventRef,
}

impl<K> SealedCollection<K>
where
    K: CollectionKind,
{
    /// Creates a sealed local transition marker.
    #[must_use]
    pub fn new(collection: CollectionRef<K>, event: EventRef) -> Self {
        Self { collection, event }
    }

    /// Returns the collection reference.
    #[must_use]
    pub fn collection(&self) -> &CollectionRef<K> {
        &self.collection
    }

    /// Returns the event that owns the sealed operations.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }
}

/// Pending operations for one collection, with a typed non-empty proof.
///
/// Returned by
/// [`PendingOpSource::pending_ops`](super::value::PendingOpSource::pending_ops)
/// wrapped in [`Option`]: `None` means no dirty work is buffered for the
/// collection, `Some(PendingOps { count, ops })` means at least one
/// operation exists and `count` matches the iterator. The [`NonZeroU64`]
/// count lets callers size the seal without materializing `ops` first; the
/// iterator yields the operations themselves in order when the seal or
/// direct-apply path needs them.
pub struct PendingOps<I>
where
    I: Iterator + Send,
{
    /// Number of operations the iterator will yield.
    pub count: NonZeroU64,

    /// Ordered pending operations.
    pub ops: I,
}

impl<T> PendingOps<OptionIntoIter<T>>
where
    T: Send,
{
    /// Builds a single-operation pending stream (`count` = 1).
    ///
    /// The last-writer-wins dirty stores (memory and fjall) buffer at most
    /// one compacted op per collection, so this is their only constructor.
    pub fn single(op: T) -> Self {
        Self {
            count: NonZeroU64::MIN,
            ops: Some(op).into_iter(),
        }
    }
}
