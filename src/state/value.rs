//! Value collection contracts: the dirty-workspace store trait and the
//! Value op fold.

use super::{CollectionId, CollectionKind, CollectionKindId, PendingOps, Read};
use crate::error::ClassifyError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::future::Future;

/// Applied Value state: the raw cell bytes, or `None` when cleared.
///
/// Cell bytes are opaque to every store — typing lives in the descriptor
/// layer ([`crate::state::descriptor`]), which encodes/decodes at the
/// handle boundary.
pub type ValueApplied = Option<Bytes>;

/// Type marker for Value collections.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ValueKind;

impl CollectionKind for ValueKind {
    type CellAddr = ();
    type Op = ValueOp;

    const ID: CollectionKindId = CollectionKindId::Value;
}

/// Ordered operation for a Value collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ValueOp {
    /// Replace the current payload.
    Set {
        /// Raw payload cell bytes.
        payload: Bytes,
    },

    /// Remove the current payload.
    Clear,
}

/// Store interface for normal Value reads and writes.
pub trait ValueStore: Send + Sync + 'static {
    /// Error type for Value store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads the visible value for a collection.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Future<Output = Result<Read<Bytes>, Self::Error>> + Send + 'a;

    /// Buffers or applies a Value set.
    ///
    /// Takes the payload by shared slice so the caller never has to clone or
    /// hand over a `Bytes`: it can pass a transient or pooled, reusable
    /// serialize buffer. Each implementation frames the slice into its own
    /// cell buffer as its storage requires — the in-memory dirty store copies
    /// it once into an owned `Bytes`; the fjall cache frames it into a tagged
    /// cell.
    fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: &'a [u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Buffers or applies a Value clear.
    fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Source of compacted pending operations for a collection kind.
pub trait PendingOpSource<K>: Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for pending operation access.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Iterator returned alongside the operation count.
    ///
    /// `'a` is the borrow lifetime of `&self` at the call site; concrete
    /// implementations may own the iterator (`Send + 'static`) or borrow
    /// from the store's internal state.
    type Ops<'a>: Iterator<Item = K::Op> + Send + 'a
    where
        Self: 'a;

    /// Returns the pending operation stream for a collection when any exist.
    ///
    /// `None` means no operations are buffered for this collection;
    /// `Some(PendingOps { count, ops })` means `count` ordered operations
    /// are available and `ops` will yield exactly that many. The non-zero
    /// count lets callers size the work without materializing the iterator.
    ///
    /// # Errors
    ///
    /// Returns a store error if pending operations cannot be read.
    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error>;

    /// Clears compacted pending operations for the collection.
    ///
    /// # Errors
    ///
    /// Returns a store error if pending operations cannot be cleared.
    fn clear_pending_ops(&self, collection: &CollectionId<K>) -> Result<(), Self::Error>;
}

/// Folds ordered Value operations into applied state.
///
/// Value operations are last-writer-wins, so only the final op in the slice
/// affects the applied state.
#[must_use]
pub fn fold_value_ops<I>(applied: ValueApplied, ops: I) -> ValueApplied
where
    I: IntoIterator<Item = ValueOp>,
{
    ops.into_iter().last().map_or(applied, |op| match op {
        ValueOp::Set { payload } => Some(payload),
        ValueOp::Clear => None,
    })
}
