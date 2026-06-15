//! Value collection contracts: the kind marker, its op, and the
//! dirty-workspace store trait.

use super::{CollectionId, CollectionKind, CollectionKindId, Read};
use crate::error::ClassifyError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::future::Future;

/// Type marker for Value collections.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ValueKind;

impl CollectionKind for ValueKind {
    type CellAddr = ();
    type Op = ValueOp;

    const ID: CollectionKindId = CollectionKindId::Value;

    fn set_op(cell: &[u8]) -> ValueOp {
        ValueOp::Set {
            payload: Bytes::copy_from_slice(cell),
        }
    }

    fn clear_op() -> ValueOp {
        ValueOp::Clear
    }

    /// Last-writer-wins: the newest op wholly determines the cell, so
    /// `combine` discards the older op.
    fn combine(_existing: ValueOp, newest: ValueOp) -> ValueOp {
        newest
    }

    /// A buffered Value op fully determines the cell without the committed
    /// base, so the read never falls through.
    fn read_overlay(op: &ValueOp) -> Read<Bytes> {
        match op {
            // Cheap `Bytes` refcount bump, not a payload copy.
            ValueOp::Set { payload } => Read::Present(payload.clone()),
            ValueOp::Clear => Read::Absent,
        }
    }

    /// Last-writer-wins ignores the committed base: the combined op alone is
    /// the outcome.
    fn apply(_committed_base: Option<Bytes>, op: &ValueOp) -> Option<Bytes> {
        match op {
            ValueOp::Set { payload } => Some(payload.clone()),
            ValueOp::Clear => None,
        }
    }
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
