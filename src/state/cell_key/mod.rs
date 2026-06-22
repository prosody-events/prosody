//! Intra-collection cell addressing.
//!
//! One Cassandra partition `(segment_id, key, state_type, name)` is one
//! collection; each clustering row is one cell, addressed by a [`CellKey`] =
//! [`Namespace`] + [`OrderKey`]. The namespace discriminates sub-structures
//! (`Meta` bookkeeping vs `Entries` data); the order key orders cells within a
//! namespace by **unsigned lexicographic byte order**, which is the
//! order-preserving key codec's contract (see [`order_codec`]). A [`Scan`]
//! addresses a contiguous single-namespace range.
//!
//! These types name no collection family — Value/Map/Deque never appear here —
//! so the cell layer cannot dispatch on or escape its partition.
//!
//! [`order_codec`]: crate::state::order_codec

use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use thiserror::Error;

/// Structural sub-structure of a collection partition.
///
/// Not a collection type — Value/Map/Deque never appear here. `Meta` cells hold
/// min/max bounds and bookkeeping; `Entries` cells hold the collection's data.
///
/// The wire discriminator persisted in the durable `namespace` column is the
/// `i8` the [`From`]/[`TryFrom`] pair round-trips through, so a variant rename
/// cannot drift the on-wire encoding from the type it encodes; an unknown
/// discriminator decodes as [`UnknownNamespace`], which classifies `Permanent`.
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Namespace {
    /// Bookkeeping cells (min/max bounds and similar).
    Meta = 0,

    /// The collection's data cells.
    Entries = 1,
}

impl From<Namespace> for i8 {
    fn from(namespace: Namespace) -> Self {
        namespace as i8
    }
}

impl TryFrom<i8> for Namespace {
    type Error = UnknownNamespace;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Entries),
            _ => Err(UnknownNamespace(value)),
        }
    }
}

/// Order-preserving cell key within a namespace.
///
/// Lexicographic (memcmp) byte order **is** the collection's logical order —
/// the order-preserving key codec's contract. The bytes are opaque to the cell
/// layer; the collection layer owns the encoding.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderKey(Bytes);

impl OrderKey {
    /// The empty key, addressing the single cell of a one-cell collection
    /// (Value).
    #[must_use]
    pub fn empty() -> Self {
        Self(Bytes::new())
    }

    /// Wraps order-preserving bytes as a key.
    #[must_use]
    pub fn from_bytes<B: Into<Bytes>>(bytes: B) -> Self {
        Self(bytes.into())
    }

    /// Returns the order-preserving key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Full intra-collection cell address. `Ord` is `(namespace, order_key)`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellKey {
    /// The cell's sub-structure namespace.
    pub namespace: Namespace,

    /// The cell's order-preserving key within the namespace.
    pub order_key: OrderKey,
}

/// Direction a [`Scan`] walks the clustering range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Ascending `order_key` byte order.
    Forward,

    /// Descending `order_key` byte order.
    Backward,
}

/// A single-namespace, start-anchored cell scan request.
///
/// `start` is positional (non-`Option`) and `namespace` is required, so an
/// unanchored or cross-namespace scan cannot be constructed.
pub struct Scan<'a> {
    /// The namespace whose cells the scan walks.
    pub namespace: Namespace,

    /// The inclusive anchor the scan starts from.
    pub start: &'a OrderKey,

    /// The direction the scan walks from `start`.
    pub dir: Direction,

    /// The optional inclusive bound the scan stops at.
    pub end: Option<&'a OrderKey>,

    /// The optional maximum number of cells to yield.
    pub limit: Option<usize>,
}

/// Error converting an `i8` that matches no [`Namespace`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown namespace discriminator: {0}")]
pub struct UnknownNamespace(i8);

impl ClassifyError for UnknownNamespace {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
