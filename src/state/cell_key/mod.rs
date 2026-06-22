//! Intra-collection cell addressing.
//!
//! One Cassandra partition `(segment_id, key, state_type, name)` is one
//! collection; each clustering row is one cell, addressed by a [`CellKey`] =
//! [`Section`] + [`Coordinate`]. The section groups a collection's cells into
//! disjoint sub-structures (e.g. bookkeeping vs data); the coordinate orders
//! cells within a section by **unsigned lexicographic byte order**, which is
//! the order-preserving key codec's contract (see [`order_codec`]). A [`Scan`]
//! addresses a contiguous single-section range.
//!
//! Both components are **opaque to the cell layer** — it only stores them,
//! sorts by them, and scopes scans to them; it never interprets their meaning.
//! That meaning is owned by the collection layer: each collection defines its
//! own section enum and coordinate encoding and lowers them to the wire
//! `i8`/bytes. So these types name no collection family — Value/Map/Deque never
//! appear here — and the cell layer cannot dispatch on or escape its partition.
//!
//! [`order_codec`]: crate::state::order_codec

use bytes::Bytes;

/// Disjoint, orderable sub-grouping of one collection's cells.
///
/// The high-order component of a [`CellKey`], paired with the low-order
/// [`Coordinate`]. **Opaque to the cell-store core**, which only stores it,
/// sorts by it, and scopes single-section scans to it — it never interprets the
/// meaning. Each collection owns the meaning of its sections (e.g. a Map's
/// bound-bookkeeping section vs its entry section) and lowers its own section
/// enum to the wire `i8` via the standard discriminator idiom
/// (`Section::new(i8::from(my_section))`). The cell layer round-trips that `i8`
/// without validating it, exactly as it treats [`Coordinate`] bytes — so an
/// unknown discriminant is not an error *here*; it is the owning collection's
/// `TryFrom` that classifies a bad section `Permanent`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Section(i8);

impl Section {
    /// Wraps a collection-defined section discriminant. `const` so collections
    /// can pin their section cells as constants.
    #[must_use]
    pub const fn new(discriminant: i8) -> Self {
        Self(discriminant)
    }
}

impl From<Section> for i8 {
    fn from(section: Section) -> Self {
        section.0
    }
}

/// A cell's order-preserving coordinate within a section.
///
/// The low-order component of a [`CellKey`]: the bytes whose unsigned
/// lexicographic (memcmp) order **is** the collection's logical order — the
/// order-preserving key codec's contract. Opaque to the cell layer; the
/// collection layer owns the encoding (a Map's encoded user key, a Deque's
/// sign-flipped index, the empty coordinate for Value's one cell).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Coordinate(Bytes);

impl Coordinate {
    /// The empty coordinate, addressing the single cell of a one-cell
    /// collection (Value).
    #[must_use]
    pub fn empty() -> Self {
        Self(Bytes::new())
    }

    /// Wraps order-preserving bytes as a coordinate.
    #[must_use]
    pub fn from_bytes<B: Into<Bytes>>(bytes: B) -> Self {
        Self(bytes.into())
    }

    /// Returns the order-preserving coordinate bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Full intra-collection cell address. `Ord` is `(section, coordinate)`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellKey {
    /// The cell's sub-grouping section.
    pub section: Section,

    /// The cell's order-preserving coordinate within the section.
    pub coordinate: Coordinate,
}

/// Direction a [`Scan`] walks the clustering range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Ascending `coordinate` byte order.
    Forward,

    /// Descending `coordinate` byte order.
    Backward,
}

/// A single-section, start-anchored cell scan request.
///
/// `start` is positional (non-`Option`) and `section` is required, so an
/// unanchored or cross-section scan cannot be constructed.
pub struct Scan<'a> {
    /// The section whose cells the scan walks.
    pub section: Section,

    /// The inclusive anchor the scan starts from.
    pub start: &'a Coordinate,

    /// The direction the scan walks from `start`.
    pub dir: Direction,

    /// The optional inclusive bound the scan stops at.
    pub end: Option<&'a Coordinate>,

    /// The optional maximum number of cells to yield.
    pub limit: Option<usize>,
}

#[cfg(test)]
mod tests;
