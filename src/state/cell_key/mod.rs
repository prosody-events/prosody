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
use std::ops::Bound;

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
    pub const fn empty() -> Self {
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
///
/// It carries **only** `(section, coordinate)` — never the cell store's
/// internal `kind` discriminant (the reserved-`kind` safety invariant). A
/// backend that splits its partition into a data slice and a recovery-marker
/// slice binds that discriminant itself as a compile-time constant; because it
/// is unnameable here, no collection can address the marker slice.
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

/// A single-section cell scan request over a half-open / closed coordinate
/// range.
///
/// `section` is required, so a cross-section scan cannot be constructed. The
/// `start`/`end` [`Bound`]s are **direction-relative**: forward walks from
/// `start` (the low side) toward `end` (the high side); backward walks from
/// `start` (the high side) toward `end` (the low side). `Unbounded` on either
/// side runs to the section edge in that direction.
///
/// The exclusive bounds exist for the coverage cache's gap fall-through: the
/// open `(p, q)` interval between two separately-covered sub-ranges (whose
/// endpoints `p`/`q` are already covered) is exactly an `Excluded`/`Excluded`
/// scan, and a punched singleton `{X}` is `Included(X)`/`Included(X)`.
#[derive(Clone, Copy)]
pub struct Scan<'a> {
    /// The section whose cells the scan walks.
    pub section: Section,

    /// The bound the scan starts walking from (low side forward, high side
    /// backward).
    pub start: Bound<&'a Coordinate>,

    /// The direction the scan walks from `start`.
    pub dir: Direction,

    /// The bound the scan stops at (high side forward, low side backward).
    pub end: Bound<&'a Coordinate>,

    /// The optional maximum number of cells to yield.
    pub limit: Option<usize>,
}

impl Scan<'_> {
    /// The scan's direction-relative bounds resolved to absolute `(low, high)`:
    /// forward keeps `(start, end)`, backward swaps to `(end, start)`.
    #[must_use]
    pub fn low_high(&self) -> (Bound<&Coordinate>, Bound<&Coordinate>) {
        match self.dir {
            Direction::Forward => (self.start, self.end),
            Direction::Backward => (self.end, self.start),
        }
    }

    /// Whether `coordinate` lies within the scan's coordinate range, accounting
    /// for direction and bound exclusivity.
    ///
    /// One of three equivalent range predicates — this one (used by the
    /// in-memory and overlay legs), the Cassandra hand-roll (CQL comparators
    /// plus `past_end`), and the test oracle `in_scan_range`. Their parity is
    /// pinned by the backend-generic property test `run_bottom_scan_trace`,
    /// which runs one generator against both `MemoryCellStore` and a live
    /// `CassandraStore` under the shared oracle across the full
    /// Direction × exclusivity space, so a one-sided edit fails the suite.
    #[must_use]
    pub fn contains(&self, coordinate: &Coordinate) -> bool {
        let (low, high) = self.low_high();
        above_low(coordinate, low) && below_high(coordinate, high)
    }
}

/// Whether `coordinate` is at or above the low `bound`.
fn above_low(coordinate: &Coordinate, bound: Bound<&Coordinate>) -> bool {
    match bound {
        Bound::Unbounded => true,
        Bound::Included(lo) => coordinate >= lo,
        Bound::Excluded(lo) => coordinate > lo,
    }
}

/// Whether `coordinate` is at or below the high `bound`.
fn below_high(coordinate: &Coordinate, bound: Bound<&Coordinate>) -> bool {
    match bound {
        Bound::Unbounded => true,
        Bound::Included(hi) => coordinate <= hi,
        Bound::Excluded(hi) => coordinate < hi,
    }
}

#[cfg(test)]
mod tests;
