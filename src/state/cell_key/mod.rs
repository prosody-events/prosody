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
//! `i8`/bytes. So these types name no collection family — no collection kind
//! appears here — and the cell layer cannot dispatch on or escape its
//! partition.
//!
//! [`order_codec`]: crate::state::order_codec

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// Disjoint, orderable sub-grouping of one collection's cells.
///
/// The high-order component of a [`CellKey`], paired with the low-order
/// [`Coordinate`]. **Opaque to the cell-store core**, which only stores it,
/// sorts by it, and scopes single-section scans to it — it never interprets the
/// meaning. Each collection owns the meaning of its sections (e.g. a Map's
/// keyset (meta) section vs its entry section) and lowers its own section
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

impl AsRef<[u8]> for Coordinate {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// A read address that borrows encoded coordinate bytes.
/// Its bytes must remain valid until the read completes.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellRef<'a> {
    /// The cell's section.
    pub section: Section,
    /// The encoded coordinate within the section.
    pub coordinate: &'a [u8],
}

impl CellRef<'_> {
    /// Copies the address for storage beyond the read.
    #[must_use]
    pub fn into_owned(self) -> CellKey {
        CellKey {
            section: self.section,
            coordinate: Coordinate::from_bytes(Bytes::copy_from_slice(self.coordinate)),
        }
    }
}

/// Full intra-collection cell address. `Ord` is `(section, coordinate)`.
///
/// Equality, order, and hashing delegate to [`CellRef`]. A borrowed lookup
/// therefore finds the owned key in any map or tree.
///
/// It carries **only** `(section, coordinate)` — never the cell store's
/// internal `kind` discriminant (the reserved-`kind` safety invariant). A
/// backend that splits its partition into a data slice and an event-marker
/// slice binds that discriminant itself as a compile-time constant; because it
/// is unnameable here, no collection can address the marker slice.
#[derive(Clone, Debug)]
pub struct CellKey {
    /// The cell's sub-grouping section.
    pub section: Section,

    /// The cell's order-preserving coordinate within the section.
    pub coordinate: Coordinate,
}

impl CellKey {
    /// Borrows this address for a read without a coordinate copy.
    #[must_use]
    pub fn as_ref(&self) -> CellRef<'_> {
        CellRef {
            section: self.section,
            coordinate: self.coordinate.as_bytes(),
        }
    }
}

impl PartialEq for CellKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for CellKey {}

impl PartialOrd for CellKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl Hash for CellKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

/// Direction a [`Scan`] walks the clustering range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Ascending `coordinate` byte order.
    Forward,

    /// Descending `coordinate` byte order.
    Backward,
}

impl Direction {
    /// Converts ascending `(low, high)` edges into `(start, end)` edges.
    /// Backward swaps the pair. The swap is its own inverse, so this also
    /// converts `(start, end)` into `(low, high)`.
    pub(crate) fn orient<T>(self, low: T, high: T) -> (T, T) {
        match self {
            Self::Forward => (low, high),
            Self::Backward => (high, low),
        }
    }
}

/// A cell scan within one section.
/// Bounds follow the scan direction. Forward scans start low and end high.
/// Backward scans start high and end low. Either bound can be unbounded.
#[derive(Clone, Copy)]
pub struct Scan<'a> {
    /// The section whose cells the scan walks.
    pub section: Section,

    /// The edge the scan starts walking from (low side forward, high side
    /// backward).
    pub start: Bound<&'a [u8]>,

    /// The direction the scan walks from `start`.
    pub dir: Direction,

    /// The edge the scan stops at (high side forward, low side backward).
    pub end: Bound<&'a [u8]>,

    /// The preferred size of the first fetch. A backend sizes its first page or
    /// batch from it and grows later fetches. It never limits results.
    pub fetch_hint: Option<NonZeroUsize>,
}

impl Scan<'_> {
    /// The scan's direction-relative edges resolved to absolute `(low, high)`:
    /// forward keeps `(start, end)`, backward swaps to `(end, start)`.
    #[must_use]
    pub fn low_high(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        self.dir.orient(self.start, self.end)
    }

    /// Tests whether a coordinate lies within the bounds in scan order.
    #[must_use]
    pub fn contains(&self, coordinate: &Coordinate) -> bool {
        RangeBounds::<[u8]>::contains(&self.low_high(), coordinate.as_bytes())
    }
}

#[cfg(test)]
mod tests;
