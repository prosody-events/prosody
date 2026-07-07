//! Cache key + cell codec for the fjall cell cache.
//!
//! Two requirements drive the cache key shape:
//!
//! 1. **Point reads (Value).** Cheap, well-defined lookups by full collection
//!    identifier.
//! 2. **Prefix scans (Map, Deque).** "All entries for one collection" must be a
//!    contiguous range; range queries within a collection must preserve user
//!    ordering.
//!
//! The hierarchy is `[16-byte collection hash][1-byte section][coordinate
//! bytes]`:
//!
//! - The collection hash is `xxh3_128` over an **injective** encoding of the
//!   collection identity: the fixed-width fields first (`segment_id` then the
//!   one-byte `state_type`), then each variable-length field length-prefixed
//!   (`key_len` as 8 big-endian bytes, then `key`; `name_len`, then `name`).
//!   The hash is serialized **big-endian** for stable cross-platform ordering.
//!   Length-prefixing (rather than a delimiter byte) keeps the encoding
//!   injective even when `key` or `name` contain the delimiter — Kafka keys are
//!   arbitrary bytes — so distinct collections cannot share an input buffer and
//!   the only residual collision risk is the hash's own ≈ 2⁻⁶⁴.
//! - The **section** byte ([`Section`]'s `i8` discriminant) groups one
//!   collection's cells by section, so a section range scan is contiguous.
//! - The **coordinate** tail is the cell's order-preserving coordinate bytes
//!   (empty for Value, the `EncodedMapKey` for Map, the big-endian index for
//!   Deque), so a Map/Deque prefix range preserves user order.
//!
//! Collision probability for `xxh3_128` is ≈ 2⁻⁶⁴ (birthday bound) — well
//! below practical concern for non-adversarial caches. Collisions resolve
//! via miss-then-populate cycles; the read path does not verify
//! collisions.
//!
//! "Drop all cache state on partition revocation" = drop the fjall keyspace.
//!
//! # Cell frame and TTL co-expiry
//!
//! Each stored cell is framed `[tag][expiry_millis: u64 BE][payload]`. The
//! `expiry` is an absolute wall-clock millisecond deadline mirroring the
//! durable Cassandra row's TTL death; `0` means "never expires" (a `None`-TTL
//! collection). Fjall has no native per-entry TTL, so the cache enforces it on
//! read: [`decode_cell`] returns the expiry and the caller treats `now >=
//! expiry` (a non-zero expiry) as a miss/skip — exactly as the oracle resolves
//! the expired durable row to absent. Stamping rounds **down** (the expiry is
//! `now + remaining` where `remaining` is the row's whole-second `TTL(data)`),
//! so a fjall entry never outlives its durable value; an entry that expires
//! slightly early falls through and re-populates.

use super::error::FjallCellCacheError;
use crate::state::CollectionId;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use bytes::Bytes;
use std::ops::Bound;
use xxhash_rust::xxh3::Xxh3;

/// Length of the collection hash prefix that leads every fjall key (cell and
/// index alike).
const COLLECTION_PREFIX_LEN: usize = 16;

/// The row family within the per-partition warm `index` keyspace, discriminated
/// by the byte immediately after the 16-byte collection hash so each family
/// forms a contiguous prefix range.
///
/// A serialize-only discriminator (`From<_> for u8`) that leads each key we
/// write. Reads are always prefix-scoped to one family (a `Coord`/`Cover`
/// range, or a `Seeded` point key), so the discriminator is never decoded back
/// — the family is known from the range that produced the key.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexKind {
    /// A live provisional coordinate: key `[hash][Coord][section][coordinate]`,
    /// empty value. Presence ⟺ `(collection, cell)` is durably provisional.
    Coord = 0x00,
    /// The one-time cold-seed latch: key `[hash][Seeded]`, empty value.
    Seeded = 0x01,
    /// One stored scan-coverage interval: key
    /// `[hash][Cover][section][lo-bound frame]`, value `[hi-bound frame]`.
    Cover = 0x02,
}

impl From<IndexKind> for u8 {
    fn from(kind: IndexKind) -> Self {
        kind as u8
    }
}

/// A [`Bound<Coordinate>`] endpoint frame: `[BoundTag][coordinate bytes?]`.
///
/// The tag leads the frame, so a coverage `Cover` prefix range keys each stored
/// interval by its low-bound frame uniquely (two live intervals are disjoint,
/// so their low bounds never collide). The frame's raw byte order is *not* the
/// coverage `cmp_low` continuum order — the tag dominates the coordinate — so
/// the coverage layer re-sorts the loaded pairs into a `BTreeMap` keyed by
/// `cmp_low` (`IntervalSet::from_pairs`); the fjall range is only a bounded,
/// per-section batch read, never relied on for order.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundTag {
    Unbounded = 0x00,
    Included = 0x01,
    Excluded = 0x02,
}

impl From<BoundTag> for u8 {
    fn from(tag: BoundTag) -> Self {
        tag as u8
    }
}

/// The cache's three-valued read, decoded from a stored cell frame by
/// [`decode_cell`].
///
/// `Unknown` (no entry) is what makes the cache a pass-through layer: only a
/// stored frame may answer `Present`/`Absent`, so a miss always falls through
/// to the durable store instead of being mistaken for a known-absent value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read<T> {
    /// Value is present.
    Present(T),

    /// Value is known absent.
    Absent,

    /// This layer has not observed the value.
    Unknown,
}

/// Tag byte for "known absent" entries.
const CACHE_TAG_ABSENT: u8 = 0x00;

/// Tag byte for "known present" entries.
const CACHE_TAG_PRESENT: u8 = 0x01;

/// Width of the absolute-expiry header (`u64` big-endian millis) carried after
/// the tag byte by every cell frame. `0` means "never expires".
const EXPIRY_LEN: usize = 8;

/// An expiry value meaning "never expires" — stamped for a `None`-TTL
/// collection.
pub(super) const NEVER_EXPIRES: u64 = 0;

/// Length of the fixed key prefix shared by every cell of one collection
/// section: the 16-byte collection hash plus the 1-byte section discriminant.
/// A range scan over `[section_prefix, …]` stays within one section of one
/// collection; the order-preserving coordinate bytes follow.
pub(super) const SECTION_PREFIX_LEN: usize = COLLECTION_PREFIX_LEN + 1;

/// Returns the full fjall key for one cell: the 16-byte collection prefix
/// followed by the cell's `section` byte and order-preserving `coordinate`
/// bytes. The prefix groups a collection's cells contiguously; the section +
/// coordinate suffix orders them, so a Map/Deque prefix range is a contiguous
/// fjall range that preserves user order.
#[must_use]
pub(super) fn cell_key(id: &CollectionId, cell: &CellKey) -> Vec<u8> {
    let prefix = collection_prefix(id);
    let coordinate = cell.coordinate.as_bytes();
    let mut key = Vec::with_capacity(prefix.len() + 1 + coordinate.len());
    key.extend_from_slice(&prefix);
    key.push(i8::from(cell.section).cast_unsigned());
    key.extend_from_slice(coordinate);
    key
}

/// Returns the [`SECTION_PREFIX_LEN`]-byte prefix shared by every cell of one
/// `(collection, section)`: the 16-byte collection hash followed by the
/// section's `i8` discriminant. Range scans build their byte bounds by
/// appending coordinate bytes to this prefix.
#[must_use]
pub(super) fn section_prefix(id: &CollectionId, section: Section) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(SECTION_PREFIX_LEN);
    prefix.extend_from_slice(&collection_prefix(id));
    prefix.push(i8::from(section).cast_unsigned());
    prefix
}

/// Reconstructs a cell's [`Coordinate`] from a full fjall key by dropping the
/// [`SECTION_PREFIX_LEN`]-byte section prefix. The caller already knows the
/// section (it scoped the scan to it), so only the coordinate tail is
/// recovered.
#[must_use]
pub(super) fn coordinate_of(key: &[u8]) -> Coordinate {
    Coordinate::from_bytes(key[SECTION_PREFIX_LEN..].to_vec())
}

/// The warm-index key for a provisional coordinate:
/// `[hash][Coord][section][coordinate]`. Presence ⟺ the cell is provisional.
#[must_use]
pub(super) fn index_coord_key(id: &CollectionId, cell: &CellKey) -> Vec<u8> {
    let coordinate = cell.coordinate.as_bytes();
    let mut key = Vec::with_capacity(COLLECTION_PREFIX_LEN + 2 + coordinate.len());
    key.extend_from_slice(&collection_prefix(id));
    key.push(IndexKind::Coord.into());
    key.push(i8::from(cell.section).cast_unsigned());
    key.extend_from_slice(coordinate);
    key
}

/// The `[hash][Coord]` prefix bounding a collection's provisional-coordinate
/// range — the ascending scan `snapshot` drains.
#[must_use]
pub(super) fn index_coord_prefix(id: &CollectionId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(COLLECTION_PREFIX_LEN + 1);
    prefix.extend_from_slice(&collection_prefix(id));
    prefix.push(IndexKind::Coord.into());
    prefix
}

/// Reconstructs a [`CellKey`] from a `Coord` index key produced by
/// [`index_coord_key`]: the byte after the collection hash is the family
/// discriminator, the next is the section, and the tail is the coordinate.
#[must_use]
pub(super) fn coord_cell_key(key: &[u8]) -> CellKey {
    let section = Section::new(key[COLLECTION_PREFIX_LEN + 1].cast_signed());
    let coordinate = Coordinate::from_bytes(key[COLLECTION_PREFIX_LEN + 2..].to_vec());
    CellKey {
        section,
        coordinate,
    }
}

/// The warm-index key for a collection's one-time cold-seed latch:
/// `[hash][Seeded]`. Presence ⟺ the seed has run.
#[must_use]
pub(super) fn index_seeded_key(id: &CollectionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(COLLECTION_PREFIX_LEN + 1);
    key.extend_from_slice(&collection_prefix(id));
    key.push(IndexKind::Seeded.into());
    key
}

/// The `[hash][Cover][section]` prefix bounding one `(collection, section)`'s
/// stored coverage intervals — the range a coverage read-modify-write scans.
#[must_use]
pub(super) fn index_cover_prefix(id: &CollectionId, section: Section) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(COLLECTION_PREFIX_LEN + 2);
    prefix.extend_from_slice(&collection_prefix(id));
    prefix.push(IndexKind::Cover.into());
    prefix.push(i8::from(section).cast_unsigned());
    prefix
}

/// The full warm-index key for one stored coverage interval: the
/// `[hash][Cover][section]` prefix followed by the interval's low-bound frame.
/// The frame is tag-first, so its raw byte order is **not** `cmp_low` (see
/// [`BoundTag`]); the coverage layer re-sorts the loaded pairs via
/// [`IntervalSet::from_pairs`](super::super::cached::coverage), so the fjall
/// range order is not relied on.
#[must_use]
pub(super) fn index_cover_key(
    id: &CollectionId,
    section: Section,
    lo: &Bound<Coordinate>,
) -> Vec<u8> {
    let prefix = index_cover_prefix(id, section);
    let frame = encode_bound(lo);
    let mut key = Vec::with_capacity(prefix.len() + frame.len());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(&frame);
    key
}

/// Decodes the low bound stored in a coverage key's frame tail (the bytes after
/// the `[hash][Cover][section]` prefix).
///
/// # Errors
///
/// Returns [`FjallCellCacheError`] when the frame tag is unknown.
pub(super) fn cover_low_bound(key: &[u8]) -> Result<Bound<Coordinate>, FjallCellCacheError> {
    decode_bound(&key[COLLECTION_PREFIX_LEN + 2..])
}

/// Encodes a [`Bound<Coordinate>`] as a frame `[BoundTag][coordinate bytes?]`.
#[must_use]
pub(super) fn encode_bound(bound: &Bound<Coordinate>) -> Vec<u8> {
    match bound {
        Bound::Unbounded => vec![BoundTag::Unbounded.into()],
        Bound::Included(c) | Bound::Excluded(c) => {
            let tag = if matches!(bound, Bound::Included(_)) {
                BoundTag::Included
            } else {
                BoundTag::Excluded
            };
            let coordinate = c.as_bytes();
            let mut frame = Vec::with_capacity(1 + coordinate.len());
            frame.push(tag.into());
            frame.extend_from_slice(coordinate);
            frame
        }
    }
}

/// Decodes a [`Bound<Coordinate>`] frame produced by [`encode_bound`].
///
/// # Errors
///
/// Returns [`FjallCellCacheError::UnknownBoundTag`] when the leading tag byte
/// is unrecognized, or [`FjallCellCacheError::EmptyCacheCell`] on a truncated
/// frame (both are our own writes into a fresh keyspace, so either is a bug).
pub(super) fn decode_bound(frame: &[u8]) -> Result<Bound<Coordinate>, FjallCellCacheError> {
    let (tag, rest) = frame
        .split_first()
        .ok_or(FjallCellCacheError::EmptyCacheCell)?;
    match *tag {
        t if t == u8::from(BoundTag::Unbounded) => Ok(Bound::Unbounded),
        t if t == u8::from(BoundTag::Included) => {
            Ok(Bound::Included(Coordinate::from_bytes(rest.to_vec())))
        }
        t if t == u8::from(BoundTag::Excluded) => {
            Ok(Bound::Excluded(Coordinate::from_bytes(rest.to_vec())))
        }
        other => Err(FjallCellCacheError::UnknownBoundTag(other)),
    }
}

/// Returns the 16-byte collection prefix for a collection identity.
///
/// See module docs for the field layout and rationale.
#[must_use]
pub(super) fn collection_prefix(id: &CollectionId) -> [u8; COLLECTION_PREFIX_LEN] {
    let segment_bytes = id.state_key().segment_id.as_bytes();
    let key_bytes = id.state_key().key.as_bytes();
    let state_type_byte = i8::from(id.state_type()).cast_unsigned();
    let name_bytes = id.name().as_str().as_bytes();

    // Injective layout: fixed-width fields first, then each variable-length
    // field length-prefixed. A delimiter byte would not be injective — a key
    // or name containing it could shift the field boundary (Kafka keys are
    // arbitrary bytes) — so two distinct collections could share a buffer.
    //
    // Streamed through `Xxh3` (seed 0, identical to `xxh3_128`) so no transient
    // buffer is allocated. The byte sequence fed here is load-bearing: it is the
    // durable cache key, so the field order and the big-endian `u64` length
    // prefixes must stay byte-for-byte what the buffer build produced. Never
    // substitute `write_u64`/`write_u32` — those are native-endian.
    let mut hasher = Xxh3::new();
    hasher.update(segment_bytes);
    hasher.update(&[state_type_byte]);
    hasher.update(&(key_bytes.len() as u64).to_be_bytes());
    hasher.update(key_bytes);
    hasher.update(&(name_bytes.len() as u64).to_be_bytes());
    hasher.update(name_bytes);

    hasher.digest128().to_be_bytes()
}

/// Encodes an `Absent` cache cell with its absolute `expiry` (`0` = never).
#[must_use]
pub fn encode_absent_cell(expiry: u64) -> Bytes {
    let mut buf = Vec::with_capacity(1 + EXPIRY_LEN);
    buf.push(CACHE_TAG_ABSENT);
    buf.extend_from_slice(&expiry.to_be_bytes());
    Bytes::from(buf)
}

/// Encodes a `Present` cache cell from raw payload bytes and its absolute
/// `expiry` (`0` = never).
///
/// The cell is framed `[CACHE_TAG_PRESENT][expiry: u64 BE][raw payload]` — the
/// payload is stored verbatim, with no app-level compression. fjall
/// block-compresses the containing data block (LZ4) on disk at
/// flush/compaction, so a redundant per-cell codec layer is neither needed nor
/// applied.
#[must_use]
pub fn encode_present_cell(payload: &[u8], expiry: u64) -> Bytes {
    let mut buf = Vec::with_capacity(1 + EXPIRY_LEN + payload.len());
    buf.push(CACHE_TAG_PRESENT);
    buf.extend_from_slice(&expiry.to_be_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

/// Decodes a cache cell into its absolute expiry and three-valued read. The
/// caller checks the expiry against its clock (`now >= expiry`, expiry non-zero
/// ⇒ treat as a miss/skip); keeping the check in the caller lets one clock
/// drive every read.
///
/// Returns:
/// - `Ok((0, Read::Unknown))` only when the cache had no entry (caller signals
///   this by passing `None` here).
/// - `Ok((expiry, Read::Absent))` for a `0x00`-tagged cell.
/// - `Ok((expiry, Read::Present(payload)))` for a `0x01`-tagged cell with the
///   raw payload tail (which may be empty — a `Set` of empty bytes is a present
///   value distinct from `Absent`).
/// - `Err(_)` for any malformed cell (buffer too short for the tag + expiry
///   header, unknown tag).
///
/// The returned `Bytes` is a fresh `copy_from_slice` of the payload tail, so
/// it is uniquely owned — preserving the `try_into_mut` read fast path the
/// handle relies on.
///
/// # Errors
///
/// Returns a [`FjallCellCacheError`] when the cell is malformed.
pub fn decode_cell(bytes: Option<&[u8]>) -> Result<(u64, Read<Bytes>), FjallCellCacheError> {
    let Some(bytes) = bytes else {
        return Ok((NEVER_EXPIRES, Read::Unknown));
    };
    let (tag, rest) = bytes
        .split_first()
        .ok_or(FjallCellCacheError::EmptyCacheCell)?;
    // The expiry header follows the tag for both Present and Absent frames.
    let expiry_bytes: [u8; EXPIRY_LEN] = rest
        .get(..EXPIRY_LEN)
        .and_then(|s| s.try_into().ok())
        .ok_or(FjallCellCacheError::EmptyCacheCell)?;
    let expiry = u64::from_be_bytes(expiry_bytes);
    let payload = &rest[EXPIRY_LEN..];
    match *tag {
        CACHE_TAG_ABSENT => Ok((expiry, Read::Absent)),
        // An empty payload tail is valid: a `Set` of empty bytes frames as
        // `[0x01][expiry]`, so do NOT re-add an "empty tail ⇒ corrupt" guard.
        CACHE_TAG_PRESENT => Ok((expiry, Read::Present(Bytes::copy_from_slice(payload)))),
        other => Err(FjallCellCacheError::UnknownCacheTag(other)),
    }
}

#[cfg(test)]
mod tests;
