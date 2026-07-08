//! Decoder for Cassandra cell rows into [`Cell`].
//!
//! Cassandra physically allows the value row to land in arbitrary
//! combinations of NULL/non-NULL across `data`, `prev_data`,
//! `encoding`, `version`, and `event`. The decoder collapses
//! any shape into a [`Cell`] or a typed Permanent corruption error:
//!
//! | Shape                                                    | Decoded as                         |
//! |----------------------------------------------------------|------------------------------------|
//! | Absent row (no row returned)                             | `Resolved(None)` (caller)          |
//! | `event` NULL, `prev_data` NULL                           | `Resolved(decode(data))`           |
//! | `event` non-NULL                                         | `Provisional { data, prev, ev }`   |
//! | `event` NULL, `prev_data` non-NULL                       | `CorruptCell::PrevWithoutEvent`    |
//! | a blob (`data`/`prev_data`) present with `encoding` NULL | `CorruptCell::BlobWithoutEncoding` |
//! | `version` present and ≠ [`INITIAL_VERSION`]              | `VersionMismatch`                  |
//! | semantically-corrupt `event` UDT (e.g. `kind == 7`)      | `CorruptUdt`                       |
//!
//! `encoding` and `version` are **shared** by `data` and
//! `prev_data`: a single build encodes both with the same codec. The pairing
//! is therefore validated *per blob* (a present blob needs an encoding), never
//! as a row-level "encoding implies a blob" rule. That distinction is
//! load-bearing: promoting a staged clear leaves the row with
//! `data`/`prev_data` both NULL but `encoding`/`version`
//! still populated (promote is O(1) and does not touch them), which is a
//! legitimate `Resolved(None)`, not corruption.
//!
//! The `event` column deserializes structurally into a [`RawEventRef`] and is
//! validated into an [`EventRef`](crate::state::EventRef) here via
//! [`RawEventRef::try_into_event`] — running that validation in this fallible
//! post-step (rather than inside scylla's `DeserializeValue`) keeps a corrupt
//! UDT classifiable as `Permanent` (skip the row) instead of `Terminal` (tear
//! the partition down).

use super::encoding::{Encoding, decode_payload};
use crate::state::cassandra::cell::INITIAL_VERSION;
use crate::state::cassandra::error::CassandraCellStoreError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Cell, Committed, ProvisionalCell};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use bytes::Bytes;
use thiserror::Error;

/// Five-column shape produced by `SELECT data, prev_data, encoding,
/// version, event` against `keyed_state_cell`.
///
/// Module-private — callers never observe the intermediate tuple.
pub(super) type RawCellRow = (
    Option<Vec<u8>>,     // data
    Option<Vec<u8>>,     // prev_data
    Option<i16>,         // encoding (shared by data + prev_data)
    Option<i32>,         // version (shared by data + prev_data)
    Option<RawEventRef>, // event (validated into EventRef during decode)
);

/// Nine-column shape produced by `SELECT section, coordinate, data,
/// prev_data, encoding, version, event, TTL(data), TTL(prev_data)` — a
/// [`RawCellRow`] prefixed with the clustering columns and suffixed with the
/// per-blob remaining TTLs [`blob_ttl`] coalesces. Used by scans, the recovery
/// sweep, and the cache-fill scan; the resolving paths discard the trailing
/// TTL, the cache-fill path keeps it.
pub(super) type KeyedCellRow = (
    i8,      // section
    Vec<u8>, // coordinate
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<i16>,
    Option<i32>,
    Option<RawEventRef>,
    Option<i32>, // TTL(data) in whole seconds
    Option<i32>, // TTL(prev_data) in whole seconds
);

/// Seven-column shape produced by `SELECT data, prev_data, encoding, version,
/// event, TTL(data), TTL(prev_data)` — a [`RawCellRow`] suffixed with the
/// per-blob remaining TTLs [`blob_ttl`] coalesces, for the cache-fill point
/// read.
pub(super) type CellTtlRow = (
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<i16>,
    Option<i32>,
    Option<RawEventRef>,
    Option<i32>, // TTL(data) in whole seconds
    Option<i32>, // TTL(prev_data) in whole seconds
);

/// Two-column shape produced by `SELECT section, coordinate` against the
/// `kind=Index` marker range — one bare provisional-coordinate per row.
pub(super) type IndexRow = (
    i8,      // section
    Vec<u8>, // coordinate
);

/// Builds the [`CellKey`] a bare `kind=Index` marker row addresses. Infallible:
/// the marker carries only the clustering key, which is opaque to the cell
/// layer (validated, if at all, by the owning collection).
pub(super) fn index_cell_key(row: IndexRow) -> CellKey {
    let (section, coordinate) = row;
    CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(coordinate),
    }
}

/// Decodes a keyed cell row into its [`CellKey`], [`Cell`], and cache-fill
/// co-expiry TTL ([`blob_ttl`], whole seconds), for the cache-fill scan.
///
/// # Errors
///
/// Returns the same corruption errors as [`try_decode_cell`].
pub(super) fn try_decode_keyed_cell_ttl(
    row: KeyedCellRow,
) -> Result<(CellKey, Cell, Option<i32>), CassandraCellStoreError> {
    let (section, coordinate, data, prev_data, encoding, version, event, ttl_data, ttl_prev) = row;
    let key = index_cell_key((section, coordinate));
    let (cell, ttl) = try_decode_cell_ttl((
        data, prev_data, encoding, version, event, ttl_data, ttl_prev,
    ))?;
    Ok((key, cell, ttl))
}

/// Decodes a cache-fill point row into its [`Cell`] and co-expiry TTL
/// ([`blob_ttl`]).
///
/// # Errors
///
/// Returns the same corruption errors as [`try_decode_cell`].
pub(super) fn try_decode_cell_ttl(
    row: CellTtlRow,
) -> Result<(Cell, Option<i32>), CassandraCellStoreError> {
    let (data, prev_data, encoding, version, event, ttl_data, ttl_prev) = row;
    let cell = try_decode_cell((data, prev_data, encoding, version, event))?;
    Ok((cell, blob_ttl(ttl_data, ttl_prev)))
}

/// The cache-fill co-expiry: the remaining TTL of whichever blob the row
/// carries.
///
/// `data` and `prev_data` are only ever written together, by one stage
/// statement, so a present blob's TTL **is** the row's shared write TTL —
/// coalescing keeps the co-expiry valid for whichever blob resolution ends up
/// returning. Reading `TTL(data)` alone was wrong: a staged clear (`data`
/// NULL, `prev_data` present) that resolution rolls back to `prev` reported
/// "no TTL", stamping the cache entry *never expires* while the durable row
/// kept a finite TTL. For a rollback the write-back re-binds the full
/// collection TTL, so this pre-resolution remainder is a conservative lower
/// bound — the cache entry can only under-live the durable row (an early
/// fall-through re-fetch), never outlive it.
fn blob_ttl(ttl_data: Option<i32>, ttl_prev: Option<i32>) -> Option<i32> {
    ttl_data.or(ttl_prev)
}

/// Decodes a cell row into a [`Cell`].
///
/// # Errors
///
/// Returns [`CassandraCellStoreError::CorruptCell`] for a forbidden column
/// shape, [`CassandraCellStoreError::CorruptUdt`] for a bad `event` UDT,
/// [`CassandraCellStoreError::VersionMismatch`] for an unknown
/// version stamp, or [`CassandraCellStoreError::Encoding`] when a blob fails
/// to deserialize.
pub(super) fn try_decode_cell(row: RawCellRow) -> Result<Cell, CassandraCellStoreError> {
    let (data, prev_data, encoding, version, event) = row;
    validate_version(version)?;

    let data = decode_blob(data, encoding)?;
    let prev = decode_blob(prev_data, encoding)?;

    match event {
        None => {
            if prev.is_some() {
                return Err(CellCorruptReason::PrevWithoutEvent.into());
            }
            Ok(Cell::Resolved(Committed::new(data)))
        }
        Some(raw) => {
            let event = raw.try_into_event()?;
            Ok(Cell::Provisional(ProvisionalCell::new(data, prev, event)))
        }
    }
}

/// Decodes one blob against the shared encoding. A NULL blob decodes to
/// `None` regardless of the shared encoding (the other blob may own it); a
/// present blob without an encoding is corrupt.
fn decode_blob(
    blob: Option<Vec<u8>>,
    encoding: Option<i16>,
) -> Result<Option<Bytes>, CassandraCellStoreError> {
    match (blob, encoding) {
        (None, _) => Ok(None),
        (Some(bytes), Some(encoding)) => {
            let encoding = Encoding::try_from(encoding)?;
            Ok(Some(decode_payload(&bytes, encoding)?))
        }
        (Some(_), None) => Err(CellCorruptReason::BlobWithoutEncoding.into()),
    }
}

/// Validates the `version` value when present. Absent is always fine
/// (an absent/cleared cell carries no version); a
/// non-[`INITIAL_VERSION`] stamp is unreachable until identity
/// migration ships and is rejected Permanent so a future-version cell is never
/// misread.
fn validate_version(version: Option<i32>) -> Result<(), CassandraCellStoreError> {
    match version {
        None | Some(INITIAL_VERSION) => Ok(()),
        Some(stored) => Err(CassandraCellStoreError::VersionMismatch {
            stored,
            expected: INITIAL_VERSION,
        }),
    }
}

/// Specific cell-row corruption shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum CellCorruptReason {
    /// `event` is NULL but `prev_data` is non-NULL. No statement writes this
    /// shape: `prev_data` is only ever set alongside `event` by a provisional
    /// write, and both are nulled together on resolution.
    #[error("prev_data column is non-NULL but event is NULL")]
    PrevWithoutEvent,

    /// A `data`/`prev_data` blob is present but the shared `encoding`
    /// is NULL, so the blob cannot be decoded.
    #[error("a cell blob is non-NULL but encoding is NULL")]
    BlobWithoutEncoding,
}

#[cfg(test)]
mod tests;
