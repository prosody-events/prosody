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

use crate::state::Encoding;
use crate::state::cassandra::error::CassandraValueStoreError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Cell, Committed, ProvisionalCell};
use crate::state::descriptor_identity::INITIAL_VERSION;
use crate::state::encoding::decode_payload;
use bytes::Bytes;
use thiserror::Error;

/// Five-column shape produced by `SELECT data, prev_data, encoding,
/// version, event` against `keyed_state_value`.
///
/// Module-private — callers never observe the intermediate tuple.
pub(super) type RawCellRow = (
    Option<Vec<u8>>,     // data
    Option<Vec<u8>>,     // prev_data
    Option<i16>,         // encoding (shared by data + prev_data)
    Option<i32>,         // version (shared by data + prev_data)
    Option<RawEventRef>, // event (validated into EventRef during decode)
);

/// Decodes a cell row into a [`Cell`].
///
/// # Errors
///
/// Returns [`CassandraValueStoreError::CorruptCell`] for a forbidden column
/// shape, [`CassandraValueStoreError::CorruptUdt`] for a bad `event` UDT,
/// [`CassandraValueStoreError::VersionMismatch`] for an unknown
/// version stamp, or [`CassandraValueStoreError::Encoding`] when a blob fails
/// to deserialize.
pub(super) fn try_decode_cell(row: RawCellRow) -> Result<Cell, CassandraValueStoreError> {
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
) -> Result<Option<Bytes>, CassandraValueStoreError> {
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
fn validate_version(version: Option<i32>) -> Result<(), CassandraValueStoreError> {
    match version {
        None | Some(INITIAL_VERSION) => Ok(()),
        Some(stored) => Err(CassandraValueStoreError::VersionMismatch {
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
