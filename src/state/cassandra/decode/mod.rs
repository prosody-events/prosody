//! Decoder for Cassandra Value partition rows into [`DurableState`].
//!
//! Cassandra physically allows the value row to land in arbitrary
//! combinations of NULL/non-NULL across `data`, `payload_encoding`,
//! `identity_version`, `wal_event`, `wal_ops`, and `wal_format`. The
//! decoder collapses any shape into one of three outcomes:
//!
//! | Shape                                                | Decoded as              |
//! |------------------------------------------------------|-------------------------|
//! | Absent row (no row returned)                         | `Idle { applied: None }`|
//! | All columns NULL                                     | `Idle { applied: None }`|
//! | `data + payload_encoding`, no WAL columns            | `Idle { applied: Some }` |
//! | WAL columns, no `data`/`payload_encoding`            | `Sealed { applied: None, wal }` |
//! | `data + payload_encoding` + WAL columns              | `Sealed { applied: Some, wal }` |
//! | Semantically-corrupt `event_ref` UDT (e.g. `kind == 7`) | `CorruptUdt { .. }` |
//! | `payload_encoding`/`identity_version` not paired with `data` | `CorruptWal { reason }` |
//! | `identity_version` ≠ [`INITIAL_IDENTITY_VERSION`]    | `IdentityVersionMismatch` |
//! | Anything else                                        | `CorruptWal { reason }` |
//!
//! `payload_encoding` and `identity_version` both pair with `data` (each
//! present iff `data` is present), independent of the WAL shape: they are
//! members of the *applied triple* (`data` + `payload_encoding` +
//! `identity_version`), written and cleared only by apply/direct-apply
//! statements. `seal` writes only the WAL columns and rollback clears only
//! the WAL columns, so the triple always shares one write timestamp and TTL
//! and a sealed row simply reuses the applied cells (if any) for its
//! pre-WAL state. `identity_version` records which descriptor identity
//! version the authoritative bytes were written under; a stamp other than
//! [`INITIAL_IDENTITY_VERSION`] is unreachable until identity migration
//! ships and is rejected Permanent defensively.
//!
//! The intermediate `Option<...>` tuple [`RawValueRow`] is private to this
//! module; callers see only the three valid outcomes plus the typed
//! corruption reasons. The `wal_event` column deserializes (structurally)
//! into a [`RawEventRef`] and is validated into an [`EventRef`] here via
//! [`RawEventRef::try_into_event`] — running that validation in this fallible
//! post-step (rather than inside scylla's `DeserializeValue`) is what keeps a
//! corrupt UDT classifiable as `Permanent` (skip the row) instead of
//! `Terminal` (tear the partition down). See [`super::udt`].

use super::error::CassandraValueStoreError;
use super::udt::RawEventRef;
use crate::state::descriptor_identity::INITIAL_IDENTITY_VERSION;
use crate::state::encoding::decode_payload;
use crate::state::value::ValueKind;
use crate::state::{DurableState, PayloadEncoding, SealedWal, WalBlob, WalFormat};
use std::fmt;
use thiserror::Error;

/// Six-column shape produced by `SELECT data, payload_encoding,
/// identity_version, wal_event, wal_ops, wal_format` against
/// `keyed_state_value`.
///
/// Module-private — callers never observe the intermediate tuple.
pub(super) type RawValueRow = (
    Option<Vec<u8>>,     // data
    Option<i16>,         // payload_encoding
    Option<i32>,         // identity_version (paired with data)
    Option<RawEventRef>, // wal_event (validated into EventRef during decode)
    Option<Vec<u8>>,     // wal_ops
    Option<i16>,         // wal_format
);

/// Decodes the value partition columns into a [`DurableState`].
///
/// # Errors
///
/// Returns [`CassandraValueStoreError::CorruptWal`] when the row carries a
/// forbidden combination, or [`CassandraValueStoreError::Encoding`] when a
/// payload or WAL cell fails to deserialize.
pub(super) fn try_decode_row(
    row: RawValueRow,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    let (data, payload_encoding, identity_version, wal_event, wal_ops, wal_format) = row;
    validate_identity_version(data.as_ref(), identity_version)?;
    let mask =
        WalColumnMask::from_options(wal_event.as_ref(), wal_ops.as_ref(), wal_format.as_ref());

    // Match the WAL triple by-move: the only valid shapes are all-Some
    // (sealed) and all-None (idle). `mask` was computed by reference above, so
    // it survives the move for the partial-shape diagnostic.
    match (wal_event, wal_ops, wal_format) {
        (Some(event), Some(ops), Some(format)) => {
            decode_sealed(data, payload_encoding, event, ops, format)
        }
        (None, None, None) => decode_idle(data, payload_encoding),
        _ => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PartialWalColumns { mask },
        }),
    }
}

/// Enforces the `data` ⇔ `identity_version` pairing and the frozen version
/// value, independent of the WAL shape (the stamp belongs to the applied
/// bytes; `seal` never writes it).
fn validate_identity_version(
    data: Option<&Vec<u8>>,
    identity_version: Option<i32>,
) -> Result<(), CassandraValueStoreError> {
    match (data, identity_version) {
        (None, None) | (Some(_), Some(INITIAL_IDENTITY_VERSION)) => Ok(()),
        (Some(_), Some(stored)) => Err(CassandraValueStoreError::IdentityVersionMismatch {
            stored,
            expected: INITIAL_IDENTITY_VERSION,
        }),
        (Some(_), None) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::MissingIdentityVersionWithData,
        }),
        (None, Some(_)) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::IdentityVersionWithoutData,
        }),
    }
}

/// Decodes the applied cells, enforcing the `data` ⇔ `payload_encoding`
/// pairing symmetrically with [`validate_identity_version`]. Both members
/// of the applied triple are present together or absent together, regardless
/// of the WAL shape — a sealed row with no prior `data` simply has no
/// applied cells.
fn decode_applied(
    data: Option<Vec<u8>>,
    payload_encoding: Option<i16>,
) -> Result<Option<bytes::Bytes>, CassandraValueStoreError> {
    match (data, payload_encoding) {
        (None, None) => Ok(None),
        (Some(bytes), Some(encoding)) => {
            let encoding = PayloadEncoding::try_from(encoding)?;
            Ok(Some(decode_payload(&bytes, encoding)?))
        }
        (Some(_), None) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::MissingPayloadEncodingWithData,
        }),
        (None, Some(_)) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PayloadEncodingWithoutData,
        }),
    }
}

fn decode_idle(
    data: Option<Vec<u8>>,
    payload_encoding: Option<i16>,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    Ok(DurableState::Idle {
        applied: decode_applied(data, payload_encoding)?,
    })
}

fn decode_sealed(
    data: Option<Vec<u8>>,
    payload_encoding: Option<i16>,
    raw_event: RawEventRef,
    ops_bytes: Vec<u8>,
    format_raw: i16,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    // Validate the structurally-decoded UDT into a typed event. A corrupt
    // shape surfaces as `CorruptUdt` (Permanent → skip), not as a laundered
    // scylla `DeserializationError` (Terminal → shut down).
    let event = raw_event.try_into_event()?;
    let format = WalFormat::try_from(format_raw)?;
    let applied = decode_applied(data, payload_encoding)?;

    let wal =
        SealedWal::<ValueKind>::new(event, WalBlob::new(bytes::Bytes::from(ops_bytes), format));
    Ok(DurableState::Sealed { applied, wal })
}

/// Which of the WAL columns were present on a row.
///
/// Encodes the (`wal_event`, `wal_ops`, `wal_format`) presence triple. The
/// `Idle` (all NULL) and `Sealed` (all non-NULL) bit-patterns are the only
/// valid shapes; everything else is reported via
/// [`CorruptReason::PartialWalColumns`] so a future contributor can read
/// the exact partial shape off a log line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalColumnMask {
    /// `wal_event` column is non-NULL.
    pub event: bool,
    /// `wal_ops` column is non-NULL.
    pub ops: bool,
    /// `wal_format` column is non-NULL.
    pub format: bool,
}

impl WalColumnMask {
    fn from_options<E, B, F>(event: Option<&E>, ops: Option<&B>, format: Option<&F>) -> Self {
        Self {
            event: event.is_some(),
            ops: ops.is_some(),
            format: format.is_some(),
        }
    }
}

impl fmt::Display for WalColumnMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "event={} ops={} format={}",
            self.event, self.ops, self.format
        )
    }
}

/// Specific row corruption shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum CorruptReason {
    /// `data` is non-NULL but `payload_encoding` is NULL on an Idle row.
    #[error("data column is non-NULL but payload_encoding is NULL")]
    MissingPayloadEncodingWithData,

    /// `data` is NULL but `payload_encoding` is non-NULL on an Idle row.
    #[error("payload_encoding column is non-NULL but data is NULL")]
    PayloadEncodingWithoutData,

    /// `data` is non-NULL but `identity_version` is NULL.
    #[error("data column is non-NULL but identity_version is NULL")]
    MissingIdentityVersionWithData,

    /// `data` is NULL but `identity_version` is non-NULL.
    #[error("identity_version column is non-NULL but data is NULL")]
    IdentityVersionWithoutData,

    /// WAL columns are partially populated (some NULL, some non-NULL).
    #[error("WAL columns are partially populated: {mask}")]
    PartialWalColumns {
        /// Which of the WAL columns were present.
        mask: WalColumnMask,
    },
}

#[cfg(test)]
mod tests;
