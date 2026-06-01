//! Decoder for Cassandra Value partition rows into [`DurableState`].
//!
//! Cassandra physically allows the value row to land in arbitrary
//! combinations of NULL/non-NULL across `data`, `payload_encoding`,
//! `wal_event`, `wal_ops`, and `wal_format`. The decoder collapses any
//! shape into one of three outcomes:
//!
//! | Shape                                                | Decoded as              |
//! |------------------------------------------------------|-------------------------|
//! | Absent row (no row returned)                         | `Idle { applied: None }`|
//! | All columns NULL                                     | `Idle { applied: None }`|
//! | `data + payload_encoding`, no WAL columns            | `Idle { applied: Some }` |
//! | `data + payload_encoding + wal_event + wal_ops + wal_format` (any data presence) | `Sealed { applied, wal }` |
//! | Semantically-corrupt `event_ref` UDT (e.g. `kind == 7`) | `CorruptUdt { .. }` |
//! | Anything else                                        | `CorruptWal { reason }` |
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
use crate::state::encoding::decode_payload;
use crate::state::value::ValueKind;
use crate::state::{DurableState, PayloadEncoding, SealedWal, StoredPayload, WalBlob, WalFormat};
use std::fmt;
use thiserror::Error;

/// Five-column shape produced by `SELECT data, payload_encoding, wal_event,
/// wal_ops, wal_format` against `keyed_state_value`.
///
/// Module-private — callers never observe the intermediate tuple.
pub(super) type RawValueRow = (
    Option<Vec<u8>>,     // data
    Option<i16>,         // payload_encoding
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
    let (data, payload_encoding, wal_event, wal_ops, wal_format) = row;
    let mask =
        WalColumnMask::from_options(wal_event.as_ref(), wal_ops.as_ref(), wal_format.as_ref());

    match (mask.is_sealed(), mask.is_idle()) {
        (true, _) => decode_sealed(data, payload_encoding, wal_event, wal_ops, wal_format),
        (_, true) => decode_idle(data, payload_encoding),
        _ => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PartialWalColumns { mask },
        }),
    }
}

fn decode_idle(
    data: Option<Vec<u8>>,
    payload_encoding: Option<i16>,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    match (data, payload_encoding) {
        (None, None) => Ok(DurableState::Idle { applied: None }),
        (Some(bytes), Some(encoding)) => {
            let encoding = PayloadEncoding::try_from_i16(encoding)?;
            let payload = decode_payload::<StoredPayload>(&bytes, encoding)?;
            Ok(DurableState::Idle {
                applied: Some(payload),
            })
        }
        (Some(_), None) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::MissingPayloadEncodingWithData,
        }),
        (None, Some(_)) => Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PayloadEncodingWithoutData,
        }),
    }
}

fn decode_sealed(
    data: Option<Vec<u8>>,
    payload_encoding: Option<i16>,
    wal_event: Option<RawEventRef>,
    wal_ops: Option<Vec<u8>>,
    wal_format: Option<i16>,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    let (Some(raw_event), Some(ops_bytes), Some(format_raw)) = (wal_event, wal_ops, wal_format)
    else {
        // is_sealed() proved all three are Some; this branch is unreachable
        // but the match-let keeps the variables typed without unwrap.
        return Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PartialWalColumns {
                mask: WalColumnMask::sealed(),
            },
        });
    };
    // Validate the structurally-decoded UDT into a typed event. A corrupt
    // shape surfaces as `CorruptUdt` (Permanent → skip), not as a laundered
    // scylla `DeserializationError` (Terminal → shut down).
    let event = raw_event
        .try_into_event()
        .map_err(CassandraValueStoreError::CorruptUdt)?;
    let Some(encoding_raw) = payload_encoding else {
        return Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::WalWithoutPayloadEncoding,
        });
    };
    let format = WalFormat::try_from_i16(format_raw)?;
    let encoding = PayloadEncoding::try_from_i16(encoding_raw)?;

    let applied = match data {
        Some(bytes) => Some(decode_payload::<StoredPayload>(&bytes, encoding)?),
        None => None,
    };

    let wal = SealedWal::<ValueKind>::new(
        event,
        WalBlob::new(bytes::Bytes::from(ops_bytes), format),
        encoding,
    );
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

    /// All WAL columns are non-NULL — the row is sealed.
    #[must_use]
    pub fn sealed() -> Self {
        Self {
            event: true,
            ops: true,
            format: true,
        }
    }

    fn is_sealed(self) -> bool {
        self.event && self.ops && self.format
    }

    fn is_idle(self) -> bool {
        !self.event && !self.ops && !self.format
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

    /// WAL columns are partially populated (some NULL, some non-NULL).
    #[error("WAL columns are partially populated: {mask}")]
    PartialWalColumns {
        /// Which of the WAL columns were present.
        mask: WalColumnMask,
    },

    /// All WAL columns are populated but `payload_encoding` is NULL.
    #[error("sealed WAL row is missing payload_encoding")]
    WalWithoutPayloadEncoding,
}

#[cfg(test)]
mod tests;
