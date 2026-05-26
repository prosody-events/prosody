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
//! | Anything else                                        | `CorruptWal { reason }` |
//!
//! The intermediate `Option<...>` tuple [`RawValueRow`] is private to this
//! module; callers see only the three valid outcomes plus the typed
//! corruption reason.

use super::error::CassandraValueStoreError;
use crate::state::encoding::decode_payload;
use crate::state::value::ValueKind;
use crate::state::{
    DurableState, EventRef, PayloadEncoding, SealedWal, StoredPayload, WalBlob, WalFormat,
};
use std::fmt;
use thiserror::Error;

/// Five-column shape produced by `SELECT data, payload_encoding, wal_event,
/// wal_ops, wal_format` against `keyed_state_value`.
///
/// Module-private — callers never observe the intermediate tuple.
pub(super) type RawValueRow = (
    Option<Vec<u8>>,  // data
    Option<i16>,      // payload_encoding
    Option<EventRef>, // wal_event
    Option<Vec<u8>>,  // wal_ops
    Option<i16>,      // wal_format
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
    wal_event: Option<EventRef>,
    wal_ops: Option<Vec<u8>>,
    wal_format: Option<i16>,
) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
    let (Some(event), Some(ops_bytes), Some(format_raw)) = (wal_event, wal_ops, wal_format) else {
        // is_sealed() proved all three are Some; this branch is unreachable
        // but the match-let keeps the variables typed without unwrap.
        return Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PartialWalColumns {
                mask: WalColumnMask::sealed(),
            },
        });
    };
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
mod tests {
    use super::*;
    use crate::state::encoding::{PayloadEncoding, WalFormat, encode_payload};
    use bytes::Bytes;
    use color_eyre::eyre::{self, Result};
    use uuid::Uuid;

    fn message_event() -> EventRef {
        EventRef::Message {
            dedup_id: Uuid::from_u128(0x42),
        }
    }

    fn inline_payload(byte: u8) -> StoredPayload {
        StoredPayload::Inline(Bytes::from(vec![byte]))
    }

    fn encoded_payload(byte: u8) -> Result<Vec<u8>> {
        let bytes = encode_payload(&inline_payload(byte), PayloadEncoding::MsgpackZstdV1)?;
        Ok(bytes.to_vec())
    }

    #[test]
    fn decodes_idle_no_data() -> Result<()> {
        let state = try_decode_row((None, None, None, None, None))?;
        match state {
            DurableState::Idle { applied: None } => Ok(()),
            other => Err(eyre::eyre!("expected Idle empty, got {other:?}")),
        }
    }

    #[test]
    fn decodes_idle_with_data() -> Result<()> {
        let data = encoded_payload(7)?;
        let state = try_decode_row((
            Some(data),
            Some(PayloadEncoding::MsgpackZstdV1.as_i16()),
            None,
            None,
            None,
        ))?;
        match state {
            DurableState::Idle {
                applied: Some(payload),
            } => {
                assert_eq!(payload, inline_payload(7));
                Ok(())
            }
            other => Err(eyre::eyre!("expected Idle with payload, got {other:?}")),
        }
    }

    #[test]
    fn decodes_sealed_with_data_and_wal() -> Result<()> {
        let data = encoded_payload(9)?;
        // A minimal "wal_ops" placeholder; the decoder does not parse it
        // into ops, it only constructs a WalBlob.
        let wal_bytes = vec![0_u8, 1, 2, 3];
        let state = try_decode_row((
            Some(data),
            Some(PayloadEncoding::MsgpackZstdV1.as_i16()),
            Some(message_event()),
            Some(wal_bytes.clone()),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        ))?;
        match state {
            DurableState::Sealed {
                applied: Some(payload),
                wal,
            } => {
                assert_eq!(payload, inline_payload(9));
                assert_eq!(wal.event(), message_event());
                assert_eq!(wal.wal().bytes(), &Bytes::from(wal_bytes));
                assert_eq!(wal.wal().format(), WalFormat::MsgpackStreamZstdV1);
                assert_eq!(wal.payload_encoding(), PayloadEncoding::MsgpackZstdV1);
                Ok(())
            }
            other => Err(eyre::eyre!("expected Sealed, got {other:?}")),
        }
    }

    #[test]
    fn decodes_sealed_no_data() -> Result<()> {
        let wal_bytes = vec![1_u8, 2, 3];
        let state = try_decode_row((
            None,
            Some(PayloadEncoding::MsgpackZstdV1.as_i16()),
            Some(message_event()),
            Some(wal_bytes.clone()),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        ))?;
        match state {
            DurableState::Sealed { applied: None, wal } => {
                assert_eq!(wal.wal().bytes(), &Bytes::from(wal_bytes));
                Ok(())
            }
            other => Err(eyre::eyre!("expected Sealed no-data, got {other:?}")),
        }
    }

    #[test]
    fn rejects_data_without_payload_encoding() -> Result<()> {
        let result = try_decode_row((Some(vec![0_u8]), None, None, None, None));
        match result {
            Err(CassandraValueStoreError::CorruptWal {
                reason: CorruptReason::MissingPayloadEncodingWithData,
            }) => Ok(()),
            other => Err(eyre::eyre!(
                "expected MissingPayloadEncodingWithData, got {other:?}"
            )),
        }
    }

    #[test]
    fn rejects_payload_encoding_without_data() -> Result<()> {
        let result = try_decode_row((
            None,
            Some(PayloadEncoding::MsgpackZstdV1.as_i16()),
            None,
            None,
            None,
        ));
        match result {
            Err(CassandraValueStoreError::CorruptWal {
                reason: CorruptReason::PayloadEncodingWithoutData,
            }) => Ok(()),
            other => Err(eyre::eyre!(
                "expected PayloadEncodingWithoutData, got {other:?}"
            )),
        }
    }

    #[test]
    fn rejects_partial_wal_columns_event_only() -> Result<()> {
        let result = try_decode_row((None, None, Some(message_event()), None, None));
        match result {
            Err(CassandraValueStoreError::CorruptWal {
                reason: CorruptReason::PartialWalColumns { mask },
            }) => {
                assert_eq!(
                    mask,
                    WalColumnMask {
                        event: true,
                        ops: false,
                        format: false
                    }
                );
                Ok(())
            }
            other => Err(eyre::eyre!("expected PartialWalColumns, got {other:?}")),
        }
    }

    #[test]
    fn rejects_partial_wal_columns_ops_and_format_only() -> Result<()> {
        let result = try_decode_row((
            None,
            None,
            None,
            Some(vec![1_u8]),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        ));
        match result {
            Err(CassandraValueStoreError::CorruptWal {
                reason: CorruptReason::PartialWalColumns { mask },
            }) => {
                assert_eq!(
                    mask,
                    WalColumnMask {
                        event: false,
                        ops: true,
                        format: true
                    }
                );
                Ok(())
            }
            other => Err(eyre::eyre!("expected PartialWalColumns, got {other:?}")),
        }
    }

    #[test]
    fn rejects_sealed_without_payload_encoding() -> Result<()> {
        let result = try_decode_row((
            None,
            None,
            Some(message_event()),
            Some(vec![1_u8]),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        ));
        match result {
            Err(CassandraValueStoreError::CorruptWal {
                reason: CorruptReason::WalWithoutPayloadEncoding,
            }) => Ok(()),
            other => Err(eyre::eyre!(
                "expected WalWithoutPayloadEncoding, got {other:?}"
            )),
        }
    }
}
