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
