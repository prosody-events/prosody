use super::*;
use crate::state::EventRef;
use crate::state::cassandra::error::CorruptUdtError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::descriptor_identity::INITIAL_IDENTITY_VERSION;
use crate::state::encoding::{PayloadEncoding, WalFormat, encode_payload};
use crate::state::tests::value_suite::bytes;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use uuid::Uuid;

fn message_event() -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(0x42),
    }
}

/// The on-wire UDT shape the `wal_event` column deserializes into, for a
/// well-formed Message event.
fn message_event_raw() -> RawEventRef {
    RawEventRef::from_event(message_event())
}

fn encoded_payload(byte: u8) -> Result<Vec<u8>> {
    let encoded = encode_payload(&bytes(byte), PayloadEncoding::RawZstdV1)?;
    Ok(encoded.to_vec())
}

/// Asserts a decode rejected the row as
/// [`CassandraValueStoreError::CorruptWal`] with exactly `expected`.
/// `CorruptReason` (and the `WalColumnMask` it carries) derive `PartialEq`, so
/// passing a full `PartialWalColumns { mask }` checks the mask too.
fn assert_corrupt_wal(
    result: Result<DurableState<ValueKind>, CassandraValueStoreError>,
    expected: CorruptReason,
) -> Result<()> {
    match result {
        Err(CassandraValueStoreError::CorruptWal { reason }) if reason == expected => Ok(()),
        other => Err(eyre::eyre!(
            "expected CorruptWal {expected:?}, got {other:?}"
        )),
    }
}

#[test]
fn decodes_idle_no_data() -> Result<()> {
    let state = try_decode_row((None, None, None, None, None, None))?;
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
        Some(PayloadEncoding::RawZstdV1.as_i16()),
        Some(INITIAL_IDENTITY_VERSION),
        None,
        None,
        None,
    ))?;
    match state {
        DurableState::Idle {
            applied: Some(payload),
        } => {
            assert_eq!(payload, bytes(7));
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
        Some(PayloadEncoding::RawZstdV1.as_i16()),
        Some(INITIAL_IDENTITY_VERSION),
        Some(message_event_raw()),
        Some(wal_bytes.clone()),
        Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
    ))?;
    match state {
        DurableState::Sealed {
            applied: Some(payload),
            wal,
        } => {
            assert_eq!(payload, bytes(9));
            assert_eq!(wal.event(), message_event());
            assert_eq!(wal.wal().bytes(), &Bytes::from(wal_bytes));
            assert_eq!(wal.wal().format(), WalFormat::MsgpackStreamZstdV1);
            assert_eq!(wal.payload_encoding(), PayloadEncoding::RawZstdV1);
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
        Some(PayloadEncoding::RawZstdV1.as_i16()),
        None,
        Some(message_event_raw()),
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
    assert_corrupt_wal(
        try_decode_row((
            Some(vec![0_u8]),
            None,
            Some(INITIAL_IDENTITY_VERSION),
            None,
            None,
            None,
        )),
        CorruptReason::MissingPayloadEncodingWithData,
    )
}

#[test]
fn rejects_payload_encoding_without_data() -> Result<()> {
    assert_corrupt_wal(
        try_decode_row((
            None,
            Some(PayloadEncoding::RawZstdV1.as_i16()),
            None,
            None,
            None,
            None,
        )),
        CorruptReason::PayloadEncodingWithoutData,
    )
}

#[test]
fn rejects_partial_wal_columns_event_only() -> Result<()> {
    assert_corrupt_wal(
        try_decode_row((None, None, None, Some(message_event_raw()), None, None)),
        CorruptReason::PartialWalColumns {
            mask: WalColumnMask {
                event: true,
                ops: false,
                format: false,
            },
        },
    )
}

#[test]
fn rejects_partial_wal_columns_ops_and_format_only() -> Result<()> {
    assert_corrupt_wal(
        try_decode_row((
            None,
            None,
            None,
            None,
            Some(vec![1_u8]),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        )),
        CorruptReason::PartialWalColumns {
            mask: WalColumnMask {
                event: false,
                ops: true,
                format: true,
            },
        },
    )
}

#[test]
fn rejects_sealed_without_payload_encoding() -> Result<()> {
    assert_corrupt_wal(
        try_decode_row((
            None,
            None,
            None,
            Some(message_event_raw()),
            Some(vec![1_u8]),
            Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
        )),
        CorruptReason::WalWithoutPayloadEncoding,
    )
}

/// B3: a structurally-valid but semantically-corrupt `event_ref` UDT (here
/// `kind == 7`) on an otherwise well-formed sealed row must decode to a typed
/// [`CassandraValueStoreError::CorruptUdt`] classified `Permanent` (skip the
/// row), never the `Terminal` classification scylla's opaque
/// `DeserializationError` would have produced. The UDT deserializes fine; the
/// decoder's `try_into_event` post-step is what rejects it.
#[test]
fn rejects_corrupt_event_ref_udt_as_permanent() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};

    let corrupt = RawEventRef {
        kind: 7,
        msg_dedup_id: None,
        timer_type: None,
        time: None,
        tag: None,
    };
    let result = try_decode_row((
        None,
        Some(PayloadEncoding::RawZstdV1.as_i16()),
        None,
        Some(corrupt),
        Some(vec![1_u8]),
        Some(WalFormat::MsgpackStreamZstdV1.as_i16()),
    ));
    match result {
        Err(error @ CassandraValueStoreError::CorruptUdt(CorruptUdtError::UnknownKind(7))) => {
            assert_eq!(
                error.classify_error(),
                ErrorCategory::Permanent,
                "corrupt UDT must classify Permanent (skip), not Terminal"
            );
            Ok(())
        }
        other => Err(eyre::eyre!(
            "expected CorruptUdt(UnknownKind(7)), got {other:?}"
        )),
    }
}

/// `data` without an `identity_version` stamp is a corrupt pairing — every
/// authoritative cell records the identity version it was written under.
#[test]
fn rejects_data_without_identity_version() -> Result<()> {
    let data = encoded_payload(3)?;
    assert_corrupt_wal(
        try_decode_row((
            Some(data),
            Some(PayloadEncoding::RawZstdV1.as_i16()),
            None,
            None,
            None,
            None,
        )),
        CorruptReason::MissingIdentityVersionWithData,
    )
}

/// An `identity_version` stamp without `data` is the inverse corrupt
/// pairing.
#[test]
fn rejects_identity_version_without_data() -> Result<()> {
    assert_corrupt_wal(
        try_decode_row((None, None, Some(INITIAL_IDENTITY_VERSION), None, None, None)),
        CorruptReason::IdentityVersionWithoutData,
    )
}

/// A stamp other than [`INITIAL_IDENTITY_VERSION`] is rejected Permanent —
/// unreachable until identity migration ships, enforced defensively so a
/// future-version cell is never misread by this build.
#[test]
fn rejects_unrecognized_identity_version_as_permanent() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};

    let data = encoded_payload(5)?;
    let result = try_decode_row((
        Some(data),
        Some(PayloadEncoding::RawZstdV1.as_i16()),
        Some(2_i32),
        None,
        None,
        None,
    ));
    match result {
        Err(
            error @ CassandraValueStoreError::IdentityVersionMismatch {
                stored: 2,
                expected: INITIAL_IDENTITY_VERSION,
            },
        ) => {
            assert_eq!(
                error.classify_error(),
                ErrorCategory::Permanent,
                "identity version mismatch must classify Permanent"
            );
            Ok(())
        }
        other => Err(eyre::eyre!(
            "expected IdentityVersionMismatch, got {other:?}"
        )),
    }
}
