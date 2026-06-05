use super::super::encoding::{
    EncodableOp, EncodingError, PayloadEncoding, WalFormat, decode_payload, decode_wal,
    encode_payload, encode_wal, raw_wal_blob_for_test,
};
use super::super::value::{ValueKind, ValueOp};
use super::super::{CollectionKind, CollectionKindId, NonEmptyOps, WalBlob, WalEnvelope};
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde::Serialize;
use std::num::NonZeroU64;

/// A hand-rolled WAL header, serialized to craft inputs the production
/// encoder would never emit (a zero `op_count`, an unsupported `version`).
/// `op_count` is a plain `u64` here — not the production `NonZeroU64` — so a
/// crafted zero survives serialization and is rejected only at decode time.
#[derive(Serialize)]
struct CraftedHeader {
    version: u16,
    kind: CollectionKindId,
    op_count: u64,
}

#[derive(Clone, Copy, Debug)]
struct SecondaryKind;

impl CollectionKind for SecondaryKind {
    type Applied = ();
    type Op = ValueOp;
    type Overlay = ();

    const ID: CollectionKindId = CollectionKindId::TestSecondary;
}

#[derive(Clone, Copy, Debug)]
struct ArbPayloadEncoding(PayloadEncoding);

impl Arbitrary for ArbPayloadEncoding {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(if bool::arbitrary(g) {
            PayloadEncoding::RawV1
        } else {
            PayloadEncoding::RawZstdV1
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ArbWalFormat(WalFormat);

impl Arbitrary for ArbWalFormat {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(if bool::arbitrary(g) {
            WalFormat::MsgpackStreamV1
        } else {
            WalFormat::MsgpackStreamZstdV1
        })
    }
}

#[derive(Clone, Debug)]
struct ArbValueOp(ValueOp);

impl Arbitrary for ArbValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self(ValueOp::Set {
                payload: Bytes::from(Vec::<u8>::arbitrary(g)),
            })
        } else {
            Self(ValueOp::Clear)
        }
    }
}

#[derive(Clone, Debug)]
struct ArbValueEnvelope(WalEnvelope<ValueKind>);

impl Arbitrary for ArbValueEnvelope {
    fn arbitrary(g: &mut Gen) -> Self {
        let first = ArbValueOp::arbitrary(g).0;
        let tail: Vec<ValueOp> = Vec::<ArbValueOp>::arbitrary(g)
            .into_iter()
            .map(|op| op.0)
            .collect();
        Self(WalEnvelope::new(NonEmptyOps::new(first, tail)))
    }
}

#[test]
fn prop_payload_roundtrip() {
    fn property(payload: Vec<u8>, encoding: ArbPayloadEncoding) -> bool {
        let payload = Bytes::from(payload);
        let Ok(encoded) = encode_payload(&payload, encoding.0) else {
            return false;
        };
        let Ok(decoded) = decode_payload(&encoded, encoding.0) else {
            return false;
        };
        decoded == payload
    }

    QuickCheck::new().quickcheck(property as fn(Vec<u8>, ArbPayloadEncoding) -> bool);
}

#[test]
fn prop_wal_roundtrip() {
    fn property(envelope: ArbValueEnvelope, format: ArbWalFormat) -> bool {
        let ArbValueEnvelope(envelope) = envelope;
        let Ok(blob) = encode_wal::<ValueKind>(&envelope, format.0) else {
            return false;
        };
        if blob.format() != format.0 {
            return false;
        }
        let Ok(decoded) = decode_wal::<ValueKind>(&blob) else {
            return false;
        };
        decoded == envelope
    }

    QuickCheck::new().quickcheck(property as fn(ArbValueEnvelope, ArbWalFormat) -> bool);
}

#[test]
fn prop_unknown_payload_encoding() {
    fn property(value: i16) -> bool {
        match value {
            3 | 4 => PayloadEncoding::try_from_i16(value).is_ok(),
            // Includes the retired MsgPack discriminants 1/2: stale cells
            // fail loudly as UnknownPayloadEncoding (Permanent).
            other => matches!(
                PayloadEncoding::try_from_i16(value),
                Err(EncodingError::UnknownPayloadEncoding(got)) if got == other
            ),
        }
    }

    QuickCheck::new().quickcheck(property as fn(i16) -> bool);
}

#[test]
fn prop_unknown_wal_format() {
    fn property(value: i16) -> bool {
        match value {
            1 | 2 => WalFormat::try_from_i16(value).is_ok(),
            other => matches!(
                WalFormat::try_from_i16(value),
                Err(EncodingError::UnknownWalFormat(got)) if got == other
            ),
        }
    }

    QuickCheck::new().quickcheck(property as fn(i16) -> bool);
}

#[test]
fn wal_kind_mismatch_returns_kind_mismatch() -> Result<()> {
    let envelope = WalEnvelope::<ValueKind>::try_from_ops(vec![ValueOp::Clear])?;
    let blob = encode_wal::<ValueKind>(&envelope, WalFormat::MsgpackStreamV1)?;

    let misrouted: WalBlob<SecondaryKind> = WalBlob::new(blob.bytes().clone(), blob.format());

    match decode_wal::<SecondaryKind>(&misrouted) {
        Err(EncodingError::KindMismatch { header, expected }) => {
            assert_eq!(header, CollectionKindId::Value);
            assert_eq!(expected, CollectionKindId::TestSecondary);
            Ok(())
        }
        Ok(_) => Err(eyre::eyre!("decode_wal accepted mismatched kind")),
        Err(other) => Err(eyre::eyre!("unexpected error: {other:?}")),
    }
}

#[test]
fn prop_wal_trailing_bytes() {
    fn property(envelope: ArbValueEnvelope) -> bool {
        let ArbValueEnvelope(envelope) = envelope;
        let Ok(blob) = encode_wal::<ValueKind>(&envelope, WalFormat::MsgpackStreamV1) else {
            return false;
        };
        let mut bytes: Vec<u8> = blob.bytes().to_vec();
        bytes.push(0xff_u8);
        let corrupted: WalBlob<ValueKind> =
            WalBlob::new(Bytes::from(bytes), WalFormat::MsgpackStreamV1);
        matches!(
            decode_wal::<ValueKind>(&corrupted),
            Err(EncodingError::TrailingBytes)
        )
    }

    QuickCheck::new().quickcheck(property as fn(ArbValueEnvelope) -> bool);
}

#[test]
fn prop_wal_truncated_bytes_fail_decode() {
    fn property(envelope: ArbValueEnvelope) -> bool {
        let ArbValueEnvelope(envelope) = envelope;
        let Ok(blob) = encode_wal::<ValueKind>(&envelope, WalFormat::MsgpackStreamV1) else {
            return false;
        };
        let raw: &[u8] = blob.bytes().as_ref();
        if raw.len() <= 1 {
            return true;
        }
        let truncated = Bytes::copy_from_slice(&raw[..raw.len() - 1]);
        let corrupted: WalBlob<ValueKind> = WalBlob::new(truncated, WalFormat::MsgpackStreamV1);
        matches!(
            decode_wal::<ValueKind>(&corrupted),
            Err(EncodingError::BadMsgPack(_))
        )
    }

    QuickCheck::new().quickcheck(property as fn(ArbValueEnvelope) -> bool);
}

#[test]
fn wal_empty_stream_rejected_by_msgpack() -> Result<()> {
    let header = CraftedHeader {
        version: 1,
        kind: CollectionKindId::Value,
        op_count: 0,
    };
    let bytes = rmp_serde::to_vec_named(&header)?;
    let blob: WalBlob<ValueKind> = WalBlob::new(Bytes::from(bytes), WalFormat::MsgpackStreamV1);

    match decode_wal::<ValueKind>(&blob) {
        Err(EncodingError::BadMsgPack(_)) => Ok(()),
        other => Err(eyre::eyre!(
            "expected BadMsgPack from NonZeroU64 deserialize rejecting zero, got {other:?}"
        )),
    }
}

#[test]
fn wal_unsupported_header_version_is_rejected() -> Result<()> {
    let header = CraftedHeader {
        version: 2,
        kind: CollectionKindId::Value,
        op_count: 1,
    };
    let mut bytes = rmp_serde::to_vec_named(&header)?;
    bytes.extend(rmp_serde::to_vec_named(&ValueOp::Clear)?);
    let blob: WalBlob<ValueKind> = WalBlob::new(Bytes::from(bytes), WalFormat::MsgpackStreamV1);

    match decode_wal::<ValueKind>(&blob) {
        Err(EncodingError::UnsupportedWalHeaderVersion { header, expected }) => {
            assert_eq!(header, 2);
            assert_eq!(expected, 1);
            Ok(())
        }
        other => Err(eyre::eyre!(
            "expected UnsupportedWalHeaderVersion, got {other:?}"
        )),
    }
}

#[test]
fn zstd_payload_at_most_plain_size_on_compressible_input() -> Result<()> {
    let payload = Bytes::from(vec![0_u8; 4096]);
    let raw = encode_payload(&payload, PayloadEncoding::RawV1)?;
    let compressed = encode_payload(&payload, PayloadEncoding::RawZstdV1)?;
    assert!(
        compressed.len() <= raw.len(),
        "expected compressed payload to be at most plain size: {} vs {}",
        compressed.len(),
        raw.len()
    );
    Ok(())
}

/// N9 (WAL path): an old-shape `Set` op whose `payload` was the retired
/// adjacently-tagged `StoredPayload` enum (a `MsgPack` map) must fail
/// loudly against the new `ValueOp` (whose `payload` is raw bytes) —
/// `BadMsgPack` ⇒ Permanent, never a silent mis-decode.
///
/// The *cell* path has no analogous loud decode failure: a raw-`Bytes`
/// cell accepts any tail. Stale cells are instead rejected by the
/// durable descriptor-identity acquisition
/// (`run_descriptor_identity_acquisition` Invariant 2) and by the
/// retired `PayloadEncoding` discriminants 1/2 failing
/// `UnknownPayloadEncoding` (see `prop_unknown_payload_encoding`).
#[test]
fn stale_enum_wal_ops_fail_permanent() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};

    #[derive(Serialize)]
    #[serde(tag = "v", content = "d", rename_all = "snake_case")]
    enum LegacyStoredPayload {
        Inline(Bytes),
    }

    #[derive(Serialize)]
    #[serde(tag = "op", rename_all = "snake_case")]
    enum LegacyValueOp {
        Set { payload: LegacyStoredPayload },
    }

    let header = CraftedHeader {
        version: 1,
        kind: CollectionKindId::Value,
        op_count: 1,
    };
    let mut raw = rmp_serde::to_vec_named(&header)?;
    raw.extend(rmp_serde::to_vec_named(&LegacyValueOp::Set {
        payload: LegacyStoredPayload::Inline(Bytes::from_static(b"stale")),
    })?);
    let blob: WalBlob<ValueKind> = WalBlob::new(Bytes::from(raw), WalFormat::MsgpackStreamV1);

    match decode_wal::<ValueKind>(&blob) {
        Err(error @ EncodingError::BadMsgPack(_)) => {
            assert_eq!(
                error.classify_error(),
                ErrorCategory::Permanent,
                "stale WAL bytes must classify Permanent"
            );
            Ok(())
        }
        other => Err(eyre::eyre!("expected BadMsgPack, got {other:?}")),
    }
}

#[test]
fn encodable_op_is_implemented_for_value_op() {
    fn assert_encodable<T: EncodableOp>() {}
    assert_encodable::<ValueOp>();
}

/// A WAL header declaring `op_count` ops over `tail` raw bytes. The
/// `op_count` is drawn from a pool biased toward boundary values so the
/// generator reliably hits `u64::MAX` — the value that reproduced the B2
/// capacity-overflow panic.
#[derive(Clone, Debug)]
struct ArbRawWal {
    op_count: u64,
    tail: Vec<u8>,
}

impl Arbitrary for ArbRawWal {
    fn arbitrary(g: &mut Gen) -> Self {
        const COUNT_POOL: &[u64] = &[
            1,
            2,
            7,
            256,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        let op_count = *g.choose(COUNT_POOL).unwrap_or(&1);
        Self {
            op_count,
            tail: Vec::<u8>::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let op_count = self.op_count;
        Box::new(self.tail.shrink().map(move |tail| Self { op_count, tail }))
    }
}

/// F3: the WAL decoder must never panic or abort on an untrusted header,
/// regardless of `op_count`. An `op_count` larger than the bytes remaining
/// after the header is provably corrupt (each op frame is at least one byte),
/// so it must return the typed [`EncodingError::CorruptWal`] rather than
/// driving an unbounded `Vec::with_capacity`. Any other input must still
/// yield a `Result` (a typed error or a decode), never a panic — quickcheck
/// turns a panic into a test failure. Iteration count comes from
/// `QUICKCHECK_TESTS`.
#[test]
fn prop_decode_wal_rejects_untrusted_op_count_without_panicking() {
    fn property(input: ArbRawWal) -> bool {
        let ArbRawWal { op_count, tail } = input;
        let Some(count) = NonZeroU64::new(op_count) else {
            return true;
        };
        let Ok(blob) = raw_wal_blob_for_test::<ValueKind>(count, &tail) else {
            return true;
        };

        let result = decode_wal::<ValueKind>(&blob);
        if op_count > tail.len() as u64 {
            // Strictly more op frames claimed than bytes available after the
            // header: must be rejected as corrupt, never preallocated.
            matches!(result, Err(EncodingError::CorruptWal))
        } else {
            // Reaching here without a panic is the property; the tail is
            // arbitrary, so a typed decode error is expected and fine.
            true
        }
    }

    QuickCheck::new().quickcheck(property as fn(ArbRawWal) -> bool);
}
