use super::encoding::{
    EncodableOp, EncodingError, PayloadEncoding, WalFormat, decode_payload, decode_wal,
    encode_payload, encode_wal,
};
use super::value::{KafkaMessageRef, StoredPayload, ValueKind, ValueOp};
use super::{CollectionKind, CollectionKindId, NonEmptyOps, WalBlob, WalEnvelope};
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde::{Deserialize, Serialize};

const TOPIC_POOL: &[&str] = &[
    "orders.v1",
    "billing.events",
    "telemetry",
    "shipments.outbound",
];

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
            PayloadEncoding::MsgpackV1
        } else {
            PayloadEncoding::MsgpackZstdV1
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
struct ArbKafkaMessageRef(KafkaMessageRef);

impl Arbitrary for ArbKafkaMessageRef {
    fn arbitrary(g: &mut Gen) -> Self {
        let topic_name = g.choose(TOPIC_POOL).copied().unwrap_or(TOPIC_POOL[0]);
        Self(KafkaMessageRef {
            topic: crate::Topic::from(topic_name),
            partition: i32::arbitrary(g),
            offset: i64::arbitrary(g),
        })
    }
}

#[derive(Clone, Debug)]
struct ArbStoredPayload(StoredPayload);

impl Arbitrary for ArbStoredPayload {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            let bytes = Vec::<u8>::arbitrary(g);
            Self(StoredPayload::Inline(Bytes::from(bytes)))
        } else {
            Self(StoredPayload::KafkaMessage(
                ArbKafkaMessageRef::arbitrary(g).0,
            ))
        }
    }
}

#[derive(Clone, Debug)]
struct ArbValueOp(ValueOp);

impl Arbitrary for ArbValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self(ValueOp::Set {
                payload: ArbStoredPayload::arbitrary(g).0,
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
    fn property(payload: ArbStoredPayload, encoding: ArbPayloadEncoding) -> bool {
        let ArbStoredPayload(payload) = payload;
        let Ok(bytes) = encode_payload(&payload, encoding.0) else {
            return false;
        };
        let Ok(decoded) = decode_payload::<StoredPayload>(&bytes, encoding.0) else {
            return false;
        };
        decoded == payload
    }

    QuickCheck::new().quickcheck(property as fn(ArbStoredPayload, ArbPayloadEncoding) -> bool);
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
            1 | 2 => PayloadEncoding::try_from_i16(value).is_ok(),
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
    #[derive(Serialize, Deserialize)]
    struct CraftedHeader {
        version: u16,
        kind: CollectionKindId,
        op_count: u64,
    }

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
fn zstd_payload_at_most_plain_size_on_compressible_input() -> Result<()> {
    let inline = StoredPayload::Inline(Bytes::from(vec![0_u8; 4096]));
    let raw = encode_payload(&inline, PayloadEncoding::MsgpackV1)?;
    let compressed = encode_payload(&inline, PayloadEncoding::MsgpackZstdV1)?;
    assert!(
        compressed.len() <= raw.len(),
        "expected compressed payload to be at most plain size: {} vs {}",
        compressed.len(),
        raw.len()
    );
    Ok(())
}

#[test]
fn encodable_op_is_implemented_for_value_op() {
    fn assert_encodable<T: EncodableOp>() {}
    assert_encodable::<ValueOp>();
}
