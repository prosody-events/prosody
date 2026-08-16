//! Pure unit tests for the cell-row shape table.
//!
//! [`try_decode_cell`] is a pure function over the [`RawCellRow`] tuple, so
//! every shape — including the legacy null-null residue and the
//! corruption arms — is checked here without a cluster. This is the cheap guard
//! against the shape-table regressions a live-Cassandra run would otherwise be
//! the first to catch.

use super::super::encoding::{
    CASSANDRA_COMPRESSION_BLOCK_BYTES, Encoding, EncodingError, decode_payload, decode_scratch,
    encode_payload, reset_codec, select_encoding, validate_decompression_bound,
};
use super::{
    CellCorruptReason, FramedKeyedCellRow, RawCellRow, blob_ttl, try_decode_cell,
    try_decode_keyed_cell,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::EventRef;
use crate::state::cassandra::cell::INITIAL_VERSION;
use crate::state::cassandra::error::CassandraCellStoreError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Cell, Committed, ProvisionalCell};
use bytes::Bytes;
use color_eyre::eyre::{Result, bail};
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;
use zstd::bulk::compress;
use zstd::stream::encode_all;
use zstd::zstd_safe::get_frame_content_size;

struct FrameOwner {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for FrameOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for FrameOwner {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}

/// The shared encoding discriminator a present blob carries.
fn enc() -> i16 {
    i16::from(Encoding::Zstd)
}

/// The version stamp paired with present bytes.
fn ver() -> i32 {
    INITIAL_VERSION
}

/// Encodes a payload exactly as the cell store would, so the decoder's
/// `decode_payload` round-trips it.
fn blob(s: &str) -> Result<Vec<u8>> {
    Ok(encode_payload(&Bytes::copy_from_slice(s.as_bytes()), Encoding::Zstd)?.to_vec())
}

fn message_event() -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(0xABCD),
    }
}

fn raw_event() -> RawEventRef {
    RawEventRef::from_event(message_event())
}

/// `event` NULL, present `data`, no `prev_data` → committed value.
#[test]
fn resolved_present() -> Result<()> {
    let row: RawCellRow = (Some(blob("v")?), None, Some(enc()), Some(ver()), None);
    let cell = try_decode_cell(row)?;
    assert_eq!(
        cell,
        Cell::Resolved(Committed::new(Some(Bytes::from_static(b"v"))))
    );
    Ok(())
}

/// The legacy null-null residue: `data`/`prev_data` both NULL but
/// `encoding`/`version` still populated → `Resolved(None)`, NOT corruption.
/// No current statement produces this shape (a committed-absent cell deletes
/// its row); the decoder tolerates it for rows written by earlier builds.
#[test]
fn resolved_clear_residue_is_not_corrupt() -> Result<()> {
    let row: RawCellRow = (None, None, Some(enc()), Some(ver()), None);
    let cell = try_decode_cell(row)?;
    assert_eq!(cell, Cell::Resolved(Committed::new(None)));
    Ok(())
}

/// A fully-null row decodes to known-absent.
#[test]
fn resolved_all_null() -> Result<()> {
    let row: RawCellRow = (None, None, None, None, None);
    assert_eq!(try_decode_cell(row)?, Cell::Resolved(Committed::new(None)));
    Ok(())
}

/// `event` non-NULL, set-over-absent.
#[test]
fn provisional_set_over_absent() -> Result<()> {
    let row: RawCellRow = (
        Some(blob("new")?),
        None,
        Some(enc()),
        Some(ver()),
        Some(raw_event()),
    );
    let cell = try_decode_cell(row)?;
    assert_eq!(
        cell,
        Cell::Provisional(ProvisionalCell::new(
            Some(Bytes::from_static(b"new")),
            None,
            message_event()
        ))
    );
    Ok(())
}

/// `event` non-NULL, clear-over-absent: every blob NULL but an event in
/// flight → `Provisional { data: None, prev: None, event }` (a clear staged
/// over a never-set cell).
#[test]
fn provisional_clear_over_absent() -> Result<()> {
    let row: RawCellRow = (None, None, None, None, Some(raw_event()));
    let cell = try_decode_cell(row)?;
    assert_eq!(
        cell,
        Cell::Provisional(ProvisionalCell::new(None, None, message_event()))
    );
    Ok(())
}

/// `event` non-NULL, clear-over-present: `data` NULL, `prev_data` present.
/// The shared encoding describes `prev_data`.
#[test]
fn provisional_clear_over_present() -> Result<()> {
    let row: RawCellRow = (
        None,
        Some(blob("old")?),
        Some(enc()),
        Some(ver()),
        Some(raw_event()),
    );
    let cell = try_decode_cell(row)?;
    assert_eq!(
        cell,
        Cell::Provisional(ProvisionalCell::new(
            None,
            Some(Bytes::from_static(b"old")),
            message_event()
        ))
    );
    Ok(())
}

/// `event` non-NULL, set-over-present: both blobs share the encoding.
#[test]
fn provisional_set_over_present() -> Result<()> {
    let row: RawCellRow = (
        Some(blob("new")?),
        Some(blob("old")?),
        Some(enc()),
        Some(ver()),
        Some(raw_event()),
    );
    let cell = try_decode_cell(row)?;
    assert_eq!(
        cell,
        Cell::Provisional(ProvisionalCell::new(
            Some(Bytes::from_static(b"new")),
            Some(Bytes::from_static(b"old")),
            message_event()
        ))
    );
    Ok(())
}

/// `event` NULL with a non-NULL `prev_data` is corrupt — no statement writes
/// this shape.
#[test]
fn prev_without_event_is_corrupt() -> Result<()> {
    let row: RawCellRow = (None, Some(blob("old")?), Some(enc()), Some(ver()), None);
    assert!(matches!(
        try_decode_cell(row),
        Err(CassandraCellStoreError::CorruptCell(
            CellCorruptReason::PrevWithoutEvent
        ))
    ));
    Ok(())
}

/// A present blob with a NULL shared encoding is corrupt (undecodable).
#[test]
fn blob_without_encoding_is_corrupt() -> Result<()> {
    let row: RawCellRow = (Some(blob("v")?), None, None, Some(ver()), None);
    assert!(matches!(
        try_decode_cell(row),
        Err(CassandraCellStoreError::CorruptCell(
            CellCorruptReason::BlobWithoutEncoding
        ))
    ));
    Ok(())
}

/// An unknown `version` stamp is rejected Permanent.
#[test]
fn unknown_version_is_rejected() -> Result<()> {
    let row: RawCellRow = (Some(blob("v")?), None, Some(enc()), Some(2_i32), None);
    assert!(matches!(
        try_decode_cell(row),
        Err(CassandraCellStoreError::VersionMismatch {
            stored: 2_i32,
            expected: INITIAL_VERSION
        })
    ));
    Ok(())
}

/// A semantically-corrupt `event` UDT classifies as a corrupt-UDT error
/// (Permanent), not a structural cell-shape error.
#[test]
fn corrupt_event_udt_is_rejected() -> Result<()> {
    let bad = RawEventRef {
        kind: 7_i8,
        msg_dedup_id: None,
        timer_type: None,
        time: None,
        tag: None,
    };
    let row: RawCellRow = (Some(blob("v")?), None, Some(enc()), Some(ver()), Some(bad));
    assert!(matches!(
        try_decode_cell(row),
        Err(CassandraCellStoreError::CorruptUdt(_))
    ));
    Ok(())
}

/// Wire-format freeze for the payload-encoding discriminants.
///
/// Zstd value 4 is part of the released durable format. Raw value 1 is the
/// first new format. Value 0 stays invalid so missing data fails loudly.
#[test]
fn encoding_wire_contract_is_frozen() -> Result<()> {
    assert_eq!(i16::from(Encoding::Zstd), 4);
    assert_eq!(i16::from(Encoding::Raw), 1);
    for unknown in [0_i16, 2, 3, 5] {
        let Err(error) = Encoding::try_from(unknown) else {
            bail!("discriminant {unknown} must stay unknown");
        };
        assert!(matches!(error, EncodingError::UnknownEncoding(value) if value == unknown));
        assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    }
    Ok(())
}

#[test]
fn encoding_selection_uses_the_strict_block_boundary() {
    assert_eq!(
        select_encoding(CASSANDRA_COMPRESSION_BLOCK_BYTES - 1),
        Encoding::Raw
    );
    assert_eq!(
        select_encoding(CASSANDRA_COMPRESSION_BLOCK_BYTES),
        Encoding::Raw
    );
    assert_eq!(
        select_encoding(CASSANDRA_COMPRESSION_BLOCK_BYTES + 1),
        Encoding::Zstd
    );
}

#[test]
fn concatenated_zstd_frames_decode() -> Result<()> {
    let first = compress(b"first", 0)?;
    let second = compress(b"second", 0)?;
    let mut frames = Vec::with_capacity(first.len() + second.len());
    frames.extend_from_slice(&first);
    frames.extend_from_slice(&second);

    assert_eq!(
        decode_payload(&frames, Encoding::Zstd)?,
        Bytes::from_static(b"firstsecond")
    );
    Ok(())
}

#[test]
fn declared_size_cannot_exceed_the_block_expansion_bound() {
    assert!(validate_decompression_bound(19, u64::MAX).is_err());
}

/// Payload round-trip over arbitrary bytes:
/// `decode_payload(encode_payload(b)) == b` — the property that proves the
/// zstd leg the shape-table examples only touch implicitly.
#[test]
fn prop_payload_encoding_round_trips() {
    fn prop(bytes: Vec<u8>) -> TestResult {
        let payload = Bytes::from(bytes);
        for encoding in [Encoding::Zstd, Encoding::Raw] {
            match encode_payload(&payload, encoding)
                .and_then(|encoded| decode_payload(&encoded, encoding))
            {
                Ok(decoded) if decoded == payload => {}
                Ok(_) => return TestResult::failed(),
                Err(error) => return TestResult::error(format!("{error}")),
            }
        }
        TestResult::passed()
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// The legacy writer used the stream encoder, which omitted the content size.
/// The current reader must decode every frame that writer produced.
#[test]
fn prop_legacy_zstd_frames_decode() {
    fn prop(bytes: Vec<u8>) -> TestResult {
        let source = Bytes::from(bytes);
        let encoded = match encode_all(source.as_ref(), 0) {
            Ok(encoded) => encoded,
            Err(error) => return TestResult::error(format!("{error}")),
        };
        match decode_payload(&encoded, Encoding::Zstd) {
            Ok(decoded) => TestResult::from_bool(decoded == source),
            Err(error) => TestResult::error(format!("{error}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

#[test]
fn failed_stream_decode_does_not_poison_the_next_frame() -> Result<()> {
    reset_codec();
    let valid = encode_all(b"valid after corrupt".as_slice(), 0)?;
    assert!(matches!(get_frame_content_size(&valid), Ok(None)));
    let mut truncated = valid.clone();
    truncated.truncate(truncated.len().saturating_sub(1));
    assert!(decode_payload(&truncated, Encoding::Zstd).is_err());
    assert_eq!(
        decode_payload(&valid, Encoding::Zstd)?,
        Bytes::from_static(b"valid after corrupt")
    );
    Ok(())
}

#[test]
fn durable_payload_bytes_are_frozen() -> Result<()> {
    const LEGACY_ZSTD: &[u8] = &[
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x31, 0x00, 0x00, 0x6c, 0x65, 0x67, 0x61, 0x63, 0x79,
        0x3c, 0x1f, 0x36, 0x87,
    ];
    assert_eq!(
        decode_payload(LEGACY_ZSTD, Encoding::Zstd)?,
        Bytes::from_static(b"legacy")
    );
    assert_eq!(
        encode_payload(&Bytes::from_static(b"raw"), Encoding::Raw)?.as_ref(),
        b"raw"
    );
    Ok(())
}

#[test]
fn decoded_cell_does_not_retain_its_response_frame() -> Result<()> {
    let dropped = Arc::new(AtomicBool::new(false));
    let frame = Bytes::from_owner(FrameOwner {
        bytes: b"prefixrawsuffix".to_vec(),
        dropped: dropped.clone(),
    });
    let row: FramedKeyedCellRow = (
        0,
        vec![1],
        Some(frame.slice(6..9)),
        None,
        Some(i16::from(Encoding::Raw)),
        Some(INITIAL_VERSION),
        None,
    );
    let (_, cell) = try_decode_keyed_cell(row)?;
    drop(frame);

    assert!(dropped.load(Ordering::Relaxed));
    assert_eq!(
        cell,
        Cell::Resolved(Committed::new(Some(Bytes::from_static(b"raw"))))
    );
    Ok(())
}

#[test]
fn decode_scratch_grows_once_and_then_stays_stable() -> Result<()> {
    reset_codec();
    let maximum = Bytes::from(vec![0x3C; 64 * 1024]);
    let minimum = Bytes::from_static(b"small");
    let encoded_maximum = encode_payload(&maximum, Encoding::Zstd)?;
    let encoded_minimum = encode_payload(&minimum, Encoding::Zstd)?;

    assert_eq!(decode_scratch(), (0, 0));
    assert_eq!(decode_payload(&encoded_maximum, Encoding::Zstd)?, maximum);
    let warmed = decode_scratch();
    assert!(warmed.1 >= maximum.len());

    for encoded in [&encoded_minimum, &encoded_maximum, &encoded_minimum] {
        drop(decode_payload(encoded, Encoding::Zstd)?);
        assert_eq!(decode_scratch(), warmed);
    }
    Ok(())
}

/// The cache-fill co-expiry coalesces whichever blob's TTL is present, `data`
/// first — the invariant whose regression ("TTL(data) alone") stamped a
/// staged clear's cache entry never-expires.
#[test]
fn blob_ttl_coalesces_the_present_blobs_ttl() {
    assert_eq!(blob_ttl(Some(5_i32), Some(9_i32)), Some(5_i32));
    assert_eq!(blob_ttl(None, Some(9_i32)), Some(9_i32));
    assert_eq!(blob_ttl(None, None), None);
}
