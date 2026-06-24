//! Pure unit tests for the cell-row shape table.
//!
//! [`try_decode_cell`] is a pure function over the [`RawCellRow`] tuple, so
//! every shape — including the promote-of-clear residue and the corruption
//! arms — is checked here without a cluster. This is the cheap guard against
//! the shape-table regressions a live-Cassandra run would otherwise be the
//! first to catch.

use super::{CellCorruptReason, RawCellRow, try_decode_cell};
use crate::state::EventRef;
use crate::state::cassandra::cell::INITIAL_VERSION;
use crate::state::cassandra::error::CassandraCellStoreError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::cell::{Cell, Committed, ProvisionalCell};
use crate::state::encoding::{Encoding, encode_payload};
use bytes::Bytes;
use color_eyre::eyre::Result;
use uuid::Uuid;

/// The shared encoding discriminator a present blob carries.
fn enc() -> i16 {
    i16::from(Encoding::RawZstdV1)
}

/// The version stamp paired with present bytes.
fn ver() -> i32 {
    INITIAL_VERSION
}

/// Encodes a payload exactly as the cell store would, so the decoder's
/// `decode_payload` round-trips it.
fn blob(s: &str) -> Result<Vec<u8>> {
    Ok(encode_payload(&Bytes::copy_from_slice(s.as_bytes()), Encoding::RawZstdV1)?.to_vec())
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

/// The promote-of-clear residue: `data`/`prev_data` both NULL but
/// `encoding`/`version` still populated → `Resolved(None)`,
/// NOT corruption. This is the case the advisor flagged.
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
        Err(CassandraCellStoreError::CorruptCell {
            reason: CellCorruptReason::PrevWithoutEvent
        })
    ));
    Ok(())
}

/// A present blob with a NULL shared encoding is corrupt (undecodable).
#[test]
fn blob_without_encoding_is_corrupt() -> Result<()> {
    let row: RawCellRow = (Some(blob("v")?), None, None, Some(ver()), None);
    assert!(matches!(
        try_decode_cell(row),
        Err(CassandraCellStoreError::CorruptCell {
            reason: CellCorruptReason::BlobWithoutEncoding
        })
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
