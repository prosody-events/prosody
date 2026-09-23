//! The frozen wire payload of the Staged and Committed marker rows.

use super::{AttemptId, EventMarker, EventMarkerData, MarkerVersion, SectionClear};
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::event_ref::EventRef;
use crate::state::identity::{StateName, StateType, StateTypeError};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use std::str::from_utf8;
use thiserror::Error;
use uuid::Uuid;

/// Width of the `u32` big-endian count/length prefixes in the frozen payload.
const LEN_PREFIX: usize = 4;

/// Encodes an [`EventMarker`]'s payload without its `event` into frozen wire
/// bytes. Sorted lists make the encoding deterministic.
///
/// The format uses fixed-width big-endian fields, as the fjall codec does:
///
/// ```text
/// [staged_count: u32 BE]
///   staged_count × [section: i8][coord_len: u32 BE][coord bytes]
/// [clears_count: u32 BE]
///   clears_count × [section: i8][survivor_count: u32 BE]
///                  survivor_count × [coord_len: u32 BE][coord bytes]
/// [touched_count: u32 BE]
///   touched_count × [state_type: i8][name_len: u32 BE][UTF-8 name]
/// [evidence_ttl_secs_minus_one: u32 BE] // finite retention only
/// [dedup_present: u8] // 0 means absent; nonzero means present
/// [dedup: 16 bytes] // only when present
/// [attempt: 16 bytes]
/// ```
///
/// Cassandra stores this frozen payload through the Staged row's `data`,
/// `encoding`, and `version` columns.
///
/// # Errors
///
/// Returns [`MarkerPayloadError::TooLarge`] if a count or coordinate length
/// exceeds the wire format's `u32` limit. The encoder never truncates a field.
pub(in crate::state) fn encode_marker_payload(
    marker: &EventMarker,
) -> Result<Bytes, MarkerPayloadError> {
    encode_payload(marker, marker.staged(), marker.clears())
}

/// Encodes the Committed row: the marker's evidence with no staged cells and no
/// clears. Shares the wire format and errors with [`encode_marker_payload`].
pub(in crate::state) fn encode_committed_payload(
    marker: &EventMarker,
) -> Result<Bytes, MarkerPayloadError> {
    encode_payload(marker, &[], &[])
}

fn encode_payload(
    marker: &EventMarker,
    staged: &[CellKey],
    clears: &[SectionClear],
) -> Result<Bytes, MarkerPayloadError> {
    let mut len = LEN_PREFIX;
    for cell in staged {
        len += 1 + LEN_PREFIX + cell.coordinate.as_bytes().len();
    }
    len += LEN_PREFIX;
    for clear in clears {
        len += 1 + LEN_PREFIX;
        for coordinate in &clear.survivors {
            len += LEN_PREFIX + coordinate.as_bytes().len();
        }
    }

    len += 2 * LEN_PREFIX
        + marker
            .touched()
            .iter()
            .map(|(_, name)| 1 + LEN_PREFIX + name.as_str().len())
            .sum::<usize>();
    len += 16 + 1 + marker.dedup().map_or(0, |_| 16);
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&len_u32(staged.len())?.to_be_bytes());
    for cell in staged {
        buf.push(i8::from(cell.section).cast_unsigned());
        push_len_prefixed(&mut buf, &cell.coordinate)?;
    }
    buf.extend_from_slice(&len_u32(clears.len())?.to_be_bytes());
    for clear in clears {
        buf.push(i8::from(clear.section).cast_unsigned());
        buf.extend_from_slice(&len_u32(clear.survivors.len())?.to_be_bytes());
        for coordinate in &clear.survivors {
            push_len_prefixed(&mut buf, coordinate)?;
        }
    }
    buf.extend_from_slice(&len_u32(marker.touched().len())?.to_be_bytes());
    for (state_type, name) in marker.touched() {
        buf.push(i8::from(*state_type).cast_unsigned());
        buf.extend_from_slice(&len_u32(name.as_str().len())?.to_be_bytes());
        buf.extend_from_slice(name.as_str().as_bytes());
    }
    let ttl = marker
        .evidence_ttl()
        .seconds()
        .checked_sub(1)
        .filter(|seconds| i64::from(*seconds) < MAX_CASSANDRA_TTL_SECS)
        .ok_or(MarkerPayloadError::TooLarge)?;
    buf.extend_from_slice(&ttl.to_be_bytes());
    buf.push(u8::from(marker.dedup().is_some()));
    if let Some(dedup) = marker.dedup() {
        buf.extend_from_slice(dedup.as_bytes());
    }
    buf.extend_from_slice(marker.attempt().0.as_bytes());
    Ok(Bytes::from(buf))
}

/// Decodes a payload from [`encode_marker_payload`] and associates it with
/// `event`. The Staged row stores `event` in a separate column.
///
/// # Errors
///
/// Returns [`MarkerPayloadError`] for truncated input or trailing bytes. Both
/// errors classify as [`Permanent`](ErrorCategory::Permanent) data rejections.
pub(in crate::state) fn decode_marker_payload(
    event: EventRef,
    bytes: &[u8],
    version: MarkerVersion,
    legacy_ttl: Option<CompactDuration>,
) -> Result<EventMarker, MarkerPayloadError> {
    let mut cursor = Cursor::new(bytes);

    // Every `with_capacity` below is capped at the cursor's remaining byte
    // count: the count came from an untrusted durable blob, so a lying value
    // must not demand an unbounded allocation — it still fails `Truncated`
    // once the bytes run out.
    let staged_count = cursor.take_u32()?;
    let mut staged = Vec::with_capacity((staged_count as usize).min(cursor.remaining()));
    for _ in 0..staged_count {
        let section = Section::new(cursor.take_section()?);
        let coordinate = cursor.take_coordinate()?;
        staged.push(CellKey {
            section,
            coordinate,
        });
    }

    let clears_count = cursor.take_u32()?;
    let mut clears = Vec::with_capacity((clears_count as usize).min(cursor.remaining()));
    for _ in 0..clears_count {
        let section = Section::new(cursor.take_section()?);
        let survivor_count = cursor.take_u32()?;
        let mut survivors = Vec::with_capacity((survivor_count as usize).min(cursor.remaining()));
        for _ in 0..survivor_count {
            survivors.push(cursor.take_coordinate()?);
        }
        clears.push(SectionClear { section, survivors });
    }

    let (mut touched, ttl, dedup, attempt) = match version {
        MarkerVersion::V1 => (
            Vec::new(),
            legacy_ttl.unwrap_or(CompactDuration::new(0)),
            match event {
                EventRef::Message { dedup_id } => Some(dedup_id),
                EventRef::Timer(_) => None,
            },
            AttemptId::new(),
        ),
        MarkerVersion::V2 => {
            let count = cursor.take_u32()? as usize;
            let mut touched = Vec::with_capacity(count.min(cursor.remaining()));
            for _ in 0..count {
                let state_type = StateType::try_from(cursor.take_section()?)?;
                let len = cursor.take_u32()? as usize;
                let name = from_utf8(cursor.take(len)?).map_err(|_| MarkerPayloadError::Name)?;
                touched.push((
                    state_type,
                    StateName::try_new(name).map_err(|_| MarkerPayloadError::Name)?,
                ));
            }
            let seconds = cursor
                .take_u32()?
                .checked_add(1)
                .filter(|seconds| i64::from(*seconds) <= MAX_CASSANDRA_TTL_SECS)
                .ok_or(MarkerPayloadError::TooLarge)?;
            let dedup = match cursor.take_section()? {
                0 => None,
                _ => Some(Uuid::from_bytes(
                    cursor
                        .take(16)?
                        .try_into()
                        .map_err(|_| MarkerPayloadError::Truncated)?,
                )),
            };
            let attempt = AttemptId(Uuid::from_bytes(
                cursor
                    .take(16)?
                    .try_into()
                    .map_err(|_| MarkerPayloadError::Truncated)?,
            ));
            (touched, CompactDuration::new(seconds), dedup, attempt)
        }
    };
    if !cursor.is_empty() {
        return Err(MarkerPayloadError::TrailingGarbage);
    }
    touched.sort_unstable();
    touched.dedup();
    Ok(EventMarker::from_parts(EventMarkerData {
        version,
        attempt,
        event,
        staged,
        clears,
        touched: touched.into(),
        evidence_ttl: ttl,
        dedup,
    }))
}

/// A `usize` length as the `u32` wire prefix, or
/// [`MarkerPayloadError::TooLarge`].
fn len_u32(len: usize) -> Result<u32, MarkerPayloadError> {
    u32::try_from(len).map_err(|_| MarkerPayloadError::TooLarge)
}

/// Appends `[coord_len: u32 BE][coord bytes]` to `buf`.
fn push_len_prefixed(buf: &mut Vec<u8>, coordinate: &Coordinate) -> Result<(), MarkerPayloadError> {
    let coordinate = coordinate.as_bytes();
    buf.extend_from_slice(&len_u32(coordinate.len())?.to_be_bytes());
    buf.extend_from_slice(coordinate);
    Ok(())
}

/// A forward-only reader over a marker payload buffer, failing loudly on any
/// short read rather than truncating.
struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Bytes not yet consumed — the allocation cap for count-sized buffers.
    fn remaining(&self) -> usize {
        self.bytes.len()
    }

    /// Splits off the next `n` bytes, or fails on a short buffer.
    fn take(&mut self, n: usize) -> Result<&'a [u8], MarkerPayloadError> {
        if self.bytes.len() < n {
            return Err(MarkerPayloadError::Truncated);
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }

    fn take_u32(&mut self) -> Result<u32, MarkerPayloadError> {
        let head = self.take(LEN_PREFIX)?;
        let array: [u8; LEN_PREFIX] = head.try_into().map_err(|_| MarkerPayloadError::Truncated)?;
        Ok(u32::from_be_bytes(array))
    }

    fn take_section(&mut self) -> Result<i8, MarkerPayloadError> {
        let head = self.take(1)?;
        Ok(head[0].cast_signed())
    }

    fn take_coordinate(&mut self) -> Result<Coordinate, MarkerPayloadError> {
        let len = self.take_u32()? as usize;
        let bytes = self.take(len)?;
        Ok(Coordinate::from_bytes(bytes.to_vec()))
    }
}

/// Failure encoding or decoding a frozen event-marker payload.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarkerPayloadError {
    /// The payload version is unknown.
    #[error("unknown marker version {0}")]
    Version(i32),
    /// A collection name is invalid.
    #[error("invalid collection name in marker")]
    Name,
    /// A collection namespace is unknown.
    #[error(transparent)]
    StateType(#[from] StateTypeError),
    /// The buffer ended before a length-prefixed field was fully read.
    #[error("event-marker payload truncated")]
    Truncated,

    /// Bytes remained after the last declared field was read.
    #[error("event-marker payload has trailing garbage")]
    TrailingGarbage,

    /// A count, coordinate length, or TTL cannot fit its wire range.
    #[error("event-marker payload field is outside its wire range")]
    TooLarge,
}

impl ClassifyError for MarkerPayloadError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
