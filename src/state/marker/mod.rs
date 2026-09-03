//! The **event marker**: the durable recovery handle for one collection's
//! in-flight stage.
//!
//! While an event's outcome is unresolved, a collection carries exactly one
//! event marker naming that event and the coordinates it staged, so recovery
//! can resolve the whole stage as a unit — promote or roll back every listed
//! cell — from a single point read rather than a scan over per-coordinate
//! rows. [`EventMarker`] represents this unsettled state.
//! This module also encodes and decodes the Cassandra marker value.
//!
//! # Invariants
//!
//! * **Frozen at stage time.** The staged coordinate list and each cleared
//!   section's survivor list are captured when the stage is written and never
//!   re-derived from live provisional state — re-applying a stage during
//!   recovery must be a pure function of durable staged data, correct under
//!   partial promotion.
//! * **Coordinates only, never values.** A marker lists `(section,
//!   coordinate)`s, so its size scales with one event's staged write set (the
//!   quantity the stage's batch budget already bounds), never with value bytes.
//! * **Qualified vocabulary.** This is the *event marker*, distinct from the
//!   dedup *commit marker* (the oracle's per-message row) and the in-RAM *dirty
//!   clear marker* (the per-event overlay's pending clear). "Marker"
//!   unqualified is ambiguous — always name which.
//!
//! The owning `event` is **not** part of the payload: on Cassandra it rides
//! the marker row's own `event` column, so [`decode_marker_payload`] takes it
//! as a parameter. The wire format carries no version byte — the Cassandra
//! row's existing `encoding`/`version` columns version the blob.

use super::cell::ProvisionalWrite;
use super::cell_key::{CellKey, Coordinate, Section};
use super::event_ref::EventRef;
use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use std::sync::Arc;
use thiserror::Error;

/// Width of the `u32` big-endian count/length prefixes in the frozen payload.
const LEN_PREFIX: usize = 4;

/// One cleared section paired with its **frozen survivor list**: the
/// coordinates that outlive the clear (the section's post-clear `Set` cells).
///
/// The survivor list is derived once, at stage time, by
/// [`SectionClear::frozen`] and thereafter replayed verbatim — never recomputed
/// from whatever cells are still provisional at resolve time. That is the
/// single survivor definition the design relies on for re-apply purity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SectionClear {
    section: Section,
    survivors: Vec<Coordinate>,
}

impl SectionClear {
    /// Freezes `section`'s survivors from the event's staged cells: the
    /// coordinates of that section's staged cells whose data is present,
    /// ascending. The one survivor definition — the session's `finalize`
    /// builds these from its staged record, the stage freezes them into the
    /// payload verbatim, and the settle/sweep replay them from the payload
    /// verbatim.
    #[must_use]
    pub(in crate::state) fn frozen(
        section: Section,
        staged: &[(CellKey, ProvisionalWrite)],
    ) -> Self {
        Self::from_survivors(
            section,
            staged
                .iter()
                .filter(|(cell, write)| cell.section == section && write.data().is_some())
                .map(|(cell, _)| cell.coordinate.clone())
                .collect(),
        )
    }

    /// [`Self::frozen`]'s resolved-shape twin for the direct-apply paths
    /// (`ReadUncommitted` finalize, the mid-handler `commit()`): survivors are
    /// the section's present-data resolved cells. Shares the survivor
    /// definition with `frozen` — only the input shape differs.
    #[must_use]
    pub(in crate::state) fn frozen_resolved(
        section: Section,
        cells: &[(CellKey, Option<Bytes>)],
    ) -> Self {
        Self::from_survivors(
            section,
            cells
                .iter()
                .filter(|(cell, data)| cell.section == section && data.is_some())
                .map(|(cell, _)| cell.coordinate.clone())
                .collect(),
        )
    }

    /// The shared survivor-definition tail: ascending, deduped.
    fn from_survivors(section: Section, mut survivors: Vec<Coordinate>) -> Self {
        survivors.sort_unstable();
        survivors.dedup();
        Self { section, survivors }
    }

    /// The cleared section.
    #[must_use]
    pub fn section(&self) -> Section {
        self.section
    }

    /// The frozen survivor coordinates, ascending.
    #[must_use]
    pub fn survivors(&self) -> &[Coordinate] {
        &self.survivors
    }
}

/// The unsettled event marker for one collection: the owning event, its full
/// staged coordinate set, and each cleared section's frozen survivors.
///
/// See the module docs for the invariants it carries. Constructed only inside
/// the state module (mirroring
/// [`ProvisionalCell`](super::cell::ProvisionalCell)): only the stage path
/// mints a marker. Clones share one immutable payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventMarker {
    inner: Arc<EventMarkerData>,
}

#[derive(Debug, PartialEq, Eq)]
struct EventMarkerData {
    event: EventRef,
    staged: Vec<CellKey>,
    clears: Vec<SectionClear>,
}

impl EventMarker {
    /// Freezes the marker for `event` from its staged cells and cleared
    /// sections. The staged coordinate list is sorted by `(section,
    /// coordinate)` for a deterministic payload; an event stages each cell at
    /// most once, so no dedup is required.
    #[must_use]
    pub(in crate::state) fn frozen(
        event: EventRef,
        staged: &[(CellKey, ProvisionalWrite)],
        clears: &[SectionClear],
    ) -> Self {
        let mut coordinates: Vec<CellKey> = staged.iter().map(|(cell, _)| cell.clone()).collect();
        coordinates.sort_unstable();
        Self::from_parts(event, coordinates, clears.to_vec())
    }

    /// Reconstructs a marker from decoded parts (the payload decoder).
    #[must_use]
    fn from_parts(event: EventRef, staged: Vec<CellKey>, clears: Vec<SectionClear>) -> Self {
        Self {
            inner: Arc::new(EventMarkerData {
                event,
                staged,
                clears,
            }),
        }
    }

    /// The owning event.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.inner.event
    }

    /// The event's full staged coordinate set, ascending.
    #[must_use]
    pub fn staged(&self) -> &[CellKey] {
        &self.inner.staged
    }

    /// Each cleared section with its frozen survivors.
    #[must_use]
    pub fn clears(&self) -> &[SectionClear] {
        &self.inner.clears
    }

    /// Reports whether the marker carries at least one section clear.
    #[must_use]
    pub(crate) fn has_clears(&self) -> bool {
        !self.inner.clears.is_empty()
    }

    /// Reports whether the marker is another event's marker with a section
    /// clear.
    ///
    /// A read by `own` must resolve such a marker before it reads.
    #[must_use]
    pub(crate) fn is_prior_clear(&self, own: EventRef) -> bool {
        self.event() != own && self.has_clears()
    }
}

/// Encodes an [`EventMarker`]'s payload — everything but its `event` — to the
/// frozen wire bytes. Deterministic: the lists are already in
/// constructor-sorted order.
///
/// Wire format (fixed-width big-endian, matching the fjall-codec house style):
///
/// ```text
/// [staged_count: u32 BE]
///   staged_count × [section: i8][coord_len: u32 BE][coord bytes]
/// [clears_count: u32 BE]
///   clears_count × [section: i8][survivor_count: u32 BE]
///                  survivor_count × [coord_len: u32 BE][coord bytes]
/// ```
///
/// Frozen and pinned; the Cassandra marker row is its production caller (the
/// payload rides the row's `data`/`encoding`/`version` columns).
///
/// # Errors
///
/// Returns [`MarkerPayloadError::TooLarge`] if a count or coordinate length
/// exceeds the `u32` the wire format carries — never a silent truncation.
pub(in crate::state) fn encode_marker_payload(
    marker: &EventMarker,
) -> Result<Bytes, MarkerPayloadError> {
    let mut len = LEN_PREFIX;
    for cell in marker.staged() {
        len += 1 + LEN_PREFIX + cell.coordinate.as_bytes().len();
    }
    len += LEN_PREFIX;
    for clear in marker.clears() {
        len += 1 + LEN_PREFIX;
        for coordinate in &clear.survivors {
            len += LEN_PREFIX + coordinate.as_bytes().len();
        }
    }

    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&len_u32(marker.staged().len())?.to_be_bytes());
    for cell in marker.staged() {
        buf.push(i8::from(cell.section).cast_unsigned());
        push_len_prefixed(&mut buf, &cell.coordinate)?;
    }
    buf.extend_from_slice(&len_u32(marker.clears().len())?.to_be_bytes());
    for clear in marker.clears() {
        buf.push(i8::from(clear.section).cast_unsigned());
        buf.extend_from_slice(&len_u32(clear.survivors.len())?.to_be_bytes());
        for coordinate in &clear.survivors {
            push_len_prefixed(&mut buf, coordinate)?;
        }
    }
    Ok(Bytes::from(buf))
}

/// Decodes a marker payload produced by [`encode_marker_payload`], binding it
/// to `event` (which rides the marker row's own column, not the payload).
///
/// # Errors
///
/// Returns [`MarkerPayloadError`] on a truncated buffer or trailing garbage —
/// both classify [`Permanent`](ErrorCategory::Permanent), a data rejection.
pub(in crate::state) fn decode_marker_payload(
    event: EventRef,
    bytes: &[u8],
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

    if !cursor.is_empty() {
        return Err(MarkerPayloadError::TrailingGarbage);
    }
    Ok(EventMarker::from_parts(event, staged, clears))
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
    /// The buffer ended before a length-prefixed field was fully read.
    #[error("event-marker payload truncated")]
    Truncated,

    /// Bytes remained after the last declared field was read.
    #[error("event-marker payload has trailing garbage")]
    TrailingGarbage,

    /// A count or coordinate length exceeded the `u32` the wire format
    /// carries (encode-side; a stage this size is unreachable in practice).
    #[error("event-marker payload field exceeds the u32 wire limit")]
    TooLarge,
}

impl ClassifyError for MarkerPayloadError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
