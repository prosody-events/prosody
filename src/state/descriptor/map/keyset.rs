//! The shared keyset address, bounded wire format, and frame validation.

use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::Coordinate;
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

/// Maximum encoded keyset size. Insertions above this bound write `Overflowed`.
pub(super) const KEYSET_BYTE_CEILING: usize = 64 * 1024;

/// The durable tag for tracked membership.
pub(super) const TRACKED_TAG: u8 = 0;

/// The durable tag for unknown membership.
pub(super) const OVERFLOWED_TAG: u8 = 1;

/// Maximum decoded coordinate count. Each entry requires a four-byte length.
const KEYSET_MAX_ENTRIES: usize = KEYSET_BYTE_CEILING / 4;

/// The shared keyset address, coordinate `[2]`.
/// Coordinates `[0]` and `[1]` remain reserved for retired map metadata.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MapKeysetKey;

impl OrderedKeyCodec for MapKeysetKey {
    type Key = ();

    fn encode((): &()) -> Coordinate {
        Coordinate::from_bytes(Bytes::from_static(&[2]))
    }

    fn decode(bytes: &[u8]) -> Result<(), KeyCodecError> {
        match bytes {
            [2] => Ok(()),
            &[actual] => Err(KeyCodecError::BadDiscriminant { actual }),
            _ => Err(KeyCodecError::BadLength {
                expected: 1,
                actual: bytes.len(),
            }),
        }
    }
}

/// Encodes the fixed keyset address through each codec input form.
impl Codec for MapKeysetKey {
    type Error = KeyCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "map-keyset-key.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<(), KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<(), KeyCodecError> {
        Self::decode(&buf)
    }

    fn serialize_ref(&mut self, (): &(), buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&()).as_bytes());
        Ok(())
    }
}

/// Current membership in coordinate order, or a persistent overflow state.
///
/// Tracked coordinates are strictly ascending and unique. Removal subtracts a
/// coordinate. Insertion above the count or byte bound writes `Overflowed`.
/// Clear or expiry resets that state. TTL expiry can leave stale coordinates;
/// presence reads skip absent members.
///
/// Each insertion stages a member and its keyset in the same scoped operation.
/// The keyset must outlive every member, so TTL insertions refresh it even when
/// membership does not change. An absent keyset permits an empty point plan. A
/// split mid-handler commit can temporarily violate this assumption.
/// The handler retry repairs that residue. The `is_empty` operation scans the
/// member family directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Keyset {
    /// The distinct-key coordinates currently tracked, strictly ascending.
    Tracked(Vec<Coordinate>),

    /// The map overflowed its keyset bound; membership is no longer tracked.
    Overflowed,
}

/// The shared membership codec. Its format identifier remains `map-keyset.v1`.
/// Owned decoding retains the frame allocation through coordinate slices.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MapKeysetCodec;

impl Codec for MapKeysetCodec {
    type Error = KeysetFrameError;
    type Payload = Keyset;

    const FORMAT_ID: &'static str = "map-keyset.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Keyset, KeysetFrameError> {
        // The trait boundary hands a `&mut [u8]`, which cannot alias into
        // `Bytes`; one upfront copy of the frame — sized by the stored cell,
        // which a lowered bound or an older writer can leave above
        // `KEYSET_BYTE_CEILING` — buys zero-copy coordinate slicing for the
        // rest of the parse.
        decode_keyset(&Bytes::copy_from_slice(buf))
    }

    fn deserialize_owned(&mut self, buf: BytesMut) -> Result<Keyset, KeysetFrameError> {
        // Freezing transfers the allocation into each coordinate slice.
        decode_keyset(&buf.freeze())
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Keyset, KeysetFrameError> {
        decode_keyset(&buf)
    }

    fn serialize_ref(
        &mut self,
        payload: &Keyset,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeysetFrameError> {
        encode_keyset(payload, buf)
    }
}

fn encode_keyset(payload: &Keyset, buf: &mut Vec<u8>) -> Result<(), KeysetFrameError> {
    match payload {
        Keyset::Overflowed => {
            buf.reserve(1);
            buf.push(OVERFLOWED_TAG);
        }
        Keyset::Tracked(keys) => {
            let count = u32::try_from(keys.len()).map_err(|_| KeysetFrameError::CountOverflow)?;
            let total = tracked_frame_len(keys).ok_or(KeysetFrameError::CountOverflow)?;
            buf.reserve(total);
            buf.push(TRACKED_TAG);
            buf.extend_from_slice(&count.to_be_bytes());
            for coordinate in keys {
                let len = u32::try_from(coordinate.as_bytes().len())
                    .map_err(|_| KeysetFrameError::CountOverflow)?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(coordinate.as_bytes());
            }
        }
    }
    Ok(())
}

/// Returns the encoded size, or `None` when the size overflows.
pub(super) fn tracked_frame_len(keys: &[Coordinate]) -> Option<usize> {
    let mut total = 1usize.checked_add(4)?;
    for coordinate in keys {
        total = total
            .checked_add(4)?
            .checked_add(coordinate.as_bytes().len())?;
    }
    Some(total)
}

/// Tests both the registered count limit and the encoded byte limit.
pub(super) fn is_oversized(keys: &[Coordinate], limit: usize) -> bool {
    keys.len() > limit || tracked_frame_len(keys).is_none_or(|len| len > KEYSET_BYTE_CEILING)
}

/// Decodes the frame with checked lengths and strictly ordered coordinates.
pub(super) fn decode_keyset(bytes: &Bytes) -> Result<Keyset, KeysetFrameError> {
    let buf = bytes.as_ref();
    match buf.first().copied() {
        None => Err(KeysetFrameError::Truncated),
        Some(OVERFLOWED_TAG) => {
            if buf.len() == 1 {
                Ok(Keyset::Overflowed)
            } else {
                Err(KeysetFrameError::TrailingBytes)
            }
        }
        Some(TRACKED_TAG) => decode_tracked(bytes),
        Some(other) => Err(KeysetFrameError::UnknownTag(other)),
    }
}

/// Decodes a tracked frame. Rejects duplicate coordinates and trailing bytes.
fn decode_tracked(bytes: &Bytes) -> Result<Keyset, KeysetFrameError> {
    let buf = bytes.as_ref();
    let count_bytes: [u8; 4] = buf
        .get(1..5)
        .ok_or(KeysetFrameError::Truncated)?
        .try_into()
        .map_err(|_| KeysetFrameError::Truncated)?;
    let count = u32::from_be_bytes(count_bytes) as usize;
    // Reject impossible counts before allocation. Each coordinate needs a length.
    if count > buf.len().saturating_sub(5) / 4 {
        return Err(KeysetFrameError::Truncated);
    }
    if count > KEYSET_MAX_ENTRIES {
        return Err(KeysetFrameError::CountOverflow);
    }
    // Point plans reuse this vector. Extra capacity penalizes read-only calls.
    let mut keys = Vec::with_capacity(count);
    let mut offset = 5usize;
    let mut prev: Option<&[u8]> = None;
    for _ in 0..count {
        let len_end = offset.checked_add(4).ok_or(KeysetFrameError::Truncated)?;
        let len_bytes: [u8; 4] = buf
            .get(offset..len_end)
            .ok_or(KeysetFrameError::Truncated)?
            .try_into()
            .map_err(|_| KeysetFrameError::Truncated)?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        let coord_end = len_end
            .checked_add(len)
            .ok_or(KeysetFrameError::Truncated)?;
        let coordinate = buf
            .get(len_end..coord_end)
            .ok_or(KeysetFrameError::Truncated)?;
        if let Some(previous) = prev
            && coordinate <= previous
        {
            return Err(KeysetFrameError::Unsorted);
        }
        keys.push(Coordinate::from_bytes(bytes.slice(len_end..coord_end)));
        prev = Some(coordinate);
        offset = coord_end;
    }
    if offset != buf.len() {
        return Err(KeysetFrameError::TrailingBytes);
    }
    Ok(Keyset::Tracked(keys))
}

/// A keyset frame failed validation. These errors are permanent.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum KeysetFrameError {
    /// The frame's leading tag byte was neither `Tracked` nor `Overflowed`.
    #[error("unknown map keyset tag: {0}")]
    UnknownTag(u8),

    /// The frame ended before a declared count or coordinate length.
    #[error("truncated map keyset frame")]
    Truncated,

    /// Bytes remained after the frame's declared contents.
    #[error("trailing bytes after map keyset frame")]
    TrailingBytes,

    /// Two coordinates were out of strictly-ascending order (unsorted or a
    /// duplicate).
    #[error("map keyset coordinates are not strictly ascending")]
    Unsorted,

    /// A frame exceeded the coordinate count or length bound.
    #[error("map keyset count or length exceeds its bound")]
    CountOverflow,
}

impl ClassifyError for KeysetFrameError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
