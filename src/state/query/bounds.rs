//! Encoded map and set bounds and ordered coordinate selection.

use super::KeyQuery;
use crate::state::cell_key::{Coordinate, Direction};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec};
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::ops::{Bound, Range};

/// The encoded bounds, direction, and result limit of a map or set query.
#[derive(Clone, Debug)]
pub(crate) struct Query<'a> {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    pub(crate) start: Bound<&'a [u8]>,
    pub(crate) end: Bound<&'a [u8]>,
}

impl Query<'_> {
    /// Keeps the ascending stored coordinates within the query bounds, in
    /// query order. The trim reuses the stored vector.
    pub(crate) fn select(&self, mut coordinates: Vec<Coordinate>) -> Vec<Coordinate> {
        let (low, high) = self.dir.orient(&self.start, &self.end);
        let start = match low {
            Bound::Included(edge) => coordinates.partition_point(|c| c.as_bytes() < *edge),
            Bound::Excluded(edge) => coordinates.partition_point(|c| c.as_bytes() <= *edge),
            Bound::Unbounded => 0,
        };
        let end = match high {
            Bound::Included(edge) => coordinates.partition_point(|c| c.as_bytes() <= *edge),
            Bound::Excluded(edge) => coordinates.partition_point(|c| c.as_bytes() < *edge),
            Bound::Unbounded => coordinates.len(),
        };
        coordinates.truncate(end.max(start));
        coordinates.drain(..start);
        if self.dir == Direction::Backward {
            coordinates.reverse();
        }
        coordinates
    }
}

impl<KC: OrderedKeyCodec, B: Borrow<KC::Borrowed>> KeyQuery<KC, B> {
    /// Writes the edges into reusable storage and returns a borrowed view.
    /// The view narrows the edges to the prefix range. Edges that cannot hold
    /// a key return `None`, so the query reads nothing.
    pub(crate) fn encode<'a>(
        &self,
        buf: &'a mut Vec<u8>,
    ) -> Result<Option<Query<'a>>, KeyCodecError> {
        buf.clear();
        let (low, high, prefix) = KC::with_cached_local(|codec| {
            Ok::<_, KeyCodecError>((
                encode_bound(codec, self.edges.low.as_ref(), buf)?,
                encode_bound(codec, self.edges.high.as_ref(), buf)?,
                self.prefix
                    .as_ref()
                    .map(|prefix| encode_prefix(codec, prefix.borrow(), buf))
                    .transpose()?,
            ))
        })?;

        let buf: &'a [u8] = buf;
        let mut low = low.map(|range| &buf[range]);
        let mut high = high.map(|range| &buf[range]);
        if let Some((prefix, prefix_end)) = prefix {
            let prefix = &buf[prefix];
            if edge(low).is_none_or(|edge| edge < prefix) {
                low = Bound::Included(prefix);
            }
            if let Some(prefix_end) = prefix_end.map(|range| &buf[range])
                && edge(high).is_none_or(|edge| edge >= prefix_end)
            {
                high = Bound::Excluded(prefix_end);
            }
        }

        if !can_hold_key(low, high) {
            return Ok(None);
        }
        let (start, end) = self.dir.orient(low, high);
        Ok(Some(Query {
            dir: self.dir,
            limit: self.limit,
            start,
            end,
        }))
    }
}

/// Reports whether ascending edges can hold a key. The test is exact when both
/// edges are equal or ordered. Excluded edges that no byte string separates
/// still pass.
fn can_hold_key(low: Bound<&[u8]>, high: Bound<&[u8]>) -> bool {
    match (low, high) {
        (Bound::Included(low), Bound::Included(high)) => low <= high,
        (
            Bound::Included(low) | Bound::Excluded(low),
            Bound::Included(high) | Bound::Excluded(high),
        ) => low < high,
        _ => true,
    }
}

/// Writes one bound key and returns its range in `buf`.
fn encode_bound<KC: OrderedKeyCodec, B: Borrow<KC::Borrowed>>(
    codec: &mut KC,
    bound: Bound<&B>,
    buf: &mut Vec<u8>,
) -> Result<Bound<Range<usize>>, KeyCodecError> {
    let start = buf.len();
    let mut write = |key: &B| {
        codec
            .serialize_key(key.borrow(), buf)
            .map(|()| start..buf.len())
    };
    Ok(match bound {
        Bound::Included(key) => Bound::Included(write(key)?),
        Bound::Excluded(key) => Bound::Excluded(write(key)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

/// Writes a prefix and its exclusive endpoint. It returns their ranges in
/// `buf`. A prefix without a finite endpoint returns no endpoint range.
fn encode_prefix<KC: OrderedKeyCodec>(
    codec: &mut KC,
    prefix: &KC::Borrowed,
    buf: &mut Vec<u8>,
) -> Result<(Range<usize>, Option<Range<usize>>), KeyCodecError> {
    let start = buf.len();
    codec.serialize_key(prefix, buf)?;
    let prefix = start..buf.len();
    buf.extend_from_within(prefix.clone());
    let bounded = prefix_end(buf, prefix.end);
    let end = bounded.then_some(prefix.end..buf.len());
    Ok((prefix, end))
}

/// Returns the key of a finite bound.
fn edge(bound: Bound<&[u8]>) -> Option<&[u8]> {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
        Bound::Unbounded => None,
    }
}

/// Replaces the encoded suffix with its exclusive prefix endpoint.
/// An empty or all-`0xFF` suffix has no finite endpoint.
pub(super) fn prefix_end(buf: &mut Vec<u8>, start: usize) -> bool {
    let Some(last) = buf[start..].iter().rposition(|&byte| byte != u8::MAX) else {
        buf.truncate(start);
        return false;
    };
    let last = start + last;
    buf[last] += 1;
    buf.truncate(last + 1);
    true
}
