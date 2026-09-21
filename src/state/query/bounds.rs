//! Encoded map and set bounds and ordered coordinate selection.

use super::{Edge, KeyQuery};
use crate::state::cell_key::{Coordinate, Direction, ScanEdge};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec};
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::ops::{Bound, Range};

/// The encoded bounds, direction, and result limit of a map or set query.
#[derive(Clone, Debug)]
pub(crate) struct Query<'a> {
    pub(crate) dir: Direction,
    pub(crate) limit: Option<NonZeroUsize>,
    pub(crate) start: ScanEdge<&'a [u8]>,
    pub(crate) end: ScanEdge<&'a [u8]>,
}

impl Query<'_> {
    /// Keeps the ascending stored coordinates within the query bounds, in
    /// query order. The trim reuses the stored vector.
    pub(crate) fn select(&self, mut coordinates: Vec<Coordinate>) -> Vec<Coordinate> {
        let (low, high) = match self.dir {
            Direction::Forward => (&self.start, &self.end),
            Direction::Backward => (&self.end, &self.start),
        };
        let start = match low {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c.as_bytes() < *edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c.as_bytes() <= *edge),
            ScanEdge::Unbounded => 0,
        };
        let end = match high {
            ScanEdge::Included(edge) => coordinates.partition_point(|c| c.as_bytes() <= *edge),
            ScanEdge::Excluded(edge) => coordinates.partition_point(|c| c.as_bytes() < *edge),
            ScanEdge::Unbounded => coordinates.len(),
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
    /// Writes both bounds into reusable storage and returns a borrowed view.
    pub(crate) fn encode<'a>(&self, buf: &'a mut Vec<u8>) -> Result<Query<'a>, KeyCodecError> {
        buf.clear();
        let (start, end) = KC::with_cached_local(|codec| {
            Ok::<_, KeyCodecError>((
                encode_edge(codec, &self.start, buf)?,
                encode_edge(codec, &self.end, buf)?,
            ))
        })?;
        Ok(Query {
            dir: self.dir,
            limit: self.limit,
            start: start.map(|range| &buf[range]),
            end: end.map(|range| &buf[range]),
        })
    }
}

fn encode_edge<KC: OrderedKeyCodec, B: Borrow<KC::Borrowed>>(
    codec: &mut KC,
    edge: &Edge<B>,
    buf: &mut Vec<u8>,
) -> Result<ScanEdge<Range<usize>>, KeyCodecError> {
    let start = buf.len();
    let (key, included) = match edge {
        Edge::Bound(Bound::Unbounded) => return Ok(ScanEdge::Unbounded),
        Edge::Bound(Bound::Included(key)) => (key, true),
        Edge::Bound(Bound::Excluded(key)) | Edge::PrefixEnd(key) => (key, false),
    };
    codec.serialize_key(key.borrow(), buf)?;
    if matches!(edge, Edge::PrefixEnd(_)) && !prefix_end(buf, start) {
        return Ok(ScanEdge::Unbounded);
    }
    let range = start..buf.len();
    Ok(if included {
        ScanEdge::Included(range)
    } else {
        ScanEdge::Excluded(range)
    })
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
