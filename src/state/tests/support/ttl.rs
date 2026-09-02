//! Fixed-TTL cell store used by cache metadata tests.

use super::*;
use std::future::ready;

#[derive(Clone)]
pub(crate) struct TtlStub {
    value: Bytes,
    ttl: Option<CompactDuration>,
}

impl TtlStub {
    pub(crate) fn new(value: Bytes, ttl: Option<CompactDuration>) -> Self {
        Self { value, ttl }
    }
}

impl CellStore for TtlStub {
    type Error = Infallible;

    fn get<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        ready(Ok(Committed::new(Some(self.value.clone()))))
    }

    fn get_for_cache<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> impl Future<Output = Result<(Committed, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        ready(Ok((Committed::new(Some(self.value.clone())), self.ttl)))
    }

    fn scan_cells<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _scan: Scan<'a>,
        _own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        stream::empty()
    }

    fn provisional_cells<'a>(
        &'a self,
        _collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        stream::empty()
    }

    fn provisional_cell_at<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a {
        ready(Ok(None))
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        provisional_point_loop(self, collection, section, batch)
    }

    fn write_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _marker: Option<&'a EventMarker>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    fn write_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [(CellKey, Option<Bytes>)],
        _clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    fn mark_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    fn unsettled_marker<'a>(
        &'a self,
        _collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a {
        ready(Ok(None))
    }

    fn commit_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _clears: &'a [SectionClear],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    fn abort_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }
}
