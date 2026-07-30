//! Fixed-TTL cell store used by cache metadata tests.

use super::*;

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

    async fn get<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> Result<Committed, Self::Error> {
        Ok(Committed::new(Some(self.value.clone())))
    }

    async fn get_for_cache<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        Ok((Committed::new(Some(self.value.clone())), self.ttl))
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

    async fn provisional_cell_at<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        Ok(None)
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

    async fn write_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [(CellKey, Option<Bytes>)],
        _clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn standing_marker<'a>(
        &'a self,
        _collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        Ok(None)
    }

    async fn commit_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
