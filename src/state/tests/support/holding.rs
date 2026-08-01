//! Deterministic response gates for state-store concurrency tests.

use super::*;

#[derive(Clone)]
pub(crate) struct HoldingCellStore<S> {
    inner: S,
    holds: Arc<Holds>,
}

#[derive(Default)]
pub(crate) struct Holds {
    get_for_cache: Hold,
    write_resolved: Hold,
    commit_provisional: Hold,
}

#[derive(Default)]
pub(crate) struct Hold {
    armed: AtomicUsize,
    entered: Notify,
    release: Notify,
    landed: AtomicUsize,
}

impl Hold {
    pub(crate) fn arm(&self, n: usize) {
        self.armed.store(n, Ordering::Relaxed);
    }

    pub(crate) async fn entered(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }

    pub(crate) fn landed(&self) -> usize {
        self.landed.load(Ordering::Relaxed)
    }

    async fn pass<T>(&self, inner: impl Future<Output = T>) -> T {
        let out = inner.await;
        self.landed.fetch_add(1, Ordering::Relaxed);
        let parked = self
            .armed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok();
        if parked {
            self.entered.notify_one();
            self.release.notified().await;
        }
        out
    }
}

impl<S> HoldingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            holds: Arc::new(Holds::default()),
        }
    }

    pub(crate) fn holds(&self) -> Arc<Holds> {
        self.holds.clone()
    }
}

impl Holds {
    pub(crate) fn get_for_cache(&self) -> &Hold {
        &self.get_for_cache
    }

    pub(crate) fn write_resolved(&self) -> &Hold {
        &self.write_resolved
    }

    pub(crate) fn commit_provisional(&self) -> &Hold {
        &self.commit_provisional
    }
}

impl<S> CellStore for HoldingCellStore<S>
where
    S: CellStore,
{
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.inner.get(collection, cell, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.inner.scan_cells(collection, scan, own)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        self.holds
            .get_for_cache
            .pass(self.inner.get_for_cache(collection, cell, own))
            .await
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner.provisional_cells(collection)
    }

    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a {
        self.inner.provisional_cell_at(collection, cell)
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
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_provisional(collection, writes, marker)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.holds
            .write_resolved
            .pass(self.inner.write_resolved(collection, cells, clears))
            .await
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.mark_resolved(collection, cells)
    }

    fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a {
        self.inner.standing_marker(collection)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.holds
            .commit_provisional
            .pass(self.inner.commit_provisional(collection, writes, clears))
            .await
    }

    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.abort_provisional(collection, writes)
    }
}
