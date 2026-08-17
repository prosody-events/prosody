//! Store and resolver counters used by query-budget tests.

use super::*;

#[derive(Clone)]
pub(crate) struct CountingCellStore<S> {
    inner: S,
    counts: Arc<OpCounts>,
}

#[derive(Default)]
struct OpCounts {
    write_provisional: AtomicUsize,
    write_resolved: AtomicUsize,
    mark_resolved: AtomicUsize,
    commit_provisional: AtomicUsize,
    abort_provisional: AtomicUsize,
    standing_marker: AtomicUsize,
    get: AtomicUsize,
    get_many: AtomicUsize,
    get_many_for_cache: AtomicUsize,
    contains_many: AtomicUsize,
    scan_cells: AtomicUsize,
    scan_keys: AtomicUsize,
    provisional_cells: AtomicUsize,
    provisional_cell_at: AtomicUsize,
    provisional_many: AtomicUsize,
}

impl<S> CountingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
        }
    }

    pub(crate) fn durable_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
            + self.counts.write_resolved.load(Ordering::Relaxed)
            + self.counts.mark_resolved.load(Ordering::Relaxed)
            + self.counts.commit_provisional.load(Ordering::Relaxed)
            + self.counts.abort_provisional.load(Ordering::Relaxed)
    }

    pub(crate) fn marker_reads(&self) -> usize {
        self.counts.standing_marker.load(Ordering::Relaxed)
    }

    pub(crate) fn lower_reads(&self) -> usize {
        self.counts.get.load(Ordering::Relaxed)
    }

    pub(crate) fn visible_point_reads(&self) -> usize {
        self.counts.get.load(Ordering::Relaxed)
    }

    pub(crate) fn batch_reads(&self) -> usize {
        self.counts.get_many.load(Ordering::Relaxed)
    }

    pub(crate) fn batch_cache_reads(&self) -> usize {
        self.counts.get_many_for_cache.load(Ordering::Relaxed)
    }

    pub(crate) fn lower_scans(&self) -> usize {
        self.counts.scan_cells.load(Ordering::Relaxed)
    }

    pub(crate) fn presence_reads(&self) -> usize {
        self.counts.contains_many.load(Ordering::Relaxed)
    }

    pub(crate) fn recovery_sweeps(&self) -> usize {
        self.counts.provisional_cells.load(Ordering::Relaxed)
    }

    pub(crate) fn warm_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    pub(crate) fn raw_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    pub(crate) fn raw_batch_reads(&self) -> usize {
        self.counts.provisional_many.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
        self.counts.commit_provisional.store(0, Ordering::Relaxed);
        self.counts.abort_provisional.store(0, Ordering::Relaxed);
        self.counts.standing_marker.store(0, Ordering::Relaxed);
        self.counts.get.store(0, Ordering::Relaxed);
        self.counts.get_many.store(0, Ordering::Relaxed);
        self.counts.get_many_for_cache.store(0, Ordering::Relaxed);
        self.counts.contains_many.store(0, Ordering::Relaxed);
        self.counts.scan_cells.store(0, Ordering::Relaxed);
        self.counts.scan_keys.store(0, Ordering::Relaxed);
        self.counts.provisional_cells.store(0, Ordering::Relaxed);
        self.counts.provisional_cell_at.store(0, Ordering::Relaxed);
        self.counts.provisional_many.store(0, Ordering::Relaxed);
    }
}

impl<S: CellStore> CellStore for CountingCellStore<S> {
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(collection, cell, own)
    }

    fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CommittedBatch, Self::Error>> + Send + 'a {
        self.counts.get_many.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(collection, section, batch, own)
    }

    fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CacheBatch, Self::Error>> + Send + 'a {
        self.counts
            .get_many_for_cache
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .get_many_for_cache(collection, section, batch, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.counts.scan_cells.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_cells(collection, scan, own)
    }

    fn scan_keys<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<CellKey, Self::Error>> + Send + 'a {
        self.counts.scan_keys.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_keys(collection, scan, own)
    }

    fn contains_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<PresenceBatch, Self::Error>> + Send + 'a {
        self.counts.contains_many.fetch_add(1, Ordering::Relaxed);
        self.inner.contains_many(collection, section, batch, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.counts
            .provisional_cells
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cells(collection)
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        self.counts
            .provisional_cell_at
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cell_at(collection, cell).await
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        self.counts.provisional_many.fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_many(collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_provisional(collection, writes, marker)
            .await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.counts.write_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.write_resolved(collection, cells, clears).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.counts.mark_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.mark_resolved(collection, cells).await
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        self.counts.standing_marker.fetch_add(1, Ordering::Relaxed);
        self.inner.standing_marker(collection).await
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.counts
            .commit_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .commit_provisional(collection, writes, clears)
            .await
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.counts
            .abort_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort_provisional(collection, writes).await
    }
}

#[derive(Clone, Default)]
pub(crate) struct ResolveCounter(Arc<AtomicUsize>);

impl ResolveCounter {
    pub(crate) fn resolves(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn bump(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct CountingResolver;

impl CellResolver for CountingResolver {
    type Context<'s> = &'s ResolveCounter;
    type Resolved = Value;
    type Stored = Value;
    type Write<'a> = Value;

    const RESOLVER_ID: Option<&'static str> = Some("test-counting-resolver.v1");

    fn resolve(
        ctx: Self::Context<'_>,
        stored: Value,
    ) -> impl Future<Output = Result<Value, StateAccessError>> + Send {
        ctx.bump();
        ready(Ok(stored))
    }

    fn stored_from(write: Value) -> Value {
        write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::registry::CollectionDefRegistry;

    #[tokio::test]
    async fn counters_increment_once_per_store_call() -> Result<()> {
        let store = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = fresh_collection("counter-probe")?;
        let cell = CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(vec![0]),
        };
        let own = probe(1);

        store.reset();
        store.get(&id, &cell, own).await?;
        assert_eq!(store.visible_point_reads(), 1);
        assert_eq!(store.batch_reads(), 0);
        assert_eq!(store.raw_point_reads(), 0);

        store.reset();
        let batch = batch_of([0])?;
        store.get_many(&id, Section::new(0), &batch, own).await?;
        assert_eq!(store.batch_reads(), 1);
        assert_eq!(store.visible_point_reads(), 0);
        assert_eq!(store.raw_point_reads(), 0);

        store.reset();
        store.provisional_cell_at(&id, &cell).await?;
        assert_eq!(store.raw_point_reads(), 1);
        assert_eq!(store.visible_point_reads(), 0);
        assert_eq!(store.batch_reads(), 0);

        store.reset();
        let batch = batch_of([0])?;
        store.provisional_many(&id, Section::new(0), &batch).await?;
        assert_eq!(store.raw_batch_reads(), 1);
        assert_eq!(store.raw_point_reads(), 0);
        assert_eq!(store.batch_reads(), 0);
        Ok(())
    }
}
