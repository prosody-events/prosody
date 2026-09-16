//! Store and resolver counters used by query-budget tests.

use super::*;
use crate::state::cell::Values;
use crate::state::marker::MarkerState;
use crate::state::store::CellRead;
use crate::state::store::CommittedBatch;
use std::num::NonZeroUsize;

#[derive(Clone)]
pub(crate) struct CountingCellStore<S> {
    inner: S,
    counts: Arc<OpCounts>,
    marker_counts: Arc<[(CollectionId, AtomicUsize)]>,
}

#[derive(Default)]
pub(crate) struct OpCounts {
    write_provisional: AtomicUsize,
    write_resolved: AtomicUsize,
    mark_resolved: AtomicUsize,
    commit_provisional: AtomicUsize,
    abort_provisional: AtomicUsize,
    marker_state: AtomicUsize,
    value_reads: AtomicUsize,
    value_batches: AtomicUsize,
    presence_reads: AtomicUsize,
    value_scans: AtomicUsize,
    presence_scans: AtomicUsize,
    provisional_cell_at: AtomicUsize,
    provisional_many: AtomicUsize,
    batch_width: AtomicUsize,
    scan_hint: AtomicUsize,
    scan_limit: AtomicUsize,
}

pub(crate) trait CountProjection: Projection {
    fn point(counts: &OpCounts) -> &AtomicUsize;
    fn batch(counts: &OpCounts) -> &AtomicUsize;
    fn scan(counts: &OpCounts) -> &AtomicUsize;
}
impl CountProjection for Values {
    fn point(counts: &OpCounts) -> &AtomicUsize {
        &counts.value_reads
    }

    fn batch(counts: &OpCounts) -> &AtomicUsize {
        &counts.value_batches
    }

    fn scan(counts: &OpCounts) -> &AtomicUsize {
        &counts.value_scans
    }
}
impl CountProjection for Presence {
    fn point(counts: &OpCounts) -> &AtomicUsize {
        &counts.presence_reads
    }

    fn batch(counts: &OpCounts) -> &AtomicUsize {
        &counts.presence_reads
    }

    fn scan(counts: &OpCounts) -> &AtomicUsize {
        &counts.presence_scans
    }
}

impl<S> CountingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
            marker_counts: Arc::default(),
        }
    }

    /// Counts each marker in a fixed collection set for this test.
    pub(crate) fn with_marker_counts(mut self, collections: &[CollectionRef]) -> Self {
        self.marker_counts = collections
            .iter()
            .map(|collection| (collection.id().clone(), AtomicUsize::new(0)))
            .collect();
        self
    }

    pub(crate) fn marker_reads_for(&self, collection: &CollectionId) -> usize {
        self.marker_counts
            .iter()
            .find(|(id, _)| id == collection)
            .map_or(0, |(_, count)| count.load(Ordering::Relaxed))
    }

    pub(crate) fn durable_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
            + self.counts.write_resolved.load(Ordering::Relaxed)
            + self.counts.mark_resolved.load(Ordering::Relaxed)
            + self.counts.commit_provisional.load(Ordering::Relaxed)
            + self.counts.abort_provisional.load(Ordering::Relaxed)
    }

    pub(crate) fn marker_reads(&self) -> usize {
        self.counts.marker_state.load(Ordering::Relaxed)
    }

    pub(crate) fn lower_reads(&self) -> usize {
        self.counts.value_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn visible_point_reads(&self) -> usize {
        self.counts.value_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn batch_reads(&self) -> usize {
        self.counts.value_batches.load(Ordering::Relaxed)
    }

    pub(crate) fn batch_cache_reads(&self) -> usize {
        self.counts.value_batches.load(Ordering::Relaxed)
    }

    pub(crate) fn lower_scans(&self) -> usize {
        self.counts.value_scans.load(Ordering::Relaxed)
    }

    pub(crate) fn presence_scans(&self) -> usize {
        self.counts.presence_scans.load(Ordering::Relaxed)
    }

    pub(crate) fn presence_reads(&self) -> usize {
        self.counts.presence_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn raw_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    pub(crate) fn raw_batch_reads(&self) -> usize {
        self.counts.provisional_many.load(Ordering::Relaxed)
    }

    pub(crate) fn batch_width(&self) -> usize {
        self.counts.batch_width.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_hint(&self) -> usize {
        self.counts.scan_hint.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_limit(&self) -> usize {
        self.counts.scan_limit.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        for (_, count) in self.marker_counts.iter() {
            count.store(0, Ordering::Relaxed);
        }
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
        self.counts.commit_provisional.store(0, Ordering::Relaxed);
        self.counts.abort_provisional.store(0, Ordering::Relaxed);
        self.counts.marker_state.store(0, Ordering::Relaxed);
        self.counts.value_reads.store(0, Ordering::Relaxed);
        self.counts.value_batches.store(0, Ordering::Relaxed);
        self.counts.presence_reads.store(0, Ordering::Relaxed);
        self.counts.value_scans.store(0, Ordering::Relaxed);
        self.counts.presence_scans.store(0, Ordering::Relaxed);
        self.counts.provisional_cell_at.store(0, Ordering::Relaxed);
        self.counts.provisional_many.store(0, Ordering::Relaxed);
        self.counts.batch_width.store(0, Ordering::Relaxed);
        self.counts.scan_hint.store(0, Ordering::Relaxed);
        self.counts.scan_limit.store(0, Ordering::Relaxed);
    }
}

impl<S: CellBackend> CellBackend for CountingCellStore<S> {
    type Error = S::Error;
}

impl<S: CellRead<P>, P: CountProjection> CellRead<P> for CountingCellStore<S> {
    async fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Durable<P>, Self::Error> {
        {
            P::point(&self.counts).fetch_add(1, Ordering::Relaxed);
            CellRead::<P>::read(&self.inner, collection, cell).await
        }
    }

    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + 'a {
        {
            P::scan(&self.counts).fetch_add(1, Ordering::Relaxed);
            self.counts.scan_hint.store(
                scan.fetch_hint.map_or(0, NonZeroUsize::get),
                Ordering::Relaxed,
            );
            self.counts
                .scan_limit
                .store(scan.limit.unwrap_or(0), Ordering::Relaxed);
            CellRead::<P>::scan(&self.inner, collection, scan)
        }
    }

    async fn read_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CacheBatch<P>, Self::Error> {
        P::batch(&self.counts).fetch_add(1, Ordering::Relaxed);
        self.counts
            .batch_width
            .store(batch.len(), Ordering::Relaxed);
        CellRead::<P>::read_many(&self.inner, collection, section, batch).await
    }
}

impl<S: CellStore> CellStore for CountingCellStore<S> {
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

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        self.counts.marker_state.fetch_add(1, Ordering::Relaxed);
        if let Some((_, count)) = self.marker_counts.iter().find(|(id, _)| id == collection) {
            count.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.marker_state(collection).await
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.counts
            .commit_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .commit_provisional(collection, marker, writes)
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

    #[tokio::test]
    async fn counters_increment_once_per_store_call() -> Result<()> {
        let store = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
        let id = fresh_collection("counter-probe")?;
        let cell = CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(vec![0]),
        };

        store.reset();
        CellRead::<Values>::read(&store, &id, &cell).await?;
        assert_eq!(store.visible_point_reads(), 1);
        assert_eq!(store.batch_reads(), 0);
        assert_eq!(store.raw_point_reads(), 0);

        store.reset();
        let batch = batch_of([0])?;
        CellRead::<Values>::read_many(&store, &id, Section::new(0), &batch)
            .await
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(committed, _)| committed)
                    .collect::<CommittedBatch>()
            })?;
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
