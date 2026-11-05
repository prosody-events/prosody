//! A cell store that fails chosen operations on purpose.

use super::*;

/// Which surface a [`FailingCellStore`] poisons, and for what target.
#[derive(Clone)]
pub(crate) enum Poison {
    /// Rejects the marker read before any handler can start.
    MarkerRead(ErrorCategory),
    /// Rejects promotes for one collection with the given error category.
    Collection(StateName, ErrorCategory),
    /// Rejects stage writes for one collection with the given error category.
    WriteProvisional(StateName, ErrorCategory),
    /// Direct-apply path: `write_resolved` fails with the given category for
    /// one named collection — the establish-then-publish test's lower-write
    /// fault (a failed lower write must leave the cache untouched).
    WriteResolved(StateName, ErrorCategory),
    /// Rejects selected coordinates during projected reads.
    /// The default batch read propagates the first coordinate error and
    /// publishes nothing.
    Read(BTreeMap<u8, ErrorCategory>),
}

/// A runtime-armable poison slot shared by a [`FailingCellStore`], its
/// clones, and the trace runner (which arms a settle failure for exactly one
/// settle and disarms after): `None` delegates cleanly.
pub(crate) type PoisonHandle = Arc<parking_lot::Mutex<Option<Poison>>>;

/// Rejects selected stage, promote, direct-write, or read operations.
/// The shared [`PoisonHandle`] selects the operation and error category.
#[derive(Clone)]
pub(crate) struct FailingCellStore<S> {
    inner: S,
    poison: PoisonHandle,
}

impl<S> FailingCellStore<S> {
    /// Wraps `inner`, poisoning `mark_resolved` `Permanent` for every cell of
    /// the `poison` collection.
    pub(crate) fn new(inner: S, poison: StateName) -> Self {
        Self::new_with_category(inner, poison, ErrorCategory::Permanent)
    }

    /// Rejects promotes for `poison` with `category`.
    pub(crate) fn new_with_category(inner: S, poison: StateName, category: ErrorCategory) -> Self {
        Self::armed(inner, Poison::Collection(poison, category))
    }

    /// Wraps `inner`, poisoning `write_provisional` with `category` for the
    /// `poison` collection — the stage path (`mark_resolved` stays healthy).
    pub(crate) fn failing_write_provisional(
        inner: S,
        poison: StateName,
        category: ErrorCategory,
    ) -> Self {
        Self::armed(inner, Poison::WriteProvisional(poison, category))
    }

    /// Rejects reads for each coordinate in `cells` with its assigned error
    /// category.
    pub(crate) fn failing_read(inner: S, cells: BTreeMap<u8, ErrorCategory>) -> Self {
        Self::armed(inner, Poison::Read(cells))
    }

    /// Wraps `inner` around a shared runtime `poison` slot — the trace
    /// runner's constructor, re-wrapping each crash-rebuilt store around one
    /// handle.
    pub(crate) fn with_handle(inner: S, poison: PoisonHandle) -> Self {
        Self { inner, poison }
    }

    fn armed(inner: S, poison: Poison) -> Self {
        Self::with_handle(inner, Arc::new(parking_lot::Mutex::new(Some(poison))))
    }

    /// Arms (`Some`) or disarms (`None`) the shared poison slot for
    /// subsequent ops on every clone sharing the handle.
    pub(crate) fn set_poison(&self, poison: Option<Poison>) {
        *self.poison.lock() = poison;
    }

    fn injected(&self, collection: &CollectionRef, _cells: &[CellKey]) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::Collection(name, category)) if collection.id().name() == name => {
                Some(*category)
            }
            _ => None,
        }
    }

    fn injected_stage(&self, collection: &CollectionRef) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::WriteProvisional(name, category)) if collection.id().name() == name => {
                Some(*category)
            }
            _ => None,
        }
    }

    fn injected_resolved(&self, collection: &CollectionRef) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::WriteResolved(name, category)) if collection.id().name() == name => {
                Some(*category)
            }
            _ => None,
        }
    }

    fn injected_read(&self, cell: CellRef<'_>) -> Option<ErrorCategory> {
        match &*self.poison.lock() {
            Some(Poison::Read(targets)) => targets.get(&cell.coordinate[0]).copied(),
            _ => None,
        }
    }
}

/// Error of a [`FailingCellStore`]: the injected poison (with its category), or
/// a delegated inner error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FailCellError<E>
where
    E: Error + 'static,
{
    /// `mark_resolved` or `write_provisional` touched a poisoned target; the
    /// category is what the wrapper was asked to inject.
    #[error("cell-store poison ({0:?})")]
    Poison(ErrorCategory),

    /// A delegated inner-store error.
    #[error(transparent)]
    Inner(#[from] E),
}

impl<E> ClassifyError for FailCellError<E>
where
    E: ClassifyError + Error + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Poison(category) => *category,
            Self::Inner(e) => e.classify_error(),
        }
    }
}

impl<S: CellBackend> CellBackend for FailingCellStore<S> {
    type Error = FailCellError<S::Error>;
}

impl<S: CellRead<P>, P: Projection> CellRead<P> for FailingCellStore<S> {
    async fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: CellRef<'a>,
    ) -> Result<Durable<P>, Self::Error> {
        {
            if let Some(category) = self.injected_read(cell) {
                return Err(FailCellError::Poison(category));
            }
            CellRead::<P>::read(&self.inner, collection, cell)
                .await
                .map_err(FailCellError::Inner)
        }
    }

    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, S, P> {
        CellRead::<P>::scan(&self.inner, collection, scan)
            .map(|item| item.map_err(FailCellError::Inner))
    }
}

impl<S> CellStore for FailingCellStore<S>
where
    S: CellStore,
{
    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        self.inner
            .provisional_cell_at(collection, cell)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error> {
        // A poisoned collection fails `Ready(Err)` on the first poll (the
        // overlap-precedence test's cell-read leg). Otherwise inherit the inner
        // store's survivors through this wrapper's `provisional_cell_at` (which
        // wraps the inner error).
        provisional_point_loop(self, collection, section, batch).await
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        stage: ProvisionalStage<'a>,
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected_stage(collection) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .write_provisional(collection, stage)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected_resolved(collection) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .write_resolved(collection, cells, clears)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        if let Some(category) = self.injected(collection, cells) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .mark_resolved(collection, cells)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn marker_state<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<MarkerState, Self::Error> {
        if let Some(Poison::MarkerRead(category)) = self.poison.lock().as_ref() {
            return Err(FailCellError::Poison(*category));
        }
        self.inner
            .marker_state(collection)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        marker: &'a EventMarker,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // A settle arriving via the admission's marker leg routes through the inner
        // store's `mark_resolved` on the *inner* store, bypassing the poison on
        // this wrapper's `mark_resolved`. Re-check the poison here against the
        // promoted (present-data) cells so a per-cell promote poison still fires
        // on the batch settle.
        let keeps: Vec<CellKey> = writes
            .iter()
            .filter(|(_, write)| write.data().is_some())
            .map(|(cell, _)| cell.clone())
            .collect();
        if let Some(category) = self.injected(collection, &keeps) {
            return Err(FailCellError::Poison(category));
        }
        self.inner
            .commit_provisional(collection, marker, writes)
            .await
            .map_err(FailCellError::Inner)
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.inner
            .abort_provisional(collection, writes)
            .await
            .map_err(FailCellError::Inner)
    }
}
