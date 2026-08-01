//! Fault-injectable reader stores and the composed environment the probe and
//! refresh suites drive: a scripted cell source, a read-counting identity
//! store, and the deps bundle that wires them together.

use super::owner::{OwnerSession, owner_commit, registry_of, source_state_key};
use super::{mock_count, state_name, subsystem};
use crate::codec::JsonCodec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StateDescriptor;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use crate::state::identity::{CollectionId, StateKey};
use crate::state::memory::MemoryCellStore;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::publication::StatePublication;
use crate::state::registry::CollectionDef;
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::tests::support::{FixedOracle, ScriptedPublicationStore};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::{ReaderComponents, ScriptedReaderBackend};
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::deps::StateReaderDependencies;
use crate::state_reader::{PartitionCount, StateReader};
use crate::subsystem::SubsystemName;
use crate::{Key, SegmentId, Topic};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;

/// [`publish_source`](super::publish_source) against the scripted control-plane
/// stores, freezing an identity that matches `descriptor`.
async fn publish_scripted<D: StateDescriptor>(
    stores: (&ScriptedPublicationStore, &CountingIdentityStore),
    subsystem: &SubsystemName,
    name: &StateName,
    group: &str,
    topic: Topic,
    count: PartitionCount,
    descriptor: &D,
) {
    let (publications, identities) = stores;
    publications
        .seed(
            subsystem,
            StateType::Application,
            name,
            &StatePublication {
                group_id: Arc::from(group),
                topic,
                partition_count: count,
            },
        )
        .await;
    let row = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    identities.seed(group, &row).await;
}

// --- Scripted fault source --------------------------------------------------

/// Where a scripted read faults.
#[derive(Clone, Copy, Debug)]
pub(in crate::state_reader::tests) enum FaultPoint {
    /// Error before any row is read (a point read errors; a scan errors at
    /// stream open).
    AtOpen,
    /// Yield `n` present cells, then error (scan only).
    AfterYields(usize),
}

/// A committed cell source that can fault deterministically per source. It
/// wraps a real [`MemoryCells`] for the actual committed data, seeded
/// through the owner harness. Neither production source can script a
/// mid-stream scan error.
#[derive(Clone, Default)]
pub(crate) struct ScriptedCellSource {
    inner: MemoryCells,
    faults: Arc<scc::HashMap<SegmentId, FaultPoint, RandomState>>,
    /// Per-source committed-read counter — the source-call trace. Cloning
    /// shares it, so a test reads the count after moving the source into a
    /// bundle.
    reads: Arc<scc::HashMap<SegmentId, usize, RandomState>>,
}

impl ScriptedCellSource {
    pub(in crate::state_reader::tests) fn new() -> Self {
        Self::default()
    }

    /// The backing cells, for the owner harness to seed committed values into.
    pub(in crate::state_reader::tests) fn cells(&self) -> MemoryCells {
        self.inner.clone()
    }

    /// Arms a fault for the source addressed by `segment`.
    pub(in crate::state_reader::tests) fn fault_at(&self, segment: SegmentId, fault: FaultPoint) {
        self.faults.upsert_sync(segment, fault);
    }

    fn fault_of(&self, segment: SegmentId) -> Option<FaultPoint> {
        self.faults.read_sync(&segment, |_, fault| *fault)
    }

    /// Committed reads recorded for the source addressed by `segment`. This
    /// mirrors [`CountingIdentityStore::reads`], keyed per source so a test
    /// can assert which sources a probe touched. For example, a scan can pin
    /// one source and never open the decoy.
    pub(in crate::state_reader::tests) fn reads(&self, segment: SegmentId) -> usize {
        self.reads
            .read_sync(&segment, |_, count| *count)
            .unwrap_or(0)
    }

    fn record_read(&self, segment: SegmentId) {
        match self.reads.entry_sync(segment) {
            Entry::Vacant(slot) => {
                slot.insert_entry(1);
            }
            Entry::Occupied(mut entry) => *entry.get_mut() += 1,
        }
    }

    pub(crate) fn read_committed(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let segment = id.state_key().segment_id;
        self.record_read(segment);
        if matches!(self.fault_of(segment), Some(FaultPoint::AtOpen)) {
            return Err(StateAccessError::store(&ScriptedFaultError));
        }
        Ok(self.inner.read_committed(id, cell))
    }

    pub(crate) fn read_committed_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        let segment = id.state_key().segment_id;
        self.record_read(segment);
        if matches!(self.fault_of(segment), Some(FaultPoint::AtOpen)) {
            return Err(StateAccessError::store(&ScriptedFaultError));
        }
        Ok(self.inner.read_committed_many(id, section, batch))
    }

    pub(crate) fn scan_committed<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        let segment = id.state_key().segment_id;
        self.record_read(segment);
        let fault = self.fault_of(segment);
        let inner = self.inner.clone();
        try_stream! {
            if matches!(fault, Some(FaultPoint::AtOpen)) {
                Err(StateAccessError::store(&ScriptedFaultError))?;
            }
            let limit = match fault {
                Some(FaultPoint::AfterYields(n)) => Some(n),
                _ => None,
            };
            let source = inner.scan_committed(id, scan);
            futures::pin_mut!(source);
            let mut yielded = 0usize;
            loop {
                // The budget check happens before pulling the next item, not
                // after. The fault fires after exactly `n` yields, even when
                // the stream would have ended exactly there. No item beyond
                // the nth is ever fetched.
                if limit.is_some_and(|n| yielded >= n) {
                    Err(StateAccessError::store(&ScriptedFaultError))?;
                }
                match source.next().await {
                    // Memory scan errors are `Infallible`.
                    Some(item) => {
                        let (key, value) =
                            item.map_err(|e: Infallible| -> StateAccessError { match e {} })?;
                        yield (key, value);
                        yielded += 1;
                    }
                    None => break,
                }
            }
        }
    }
}

/// A scripted store fault. It always classifies as `Transient`, never
/// `Terminal`, matching the reader layer's posture.
#[derive(Debug, Error)]
#[error("scripted cell-source fault")]
pub(in crate::state_reader::tests) struct ScriptedFaultError;

impl ClassifyError for ScriptedFaultError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

// --- Counting identity store ------------------------------------------------

/// A [`DescriptorIdentityStore`] that counts every `read_identity` call. It
/// tests that an already-admitted source is never re-validated. It wraps a
/// real memory identity store; cloning shares the counter.
#[derive(Clone, Default)]
pub(crate) struct CountingIdentityStore {
    inner: MemoryDescriptorIdentityStore,
    reads: Arc<AtomicUsize>,
}

impl CountingIdentityStore {
    pub(in crate::state_reader::tests) fn new() -> Self {
        Self::default()
    }

    /// Identity reads counted so far.
    pub(in crate::state_reader::tests) fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// Freezes an identity row directly (bypassing the counter).
    pub(in crate::state_reader::tests) async fn seed(
        &self,
        group: &str,
        row: &DurableDescriptorIdentity,
    ) {
        self.inner
            .register_identity(group, row)
            .await
            .unwrap_or_else(|e| match e {});
    }
}

impl DescriptorIdentityStore for CountingIdentityStore {
    type Error = Infallible;

    async fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, Self::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_identity(group_id, state_type, name).await
    }

    async fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> Result<RegisterOutcome, Self::Error> {
        self.inner.register_identity(group_id, row).await
    }
}

/// A scripted bundle: the fault source over its own backing cells, plus the
/// scripted publication store and counting identity store the refresh/probe
/// arms drive.
fn scripted_deps(
    cells: ScriptedCellSource,
    publications: ScriptedPublicationStore,
    identities: CountingIdentityStore,
    cache: ReaderCache,
) -> StateReaderDependencies<JsonCodec, ScriptedReaderBackend> {
    StateReaderDependencies::from_parts(
        ReaderComponents::new(cells, publications, identities, MemoryLoader::new()),
        cache,
    )
}

/// One source's store triple, plus the routing constants and descriptor a
/// probe or refresh test drives it with. It bundles the construction every
/// scripted suite repeats: the stores and registry, per-source seeding
/// through [`Self::commit`] and [`Self::fault`], control-plane advertising
/// through [`Self::publish`], and reader construction through [`Self::deps`]
/// and [`Self::reader_eager`].
///
/// Fields stay public within the module so a test can reach past the
/// builder methods: seed the control-plane stores directly instead of going
/// through [`Self::publish`], or read [`ScriptedCellSource`] directly to
/// assert its source-call trace.
pub(in crate::state_reader::tests) struct ScriptedEnv<D> {
    pub(in crate::state_reader::tests) cells: ScriptedCellSource,
    pub(in crate::state_reader::tests) publications: ScriptedPublicationStore,
    pub(in crate::state_reader::tests) identities: CountingIdentityStore,
    pub(in crate::state_reader::tests) descriptor: D,
    pub(in crate::state_reader::tests) name: StateName,
    pub(in crate::state_reader::tests) sub: SubsystemName,
    pub(in crate::state_reader::tests) count: PartitionCount,
    registry: Arc<CollectionDefRegistry>,
}

impl<D: StateDescriptor> ScriptedEnv<D> {
    /// A fresh scripted env for `descriptor`, registered under its own name.
    /// Every test calls `descriptor.name()` for this, never inventing a name
    /// by hand, so a source's frozen identity always asserts against the
    /// same string.
    pub(in crate::state_reader::tests) fn new(descriptor: D) -> Result<Self> {
        Ok(Self {
            cells: ScriptedCellSource::new(),
            publications: ScriptedPublicationStore::new(),
            identities: CountingIdentityStore::new(),
            registry: registry_of(&descriptor, CollectionDef::new(None))?,
            descriptor,
            name: state_name(descriptor.name())?,
            sub: subsystem()?,
            count: mock_count(),
        })
    }

    /// Seeds one source's committed state for `group`/`tp` through the real
    /// owner session. Returns the segment-qualified state key it wrote
    /// under; the reader recomputes the same key independently.
    pub(in crate::state_reader::tests) async fn commit<F, Fut>(
        &self,
        group: &str,
        tp: Topic,
        key: &Key,
        event: u128,
        ops: F,
    ) -> Result<StateKey>
    where
        F: FnOnce(D::Handle<OwnerSession<MemoryCellStore<FixedOracle>>>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let state_key = source_state_key(tp, group, key, self.count)?;
        owner_commit(
            &self.cells.cells(),
            &self.registry,
            &state_key,
            self.descriptor,
            event,
            ops,
        )
        .await?;
        Ok(state_key)
    }

    /// Arms a fault for `group`/`tp`'s source, returning its state key.
    pub(in crate::state_reader::tests) fn fault(
        &self,
        group: &str,
        tp: Topic,
        key: &Key,
        fault: FaultPoint,
    ) -> Result<StateKey> {
        let state_key = source_state_key(tp, group, key, self.count)?;
        self.cells.fault_at(state_key.segment_id, fault);
        Ok(state_key)
    }

    /// Advertises `group`/`tp` as a source and freezes its identity to match
    /// this env's descriptor, so the reader admits it.
    pub(in crate::state_reader::tests) async fn publish(&self, group: &str, tp: Topic) {
        publish_scripted(
            (&self.publications, &self.identities),
            &self.sub,
            &self.name,
            group,
            tp,
            self.count,
            &self.descriptor,
        )
        .await;
    }

    /// A reader-dep bundle over this env's stores, sharing `cache` (and so its
    /// clock) with every reader built from it.
    pub(in crate::state_reader::tests) fn deps_with_cache(
        &self,
        cache: ReaderCache,
    ) -> StateReaderDependencies<JsonCodec, ScriptedReaderBackend> {
        scripted_deps(
            self.cells.clone(),
            self.publications.clone(),
            self.identities.clone(),
            cache,
        )
    }

    /// An eager reader (refreshes every operation) on a wall-clock cache.
    pub(in crate::state_reader::tests) fn reader_eager(
        &self,
    ) -> Result<StateReader<D, JsonCodec, ScriptedReaderBackend>> {
        self.reader_eager_with_cache(ReaderCache::with_budget(1 << 20))
    }

    /// [`Self::reader_eager`] over an explicit `cache`, so a refresh test can
    /// drive the clock the reader paces its retries on.
    pub(in crate::state_reader::tests) fn reader_eager_with_cache(
        &self,
        cache: ReaderCache,
    ) -> Result<StateReader<D, JsonCodec, ScriptedReaderBackend>> {
        let deps = self.deps_with_cache(cache);
        StateReader::new_eager(&deps, self.sub.clone(), self.descriptor)
            .map_err(|e| eyre!("reader: {e}"))
    }

    /// A reader over an explicit `cache` with a non-zero refresh interval.
    /// The sticky-mismatch test drives its cached-snapshot fast path through
    /// this.
    pub(in crate::state_reader::tests) fn reader_with_interval(
        &self,
        cache: ReaderCache,
        refresh_interval: Duration,
    ) -> Result<StateReader<D, JsonCodec, ScriptedReaderBackend>> {
        let deps = self.deps_with_cache(cache);
        StateReader::with_refresh_interval(
            &deps,
            self.sub.clone(),
            self.descriptor,
            refresh_interval,
        )
        .map_err(|e| eyre!("reader: {e}"))
    }
}
