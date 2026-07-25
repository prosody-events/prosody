//! Shared scaffolding for the reader suites.
//!
//! Three things live here. The owner-write harness seeds committed state,
//! and for the window arm provisional state, through the real
//! [`KeyedStateSession`]. The closed [`ReaderStores::Scripted`] arm is backed
//! by [`ScriptedCellSource`] and [`CountingIdentityStore`]. The bundle and
//! reader builders compose readers for the suites.
//!
//! Committed state is never hand-written at a cell address. It always flows
//! through the owner session, so the reader reads exactly what the owner
//! wrote, under the segment that [`partition_segment_id`] computes.

use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::segment::partition_segment_id;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StateDescriptor;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use crate::state::identity::{CollectionId, StateKey};
use crate::state::manager::ArmedKeys;
use crate::state::memory::{
    MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore,
};
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::sealed::{ApplyOutcome, StateLifecycle};
use crate::state::session::{Finalized, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::{CellBuffer, CellStore, CoordinateBatch};
use crate::state::tests::support::{FixedOracle, ScriptedPublicationStore, probe};
use crate::state::{EventRef, PartitionBackend, StateName, StateType};
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::stores::ReaderStores;
use crate::state_reader::{PartitionCount, StateReader, StateReaderError, partition_for_key};
use crate::subsystem::SubsystemName;
use crate::timers::duration::CompactDuration;
use crate::{Key, SegmentId, Topic};
use ahash::RandomState;
use async_stream::try_stream;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::{Stream, StreamExt, TryStreamExt};
use internment::Intern;
use quanta::{Clock, Mock};
use scc::hash_map::Entry;
use serde_json::Value;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;

/// The subsystem every suite routes under.
pub(super) const SUBSYSTEM: &str = "orders";

/// A distinct-source key space: two groups, lexicographically ordered so
/// `GROUP_A` is the deterministic lowest source and `GROUP_B` the decoy.
pub(super) const GROUP_A: &str = "group-aaa";
pub(super) const GROUP_B: &str = "group-zzz";

/// The mock topology's fixed partition count.
pub(crate) fn mock_count() -> PartitionCount {
    PartitionCount::MOCK
}

/// The subsystem name.
pub(super) fn subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))
}

/// A collection state name.
pub(crate) fn state_name(name: &str) -> Result<StateName> {
    StateName::try_new(name).map_err(|e| eyre!("name: {e}"))
}

/// An interned topic.
pub(crate) fn topic(name: &str) -> Topic {
    Intern::<str>::from(name)
}

// --- Owner-write harness ----------------------------------------------------

/// The owner backend the seeding session runs over, generic over the cell
/// store `C`. The oracle is always the fixed committed one: a pure seed
/// never resolves a foreign provisional. The identity type is phantom: the
/// session never reads it. See [`SessionParts`] for why one type serves
/// every backend. Only `C` varies: [`MemoryCellStore`] for the memory
/// reader, `CassandraStore<FixedOracle>` for the live-Cassandra reader.
pub(super) type OwnerBackend<C> = PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, C>;

/// The real per-event session the seeding handles bind over, generic over
/// cell store `C`.
pub(super) type OwnerSession<C> = KeyedStateSession<OwnerBackend<C>, MemoryLoader<Value>>;

/// A registry with `descriptor` registered under `def`.
pub(crate) fn registry_of<D: StateDescriptor>(
    descriptor: &D,
    def: CollectionDef,
) -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::default();
    registry
        .register(descriptor, def)
        .map_err(|e| eyre!("register: {e}"))?;
    Ok(Arc::new(registry))
}

/// The segment the owner writes under for one source, which the reader also
/// recomputes independently. Tests call this instead of hand-building a
/// segment id.
pub(crate) fn source_state_key(
    topic: Topic,
    group: &str,
    key: &Key,
    count: PartitionCount,
) -> Result<StateKey> {
    let partition =
        partition_for_key(key.as_bytes(), count).map_err(|e| eyre!("partition: {e}"))?;
    Ok(StateKey::new(
        partition_segment_id(topic, partition, group),
        key.clone(),
    ))
}

/// A fresh owner session over cell store `cell` for one event.
fn owner_session<C: CellStore>(
    cell: C,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
) -> OwnerSession<C> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<OwnerBackend<C>, _> {
        cell,
        dirty: Arc::default(),
        oracle: FixedOracle::committed(),
        loader: MemoryLoader::new(),
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        recovery_delay: CompactDuration::new(30),
        armed: ArmedKeys::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    })
}

/// Finalizes and promotes: the event's staged cells become committed. This
/// is the full owner settle for a committed write. Promotion calls the
/// store's settlement verb directly; it never consults the oracle. It
/// returns `Resolved` on any healthy store, memory or Cassandra. A
/// non-`Resolved` outcome is a real failure the seed must surface.
async fn promote<C: CellStore>(session: OwnerSession<C>) -> Result<()> {
    if let Finalized::Staged(staged) = session
        .finalize()
        .await
        .map_err(|e| eyre!("finalize: {e}"))?
        && staged.certify().promote().await != ApplyOutcome::Resolved
    {
        bail!("promote incomplete on a healthy store");
    }
    Ok(())
}

/// Finalizes without promoting: the staged provisional cells are durable,
/// but the committed value stays at its prior contents. This is the
/// commit-to-promote window; a cross-group reader must read `prev` from it.
/// Dropping the receipt leaves the write in the "committed step not yet
/// applied" state.
async fn stage_only<C: CellStore>(session: OwnerSession<C>) -> Result<()> {
    let _staged = session
        .finalize()
        .await
        .map_err(|e| eyre!("finalize: {e}"))?;
    Ok(())
}

/// Binds `descriptor` to a fresh owner session over `cell`, runs `ops` against
/// the handle, then commits (promote). The backend-generic seeding primitive:
/// the memory and Cassandra reader backends both write committed state through
/// it, only their `cell` differing.
pub(super) async fn owner_commit_cell<C, D, F, Fut>(
    cell: C,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    descriptor: D,
    event: u128,
    ops: F,
) -> Result<()>
where
    C: CellStore,
    D: StateDescriptor,
    F: FnOnce(D::Handle<OwnerSession<C>>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let session = owner_session(cell, registry, state_key, probe(event));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    ops(handle).await?;
    promote(session).await
}

/// [`owner_commit_cell`] over an in-memory cell store. This is the
/// memory-backed seeding entry point the scripted probe and refresh suites
/// write through.
pub(crate) async fn owner_commit<D, F, Fut>(
    cells: &MemoryCells,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    descriptor: D,
    event: u128,
    ops: F,
) -> Result<()>
where
    D: StateDescriptor,
    F: FnOnce(D::Handle<OwnerSession<MemoryCellStore<FixedOracle>>>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let cell = MemoryCellStore::new(cells.clone(), FixedOracle::committed(), registry.clone());
    owner_commit_cell(cell, registry, state_key, descriptor, event, ops).await
}

/// [`owner_commit`], but stops after staging and never promotes. Tests use
/// it to drive reads of provisional state.
pub(super) async fn owner_stage<D, F, Fut>(
    cells: &MemoryCells,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    descriptor: D,
    event: u128,
    ops: F,
) -> Result<()>
where
    D: StateDescriptor,
    F: FnOnce(D::Handle<OwnerSession<MemoryCellStore<FixedOracle>>>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let cell = MemoryCellStore::new(cells.clone(), FixedOracle::committed(), registry.clone());
    let session = owner_session(cell, registry, state_key, probe(event));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    ops(handle).await?;
    stage_only(session).await
}

// --- Control-plane seeding --------------------------------------------------

/// Upserts `publication` as a source of `name` and freezes `identity` against
/// the memory control-plane stores (both `Infallible`, so the swallow is
/// total). The shared core of [`publish_source`] and
/// [`MemoryReaderBackend::publish`].
async fn seed_memory_publication(
    publications: &MemoryPublicationStore,
    identities: &MemoryDescriptorIdentityStore,
    subsystem: &SubsystemName,
    name: &StateName,
    publication: &StatePublication,
    identity: &DurableDescriptorIdentity,
) {
    publications
        .upsert(subsystem, StateType::Application, name, publication)
        .await
        .unwrap_or_else(|e| match e {});
    identities
        .register_identity(&publication.group_id, identity)
        .await
        .unwrap_or_else(|e| match e {});
}

/// Advertises `(group, topic)` as a source of `name` and freezes its identity
/// to match `descriptor`, so the reader admits it. The identity row is derived
/// from the same descriptor the reader carries, so acquisition validates equal.
pub(crate) async fn publish_source<D: StateDescriptor>(
    stores: (&MemoryPublicationStore, &MemoryDescriptorIdentityStore),
    subsystem: &SubsystemName,
    name: &StateName,
    group: &str,
    topic: Topic,
    count: PartitionCount,
    descriptor: &D,
) {
    let (publications, identities) = stores;
    let publication = StatePublication {
        group_id: Arc::from(group),
        topic,
        partition_count: count,
    };
    let row = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    seed_memory_publication(
        publications,
        identities,
        subsystem,
        name,
        &publication,
        &row,
    )
    .await;
}

/// [`publish_source`] against the scripted control-plane stores (the probe and
/// refresh suites), freezing an identity that matches `descriptor`.
pub(super) async fn publish_scripted<D: StateDescriptor>(
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

// --- Memory reader bundle ---------------------------------------------------

/// The shared handles the memory suites hold: the stores the owner writes into
/// and the reader reads from, plus the publication and identity control-plane
/// stores.
#[derive(Clone)]
pub(super) struct MemoryHarness {
    pub(super) cells: MemoryCells,
    pub(super) publications: MemoryPublicationStore,
    pub(super) identities: MemoryDescriptorIdentityStore,
}

impl MemoryHarness {
    pub(super) fn new() -> Self {
        Self {
            cells: MemoryCells::new(),
            publications: MemoryPublicationStore::new(),
            identities: MemoryDescriptorIdentityStore::new(),
        }
    }

    /// A shared-deps bundle over these handles with a wall-clock cache.
    pub(super) fn deps(&self, budget: u64) -> SharedDeps<JsonCodec> {
        SharedDeps::memory(
            "reader-test".to_owned(),
            Duration::from_secs(30),
            self.cells.clone(),
            self.publications.clone(),
            self.identities.clone(),
            MemoryLoader::new(),
            budget,
        )
    }
}

// --- Backend-generic reader seam --------------------------------------------

/// The seam that `reader_suite::run_reader_*_trace` drives. One runner body
/// proves that committed state matches the oracle for both the memory
/// reader and a live Cassandra reader. An implementation supplies the three
/// pieces that differ by backend: the owner-seed cell store, seeded through
/// the real [`KeyedStateSession`] via [`owner_commit_cell`]; the
/// control-plane seeding; and the reader's `deps` bundle.
pub(super) trait ReaderBackend {
    /// The owner-seed cell store: [`MemoryCellStore`] for memory, the shared
    /// `CassandraStore<FixedOracle>` for Cassandra.
    type OwnerCell: CellStore;

    /// The registry the sessions and the owner cell store share.
    fn registry(&self) -> Arc<CollectionDefRegistry>;

    /// A cell store to seed one event through. Cloning shares the committed
    /// backing, memory cells or Cassandra rows, across a trace's events. On
    /// Cassandra, cloning also shares the one `MarkerMemo`/`MarkerPresence`
    /// lifecycle the store owns.
    fn owner_cell(&self) -> Self::OwnerCell;

    /// Advertises `(group, topic)` as a source of `name` and freezes `identity`
    /// so the reader admits it.
    async fn publish(
        &self,
        subsystem: &SubsystemName,
        name: &StateName,
        group: &str,
        topic: Topic,
        count: PartitionCount,
        identity: &DurableDescriptorIdentity,
    ) -> Result<()>;

    /// A fresh reader bundle over this backend's stores. Each call gets a
    /// fresh cache, so a per-event reader observes current committed state
    /// instead of a stale one.
    fn deps(&self) -> SharedDeps<JsonCodec>;
}

/// The memory [`ReaderBackend`]: a fresh [`MemoryHarness`] plus a registry
/// carrying the trace's single per-kind def.
pub(super) struct MemoryReaderBackend {
    harness: MemoryHarness,
    registry: Arc<CollectionDefRegistry>,
}

impl MemoryReaderBackend {
    /// A backend registering `descriptor` under `def`.
    pub(super) fn new<D: StateDescriptor>(descriptor: &D, def: CollectionDef) -> Result<Self> {
        Ok(Self {
            harness: MemoryHarness::new(),
            registry: registry_of(descriptor, def)?,
        })
    }
}

impl ReaderBackend for MemoryReaderBackend {
    type OwnerCell = MemoryCellStore<FixedOracle>;

    fn registry(&self) -> Arc<CollectionDefRegistry> {
        self.registry.clone()
    }

    fn owner_cell(&self) -> Self::OwnerCell {
        MemoryCellStore::new(
            self.harness.cells.clone(),
            FixedOracle::committed(),
            self.registry.clone(),
        )
    }

    async fn publish(
        &self,
        subsystem: &SubsystemName,
        name: &StateName,
        group: &str,
        topic: Topic,
        count: PartitionCount,
        identity: &DurableDescriptorIdentity,
    ) -> Result<()> {
        let publication = StatePublication {
            group_id: Arc::from(group),
            topic,
            partition_count: count,
        };
        seed_memory_publication(
            &self.harness.publications,
            &self.harness.identities,
            subsystem,
            name,
            &publication,
            identity,
        )
        .await;
        Ok(())
    }

    fn deps(&self) -> SharedDeps<JsonCodec> {
        self.harness.deps(1 << 20)
    }
}

// --- Scripted fault source --------------------------------------------------

/// Where a scripted read faults.
#[derive(Clone, Copy, Debug)]
pub(super) enum FaultPoint {
    /// Error before any row is read (a point read errors; a scan errors at
    /// stream open).
    AtOpen,
    /// Yield `n` present cells, then error (scan only).
    AfterYields(usize),
}

/// A committed cell source that can fault deterministically per source. It
/// wraps a real [`MemoryCells`] for the actual committed data, seeded
/// through the owner harness. The closed [`ReaderStores::Scripted`] arm
/// carries it because neither production arm can script a mid-stream scan
/// error: Memory is `Infallible`, and Cassandra needs a live cluster.
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
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// The backing cells, for the owner harness to seed committed values into.
    pub(super) fn cells(&self) -> MemoryCells {
        self.inner.clone()
    }

    /// Arms a fault for the source addressed by `segment`.
    pub(super) fn fault_at(&self, segment: SegmentId, fault: FaultPoint) {
        self.faults.upsert_sync(segment, fault);
    }

    fn fault_of(&self, segment: SegmentId) -> Option<FaultPoint> {
        self.faults.read_sync(&segment, |_, fault| *fault)
    }

    /// Committed reads recorded for the source addressed by `segment`. This
    /// mirrors [`CountingIdentityStore::reads`], keyed per source so a test
    /// can assert which sources a probe touched. For example, a scan can pin
    /// one source and never open the decoy.
    pub(super) fn reads(&self, segment: SegmentId) -> usize {
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
pub(super) struct ScriptedFaultError;

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
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Identity reads counted so far.
    pub(super) fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// Freezes an identity row directly (bypassing the counter).
    pub(super) async fn seed(&self, group: &str, row: &DurableDescriptorIdentity) {
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
pub(super) fn scripted_deps(
    cells: ScriptedCellSource,
    publications: ScriptedPublicationStore,
    identities: CountingIdentityStore,
    cache: ReaderCache,
) -> SharedDeps<JsonCodec> {
    SharedDeps::from_parts(
        ReaderStores::Scripted {
            cells,
            publications,
            identities,
        },
        ReaderLoader::Memory(MemoryLoader::new()),
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
pub(super) struct ScriptedEnv<D> {
    pub(super) cells: ScriptedCellSource,
    pub(super) publications: ScriptedPublicationStore,
    pub(super) identities: CountingIdentityStore,
    pub(super) descriptor: D,
    pub(super) name: StateName,
    pub(super) sub: SubsystemName,
    pub(super) count: PartitionCount,
    registry: Arc<CollectionDefRegistry>,
}

impl<D: StateDescriptor> ScriptedEnv<D> {
    /// A fresh scripted env for `descriptor`, registered under its own name.
    /// Every test calls `descriptor.name()` for this, never inventing a name
    /// by hand, so a source's frozen identity always asserts against the
    /// same string.
    pub(super) fn new(descriptor: D) -> Result<Self> {
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
    pub(super) async fn commit<F, Fut>(
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
    pub(super) fn fault(
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
    pub(super) async fn publish(&self, group: &str, tp: Topic) {
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
    pub(super) fn deps_with_cache(&self, cache: ReaderCache) -> SharedDeps<JsonCodec> {
        scripted_deps(
            self.cells.clone(),
            self.publications.clone(),
            self.identities.clone(),
            cache,
        )
    }

    /// An eager reader (refreshes every operation) on a wall-clock cache.
    pub(super) fn reader_eager(&self) -> Result<StateReader<D, JsonCodec>> {
        self.reader_eager_with_cache(ReaderCache::with_budget(1 << 20))
    }

    /// [`Self::reader_eager`] over an explicit `cache`, so a refresh test can
    /// drive the clock the reader paces its retries on.
    pub(super) fn reader_eager_with_cache(
        &self,
        cache: ReaderCache,
    ) -> Result<StateReader<D, JsonCodec>> {
        let deps = self.deps_with_cache(cache);
        StateReader::new_eager(&deps, self.sub.clone(), self.descriptor)
            .map_err(|e| eyre!("reader: {e}"))
    }

    /// A reader over an explicit `cache` with a non-zero refresh interval.
    /// The sticky-mismatch test drives its cached-snapshot fast path through
    /// this.
    pub(super) fn reader_with_interval(
        &self,
        cache: ReaderCache,
        refresh_interval: Duration,
    ) -> Result<StateReader<D, JsonCodec>> {
        let deps = self.deps_with_cache(cache);
        StateReader::new_with_interval(&deps, self.sub.clone(), self.descriptor, refresh_interval)
            .map_err(|e| eyre!("reader: {e}"))
    }
}

/// A cache with a mocked clock over `budget` declared bytes, returning the
/// [`Mock`] handle the test advances (never a sleep). The mock starts at zero
/// and only moves forward, mirroring the monotonic clock production uses.
pub(super) fn mock_clock_cache(budget: u64) -> (ReaderCache, Arc<Mock>) {
    let (clock, mock) = Clock::mock();
    (ReaderCache::with_clock(budget, clock), mock)
}

/// Collects a fallible reader stream into a `Vec`, surfacing the first error.
pub(super) async fn collect_stream<T>(
    stream: impl Stream<Item = Result<T, StateReaderError>>,
) -> Result<Vec<T>> {
    Ok(stream.try_collect().await?)
}
