//! Owner-session construction, and the committed or provisional state seeding
//! that runs through the real keyed-state lifecycle.

use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::segment::partition_segment_id;
use crate::state::descriptor::StateDescriptor;
use crate::state::identity::StateKey;
use crate::state::manager::ArmedKeys;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::sealed::{ApplyOutcome, StateLifecycle};
use crate::state::session::{Finalized, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::tests::support::{FixedOracle, probe};
use crate::state::{EventRef, PartitionBackend};
use crate::state_reader::PartitionCount;
use crate::state_reader::partition_for_key;
use crate::timers::duration::CompactDuration;
use crate::{Key, Topic};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;

// --- Owner-write harness ----------------------------------------------------

/// The owner backend the seeding session runs over, generic over the cell
/// store `C`. The oracle is always the fixed committed one: a pure seed
/// never resolves a foreign provisional. The identity type is phantom: the
/// session never reads it. See [`SessionParts`] for why one type serves
/// every backend. Only `C` varies: [`MemoryCellStore`] for the memory
/// reader, `CassandraStore<FixedOracle>` for the live-Cassandra reader.
pub(in crate::state_reader::tests) type OwnerBackend<C> =
    PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, C>;

/// The real per-event session the seeding handles bind over, generic over
/// cell store `C`.
pub(in crate::state_reader::tests) type OwnerSession<C> =
    KeyedStateSession<OwnerBackend<C>, MemoryLoader<Value>>;

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
pub(in crate::state_reader::tests) async fn owner_commit_cell<C, D, F, Fut>(
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
pub(in crate::state_reader::tests) async fn owner_stage<D, F, Fut>(
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
