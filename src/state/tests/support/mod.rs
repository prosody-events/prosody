//! Shared test doubles and fixture constructors for the keyed-state suites
//! and the consumer tests that mount them. The backend-generic suite
//! runners and their trace types stay in `cell_suite`/`collection_suite`/
//! `identity_suite`; this module holds the standalone doubles they don't own.

use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Scan, Section};
use crate::state::collection::{MutationJournal, StateSession, WritableStateSession, sealed};
use crate::state::descriptor::{CellResolver, StructuralIdentity};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::MemoryPublicationStore;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::oracle::CommitOracle;
use crate::state::publication::{PublicationRows, PublicationStore, StatePublication};
use crate::state::registry::CollectionDef;
use crate::state::session::sealed::{MarkerIdentity, StateLifecycle};
use crate::state::session::{Finalized, MessageMarker, OpPermit, SessionGate};
use crate::state::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, provisional_point_loop,
};
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, EventRef, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::subsystem::SubsystemName;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::stream::{self, Stream};
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::convert::Infallible;
use std::fmt;
use std::future::{Future, ready};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

mod counting;
mod holding;
mod publication;
mod ttl;

pub(crate) use counting::{CountingCellStore, CountingResolver, ResolveCounter};
pub(crate) use holding::{HoldingCellStore, Holds};
pub(crate) use publication::{ParkedRead, ScriptedPublicationStore};
pub(crate) use ttl::TtlStub;

/// Get-out-of-the-way commit oracle: `record_message` is a no-op and every
/// event resolves to the one fixed decision. Use it where the test is not
/// about commit resolution; the commit-tracking double is
/// [`ScriptedOracle`](super::cell_suite::ScriptedOracle).
#[derive(Clone)]
pub struct FixedOracle(CommitDecision);

impl FixedOracle {
    pub(crate) fn committed() -> Self {
        Self(CommitDecision::Committed)
    }

    pub(crate) fn not_committed() -> Self {
        Self(CommitDecision::NotCommitted)
    }
}

impl CommitOracle for FixedOracle {
    type Error = Infallible;

    fn record_message(
        &self,
        _dedup_id: Uuid,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a {
        ready(Ok(self.0))
    }
}

/// A commit oracle counting every `resolve` consult — the no-oracle tests'
/// probe: a verb that must never resolve leaves the counter at zero.
/// `record_message` is a no-op; `resolve` bumps and returns a fixed
/// `NotCommitted`.
#[derive(Clone, Default)]
pub(crate) struct CountingOracle(Arc<AtomicUsize>);

impl CountingOracle {
    /// Resolutions counted so far.
    pub(crate) fn resolves(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl CommitOracle for CountingOracle {
    type Error = Infallible;

    fn record_message(
        &self,
        _dedup_id: Uuid,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> + Send + 'a {
        self.0.fetch_add(1, Ordering::Relaxed);
        ready(Ok(CommitDecision::NotCommitted))
    }
}

/// Stateless session stub: every state op reports
/// [`StateAccessError::Unavailable`] and the lifecycle is inert. Mounted by
/// contexts that carry no keyed state, so a bind against them fails Permanent.
#[derive(Clone)]
pub struct UnavailableState<P> {
    loader: MemoryLoader<P>,
    /// Inert but present: the sealed lifecycle requires a gate accessor.
    gate: Arc<SessionGate>,
}

impl<P> UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    /// Creates the stateless stub.
    #[must_use]
    pub fn new() -> Self {
        Self {
            loader: MemoryLoader::new(),
            gate: Arc::new(SessionGate::new()),
        }
    }
}

impl<P> Default for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<P> fmt::Debug for UnavailableState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnavailableState")
    }
}

/// The stateless stub's engine. Every command is unreachable by construction:
/// `verify_registration` refuses before any operation can open over the stub.
///
/// `pub` for the same reason `OwnerEngine` is: it is the value of the sealed
/// `Session::Engine` associated type. The test module's own visibility is the
/// seal.
pub struct UnavailableEngine;

/// The stub's write state. `begin_write` always refuses, so one is never built;
/// the type is uninhabited, so it cannot be built any other way either.
pub enum NoWrite {}

impl Deref for NoWrite {
    type Target = ();

    fn deref(&self) -> &() {
        match *self {}
    }
}

impl DerefMut for NoWrite {
    fn deref_mut(&mut self) -> &mut () {
        match *self {}
    }
}

impl<P> sealed::ReadEngine<UnavailableState<P>> for UnavailableEngine
where
    P: Clone + Send + Sync + 'static,
{
    type Plan = ();
    type ReadInner<'a> = ();

    fn verify_registration(
        _session: &UnavailableState<P>,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn collection_def(
        _session: &UnavailableState<P>,
        _state_type: StateType,
        _name: &StateName,
    ) -> CollectionDef {
        // Unreachable: a bind against the stub refuses first. `CollectionDef`'s
        // own defaults keep the engine total.
        CollectionDef::new(None)
    }

    async fn begin_read(_session: &UnavailableState<P>) {}

    fn read_point(
        _session: &UnavailableState<P>,
        _inner: &mut Self::ReadInner<'_>,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send {
        ready(Err(StateAccessError::Unavailable))
    }

    fn read_batch(
        _session: &UnavailableState<P>,
        _inner: &mut Self::ReadInner<'_>,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
        _batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<Option<Bytes>>, StateAccessError>> + Send {
        ready(Err(StateAccessError::Unavailable))
    }

    fn capture((): &()) {}

    async fn resume(_session: &UnavailableState<P>, (): &()) {}

    fn page<'a>(
        _session: &'a UnavailableState<P>,
        (): &'a (),
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
    }

    fn fence(_session: &UnavailableState<P>) -> Result<(), StateAccessError> {
        Ok(())
    }
}

impl<P> sealed::WriteEngine<UnavailableState<P>> for UnavailableEngine
where
    P: Clone + Send + Sync + 'static,
{
    type WriteInner<'a> = NoWrite;

    fn begin_write(
        _session: &UnavailableState<P>,
    ) -> impl Future<Output = Result<NoWrite, StateAccessError>> + Send {
        ready(Err(StateAccessError::Unavailable))
    }

    fn validate_write(
        _session: &UnavailableState<P>,
        _inner: &NoWrite,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn apply(
        _session: &UnavailableState<P>,
        _state_type: StateType,
        _name: &StateName,
        _inner: &NoWrite,
        _journal: MutationJournal,
    ) {
    }

    fn commit(
        _session: &UnavailableState<P>,
        _state_type: StateType,
        _name: &StateName,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send {
        ready(Err(StateAccessError::Unavailable))
    }

    fn rollback(
        _session: &UnavailableState<P>,
        _state_type: StateType,
        _name: &StateName,
    ) -> impl Future<Output = StoreOutcome> + Send {
        // Stateless: nothing is ever buffered, so the discard is a NoOp.
        ready(StoreOutcome::NoOp)
    }
}

impl<P> sealed::Session for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Engine = UnavailableEngine;
}

impl<P> sealed::WritableSession for UnavailableState<P> where P: Clone + Send + Sync + 'static {}

impl<P> StateSession for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Loader = MemoryLoader<P>;

    fn loader(&self) -> &MemoryLoader<P> {
        &self.loader
    }
}

impl<P> WritableStateSession for UnavailableState<P> where P: Clone + Send + Sync + 'static {}

impl<P> StateLifecycle for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Cell = MemoryCellStore<FixedOracle>;

    fn sweep(&self) -> impl Future<Output = Result<(), StateAccessError>> + Send {
        ready(Ok(()))
    }

    fn gate(&self) -> &SessionGate {
        &self.gate
    }

    async fn close_gate(&self) -> OpPermit<'_> {
        // Inert session: nothing contends, so no wait tags are needed.
        self.gate.close(|_waited_s| {}).await
    }

    fn finalize(
        &self,
    ) -> impl Future<Output = Result<Finalized<Self::Cell>, StateAccessError>> + Send {
        ready(Ok(Finalized::Clean))
    }

    fn record_marker(
        &self,
        _marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send {
        ready(Ok(()))
    }

    fn discard_dirty(&self) {}

    fn terminate(&self) {}

    async fn reset(&self, _proof: RepinProof) {}

    fn repin(&self, _proof: RepinProof) -> Self {
        self.clone()
    }

    fn recovery_floor(&self) -> CompactDuration {
        CompactDuration::MIN
    }

    fn backstop_armed(&self) -> impl Future<Output = Option<CompactDateTime>> + Send {
        ready(None)
    }

    async fn mark_backstop_armed(&self, _fire: CompactDateTime) {}
}

impl<P> MarkerIdentity for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn set_reload_marker(&self, _marker: MessageMarker) {}

    fn message_marker(&self) -> Option<MessageMarker> {
        // Stateless: no event identity, so nothing filters or records.
        None
    }
}

/// A collection identity over a fresh v4 segment per call.
///
/// This is the row-isolation mechanism for suites sharing a process-wide
/// store (the shared fjall test database, the shared `prosody_test`
/// keyspace): distinct segments keep rows disjoint even under parallel test
/// threads. Never substitute a fixed id here — use [`fixed_collection`] when
/// determinism, not isolation, is the point.
pub(crate) fn fresh_collection(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// A collection identity with a fixed, deterministic `StateKey`
/// (`Uuid::from_u128(0xA1B2_C3D4)`, key `"user-1"`) — for codec-pinning tests
/// whose value is reproducible identity bytes. Never use it where tests share
/// a store; that is [`fresh_collection`]'s contract.
pub(crate) fn fixed_collection(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::from_u128(0xA1B2_C3D4), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// A short coordinate over a tiny null-prone byte alphabet, so a codec is
/// exercised at the empty coordinate and at coordinates containing the bytes a
/// length-delimited scheme might mishandle.
pub(crate) fn arb_coordinate(g: &mut Gen) -> Coordinate {
    const ALPHABET: [u8; 3] = [0x00, 0x01, 0xFF];
    let len = usize::arbitrary(g) % 4;
    let bytes: Vec<u8> = (0..len)
        .map(|_| g.choose(&ALPHABET).copied().unwrap_or(0))
        .collect();
    Coordinate::from_bytes(bytes)
}

/// The single [`CoordinateBatch`] over `coords`. Every batch test's read list
/// is `≤ CELL_BATCH`, so `chunks` yields exactly one batch; each coordinate is
/// one byte at [`Section`]-0 (matching `cell_at`/`batch_cell`).
pub(crate) fn batch_of(coords: impl IntoIterator<Item = u8>) -> Result<CoordinateBatch> {
    CoordinateBatch::chunks(coords.into_iter().map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("a non-empty coordinate list must yield one batch"))
}

/// A message event with the deterministic dedup id `Uuid::from_u128(n)`.
/// Callers that need the id back recompute it from `n`.
pub(crate) fn probe(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
}

/// Asserts an explicit settle (promote, rollback, or sweep) left nothing
/// behind for `id`: no provisional cell and no unsettled event marker, read
/// **raw** from the durable maps. A resolving read cannot make this check —
/// it heals a still-provisional cell to the same bytes a correct settle
/// writes, so a skipped settle reads back identically. The marker leg is
/// load-bearing for clears-only stages, which stage zero provisional cells:
/// there the stranded marker is the only raw evidence of a skipped settle.
///
/// Call only where the harness guarantees the collection is fully settled;
/// first-touch heals leave the marker unsettled by design, so an event that
/// deliberately abandons its stage (reset, final-error) leaves residue a
/// later resolving read absorbs — don't probe across such an event.
pub(crate) fn assert_no_settlement_residue(cells: &MemoryCells, id: &CollectionId) -> Result<()> {
    if !cells.provisional_coordinates(id).is_empty() {
        bail!("settlement left a provisional cell unsettled");
    }
    if cells.unsettled_marker_of(id).is_some() {
        bail!("settlement left an event marker unsettled");
    }
    Ok(())
}
