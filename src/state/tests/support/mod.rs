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
use crate::state::collection::owner::OwnerEngine;
use crate::state::collection::{StateSession, WritableStateSession, sealed};
use crate::state::descriptor::{CellResolver, StructuralIdentity};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::MemoryPublicationStore;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::oracle::CommitOracle;
use crate::state::publication::{PublicationRows, PublicationStore, StatePublication};
use crate::state::registry::CollectionDef;
use crate::state::session::sealed::{MarkerIdentity, ReadAdmission, StateLifecycle};
use crate::state::session::{
    CellRead, CellWrite, Finalized, MessageMarker, MutatePermit, OpPermit, SessionGate,
};
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

mod counting;
mod holding;
mod publication;
mod ttl;

pub(crate) use counting::{CountingCellStore, CountingResolver, ResolveCounter};
pub(crate) use holding::{HoldingCellStore, Holds};
pub(crate) use publication::{PublicationCall, ScriptedPublicationStore};
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

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(self.0)
    }
}

/// A commit oracle counting every `resolve` consult — the no-oracle pins'
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

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(CommitDecision::NotCommitted)
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

impl<P> ReadAdmission for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Permit<'s>
        = OpPermit<'s>
    where
        Self: 's;

    async fn permit(&self) -> OpPermit<'_> {
        self.gate.read().await
    }

    fn attempt_current(&self) -> bool {
        // Inert: every op errors `Unavailable` before the pin is consulted.
        true
    }
}

impl<P> sealed::Session for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Engine = OwnerEngine;
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

impl<P> CellRead for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn is_terminated(&self) -> bool {
        true
    }

    fn collection_def(&self, _state_type: StateType, _name: &StateName) -> CollectionDef {
        // Unreachable in practice: every op on this stub errors `Unavailable`
        // first. The registry defaults keep the trait total.
        CollectionDef::new(None)
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn get(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn get_many(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
        _batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
    }
}

impl<P> CellWrite for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type MutatePermit<'s>
        = MutatePermit<'s>
    where
        Self: 's;

    async fn mutate_permit(&self) -> Result<MutatePermit<'_>, StateAccessError> {
        // Inert stub: every op fails with `Unavailable` first, so this never
        // returns a witness.
        Err(StateAccessError::Unavailable)
    }

    async fn set(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
        _value: &[u8],
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear_section(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn commit(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn rollback(&self, _state_type: StateType, _name: &StateName) -> StoreOutcome {
        // Stateless: nothing is ever buffered, so the discard is a NoOp.
        StoreOutcome::NoOp
    }
}

impl<P> StateLifecycle for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Cell = MemoryCellStore<FixedOracle>;

    fn gate(&self) -> &SessionGate {
        &self.gate
    }

    async fn close_gate(&self) -> OpPermit<'_> {
        // Inert session: nothing contends, so no wait tags are needed.
        self.gate.close(|_waited_s| {}).await
    }

    async fn finalize(&self) -> Result<Finalized<Self::Cell>, StateAccessError> {
        Ok(Finalized::Clean)
    }

    async fn record_marker(
        &self,
        _marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> Result<(), StateAccessError> {
        Ok(())
    }

    /// Unreachable: [`CellWrite::mutate_permit`] errors `Unavailable` before a
    /// write operation can stage anything to replay.
    fn stage_cell(
        &self,
        _permit: &OpPermit<'_>,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
        _value: Option<Bytes>,
    ) {
    }

    /// Unreachable for the same reason as [`Self::stage_cell`].
    fn stage_section_clear(
        &self,
        _permit: &OpPermit<'_>,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
    ) {
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

    async fn backstop_armed(&self) -> Option<CompactDateTime> {
        None
    }

    async fn mark_backstop_armed(&self, _fire: CompactDateTime) {}

    async fn publish_first_writes(&self) -> Result<(), StateAccessError> {
        // Inert session: nothing is published.
        Ok(())
    }
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
/// behind for `id`: no provisional cell and no standing event marker, read
/// **raw** from the durable maps. A resolving read cannot make this check —
/// it heals a still-provisional cell to the same bytes a correct settle
/// writes, so a skipped settle reads back identically. The marker leg is
/// load-bearing for clears-only stages, which stage zero provisional cells:
/// there the stranded marker is the only raw evidence of a skipped settle.
///
/// Call only where the harness guarantees the collection is fully settled;
/// first-touch heals leave the marker standing by design, so an event that
/// deliberately abandons its stage (reset, final-error) leaves residue a
/// later resolving read absorbs — don't probe across such an event.
pub(crate) fn assert_no_settlement_residue(cells: &MemoryCells, id: &CollectionId) -> Result<()> {
    if !cells.provisional_coordinates(id).is_empty() {
        bail!("settlement left a provisional cell standing");
    }
    if cells.standing_marker_of(id).is_some() {
        bail!("settlement left an event marker standing");
    }
    Ok(())
}
