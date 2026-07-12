//! Shared test doubles and fixture constructors for the keyed-state suites
//! and the consumer tests that mount them. The backend-generic suite
//! runners and their trace types stay in `cell_suite`/`collection_suite`/
//! `identity_suite`; this module holds the standalone doubles they don't own.

use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::memory::MemoryCellStore;
use crate::state::oracle::CommitOracle;
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{CellSession, Finalized};
use crate::state::{
    CollectionId, CommitDecision, EventRef, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::stream::{self, Stream};
use parking_lot::Mutex as SyncMutex;
use quickcheck::{Arbitrary, Gen};
use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

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

/// Stateless session stub: every state op reports
/// [`StateAccessError::Unavailable`] and the lifecycle is inert. Mounted by
/// contexts that carry no keyed state, so a bind against them fails Permanent.
#[derive(Clone)]
pub struct UnavailableState<P> {
    loader: MemoryLoader<P>,
    markers: Arc<SyncMutex<Vec<Uuid>>>,
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
            markers: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    /// The markers registered against this stub (test observability).
    pub(crate) fn registered_markers(&self) -> Vec<Uuid> {
        self.markers.lock().clone()
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

impl<P> CellSession for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Loader = MemoryLoader<P>;

    fn loader(&self) -> &Self::Loader {
        &self.loader
    }

    fn is_terminated(&self) -> bool {
        true
    }

    fn collection_has_ttl(&self, _state_type: StateType, _name: &StateName) -> bool {
        false
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

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
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

    fn rollback(&self, _state_type: StateType, _name: &StateName) -> StoreOutcome {
        // Stateless: nothing is ever buffered, so the discard is a NoOp.
        StoreOutcome::NoOp
    }
}

impl<P> StateLifecycle for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Cell = MemoryCellStore<FixedOracle>;

    async fn finalize(&self) -> Result<Finalized<Self::Cell>, StateAccessError> {
        Ok(Finalized::Clean)
    }

    fn register_marker(&self, dedup_id: Uuid) {
        self.markers.lock().push(dedup_id);
    }

    async fn flush_marker(&self) -> Result<(), StateAccessError> {
        Ok(())
    }

    fn discard_dirty(&self) {}

    fn reset(&self) {
        self.markers.lock().clear();
    }

    fn recovery_floor(&self) -> CompactDuration {
        CompactDuration::MIN
    }

    async fn backstop_armed(&self) -> Option<CompactDateTime> {
        None
    }

    async fn mark_backstop_armed(&self, _fire: CompactDateTime) {}
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

/// A message event with the deterministic dedup id `Uuid::from_u128(n)`.
/// Callers that need the id back recompute it from `n`.
pub(crate) fn probe(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
}
