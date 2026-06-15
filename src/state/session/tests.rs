//! Multi-kind **composition** proof: one real [`KeyedStateSession`] driving the
//! Value lane and the `#[cfg(test)]` [`CounterKind`] lane through the **same**
//! production lifecycle (`finalize`/`commit_apply`/`rollback_aborted`/`reset`
//! over the exhaustive `Lanes` destructure + `try_join!`/`join!`).
//!
//! These pins are what make the scaffold trustworthy: both lanes stage in one
//! event; the single marker flushes **once, strictly after both stages**;
//! commit promotes both; abort rolls back both; `reset` clears both lanes plus
//! the shared marker. The counter lane's non-LWW `combine` and its header cell
//! are exercised here, so the additive fold and "the header stages
//! transactionally with its data cells" are proven, not asserted. A bulk-apply
//! pin confirms a multi-cell collection stages and promotes in **one** batched
//! store call per collection, not one per cell.

use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::{ArmedKeys, CellAccess, KeyedStateSession, SessionParts, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::state::descriptor::value_state;
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::PartitionStateStore;
use crate::state::proof_kind::{
    CounterDescriptor, CounterKind, HEADER_ADDR, MemoryCounterCache, MemoryCounterStore,
    decode_i64, encode_delta,
};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CommitDecision, EventRef, StateKey, StateName, StateType};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

const VALUE_NAME: &str = "cart";
const COUNTER_NAME: &str = "tally";

/// The per-event session type the fixture mints (loader slot unused, so `()`).
type Session = KeyedStateSession<MemoryCellStore, ScriptedOracle, MemoryCommittedCache, ()>;

/// A committed-marker oracle: `record_message` writes the durable marker,
/// `resolve` answers `Committed` for a recorded event. Shared across the
/// session and the durable stores, so a staged cell resolves against the exact
/// record the marker flush wrote.
#[derive(Clone, Default)]
struct ScriptedOracle {
    committed: Arc<scc::HashSet<Uuid, RandomState>>,
}

impl ScriptedOracle {
    async fn is_recorded(&self, dedup_id: Uuid) -> bool {
        self.committed.contains_async(&dedup_id).await
    }

    fn recorded_count(&self) -> usize {
        self.committed.len()
    }
}

impl CommitOracle for ScriptedOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        let _ = self.committed.insert_async(dedup_id).await;
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.committed.contains_async(&dedup_id).await,
            EventRef::Timer(_) => false,
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

/// Fixture sharing the partition-lifetime stores across the per-event sessions
/// it mints, so a second event reads the first's committed values.
struct Fixture {
    value_store: MemoryCellStore,
    counter_store: MemoryCounterStore,
    oracle: ScriptedOracle,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    value_name: StateName,
    counter_name: StateName,
    shutdown_rx: watch::Receiver<ShutdownPhase>,
    cancel_rx: watch::Receiver<bool>,
    armed: ArmedKeys,
    // Kept alive so the session's termination receivers stay open.
    _shutdown_tx: watch::Sender<ShutdownPhase>,
    _cancel_tx: watch::Sender<bool>,
}

impl Fixture {
    fn new() -> Result<Self> {
        let mut registry = CollectionDefRegistry::new(None);
        registry.register(
            &value_state::<JsonCodec>(VALUE_NAME),
            CollectionDef::new(None),
        )?;
        registry.register(
            &CounterDescriptor::new(COUNTER_NAME),
            CollectionDef::new(None),
        )?;
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Ok(Self {
            value_store: MemoryCellStore::new(),
            counter_store: MemoryCounterStore::new(),
            oracle: ScriptedOracle::default(),
            registry: Arc::new(registry),
            state_key: StateKey::new(Uuid::from_u128(0x00C0_FFEE), Arc::from("key")),
            value_name: StateName::try_new(VALUE_NAME)?,
            counter_name: StateName::try_new(COUNTER_NAME)?,
            shutdown_rx,
            cancel_rx,
            armed: Arc::default(),
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// Mints a session for `event` over clones of the shared stores and oracle.
    fn session(&self, event: EventRef) -> Session {
        let value_store = PartitionStateStore::new(
            self.value_store.clone(),
            self.oracle.clone(),
            MemoryCommittedCache::new(),
            self.registry.clone(),
        );
        let test_store = PartitionStateStore::new(
            self.counter_store.clone(),
            self.oracle.clone(),
            MemoryCounterCache::new(),
            self.registry.clone(),
        );
        KeyedStateSession::new(SessionParts {
            store: value_store,
            test_store,
            oracle: self.oracle.clone(),
            loader: (),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event,
            recovery_delay: CompactDuration::new(30),
            armed: self.armed.clone(),
            termination: TerminationWatch::new(self.shutdown_rx.clone(), self.cancel_rx.clone()),
        })
    }

    /// Buffers a Value set plus a counter increment on a data cell **and** the
    /// header cell (running total), all on one session/event.
    async fn write_both(&self, session: &Session, value: &[u8], delta: i64) -> Result<()> {
        CellAccess::<ValueKind>::set_cell(session, &self.value_name, &(), value).await?;
        // A data cell at addr 0 and the header cell, both in the counter
        // collection — staged transactionally in one batch.
        CellAccess::<CounterKind>::set_cell(session, &self.counter_name, &0, &encode_delta(delta))
            .await?;
        CellAccess::<CounterKind>::set_cell(
            session,
            &self.counter_name,
            &HEADER_ADDR,
            &encode_delta(delta),
        )
        .await?;
        Ok(())
    }

    fn value_id(&self) -> CollectionId<ValueKind> {
        CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            self.value_name.clone(),
        )
    }

    fn counter_id(&self) -> CollectionId<CounterKind> {
        CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            self.counter_name.clone(),
        )
    }

    /// The durable committed Value bytes.
    async fn committed_value(&self) -> Result<Option<Bytes>> {
        Ok(self
            .value_store
            .read_cell(&self.value_id(), &())
            .await?
            .project_committed()
            .cloned())
    }

    /// The durable committed counter at `addr`, decoded.
    async fn committed_counter(&self, addr: u32) -> Result<i64> {
        Ok(self
            .counter_store
            .read_cell(&self.counter_id(), &addr)
            .await?
            .project_committed()
            .map_or(0, |b| decode_i64(b)))
    }

    /// Whether the counter cell at `addr` is still provisional.
    async fn counter_is_provisional(&self, addr: u32) -> Result<bool> {
        Ok(self
            .counter_store
            .read_cell(&self.counter_id(), &addr)
            .await?
            .as_provisional()
            .is_some())
    }

    /// Whether the Value cell is still provisional.
    async fn value_is_provisional(&self) -> Result<bool> {
        Ok(self
            .value_store
            .read_cell(&self.value_id(), &())
            .await?
            .as_provisional()
            .is_some())
    }
}

fn message(n: u128) -> (EventRef, Uuid) {
    let dedup_id = Uuid::from_u128(n);
    (EventRef::Message { dedup_id }, dedup_id)
}

/// On handler success both lanes stage; the marker flushes **once, strictly
/// after both stages**; commit promotes both. The counter's data cell and
/// header cell stage and promote in **one** batched store call each.
#[tokio::test]
async fn composition_stages_both_then_commit_promotes_both() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event);

    fx.write_both(&session, b"v1", 5).await?;

    // Stage every lane.
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert!(fx.value_is_provisional().await?, "value lane staged");
    assert!(
        fx.counter_is_provisional(0).await? && fx.counter_is_provisional(HEADER_ADDR).await?,
        "counter data + header cells staged",
    );
    assert!(
        !fx.oracle.is_recorded(dedup_id).await,
        "the marker must not be flushed before the stage completes",
    );
    // Bulk apply: the counter collection's two cells staged in ONE batched call.
    assert_eq!(
        fx.counter_store.provisional_write_calls(),
        1,
        "a multi-cell collection stages in one batch, not one call per cell",
    );

    // Marker strictly after the stage.
    session.register_marker(dedup_id);
    session.flush_marker().await?;
    assert!(fx.oracle.is_recorded(dedup_id).await);
    assert_eq!(fx.oracle.recorded_count(), 1, "exactly one marker");

    // Commit promotes both lanes.
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);
    assert_eq!(fx.committed_value().await?, Some(Bytes::from_static(b"v1")));
    assert_eq!(fx.committed_counter(0).await?, 5, "counter data promoted");
    assert_eq!(
        fx.committed_counter(HEADER_ADDR).await?,
        5,
        "header promoted",
    );
    assert_eq!(
        fx.counter_store.mark_resolved_calls(),
        1,
        "the collection's cells promote in one batched call",
    );
    Ok(())
}

/// Abort rolls **both** lanes back to their committed base. A first committed
/// event seeds the base; a second event's abort restores it on both kinds.
#[tokio::test]
async fn composition_abort_rolls_back_both_lanes() -> Result<()> {
    let fx = Fixture::new()?;

    // Event 1 commits value="a", counter=5 on both data and header.
    let (event1, dedup1) = message(1);
    let session1 = fx.session(event1);
    fx.write_both(&session1, b"a", 5).await?;
    assert_eq!(session1.finalize().await?, FinalizeOutcome::Staged);
    session1.register_marker(dedup1);
    session1.flush_marker().await?;
    assert_eq!(session1.commit_apply().await, ApplyOutcome::Resolved);

    // Event 2 stages value="b", counter += 3, then aborts.
    let (event2, _dedup2) = message(2);
    let session2 = fx.session(event2);
    fx.write_both(&session2, b"b", 3).await?;
    assert_eq!(session2.finalize().await?, FinalizeOutcome::Staged);
    let resolved_writes_before = fx.counter_store.write_resolved_calls();
    assert_eq!(session2.rollback_aborted().await, ApplyOutcome::Resolved);
    assert_eq!(
        fx.counter_store.write_resolved_calls(),
        resolved_writes_before + 1,
        "rollback writes the counter collection's cells in one batch",
    );

    assert_eq!(
        fx.committed_value().await?,
        Some(Bytes::from_static(b"a")),
        "value rolled back to the committed base",
    );
    assert_eq!(
        fx.committed_counter(0).await?,
        5,
        "counter data rolled back to its committed base over the additive op",
    );
    assert_eq!(
        fx.committed_counter(HEADER_ADDR).await?,
        5,
        "header rolled back",
    );
    Ok(())
}

/// `reset` clears **both** lanes' dirty scope and the shared marker, so a
/// follow-up `finalize` stages nothing and `flush_marker` writes nothing.
#[tokio::test]
async fn composition_reset_clears_both_lanes_and_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event);

    fx.write_both(&session, b"v1", 5).await?;
    session.register_marker(dedup_id);

    session.reset();

    assert_eq!(
        session.finalize().await?,
        FinalizeOutcome::Clean,
        "reset cleared both lanes' dirty scope, so nothing stages",
    );
    session.flush_marker().await?;
    assert!(
        !fx.oracle.is_recorded(dedup_id).await,
        "reset cleared the marker, so flush writes nothing",
    );
    assert!(!fx.value_is_provisional().await?, "no value cell staged");
    assert!(
        !fx.counter_is_provisional(0).await?,
        "no counter cell staged"
    );
    Ok(())
}

/// An in-place transient retry re-stages the identical write: the additive
/// counter re-applies over the same own-event `prev`, so the staged data is
/// idempotent across the retried `finalize` (the contract `Lane::stage` relies
/// on). Exercises the non-LWW combine where the base genuinely matters.
#[tokio::test]
async fn composition_counter_stage_is_idempotent_under_retry() -> Result<()> {
    let fx = Fixture::new()?;

    // Seed a committed counter of 10.
    let (event1, dedup1) = message(1);
    let session1 = fx.session(event1);
    CellAccess::<CounterKind>::set_cell(&session1, &fx.counter_name, &0, &encode_delta(10)).await?;
    session1.finalize().await?;
    session1.register_marker(dedup1);
    session1.flush_marker().await?;
    session1.commit_apply().await;
    assert_eq!(fx.committed_counter(0).await?, 10);

    // Event 2 adds 3, then `finalize` runs twice in place (transient retry).
    let (event2, _) = message(2);
    let session = fx.session(event2);
    CellAccess::<CounterKind>::set_cell(&session, &fx.counter_name, &0, &encode_delta(3)).await?;
    session.finalize().await?;
    // The re-stage reads its own provisional cell's prev (10), not the staged
    // 13, so it re-applies to 13 — idempotent.
    session.finalize().await?;

    let cell = fx.counter_store.read_cell(&fx.counter_id(), &0).await?;
    let staged = cell
        .as_provisional()
        .and_then(super::super::cell::ProvisionalCell::data)
        .map_or(0, |b| decode_i64(b));
    assert_eq!(
        staged, 13,
        "re-stage applies over the same prev, idempotently"
    );
    Ok(())
}

/// One event in the two-kind lifecycle trace: an optional Value mutation, an
/// optional counter delta, and the commit-vs-abort outcome.
#[derive(Clone, Copy, Debug)]
struct TwoKindEvent {
    value: ValueMut,
    counter_delta: Option<i16>,
    commit: bool,
}

#[derive(Clone, Copy, Debug)]
enum ValueMut {
    Set(u8),
    Clear,
    Skip,
}

impl Arbitrary for TwoKindEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let value = match u8::arbitrary(g) % 3 {
            0 => ValueMut::Set(u8::arbitrary(g)),
            1 => ValueMut::Clear,
            _ => ValueMut::Skip,
        };
        // `Some` ~3/4 of the time so most events touch both lanes.
        let counter_delta = (u8::arbitrary(g) % 4 != 0).then(|| i16::arbitrary(g));
        Self {
            value,
            counter_delta,
            commit: bool::arbitrary(g),
        }
    }
}

/// A shrinkable trace of two-kind events over one key.
#[derive(Clone, Debug)]
struct TwoKindTrace {
    events: Vec<TwoKindEvent>,
}

impl Arbitrary for TwoKindTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: Vec::<TwoKindEvent>::arbitrary(g)
                .into_iter()
                .take(40)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// Drives a random sequence of mixed-outcome events that each touch the Value
/// lane and/or the counter lane on **one key**, through the production session
/// lifecycle, asserting **both** kinds' committed projections equal a simple
/// per-kind model after every event. This is the composition analog of the
/// Value crash-equivalence property: it catches a cross-lane model divergence
/// or an outcome-ordering regression that the targeted example tests cannot.
async fn run_two_kind_trace(trace: TwoKindTrace) -> Result<bool> {
    let fx = Fixture::new()?;
    // The model committed value of each kind (counter at data addr 0).
    let mut value_model: Option<Bytes> = None;
    let mut counter_model: i64 = 0;

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        let session = fx.session(event);

        match ev.value {
            ValueMut::Set(byte) => {
                CellAccess::<ValueKind>::set_cell(&session, &fx.value_name, &(), &[byte]).await?;
            }
            ValueMut::Clear => {
                CellAccess::<ValueKind>::clear_cell(&session, &fx.value_name, &()).await?;
            }
            ValueMut::Skip => {}
        }
        if let Some(delta) = ev.counter_delta {
            CellAccess::<CounterKind>::set_cell(
                &session,
                &fx.counter_name,
                &0,
                &encode_delta(i64::from(delta)),
            )
            .await?;
        }

        session.finalize().await?;
        if ev.commit {
            session.register_marker(dedup_id);
            session.flush_marker().await?;
            session.commit_apply().await;
            // Advance the model only on commit: Value is last-writer-wins, the
            // counter accumulates its delta.
            match ev.value {
                ValueMut::Set(byte) => value_model = Some(Bytes::copy_from_slice(&[byte])),
                ValueMut::Clear => value_model = None,
                ValueMut::Skip => {}
            }
            if let Some(delta) = ev.counter_delta {
                counter_model = counter_model.wrapping_add(i64::from(delta));
            }
        } else {
            session.rollback_aborted().await;
        }

        if fx.committed_value().await? != value_model {
            return Ok(false);
        }
        if fx.committed_counter(0).await? != counter_model {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The two-kind lifecycle composition is sound over random mixed-outcome
/// traces: both kinds' committed projections track their models event by event.
#[test]
fn prop_two_kind_lifecycle_equivalence() {
    fn prop(trace: TwoKindTrace) -> TestResult {
        match executor::block_on(run_two_kind_trace(trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("a kind's committed projection diverged from the model"),
            Err(error) => TestResult::error(format!("trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(TwoKindTrace) -> TestResult);
}
