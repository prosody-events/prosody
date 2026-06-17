//! Session lifecycle property + the `merge_apply` fold.
//!
//! [`prop_value_lifecycle_equivalence`] drives a random sequence of
//! mutate/commit/abort/reset events through the **real** production session
//! lifecycle (`finalize`/`commit_apply`/`rollback_aborted`/`reset` over the
//! exhaustive `Lanes` fan-out) and asserts the Value lane's committed
//! projection tracks a plain `Option<Bytes>` model after every event — the
//! session-grain analog of the store-grain trace in `value_suite`. One focused
//! example pins the ordering a trace cannot observe: the single marker flushes
//! **exactly once, strictly after the stage** (the [`ScriptedOracle`]
//! `record_message` counter).
//!
//! [`prop_merge_apply_dominance`] pins [`merge_apply`](super::merge_apply):
//! once production has a single lane, the dominance fold has no end-to-end
//! coverage until a second kind lands, so it is exercised here directly — the
//! result is the dominance-max of its inputs (`Incomplete` > `Resolved` >
//! `NothingStaged`) and is order-independent.

use super::merge_apply;
use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::{ArmedKeys, CellAccess, KeyedStateSession, SessionParts, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::state::descriptor::value_state;
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::PartitionStateStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{
    CollectionId, CommitDecision, EventRef, SharedStateBackend, StateBackendFactory, StateKey,
    StateName, StateType,
};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::convert::Infallible;
use std::iter;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

const VALUE_NAME: &str = "cart";

/// The per-event session type the fixture mints (loader slot unused, so `()`).
/// The backend bundle is projected from the [`SharedStateBackend`] factory, so
/// the construction body names only the concrete stores it passes.
type TestBackend =
    <SharedStateBackend<MemoryCellStore, ScriptedOracle, MemoryCommittedCache> as StateBackendFactory>::Backend;
type Session = KeyedStateSession<TestBackend, ()>;

/// A committed-marker oracle: `record_message` writes the durable marker,
/// `resolve` answers `Committed` for a recorded event. Shared across the
/// session and the durable store, so a staged cell resolves against the exact
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

/// Fixture sharing the partition-lifetime store across the per-event sessions
/// it mints, so a second event reads the first's committed values.
struct Fixture {
    value_store: MemoryCellStore,
    oracle: ScriptedOracle,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    value_name: StateName,
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
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Ok(Self {
            value_store: MemoryCellStore::new(),
            oracle: ScriptedOracle::default(),
            registry: Arc::new(registry),
            state_key: StateKey::new(Uuid::from_u128(0x00C0_FFEE), Arc::from("key")),
            value_name: StateName::try_new(VALUE_NAME)?,
            shutdown_rx,
            cancel_rx,
            armed: Arc::default(),
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// Mints a session for `event` over clones of the shared store and oracle.
    fn session(&self, event: EventRef) -> Session {
        let store = PartitionStateStore::new(
            self.value_store.clone(),
            self.oracle.clone(),
            MemoryCommittedCache::new(),
            self.registry.clone(),
        );
        KeyedStateSession::new(SessionParts {
            store,
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

    fn value_id(&self) -> CollectionId<ValueKind> {
        CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            self.value_name.clone(),
        )
    }

    /// The durable committed Value bytes (the external committed-only
    /// projection: `prev` while a provisional cell stands).
    async fn committed_value(&self) -> Result<Option<Bytes>> {
        Ok(self
            .value_store
            .read_cell(&self.value_id(), &())
            .await?
            .project_committed()
            .cloned())
    }
}

fn message(n: u128) -> (EventRef, Uuid) {
    let dedup_id = Uuid::from_u128(n);
    (EventRef::Message { dedup_id }, dedup_id)
}

/// The single marker flushes **exactly once, strictly after the stage**: it is
/// not recorded after `finalize` alone, is recorded after `flush_marker`, and a
/// second flush writes nothing (the slot clears on success). Pins an ordering a
/// committed-projection trace cannot observe.
#[tokio::test]
async fn marker_flushes_exactly_once_strictly_after_stage() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event);

    CellAccess::<ValueKind>::set_cell(&session, StateType::Application, &fx.value_name, &(), b"v1")
        .await?;
    session.register_marker(dedup_id);

    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);
    assert!(
        !fx.oracle.is_recorded(dedup_id).await,
        "the marker must not be flushed by the stage",
    );

    session.flush_marker().await?;
    assert!(fx.oracle.is_recorded(dedup_id).await);
    assert_eq!(fx.oracle.recorded_count(), 1, "exactly one marker");

    session.flush_marker().await?;
    assert_eq!(
        fx.oracle.recorded_count(),
        1,
        "the slot clears on success, so a second flush is a no-op",
    );
    Ok(())
}

/// One event in the Value lifecycle trace: a mutation and a terminal outcome.
#[derive(Clone, Copy, Debug)]
struct ValueEvent {
    mutation: ValueMut,
    outcome: Outcome,
}

#[derive(Clone, Copy, Debug)]
enum ValueMut {
    Set(u8),
    Clear,
    Skip,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Commit,
    Abort,
    Reset,
}

impl Arbitrary for ValueEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let mutation = match u8::arbitrary(g) % 3 {
            0 => ValueMut::Set(u8::arbitrary(g)),
            1 => ValueMut::Clear,
            _ => ValueMut::Skip,
        };
        let outcome = match u8::arbitrary(g) % 4 {
            0 => Outcome::Reset,
            1 => Outcome::Abort,
            _ => Outcome::Commit,
        };
        Self { mutation, outcome }
    }
}

/// A shrinkable trace of Value events over one key.
#[derive(Clone, Debug)]
struct Trace {
    events: Vec<ValueEvent>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: Vec::<ValueEvent>::arbitrary(g)
                .into_iter()
                .take(40)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// Drives the trace through the real session lifecycle, asserting the Value
/// lane's committed projection equals a plain `Option<Bytes>` model after every
/// event.
async fn run(trace: Trace) -> Result<bool> {
    let fx = Fixture::new()?;
    let mut model: Option<Bytes> = None;

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        let session = fx.session(event);

        match ev.mutation {
            ValueMut::Set(byte) => {
                CellAccess::<ValueKind>::set_cell(
                    &session,
                    StateType::Application,
                    &fx.value_name,
                    &(),
                    &[byte],
                )
                .await?;
            }
            ValueMut::Clear => {
                CellAccess::<ValueKind>::clear_cell(
                    &session,
                    StateType::Application,
                    &fx.value_name,
                    &(),
                )
                .await?;
            }
            ValueMut::Skip => {}
        }

        session.finalize().await?;
        match ev.outcome {
            Outcome::Commit => {
                session.register_marker(dedup_id);
                session.flush_marker().await?;
                session.commit_apply().await;
                // Commit advances the model (last-writer-wins).
                match ev.mutation {
                    ValueMut::Set(byte) => model = Some(Bytes::copy_from_slice(&[byte])),
                    ValueMut::Clear => model = None,
                    ValueMut::Skip => {}
                }
            }
            Outcome::Abort => {
                session.rollback_aborted().await;
            }
            Outcome::Reset => {
                // Discards dirty + staged; any provisional written by `finalize`
                // lingers but projects its `prev` (the unchanged committed base).
                session.reset();
            }
        }

        if fx.committed_value().await? != model {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The Value session lifecycle is sound over random mixed-outcome traces: the
/// committed projection tracks the model event by event.
#[test]
fn prop_value_lifecycle_equivalence() {
    fn prop(trace: Trace) -> TestResult {
        match executor::block_on(run(trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("the committed projection diverged from the model"),
            Err(error) => TestResult::error(format!("trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}

impl Arbitrary for ApplyOutcome {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => ApplyOutcome::NothingStaged,
            1 => ApplyOutcome::Resolved,
            _ => ApplyOutcome::Incomplete,
        }
    }
}

/// Dominance rank: `Incomplete` (2) > `Resolved` (1) > `NothingStaged` (0).
fn rank(outcome: ApplyOutcome) -> u8 {
    match outcome {
        ApplyOutcome::NothingStaged => 0,
        ApplyOutcome::Resolved => 1,
        ApplyOutcome::Incomplete => 2,
    }
}

/// Invariant: `merge_apply` is the dominance-max of its inputs and is
/// order-independent — folding any permutation agrees. This is the only
/// coverage of the per-lane fold while production has a single lane, so it must
/// stand on its own (the advisor's load-bearing point) rather than lean on a
/// session test.
#[test]
fn prop_merge_apply_dominance() {
    fn prop(outcomes: Vec<ApplyOutcome>) -> TestResult {
        // Dominance-max: empty folds to `NothingStaged` (rank 0).
        let expected_rank = outcomes.iter().map(|o| rank(*o)).max().unwrap_or(0);
        // Snapshot the reversed input before the fold consumes `outcomes`.
        let reversed: Vec<ApplyOutcome> = outcomes.iter().rev().copied().collect();
        let folded = merge_apply(outcomes);
        if rank(folded) != expected_rank {
            return TestResult::error("merge_apply is not the dominance-max of its inputs");
        }
        // Order-independent: the reversed fold agrees.
        if folded != merge_apply(reversed) {
            return TestResult::error("merge_apply depends on input order");
        }
        TestResult::passed()
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<ApplyOutcome>) -> TestResult);
}

/// Frozen examples for the two-plus-element dominance the property generates:
/// any `Incomplete` dominates, else any `Resolved`, else `NothingStaged`.
#[test]
fn merge_apply_fixed_vectors() {
    use ApplyOutcome::{Incomplete, NothingStaged, Resolved};
    assert_eq!(merge_apply([Resolved, Incomplete]), Incomplete);
    assert_eq!(merge_apply([Resolved, NothingStaged]), Resolved);
    assert_eq!(merge_apply([NothingStaged, NothingStaged]), NothingStaged);
    assert_eq!(
        merge_apply([Resolved, Incomplete, NothingStaged]),
        Incomplete,
    );
    assert_eq!(merge_apply(iter::empty()), NothingStaged);
}
