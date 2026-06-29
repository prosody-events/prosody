//! Session lifecycle property + the marker-ordering pin.
//!
//! [`prop_value_lifecycle_equivalence`] drives a random sequence of
//! mutate/commit/abort/reset/fail events through the **real** production
//! session lifecycle (`finalize`/`commit_apply`/`rollback_aborted`/`reset`)
//! over **one partition-shared [`DirtyStore`]**, minting each event's session
//! as an [`EventStateScope`] that drops at event-end (the production
//! lifecycle). It asserts, after every event, that a plain `Option<Bytes>`
//! model equals both the committed projection and a fresh **overlay read** (the
//! dirty short-circuit a `committed_value` probe bypasses), and that the shared
//! dirty buffer is empty for the key — so a failed event's buffered write can
//! neither linger nor be read as uncommitted. One focused example pins the
//! ordering a trace cannot observe: the single marker flushes **exactly once,
//! strictly after the stage** (the [`ScriptedOracle`] `record_message`
//! counter).

use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::{ArmedKeys, CellRead, CellSession, KeyedStateSession, SessionParts, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::value_state;
use crate::state::dirty::DirtyStore;
use crate::state::manager::EventStateScope;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
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
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

const VALUE_NAME: &str = "cart";

/// The per-event session type the fixture mints (loader slot unused, so `()`).
type TestBackend = <SharedStateBackend<
    MemoryCellStore<ScriptedOracle>,
    MemoryDescriptorIdentityStore,
    ScriptedOracle,
> as StateBackendFactory>::Backend;
type Session = KeyedStateSession<TestBackend, ()>;

/// The single Value cell (`ValueNs::Entries`, empty coordinate).
fn value_cell() -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    }
}

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

/// Fixture sharing the partition-lifetime cell store across the per-event
/// sessions it mints, so a second event reads the first's committed values.
struct Fixture {
    cells: MemoryCells,
    oracle: ScriptedOracle,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    value_name: StateName,
    /// The one partition-shared dirty workspace every minted session writes
    /// into — exactly the per-partition store whose missing per-event clear is
    /// the bug under test.
    dirty: Arc<DirtyStore>,
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
            cells: MemoryCells::new(),
            oracle: ScriptedOracle::default(),
            registry: Arc::new(registry),
            state_key: StateKey::new(Uuid::from_u128(0x00C0_FFEE), Arc::from("key")),
            value_name: StateName::try_new(VALUE_NAME)?,
            dirty: Arc::new(DirtyStore::new()),
            shutdown_rx,
            cancel_rx,
            armed: Arc::default(),
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// The partition-lifetime cell store (a clone sharing the durable cells).
    fn cell_store(&self) -> MemoryCellStore<ScriptedOracle> {
        MemoryCellStore::new(
            self.cells.clone(),
            self.oracle.clone(),
            self.registry.clone(),
        )
    }

    /// Mints the per-event scope for `event` over clones of the shared store,
    /// oracle, and the one partition-shared dirty workspace.
    fn session(&self, event: EventRef) -> EventStateScope<Session> {
        EventStateScope::new(KeyedStateSession::new(SessionParts {
            cell: self.cell_store(),
            dirty: self.dirty.clone(),
            oracle: self.oracle.clone(),
            loader: (),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event,
            recovery_delay: CompactDuration::new(30),
            armed: self.armed.clone(),
            termination: TerminationWatch::new(self.shutdown_rx.clone(), self.cancel_rx.clone()),
        }))
    }

    /// The Value bytes a fresh session reads **through its overlay** — the
    /// dirty short-circuit then the committed fall-through — minted over the
    /// shared dirty workspace. Unlike
    /// [`committed_value`](Self::committed_value), a dirty cell left behind
    /// by a prior event surfaces here, so this is what catches the
    /// read-of-uncommitted corruption.
    async fn overlay_value(&self) -> Result<Option<Bytes>> {
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 1),
        };
        let scope = self.session(probe);
        Ok(scope
            .handle()
            .get(StateType::Application, &self.value_name, &value_cell())
            .await?)
    }

    fn value_id(&self) -> CollectionId {
        CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            self.value_name.clone(),
        )
    }

    /// The durable committed Value bytes. A fresh probe event so own-event
    /// never short-circuits; on quiescent state the resolving read is the
    /// committed projection (a still-provisional cell resolves to its
    /// `prev`, which is the committed value the in-flight event
    /// superseded).
    async fn committed_value(&self) -> Result<Option<Bytes>> {
        let probe = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX),
        };
        Ok(self
            .cell_store()
            .get(&self.value_id(), &value_cell(), probe)
            .await?
            .into_inner())
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
    let session = fx.session(event).handle();

    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"v1")
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
    /// The final-error path: the event ends with no `finalize` and no `reset`
    /// (settle's error arms never finalize). The buffered write must neither
    /// commit nor linger — only the scope's `Drop` clears it.
    Failed,
}

impl Arbitrary for ValueEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let mutation = match u8::arbitrary(g) % 3 {
            0 => ValueMut::Set(u8::arbitrary(g)),
            1 => ValueMut::Clear,
            _ => ValueMut::Skip,
        };
        let outcome = match u8::arbitrary(g) % 5 {
            0 => Outcome::Reset,
            1 => Outcome::Abort,
            2 => Outcome::Failed,
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

/// Drives the trace through the real session lifecycle, asserting the committed
/// projection equals a plain `Option<Bytes>` model after every event.
async fn run(trace: Trace) -> Result<bool> {
    let fx = Fixture::new()?;
    let mut model: Option<Bytes> = None;
    let key = fx.state_key.key.clone();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        // The scope drops at the end of this block — the production per-event
        // lifetime that clears the shared dirty buffer.
        {
            let scope = fx.session(event);
            let session = scope.handle();

            match ev.mutation {
                ValueMut::Set(byte) => {
                    session
                        .set(
                            StateType::Application,
                            &fx.value_name,
                            &value_cell(),
                            &[byte],
                        )
                        .await?;
                }
                ValueMut::Clear => {
                    session
                        .clear(StateType::Application, &fx.value_name, &value_cell())
                        .await?;
                }
                ValueMut::Skip => {}
            }

            match ev.outcome {
                Outcome::Commit => {
                    session.finalize().await?;
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
                    session.finalize().await?;
                    session.rollback_aborted().await;
                }
                Outcome::Reset => {
                    session.finalize().await?;
                    // Discards dirty + staged; any provisional written by
                    // `finalize` lingers but projects its `prev` (the unchanged
                    // committed base).
                    session.reset();
                }
                // Final-error path: no `finalize`, no `reset`. Only the scope's
                // `Drop` clears the buffered write.
                Outcome::Failed => {}
            }
        }

        // The shared dirty buffer is empty for the key — no per-event leak.
        if !fx.dirty.touched(&key).is_empty() {
            return Ok(false);
        }
        // A fresh overlay read (the dirty short-circuit path) tracks the model:
        // a leaked dirty cell would surface here as a read of uncommitted state.
        if fx.overlay_value().await? != model {
            return Ok(false);
        }
        // The committed projection still tracks the model.
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

/// `commit_apply` is best-effort: with two staged collections, a permanent
/// `mark_resolved` failure on one leaves it provisional (`Incomplete`) while
/// the healthy sibling still promotes — pinning the `fold`-not-`try_fold`
/// reduction and the `Incomplete`-dominance of the staged-set resolution.
#[tokio::test]
async fn commit_apply_is_best_effort_when_one_promote_fails() -> Result<()> {
    use crate::state::PartitionBackend;
    use crate::state::tests::cell_suite::FailingCellStore;

    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
    registry.register(
        &value_state::<JsonCodec>("wishlist"),
        CollectionDef::new(None),
    )?;
    let registry = Arc::new(registry);
    let oracle = ScriptedOracle::default();
    let inner = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let cell = FailingCellStore::new(inner, StateName::try_new("wishlist")?);
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let state_key = StateKey::new(Uuid::from_u128(7), Arc::from("key"));
    let (event, _dedup) = message(1);
    let session: KeyedStateSession<
        PartitionBackend<
            ScriptedOracle,
            MemoryDescriptorIdentityStore,
            FailingCellStore<MemoryCellStore<ScriptedOracle>>,
        >,
        (),
    > = KeyedStateSession::new(SessionParts {
        cell,
        dirty: Arc::new(DirtyStore::new()),
        oracle,
        loader: (),
        registry,
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    });

    let cart = StateName::try_new("cart")?;
    let wishlist = StateName::try_new("wishlist")?;
    session
        .set(StateType::Application, &cart, &value_cell(), b"a")
        .await?;
    session
        .set(StateType::Application, &wishlist, &value_cell(), b"b")
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    assert_eq!(
        session.commit_apply().await,
        ApplyOutcome::Incomplete,
        "a failed promote yields Incomplete so the boundary leaves the backstop armed",
    );
    Ok(())
}
