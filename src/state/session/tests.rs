//! Session lifecycle property + the marker-ordering pin.
//!
//! [`prop_value_lifecycle_equivalence`] drives a random sequence of events —
//! each a short op list (set/clear/mid-handler checkpoint) plus a
//! commit/abort/reset/fail outcome — through the **real** production session
//! lifecycle (`finalize`/`commit_apply`/`rollback_aborted`/`reset`) over **one
//! partition-shared [`DirtyStore`]**, minting each event's session as an
//! [`EventStateScope`] that drops at event-end (the production lifecycle). It
//! asserts, after every event, that a plain `Option<Bytes>` model equals both
//! the committed projection and a fresh **overlay read** (the dirty
//! short-circuit a `committed_value` probe bypasses), and that the shared
//! dirty buffer is empty for the key — so a failed event's buffered write can
//! neither linger nor be read as uncommitted. A checkpoint advances the
//! model's durable snapshot immediately, so the property also proves the
//! at-least-once checkpoint contract: checkpointed writes survive
//! abort/reset/failure while post-checkpoint ops still roll back. One focused
//! example pins the ordering a trace cannot observe: the single marker
//! flushes **exactly once, strictly after the stage** (the [`ScriptedOracle`]
//! `record_message` counter); another pins the checkpoint drain to its own
//! collection.

use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::{CellSession, KeyedStateSession, SessionParts, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::Section;
use crate::state::descriptor::value_state;
use crate::state::dirty::DirtyStore;
use crate::state::manager::ArmedKeys;
use crate::state::manager::EventStateScope;
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{ScriptedOracle, cell_at, value_cell};
use crate::state::tests::support::probe;
use crate::state::{
    CollectionId, CollectionRef, EventRef, PartitionBackend, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

const VALUE_NAME: &str = "cart";

/// The per-event session type the fixture mints (loader slot unused, so `()`).
type TestBackend = PartitionBackend<
    ScriptedOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<ScriptedOracle>,
>;
type Session = KeyedStateSession<TestBackend, ()>;

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

/// `probe(n)` plus its dedup id, for asserting against the marker store.
fn message(n: u128) -> (EventRef, Uuid) {
    (probe(n), Uuid::from_u128(n))
}

/// Builds a session whose registry has one `ReadCommitted` value collection per
/// entry in `bounds` (each carrying that `recovery_within`), stages one cell in
/// every collection, finalizes, and returns the resulting
/// `recovery_fire_delay`. `floor_secs` is the `recovery_delay`.
async fn staged_fire_delay(bounds: &[Option<u32>], floor_secs: u32) -> Result<CompactDuration> {
    let mut registry = CollectionDefRegistry::new(None);
    let mut names = Vec::with_capacity(bounds.len());
    for (i, within) in bounds.iter().enumerate() {
        let name = format!("c{i}");
        registry.register(
            &value_state::<JsonCodec>(&name),
            CollectionDef {
                recovery_within: within.map(CompactDuration::new),
                ..CollectionDef::new(None)
            },
        )?;
        names.push(StateName::try_new(&name)?);
    }
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let registry = Arc::new(registry);
    let oracle = ScriptedOracle::default();
    let cell = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let session: Session = KeyedStateSession::new(SessionParts {
        cell,
        dirty: Arc::new(DirtyStore::new()),
        oracle,
        loader: (),
        registry,
        state_key: StateKey::new(Uuid::from_u128(0xF01D), Arc::from("key")),
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        recovery_delay: CompactDuration::new(floor_secs),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    });
    for name in &names {
        session
            .set(StateType::Application, name, &value_cell(), b"v")
            .await?;
    }
    session.finalize().await?;
    Ok(session.recovery_fire_delay())
}

/// `recovery_fire_delay` is `min(recovery_delay, min over staged collections'
/// recovery_within)`: a `None` bound or one above the floor is inert, a tighter
/// one pulls the delay down, and a clean (empty) staged set keeps the floor.
#[test]
fn prop_recovery_fire_delay_folds_bounds_against_floor() {
    const FLOOR_SECS: u32 = 30;

    fn prop(raw: Vec<Option<u16>>) -> TestResult {
        // Cap the collection count so the interned-name set stays bounded.
        if raw.len() > 8 {
            return TestResult::discard();
        }
        let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
        // Empty (nothing staged) → floor; otherwise the floor tightened by the
        // smallest declared bound.
        let expected = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);
        match executor::block_on(staged_fire_delay(&bounds, FLOOR_SECS)) {
            Ok(delay) if delay.seconds() == expected => TestResult::passed(),
            Ok(delay) => TestResult::error(format!(
                "expected {expected}s, got {}s for {bounds:?}",
                delay.seconds(),
            )),
            Err(e) => TestResult::error(format!("staging failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>) -> TestResult);
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

/// A mid-handler checkpoint drains only its own collection: the checkpointed
/// collection's write is committed durably while the sibling's stays buffered
/// and unwritten — the drain is collection-scoped, never key-scoped.
#[tokio::test]
async fn checkpoint_drains_only_its_collection() -> Result<()> {
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
    registry.register(
        &value_state::<JsonCodec>("wishlist"),
        CollectionDef::new(None),
    )?;
    let registry = Arc::new(registry);
    let oracle = ScriptedOracle::default();
    let cell = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let state_key = StateKey::new(Uuid::from_u128(8), Arc::from("key"));
    let dirty = Arc::new(DirtyStore::new());
    let (event, _dedup) = message(1);
    let session: Session = KeyedStateSession::new(SessionParts {
        cell: cell.clone(),
        dirty: dirty.clone(),
        oracle,
        loader: (),
        registry,
        state_key: state_key.clone(),
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

    assert_eq!(
        session.checkpoint(StateType::Application, &cart).await?,
        StoreOutcome::Applied,
    );

    // Cart's write is committed durably; wishlist's is still only buffered.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    let cart_id = CollectionId::new(state_key.clone(), StateType::Application, cart);
    assert_eq!(
        cell.get(&cart_id, &value_cell(), probe).await?.into_inner(),
        Some(Bytes::from_static(b"a")),
    );
    let wishlist_id =
        CollectionId::new(state_key.clone(), StateType::Application, wishlist.clone());
    assert_eq!(
        cell.get(&wishlist_id, &value_cell(), probe)
            .await?
            .into_inner(),
        None,
        "the sibling collection's buffered op must not be written through",
    );
    let touched = dirty.touched(&state_key.key);
    assert_eq!(
        touched.len(),
        1,
        "only the un-checkpointed sibling stays dirty"
    );
    assert_eq!(touched[0].0.1, wishlist);
    Ok(())
}

/// Cap on ops per event: enough for checkpoint/mutate interleavings, small
/// enough that a failing trace stays readable.
const MAX_EVENT_OPS: usize = 4;

/// One event in the Value lifecycle trace: a short op list and a terminal
/// outcome. An empty op list is the skip event.
#[derive(Clone, Debug)]
struct ValueEvent {
    ops: Vec<ValueOp>,
    outcome: Outcome,
}

#[derive(Clone, Copy, Debug)]
enum ValueOp {
    Set(u8),
    Clear,
    /// The mid-handler write-through: everything buffered so far becomes
    /// durable immediately and survives every non-commit outcome.
    Checkpoint,
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

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // Sets weighted up so state actually accumulates between checkpoints.
        match u8::arbitrary(g) % 4 {
            0 | 1 => Self::Set(u8::arbitrary(g)),
            2 => Self::Clear,
            _ => Self::Checkpoint,
        }
    }
}

impl Arbitrary for ValueEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = Vec::<ValueOp>::arbitrary(g)
            .into_iter()
            .take(MAX_EVENT_OPS)
            .collect();
        let outcome = match u8::arbitrary(g) % 5 {
            0 => Outcome::Reset,
            1 => Outcome::Abort,
            2 => Outcome::Failed,
            _ => Outcome::Commit,
        };
        Self { ops, outcome }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        Box::new(self.ops.shrink().map(move |ops| Self { ops, outcome }))
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
///
/// A mid-event checkpoint snapshots the scratch model as immediately durable:
/// on a commit the full scratch wins, on every other outcome the durable state
/// must equal the last checkpointed snapshot (post-checkpoint ops roll back;
/// pre-checkpoint ops survive) — the at-least-once checkpoint contract.
async fn run(trace: Trace) -> Result<bool> {
    let fx = Fixture::new()?;
    let mut model: Option<Bytes> = None;
    let key = fx.state_key.key.clone();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        // Committed + buffered projection as the ops run.
        let mut scratch = model.clone();
        // The scratch as of the last checkpoint — durable regardless of
        // outcome.
        let mut checkpointed: Option<Option<Bytes>> = None;
        // Whether anything is buffered since event start / the last
        // checkpoint.
        let mut buffered = false;
        // The scope drops at the end of this block — the production per-event
        // lifetime that clears the shared dirty buffer.
        {
            let scope = fx.session(event);
            let session = scope.handle();

            for op in &ev.ops {
                match *op {
                    ValueOp::Set(byte) => {
                        session
                            .set(
                                StateType::Application,
                                &fx.value_name,
                                &value_cell(),
                                &[byte],
                            )
                            .await?;
                        scratch = Some(Bytes::copy_from_slice(&[byte]));
                        buffered = true;
                    }
                    ValueOp::Clear => {
                        session
                            .clear(StateType::Application, &fx.value_name, &value_cell())
                            .await?;
                        scratch = None;
                        buffered = true;
                    }
                    ValueOp::Checkpoint => {
                        let outcome = session
                            .checkpoint(StateType::Application, &fx.value_name)
                            .await?;
                        // The outcome contract: `Applied` iff the drain found
                        // buffered ops.
                        let expected = if buffered {
                            StoreOutcome::Applied
                        } else {
                            StoreOutcome::NoOp
                        };
                        if outcome != expected {
                            return Ok(false);
                        }
                        checkpointed = Some(scratch.clone());
                        buffered = false;
                    }
                }
            }

            match ev.outcome {
                Outcome::Commit => {
                    session.finalize().await?;
                    session.register_marker(dedup_id);
                    session.flush_marker().await?;
                    session.commit_apply().await;
                    // Commit advances the model (last-writer-wins).
                    model = scratch;
                }
                Outcome::Abort => {
                    session.finalize().await?;
                    session.rollback_aborted().await;
                    // Post-checkpoint ops roll back to their `prev`, which
                    // finalize captured *after* the checkpoint landed — the
                    // checkpointed snapshot.
                    model = checkpointed.unwrap_or(model);
                }
                Outcome::Reset => {
                    session.finalize().await?;
                    // Discards dirty + staged; any provisional written by
                    // `finalize` lingers but projects its `prev` (the
                    // checkpointed snapshot, or the unchanged committed base).
                    session.reset();
                    model = checkpointed.unwrap_or(model);
                }
                // Final-error path: no `finalize`, no `reset`. Only the
                // scope's `Drop` clears the buffered write — but a checkpoint
                // already wrote its snapshot through, and it must survive.
                Outcome::Failed => {
                    model = checkpointed.unwrap_or(model);
                }
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

/// A clears-only event still stages — durably: `clear_section` with no cell
/// writes calls `write_provisional` with an empty write set under a
/// clears-bearing marker, so the durable event marker stands after `finalize`
/// (raw probe BEFORE any resolving read — a `get` would read-help-resolve the
/// clears-bearing marker), the recorded staged-set entry makes `finalize`
/// report [`FinalizeOutcome::Staged`] — the backstop-arming rule — and
/// `commit_apply` resolves that entry (never
/// [`ApplyOutcome::NothingStaged`]).
#[tokio::test]
async fn clears_only_event_finalizes_staged() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, _dedup_id) = message(1);
    let session = fx.session(event).handle();

    session
        .clear_section(StateType::Application, &fx.value_name, Section::new(0))
        .await?;
    assert_eq!(
        session.finalize().await?,
        FinalizeOutcome::Staged,
        "a clears-only collection must stage so the backstop is armed"
    );
    let marker = fx
        .cells
        .standing_marker_of(&fx.value_id())
        .ok_or_else(|| eyre!("a clears-only stage must land a durable event marker"))?;
    assert_eq!(marker.event(), event, "the marker is this event's");
    assert!(
        marker.staged().is_empty(),
        "a clears-only stage lists no staged cells"
    );
    assert_eq!(
        marker
            .clears()
            .iter()
            .map(SectionClear::section)
            .collect::<Vec<_>>(),
        [Section::new(0)],
        "the marker carries the cleared section"
    );
    assert_eq!(
        session.commit_apply().await,
        ApplyOutcome::Resolved,
        "the staged entry exists to resolve — never NothingStaged"
    );
    Ok(())
}

/// The clears-only stage runs the stage-boundary foreign-marker resolve: a
/// standing **foreign** committed marker (seeded crash-style through a raw
/// store handle) is resolved — its cells settle per its verdict — rather than
/// blind-deleted by the clears-only event's own settle, and the session's own
/// clears-bearing marker is written by `finalize` then deleted by
/// `commit_apply` (which also applies the clear's gap erase).
async fn clears_only_session_boundary(a_committed: bool) -> Result<()> {
    let fx = Fixture::new()?;
    let raw = fx.cell_store();
    let id = fx.value_id();
    let collection = CollectionRef::new(id.clone(), None);

    // Seed event A's stage crash-style: two section-0 cells staged through
    // the raw handle, its dedup id recorded per the arm's verdict, no settle.
    let (a, a_dedup) = message(1);
    let writes_a = [
        (
            cell_at(0),
            ProvisionalWrite::new(Some(Bytes::from_static(b"a0")), Committed::new(None), a),
        ),
        (
            cell_at(1),
            ProvisionalWrite::new(Some(Bytes::from_static(b"a1")), Committed::new(None), a),
        ),
    ];
    let marker_a = EventMarker::frozen(a, &writes_a, &[]);
    raw.write_provisional(&collection, &writes_a, Some(&marker_a))
        .await?;
    if a_committed {
        fx.oracle.record_message(a_dedup).await?;
    }

    // Event B: a bare clears-only session event.
    let (b, b_dedup) = message(2);
    let session = fx.session(b).handle();
    session
        .clear_section(StateType::Application, &fx.value_name, Section::new(0))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    // Raw probes BEFORE any resolving read: the boundary resolved A's marker
    // (nothing of A stays provisional; A's cells settled per its verdict) and
    // B's clears-bearing marker replaced it.
    let standing = fx
        .cells
        .standing_marker_of(&id)
        .ok_or_else(|| eyre!("B's clears-only marker must stand after the stage"))?;
    assert_eq!(standing.event(), b, "B's marker replaced A's");
    assert!(
        fx.cells.provisional_coordinates(&id).is_empty(),
        "the boundary resolved all of A's cells; B staged nothing"
    );
    let a_rows = fx.cells.stored_coordinates(&id);
    assert_eq!(
        a_rows.len(),
        if a_committed { 2 } else { 0 },
        "A's cells settled per A's verdict at B's clears-only boundary"
    );

    // B's settle applies its clear (section 0's rows erased whole — A's
    // committed cells are pre-clear rows) and deletes B's marker.
    session.register_marker(b_dedup);
    session.flush_marker().await?;
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);
    assert!(
        fx.cells.standing_marker_of(&id).is_none(),
        "the settle deleted B's clears-bearing marker"
    );
    assert!(
        fx.cells.stored_coordinates(&id).is_empty(),
        "B's committed clear erased the section"
    );
    Ok(())
}

/// Clears-only session boundary resolve when the foreign event committed.
#[tokio::test]
async fn clears_only_session_boundary_resolves_committed_foreign_marker() -> Result<()> {
    clears_only_session_boundary(true).await
}

/// Clears-only session boundary resolve when the foreign event aborted.
#[tokio::test]
async fn clears_only_session_boundary_resolves_aborted_foreign_marker() -> Result<()> {
    clears_only_session_boundary(false).await
}

/// A retry attempt re-runs `finalize`: the second stage **rebuilds** the same
/// event's durable marker from its own staged set — never keeps the first
/// attempt's frozen payload, never resolves it as foreign — the settle
/// converges to the retried values, and no event marker stands afterwards.
/// The two attempts stage *different* cell sets so a kept (stale) marker is
/// observable: recovery resolves exactly the coordinates the marker lists, so
/// a stale list would strand the retry's extra cell.
#[tokio::test]
async fn retry_refinalize_overwrites_the_same_event_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event).handle();
    let extra = cell_at(7);

    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"v1")
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    // The retry boundary: discard dirty + staged + marker, then re-dispatch
    // the same event.
    session.reset();

    // The retry stages a superset — the Value cell again plus one more cell —
    // so the rebuilt marker's coordinate list differs from attempt one's.
    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"v2")
        .await?;
    session
        .set(StateType::Application, &fx.value_name, &extra, b"w")
        .await?;
    session.register_marker(dedup_id);
    assert_eq!(session.finalize().await?, FinalizeOutcome::Staged);

    // The standing durable marker is the retry's, rebuilt whole: same event,
    // and its frozen coordinate list is attempt two's staged set — not
    // attempt one's single cell.
    let marker = fx
        .cells
        .standing_marker_of(&fx.value_id())
        .ok_or_else(|| eyre!("no standing marker after the re-stage"))?;
    assert_eq!(marker.event(), event, "the marker stays the same event's");
    assert_eq!(
        marker.staged(),
        [value_cell(), extra.clone()],
        "the re-run rebuilds the marker from its own staged set"
    );

    session.flush_marker().await?;
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);

    assert_eq!(
        fx.committed_value().await?,
        Some(Bytes::from_static(b"v2")),
        "the retried attempt's value wins"
    );
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    assert_eq!(
        fx.cell_store()
            .get(&fx.value_id(), &extra, probe)
            .await?
            .into_inner(),
        Some(Bytes::from_static(b"w")),
        "the retry's extra cell commits with the rest of its stage"
    );
    assert!(
        fx.cells.standing_marker_of(&fx.value_id()).is_none(),
        "the settle deleted the single (overwritten) event marker"
    );
    Ok(())
}
