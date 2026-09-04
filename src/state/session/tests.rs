//! The central settlement property + the protocol tests it cannot observe.
//!
//! [`prop_value_lifecycle_equivalence`] drives a random sequence of events —
//! each a short op list (set / clear / section-clear / mid-handler
//! commit/rollback) plus a commit/abort/reset/fail outcome, some commits
//! scheduling a promote failure — through the **real** production session
//! lifecycle (the `finalize`-minted receipt's `promote`/`rollback` plus the
//! attempt-boundary `discard_dirty`) over **one partition-shared
//! [`DirtyStore`]**, minting each event's session as an [`EventStateScope`]
//! that drops at event-end (the production lifecycle). Its clauses:
//!
//! - after every operation the session's own overlay read equals a plain
//!   `Option<Bytes>` model, and the mid-handler `commit()`/`rollback()` drains
//!   report `Applied` iff anything was buffered since the last drain;
//! - `finalize` returns `Clean` iff nothing was buffered, else one linear
//!   receipt whose frozen records equal the durable event marker the stage
//!   wrote (receipt/durable-marker consistency; survivor semantics are owned by
//!   the crash-equivalence suite's clears dimension in
//!   `state::tests::cell_suite`);
//! - a second `finalize` after success returns `Clean` — the stage consumed the
//!   receipt's mint source (drain-on-success);
//! - consuming the receipt converges: `promote` to the model's post-event
//!   state, `rollback` to the commit-inclusive committed base — a successful
//!   `commit()` is an irreversible floor that abort/reset/failure can never
//!   undo, while post-`commit()` staged work never survives a non-commit
//!   outcome (never the pre-event state when a `commit()` landed);
//! - a scheduled promote failure yields `Incomplete`, and durable recovery
//!   converges: the loop-tail resolving reads heal the stranded stage through
//!   the oracle, asserted by the same overlay/committed equivalence.
//!
//! After every event the shared dirty buffer is empty for the key and both a
//! fresh overlay read (the dirty short-circuit a `committed_value` probe
//! bypasses) and the committed projection equal the model — so a failed
//! event's buffered write can neither linger nor be read as uncommitted.
//!
//! The surviving examples test what the single-collection value trace cannot
//! observe: the `commit()`/`rollback()` drains are collection-scoped (sibling
//! isolation, zero durable writes on rollback); a terminated session's
//! rollback is a `NoOp` so a stale clone cannot drain a later same-key
//! event's buffer; a mid-stage failure leaves the buffer whole for an
//! idempotent retry; a retry re-finalize rebuilds the same event's marker; an
//! own-event read leaves its in-flight marker unsettled; and the clears-only
//! stage boundary resolves a seeded prior event marker. (The settle boundary's
//! own marker-record ordering and exactly-once tests live in
//! `consumer::middleware::tests` and the defer/retry test suites, where the
//! real boundary is driven.)

use super::sealed::{ApplyOutcome, StagedState, StateLifecycle};
use super::{Finalized, KeyedStateSession, SessionParts, StateBackend, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ErrorCategory;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::value_state;
use crate::state::dirty::{DirtyStore, DirtyVal};
use crate::state::manager::ArmedKeys;
use crate::state::manager::EventStateScope;
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::{CELL_BATCH, CellStore};
use crate::state::tests::cell_suite::{
    FailingCellStore, Poison, PoisonHandle, ScriptedOracle, cell_at, value_cell,
};
use crate::state::tests::support::{CountingCellStore, assert_no_settlement_residue, probe};
use crate::state::{
    CollectionId, CollectionRef, CommitMode, EventRef, PartitionBackend, StateKey, StateName,
    StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

const VALUE_NAME: &str = "cart";

/// The poison-armable cell store the fixture mints: a [`FailingCellStore`]
/// over the shared in-memory cells, disarmed (`None`) by default so it
/// delegates cleanly — the property arms it to schedule stage and promote
/// failures at runtime.
type TestStore = FailingCellStore<MemoryCellStore<ScriptedOracle>>;
type TestBackend = PartitionBackend<ScriptedOracle, MemoryDescriptorIdentityStore, TestStore>;
/// The per-event session type the fixture mints (loader slot unused, so `()`).
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
    /// The runtime poison slot every minted store clone shares — `None`
    /// delegates cleanly; a test arms it for exactly one stage or settle.
    poison: PoisonHandle,
    shutdown_rx: watch::Receiver<ShutdownPhase>,
    cancel_rx: watch::Receiver<bool>,
    armed: ArmedKeys,
    // Kept alive so the session's termination receivers stay open.
    _shutdown_tx: watch::Sender<ShutdownPhase>,
    _cancel_tx: watch::Sender<bool>,
}

impl Fixture {
    fn new() -> Result<Self> {
        Self::with_collections(&[VALUE_NAME])
    }

    /// A fixture whose registry holds one `ReadCommitted` value collection per
    /// name; `value_name` is `names[0]`, so single-collection callers reach it
    /// through the shared helpers unchanged.
    fn with_collections(names: &[&str]) -> Result<Self> {
        let value_name = names
            .first()
            .ok_or_else(|| eyre!("with_collections needs at least one collection"))?;
        let mut registry = CollectionDefRegistry::default();
        for name in names {
            registry.register(&value_state::<JsonCodec>(name), CollectionDef::new(None))?;
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        Ok(Self {
            cells: MemoryCells::new(),
            oracle: ScriptedOracle::default(),
            registry: Arc::new(registry),
            state_key: StateKey::new(Uuid::from_u128(0x00C0_FFEE), Arc::from("key")),
            value_name: StateName::try_new(value_name)?,
            dirty: Arc::new(DirtyStore::new()),
            poison: Arc::default(),
            shutdown_rx,
            cancel_rx,
            armed: Arc::default(),
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// The partition-lifetime cell store (a clone sharing the durable cells
    /// and the runtime poison slot).
    fn cell_store(&self) -> TestStore {
        FailingCellStore::with_handle(
            MemoryCellStore::new(
                self.cells.clone(),
                self.oracle.clone(),
                self.registry.clone(),
            ),
            self.poison.clone(),
        )
    }

    /// Arms (`Some`) or disarms (`None`) the poison slot shared by every
    /// store clone the fixture minted.
    fn set_poison(&self, poison: Option<Poison>) {
        *self.poison.lock() = poison;
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

    /// Mints a session for `event` over a caller-owned cancel watch (sharing
    /// the fixture's store, oracle, dirty workspace, and key). Production gives
    /// each event its own per-event cancel signal; the fixture's shared channel
    /// cannot terminate one event alone, which the stale-clone containment test
    /// needs.
    fn session_with_cancel(
        &self,
        event: EventRef,
        cancel_rx: watch::Receiver<bool>,
    ) -> EventStateScope<Session> {
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
            termination: TerminationWatch::new(self.shutdown_rx.clone(), cancel_rx),
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
/// every collection, finalizes, and returns the receipt's `recovery_delay` —
/// `None` when nothing staged (a clean event mints no receipt). `floor_secs`
/// is the `recovery_delay` floor.
async fn staged_fire_delay(
    bounds: &[Option<u32>],
    floor_secs: u32,
) -> Result<Option<CompactDuration>> {
    let mut registry = CollectionDefRegistry::default();
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
    let cell = FailingCellStore::with_handle(
        MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
        PoisonHandle::default(),
    );
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
            .seed(StateType::Application, name, &value_cell(), Some(b"v"))
            .await;
    }
    match session.finalize().await? {
        Finalized::Staged(staged) => Ok(Some(staged.recovery_delay())),
        Finalized::Clean => Ok(None),
    }
}

/// The receipt's `recovery_delay` is `min(recovery_delay floor, min over
/// staged collections' recovery_within)`: a `None` bound or one above the
/// floor is inert, a tighter one pulls the delay down, and a clean event
/// stages nothing so it mints no receipt at all — the recovery delay of a
/// never-staged event is unrepresentable.
#[test]
fn prop_finalize_folds_recovery_delay_against_floor() {
    const FLOOR_SECS: u32 = 30;

    fn prop(raw: Vec<Option<u16>>) -> TestResult {
        // Cap the collection count so the interned-name set stays bounded.
        if raw.len() > 8 {
            return TestResult::discard();
        }
        let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
        // Non-empty → the floor tightened by the smallest declared bound.
        let expected = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);
        match executor::block_on(staged_fire_delay(&bounds, FLOOR_SECS)) {
            Ok(None) if bounds.is_empty() => TestResult::passed(),
            Ok(None) => TestResult::error(format!("expected a receipt for {bounds:?}, got Clean")),
            Ok(Some(_)) if bounds.is_empty() => {
                TestResult::error("a clean event must mint no receipt")
            }
            Ok(Some(delay)) if delay.seconds() == expected => TestResult::passed(),
            Ok(Some(delay)) => TestResult::error(format!(
                "expected {expected}s, got {}s for {bounds:?}",
                delay.seconds(),
            )),
            Err(e) => TestResult::error(format!("staging failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>) -> TestResult);
}

/// A mid-handler `commit()` drains only its own collection: the
/// `commit()`-landed write is durable while the sibling's stays buffered
/// and unwritten — the drain is collection-scoped, never key-scoped. An example
/// because the lifecycle property drives a single collection, so it cannot
/// observe sibling-collection isolation on a `commit()`.
#[tokio::test]
async fn commit_drains_only_its_collection() -> Result<()> {
    let fx = Fixture::with_collections(&["cart", "wishlist"])?;
    let (event, _dedup) = message(1);
    // The scope stays alive so its `Drop` clear does not race the dirty probe.
    let scope = fx.session(event);
    let session = scope.handle();

    let cart = StateName::try_new("cart")?;
    let wishlist = StateName::try_new("wishlist")?;
    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"a"))
        .await;
    session
        .seed(StateType::Application, &wishlist, &value_cell(), Some(b"b"))
        .await;

    let outcome = {
        let permit = session.permit().await;
        session
            .commit(&permit, StateType::Application, &cart)
            .await?
    };
    assert_eq!(outcome, StoreOutcome::Applied);

    // Cart's write is committed durably; wishlist's is still only buffered.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    let cart_id = CollectionId::new(fx.state_key.clone(), StateType::Application, cart);
    assert_eq!(
        fx.cell_store()
            .get(&cart_id, &value_cell(), probe)
            .await?
            .into_inner(),
        Some(Bytes::from_static(b"a")),
    );
    let wishlist_id = CollectionId::new(
        fx.state_key.clone(),
        StateType::Application,
        wishlist.clone(),
    );
    assert_eq!(
        fx.cell_store()
            .get(&wishlist_id, &value_cell(), probe)
            .await?
            .into_inner(),
        None,
        "the sibling collection's buffered op must not be written through",
    );
    let touched = fx.dirty.touched(&fx.state_key.key);
    assert_eq!(touched.len(), 1, "only the un-drained sibling stays dirty");
    assert_eq!(touched[0].0.1, wishlist);
    Ok(())
}

/// A mid-handler `rollback()` reverts reads to the `commit()` floor and issues
/// **zero durable writes** — it is `commit()` minus the write. `commit(V)`
/// lands `V`; a later `set(W)` then `rollback()` discards `W`, so the read is
/// `V` again, the committed row is still `V` (no provisional, no marker), and
/// the drain touched only this collection (the sibling's buffer stands).
#[tokio::test]
async fn rollback_restores_the_commit_floor_without_durable_writes() -> Result<()> {
    let fx = Fixture::with_collections(&["cart", "wishlist"])?;
    let (event, _dedup) = message(1);
    // The scope stays alive so its `Drop` clear does not race the dirty probe.
    let scope = fx.session(event);
    let session = scope.handle();

    let cart = StateName::try_new("cart")?;
    let wishlist = StateName::try_new("wishlist")?;

    // Commit V as the floor.
    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"V"))
        .await;
    let outcome = {
        let permit = session.permit().await;
        session
            .commit(&permit, StateType::Application, &cart)
            .await?
    };
    assert_eq!(outcome, StoreOutcome::Applied);

    // Buffer W over cart, and X over the sibling.
    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"W"))
        .await;
    session
        .seed(StateType::Application, &wishlist, &value_cell(), Some(b"X"))
        .await;

    // Rollback cart: the buffered W vanishes.
    assert_eq!(
        session.rollback(StateType::Application, &cart).await,
        StoreOutcome::Applied,
    );

    // The read is the floor V again.
    assert_eq!(
        session
            .get(StateType::Application, &cart, &value_cell())
            .await?,
        Some(Bytes::from_static(b"V")),
    );

    // Zero durable writes by the rollback: the committed row is still V, and
    // no provisional cell or event marker was created.
    let cart_id = CollectionId::new(fx.state_key.clone(), StateType::Application, cart);
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    assert_eq!(
        fx.cell_store()
            .get(&cart_id, &value_cell(), probe)
            .await?
            .into_inner(),
        Some(Bytes::from_static(b"V")),
    );
    assert!(fx.cells.provisional_coordinates(&cart_id).is_empty());
    assert!(fx.cells.unsettled_marker_of(&cart_id).is_none());

    // Sibling isolation: the rollback drained only cart; wishlist stands.
    let touched = fx.dirty.touched(&fx.state_key.key);
    assert_eq!(touched.len(), 1, "the rollback drained only its collection");
    assert_eq!(touched[0].0.1, wishlist);
    Ok(())
}

/// A stale, terminated session clone must **not** drain a later same-key
/// event's live buffer. The dirty workspace is shared per partition and keyed
/// only by `(key, collection)` — no event identity — so a handle a handler
/// moved into a spawned task addresses exactly the range the next same-key
/// event buffers into. `rollback()` on a terminated session is therefore a
/// `NoOp`: the same containment every fallible cell op gets from the
/// descriptor's live-guard, expressed as a `NoOp` because the infallible
/// signature cannot surface `Terminated`. Without the guard the stale clone
/// silently discards the next event's writes.
#[tokio::test]
async fn rollback_on_a_terminated_session_is_noop() -> Result<()> {
    let fx = Fixture::new()?;

    // Event 1 over its own cancel watch; leak a stale clone (the handle a
    // handler could move into a spawned task).
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let scope1 = fx.session_with_cancel(message(1).0, cancel_rx);
    let stale = scope1.handle();

    // Event 1 completes: its context is invalidated (cancel latched, exactly
    // what `PartitionEventContext::invalidate` does after every dispatch) and its
    // scope drops, clearing event 1's dirty range.
    cancel_tx.send(true)?;
    assert!(stale.is_terminated());
    drop(scope1);

    // Event 2 for the same key on a fresh, live cancel watch buffers W.
    let (_cancel_tx2, cancel_rx2) = watch::channel(false);
    let scope2 = fx.session_with_cancel(message(2).0, cancel_rx2);
    let session = scope2.handle();
    session
        .seed(
            StateType::Application,
            &fx.value_name,
            &value_cell(),
            Some(b"W"),
        )
        .await;

    // The stale clone's rollback finds a terminated session: NoOp, no drain.
    assert_eq!(
        stale.rollback(StateType::Application, &fx.value_name).await,
        StoreOutcome::NoOp,
    );

    // Event 2's buffer is intact: the key is still dirty and event 2 reads its
    // own W.
    assert!(!fx.dirty.touched(&fx.state_key.key).is_empty());
    assert_eq!(
        session
            .get(StateType::Application, &fx.value_name, &value_cell())
            .await?,
        Some(Bytes::from_static(b"W")),
    );
    Ok(())
}

/// Cap on ops per event: enough for commit/rollback/mutate interleavings,
/// small enough that a failing trace stays readable.
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
    /// The section-clear marker leg: buffers a dirty clear of the value's
    /// section, so the stage carries a durable marker with clears — a lone
    /// `ClearSection` produces a clears-only stage with an empty write set.
    ClearSection,
    /// The mid-handler write-through: everything buffered so far becomes
    /// durable immediately and survives every non-commit outcome.
    Commit,
    /// The mid-handler discard: everything buffered since the last `Commit`
    /// (or event start) vanishes; reads revert to the commit floor.
    Rollback,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// The success path. `fail_promote` schedules a transient promote failure
    /// for the event's settle, so it yields [`ApplyOutcome::Incomplete`] and
    /// durable recovery must converge through the loop-tail resolving reads.
    Commit {
        fail_promote: bool,
    },
    Abort,
    /// The attempt-boundary discard (retry between attempts): the receipt is
    /// dropped and `discard_dirty` clears the buffer.
    Reset,
    /// The final-error path: the event ends with no `finalize` and no
    /// discard (settle's error arms never finalize). The buffered write must
    /// neither commit nor linger — only the scope's `Drop` clears it.
    Failed,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // Sets weighted up so state actually accumulates between commits.
        match u8::arbitrary(g) % 7 {
            0..=2 => Self::Set(u8::arbitrary(g)),
            3 => Self::Clear,
            4 => Self::ClearSection,
            5 => Self::Commit,
            _ => Self::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set(b) => Box::new(b.shrink().map(Self::Set)),
            Self::Clear | Self::ClearSection | Self::Commit | Self::Rollback => {
                quickcheck::empty_shrinker()
            }
        }
    }
}

impl Arbitrary for ValueEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut ops: Vec<ValueOp> = Vec::<ValueOp>::arbitrary(g)
            .into_iter()
            .take(MAX_EVENT_OPS)
            .collect();
        // Precondition steering: roughly a quarter of events open with a
        // Rollback on a provably empty buffer (event start), pinning the NoOp
        // arm; the unconditioned draws above place Rollback after Set/Clear for
        // the Applied arm.
        if ops.len() < MAX_EVENT_OPS && u8::arbitrary(g) % 4 == 0 {
            ops.insert(0, ValueOp::Rollback);
        }
        let outcome = match u8::arbitrary(g) % 5 {
            0 => Outcome::Reset,
            1 => Outcome::Abort,
            2 => Outcome::Failed,
            _ => Outcome::Commit {
                fail_promote: u8::arbitrary(g) % 4 == 0,
            },
        };
        Self { ops, outcome }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        // A scheduled promote failure shrinks toward the calmer schedule
        // first, then the op list shrinks as usual.
        let calmed = matches!(outcome, Outcome::Commit { fail_promote: true }).then(|| Self {
            ops: self.ops.clone(),
            outcome: Outcome::Commit {
                fail_promote: false,
            },
        });
        Box::new(
            calmed
                .into_iter()
                .chain(self.ops.shrink().map(move |ops| Self { ops, outcome })),
        )
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

/// The scratch as of the last mid-handler `commit()` this event — durable
/// regardless of the event's outcome.
#[derive(Clone)]
enum Floor {
    /// No `commit()` landed this event; a rollback or non-commit outcome falls
    /// through to the pre-event committed value.
    Unset,
    /// The value the last `commit()` made durable.
    Committed(Option<Bytes>),
}

impl Floor {
    /// The durable value this floor tests, or `pre_event` when no `commit()`
    /// landed this event.
    fn resolve(&self, pre_event: Option<Bytes>) -> Option<Bytes> {
        match self {
            Self::Unset => pre_event,
            Self::Committed(value) => value.clone(),
        }
    }
}

/// The per-event projection the property tracks in lockstep with the session:
/// the running read (`scratch`), the last `commit()` snapshot (`floor`), and
/// whether anything is buffered since the last drain.
struct EventModel {
    scratch: Option<Bytes>,
    floor: Floor,
    buffered: bool,
}

/// The mid-handler drain's `Applied`/`NoOp` contract: `Applied` iff the buffer
/// held anything since the last drain.
fn expected_outcome(buffered: bool) -> StoreOutcome {
    if buffered {
        StoreOutcome::Applied
    } else {
        StoreOutcome::NoOp
    }
}

/// Applies one op to the session and `model`, returning `false` on a divergence
/// (a wrong `Applied`/`NoOp` outcome, or a session read that no longer tracks
/// the scratch model). `pre_event` is the committed value the event opened on —
/// the rollback fallback when no `commit()` landed this event.
async fn apply_value_op(
    session: &Session,
    name: &StateName,
    op: ValueOp,
    pre_event: Option<Bytes>,
    model: &mut EventModel,
) -> Result<bool> {
    match op {
        ValueOp::Set(byte) => {
            session
                .seed(StateType::Application, name, &value_cell(), Some(&[byte]))
                .await;
            model.scratch = Some(Bytes::copy_from_slice(&[byte]));
            model.buffered = true;
        }
        ValueOp::Clear => {
            session
                .seed(StateType::Application, name, &value_cell(), None)
                .await;
            model.scratch = None;
            model.buffered = true;
        }
        ValueOp::ClearSection => {
            // The value cell lives in section 0, so the dirty clear marker
            // masks it to absent within the event.
            session
                .seed_section_clear(StateType::Application, name, Section::new(0))
                .await;
            model.scratch = None;
            model.buffered = true;
        }
        ValueOp::Commit => {
            let outcome = {
                let permit = session.permit().await;
                session
                    .commit(&permit, StateType::Application, name)
                    .await?
            };
            if outcome != expected_outcome(model.buffered) {
                return Ok(false);
            }
            model.floor = Floor::Committed(model.scratch.clone());
            model.buffered = false;
        }
        ValueOp::Rollback => {
            let outcome = session.rollback(StateType::Application, name).await;
            if outcome != expected_outcome(model.buffered) {
                return Ok(false);
            }
            // Reads revert to the commit floor, or the pre-event committed
            // value if no commit() landed this event.
            model.scratch = model.floor.resolve(pre_event);
            model.buffered = false;
        }
    }
    // Equivalence after every operation: the session's own overlay read tracks
    // the scratch model, so a missed rollback discard or a lost buffered write
    // surfaces at the op that caused it.
    let read = session
        .get(StateType::Application, name, &value_cell())
        .await?;
    Ok(read == model.scratch)
}

/// Clause of the central property: `finalize` returns `Clean` iff nothing was
/// buffered since the event's last drain, and a `Staged` receipt's frozen
/// records equal the durable event marker the stage wrote. This is
/// receipt/durable-marker CONSISTENCY (both sides freeze through
/// [`EventMarker::frozen`]); survivor semantics are owned by the
/// crash-equivalence suite's clears dimension in `state::tests::cell_suite`.
fn finalize_matches_model(
    fx: &Fixture,
    event: EventRef,
    finalized: &Finalized<TestStore>,
    buffered: bool,
) -> Option<&'static str> {
    match finalized {
        Finalized::Clean if buffered => Some("finalize returned Clean over a buffered op"),
        Finalized::Staged(_) if !buffered => Some("finalize staged a receipt for a drained buffer"),
        Finalized::Clean => None,
        Finalized::Staged(staged) => {
            let [collection] = staged.collections.as_slice() else {
                return Some("the single-collection trace staged more than one record");
            };
            let expected = EventMarker::frozen(event, &collection.writes, &collection.clears);
            (fx.cells.unsettled_marker_of(&fx.value_id()) != Some(expected))
                .then_some("the receipt's frozen records diverge from the durable event marker")
        }
    }
}

/// Finalizes the event and checks the Clean-iff / receipt-consistency clause
/// ([`finalize_matches_model`]) plus drain-on-success — the stage consumed the
/// receipt's mint source, so a second finalize finds an empty buffer and
/// returns `Clean`. Yields the receipt; a divergence is an error.
async fn checked_finalize(
    fx: &Fixture,
    session: &Session,
    event: EventRef,
    buffered: bool,
) -> Result<Finalized<TestStore>> {
    let finalized = session.finalize().await?;
    if let Some(reason) = finalize_matches_model(fx, event, &finalized, buffered) {
        bail!("{reason}");
    }
    if !matches!(session.finalize().await?, Finalized::Clean) {
        bail!("a second finalize after success was not Clean");
    }
    Ok(finalized)
}

/// Consumes a committed event's receipt, `fail_promote` scheduling a transient
/// promote failure for exactly this settle. The memory store never fails, so a
/// healthy promote must fully resolve — and leave no residue, checked raw
/// before the loop-tail resolving reads heal a skipped settle to identical
/// bytes and mask it. A stage overwrites any marker a prior
/// Reset/Failed/Incomplete-promote event left unsettled (a clear-free marker
/// stands, cells healed, until the next stage; a marker with clears is
/// resolved by its own event's loop-tail read window), so the residue check is
/// exact on the healthy arm — and only there: those outcomes leave residue by
/// design. A poisoned promote must report `Incomplete`, leaving the stranded
/// stage for the loop-tail heals (cell-grained for staged writes, whole-marker
/// via the read window for clears — durable recovery converges).
async fn promote_receipt(
    fx: &Fixture,
    staged: StagedState<TestStore>,
    fail_promote: bool,
) -> Result<()> {
    if fail_promote {
        fx.set_poison(Some(Poison::Collection(
            fx.value_name.clone(),
            ErrorCategory::Transient,
        )));
    }
    let outcome = staged.certify().promote().await;
    fx.set_poison(None);
    match (fail_promote, outcome) {
        (false, ApplyOutcome::Resolved) => {
            assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
            Ok(())
        }
        (false, ApplyOutcome::Incomplete) => bail!("a healthy promote reported Incomplete"),
        (true, ApplyOutcome::Incomplete) => Ok(()),
        (true, ApplyOutcome::Resolved) => bail!("a poisoned promote reported Resolved"),
    }
}

/// Drives the trace through the real session lifecycle, checking the central
/// property's clauses (the module doc lists them): the session's own read
/// equals a plain `Option<Bytes>` model after every operation, `finalize`
/// answers `Clean` iff nothing was buffered (with the receipt's frozen records
/// matching the durable marker, and a second finalize `Clean`), the consumed
/// receipt converges, and the overlay + committed projections equal the model
/// after every event. Errors carry the divergence reason; `Ok(())` means the
/// trace upholds the property.
///
/// A mid-event `commit()` snapshots the scratch model as immediately durable:
/// on a commit the full scratch wins, on every other outcome the durable state
/// must equal the last `commit()`-landed snapshot (post-commit ops roll back;
/// `commit()`-landed ops survive) — the at-least-once `commit()` contract. A
/// `Rollback` reverts the scratch to the commit floor (or the pre-event
/// committed value) and must report `Applied` iff anything was buffered.
async fn run(trace: Trace) -> Result<()> {
    let fx = Fixture::new()?;
    let mut model: Option<Bytes> = None;
    let key = fx.state_key.key.clone();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        // The per-event projection, tracked in lockstep with the session.
        let mut ev_model = EventModel {
            scratch: model.clone(),
            floor: Floor::Unset,
            buffered: false,
        };
        // The scope drops at the end of this block — the production per-event
        // lifetime that clears the shared dirty buffer.
        {
            let scope = fx.session(event);
            let session = scope.handle();

            for op in &ev.ops {
                if !apply_value_op(&session, &fx.value_name, *op, model.clone(), &mut ev_model)
                    .await?
                {
                    bail!("an op outcome or read diverged from the model");
                }
            }

            match ev.outcome {
                Outcome::Commit { fail_promote } => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    // The driver simulates the settle boundary's marker
                    // record — a direct oracle write, strictly after the
                    // stage (the session exposes no marker write; the real
                    // one is settlement-module-private).
                    fx.oracle.record_message(dedup_id).await?;
                    if let Finalized::Staged(staged) = finalized {
                        promote_receipt(&fx, staged, fail_promote).await?;
                    }
                    // Commit advances the model (last-writer-wins).
                    model = ev_model.scratch;
                }
                Outcome::Abort => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    if let Finalized::Staged(staged) = finalized {
                        staged.rollback().await;
                        // Same raw probe as `promote_receipt`'s healthy arm: a
                        // rollback that skipped its store call would be healed
                        // to identical bytes by the loop-tail resolving reads
                        // and masked.
                        assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
                    }
                    // Post-commit ops roll back to their `prev`, which
                    // finalize captured *after* the `commit()` landed — the
                    // `commit()`-landed snapshot.
                    model = ev_model.floor.resolve(model);
                }
                Outcome::Reset => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    // Dropping the receipt leaves any provisional written by
                    // `finalize` unsettled, projecting its `prev` (the
                    // `commit()`-landed snapshot, or the unchanged committed
                    // base) — exactly the discarded-stage behavior the
                    // attempt-boundary `discard_dirty` pairs with.
                    drop(finalized);
                    session.discard_dirty();
                    model = ev_model.floor.resolve(model);
                }
                // Final-error path: no `finalize`, no `reset`. Only the
                // scope's `Drop` clears the buffered write — but a `commit()`
                // already wrote its snapshot through, and it must survive.
                Outcome::Failed => {
                    model = ev_model.floor.resolve(model);
                }
            }
        }

        // The shared dirty buffer is empty for the key — no per-event leak.
        if !fx.dirty.touched(&key).is_empty() {
            bail!("the shared dirty buffer leaked past the event");
        }
        // A fresh overlay read (the dirty short-circuit path) tracks the model:
        // a leaked dirty cell would surface here as a read of uncommitted state.
        if fx.overlay_value().await? != model {
            bail!("the overlay read diverged from the model");
        }
        // The committed projection still tracks the model.
        if fx.committed_value().await? != model {
            bail!("the committed projection diverged from the model");
        }
    }
    Ok(())
}

/// The central settlement property: the Value session lifecycle is sound over
/// random mixed-outcome traces — the module doc lists the clauses.
#[test]
fn prop_value_lifecycle_equivalence() {
    fn prop(trace: Trace) -> TestResult {
        match executor::block_on(run(trace)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}

/// A collection failing mid-stage exits `finalize` with the buffer **whole**
/// — the drain runs strictly after the whole per-collection aggregate
/// succeeded, never per collection — so the retried `finalize` re-stages the
/// same buffered ops idempotently and converges once the store heals. A
/// per-collection (or pre-error) drain would strand the healed retry with an
/// empty buffer.
#[tokio::test]
async fn failed_finalize_keeps_the_buffer_whole_for_retry() -> Result<()> {
    let fx = Fixture::with_collections(&["cart", "wishlist"])?;
    let cart = fx.value_name.clone();
    let wishlist = StateName::try_new("wishlist")?;
    // Poison wishlist's stage: `write_provisional` fails Transient while cart
    // (racing under buffer_unordered) may or may not have staged first.
    fx.set_poison(Some(Poison::WriteProvisional(
        wishlist.clone(),
        ErrorCategory::Transient,
    )));
    let (event, dedup_id) = message(1);
    // The scope stays alive across the dirty probes below: its `Drop` would
    // clear the very buffer the failed finalize must leave whole.
    let scope = fx.session(event);
    let session = scope.handle();

    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"c1"))
        .await;
    session
        .seed(
            StateType::Application,
            &wishlist,
            &value_cell(),
            Some(b"w1"),
        )
        .await;

    assert!(
        session.finalize().await.is_err(),
        "the poisoned stage must fail the aggregate",
    );
    let touched: Vec<StateName> = fx
        .dirty
        .touched(&fx.state_key.key)
        .into_iter()
        .map(|((_, name), ..)| name)
        .collect();
    assert_eq!(
        touched.len(),
        2,
        "a failed finalize keeps BOTH collections buffered: {touched:?}",
    );
    assert!(touched.contains(&cart) && touched.contains(&wishlist));

    // Heal the store: the retried finalize re-stages from the intact buffer
    // and the settle converges to both buffered values.
    fx.set_poison(None);
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the healed retry must re-stage from the intact buffer");
    };
    fx.oracle.record_message(dedup_id).await?;
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
    for (name, expected) in [(&cart, b"c1"), (&wishlist, b"w1")] {
        let id = CollectionId::new(fx.state_key.clone(), StateType::Application, name.clone());
        assert_eq!(
            fx.cell_store()
                .get(&id, &value_cell(), probe(u128::MAX))
                .await?
                .into_inner(),
            Some(Bytes::from_static(expected)),
            "{name:?} must commit its buffered value on the healed retry",
        );
    }
    Ok(())
}

/// The clears-only stage runs the stage-boundary prior event-marker resolve: a
/// prior event's unsettled committed marker (seeded crash-style through a raw
/// store handle) is resolved — its cells settle per its verdict — rather than
/// blind-deleted by the clears-only event's own settle, and the session's own
/// marker with clears is written by `finalize` then deleted by the
/// receipt's `promote` (which also applies the clear's gap erase).
///
/// The generated crash/reassignment alphabet (the crash-trace generator's
/// clears dimension) subsumes this shape; these two tests are kept as the fast,
/// deterministic falsifiers for the clears-only boundary arm, mirroring the
/// `boundary_resolve_pin` role in the `state::tests` crash-equivalence suite.
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
        .seed_section_clear(StateType::Application, &fx.value_name, Section::new(0))
        .await;
    // The receipt is held across the raw probes below, then consumed.
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the clears-only event must stage");
    };

    // Raw probes before any resolving read: the boundary resolved A's marker
    // (nothing of A stays provisional; A's cells settled per its verdict) and
    // B's marker with clears replaced it.
    let unsettled = fx
        .cells
        .unsettled_marker_of(&id)
        .ok_or_else(|| eyre!("B's clears-only marker must stand after the stage"))?;
    assert_eq!(unsettled.event(), b, "B's marker replaced A's");
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
    fx.oracle.record_message(b_dedup).await?;
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
    assert!(
        fx.cells.unsettled_marker_of(&id).is_none(),
        "the settle deleted B's marker with clears"
    );
    assert!(
        fx.cells.stored_coordinates(&id).is_empty(),
        "B's committed clear erased the section"
    );
    Ok(())
}

/// Clears-only session boundary resolve when the prior event committed.
#[tokio::test]
async fn clears_only_session_boundary_resolves_committed_foreign_marker() -> Result<()> {
    clears_only_session_boundary(true).await
}

/// Clears-only session boundary resolve when the prior event aborted.
#[tokio::test]
async fn clears_only_session_boundary_resolves_aborted_foreign_marker() -> Result<()> {
    clears_only_session_boundary(false).await
}

/// A retry attempt re-runs `finalize`: the second stage **rebuilds** the same
/// event's durable marker from its own staged set — never keeps the first
/// attempt's frozen payload, never resolves it as prior event — the settle
/// converges to the retried values, and no event marker stands afterwards.
/// The two attempts stage *different* cell sets so a kept (stale) marker is
/// observable: recovery resolves exactly the coordinates the marker lists, so
/// a stale list would strand the retry's extra cell. An example because the
/// lifecycle trace generator does not carry retry re-finalize, and the
/// idempotent same-event marker overwrite is a narrow protocol edge the
/// value-projection model does not observe.
#[tokio::test]
async fn retry_refinalize_overwrites_the_same_event_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event).handle();
    let extra = cell_at(7);

    session
        .seed(
            StateType::Application,
            &fx.value_name,
            &value_cell(),
            Some(b"v1"),
        )
        .await;
    // Attempt one's receipt is deliberately dropped — the discarded stage the
    // retry boundary pairs with `reset`.
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));

    // The retry boundary: discard the attempt's dirty ops, then re-dispatch
    // the same event.
    session.discard_dirty();

    // The retry stages a superset — the Value cell again plus one more cell —
    // so the rebuilt marker's coordinate list differs from attempt one's.
    session
        .seed(
            StateType::Application,
            &fx.value_name,
            &value_cell(),
            Some(b"v2"),
        )
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &extra, Some(b"w"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the retry re-stage must mint a receipt");
    };

    // The unsettled durable marker is the retry's, rebuilt whole: same event,
    // and its frozen coordinate list is attempt two's staged set — not
    // attempt one's single cell.
    let marker = fx
        .cells
        .unsettled_marker_of(&fx.value_id())
        .ok_or_else(|| eyre!("no unsettled marker after the re-stage"))?;
    assert_eq!(marker.event(), event, "the marker stays the same event's");
    assert_eq!(
        marker.staged(),
        [value_cell(), extra.clone()],
        "the re-run rebuilds the marker from its own staged set"
    );

    fx.oracle.record_message(dedup_id).await?;
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);

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
        fx.cells.unsettled_marker_of(&fx.value_id()).is_none(),
        "the settle deleted the single (overwritten) event marker"
    );
    Ok(())
}

/// Proves that an event cannot resolve its own marker during a read.
///
/// Early resolution would settle the event before its handler completes.
#[tokio::test]
async fn own_event_read_does_not_resolve_its_own_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let raw = fx.cell_store();
    let id = fx.value_id();
    let collection = CollectionRef::new(id.clone(), None);

    // Stage event E's marker with clears directly (one survivor cell in the
    // cleared section) and leave E unrecorded — in-flight, uncommitted.
    let (e, _e_dedup) = message(1);
    let writes = [(
        cell_at(0),
        ProvisionalWrite::new(Some(Bytes::from_static(b"s")), Committed::new(None), e),
    )];
    let clears = [SectionClear::frozen(Section::new(0), &writes)];
    let marker = EventMarker::frozen(e, &writes, &clears);
    raw.write_provisional(&collection, &writes, Some(&marker))
        .await?;

    // An event read must not settle its own marker.
    raw.get(&id, &cell_at(0), e).await?;
    let unsettled = fx
        .cells
        .unsettled_marker_of(&id)
        .ok_or_else(|| eyre!("an own-event read must leave the in-flight marker unsettled"))?;
    assert_eq!(
        unsettled.event(),
        e,
        "the own read left E's marker untouched"
    );
    assert!(
        !fx.cells.provisional_coordinates(&id).is_empty(),
        "the own read settled nothing — E's staged cell is still provisional",
    );

    // Contrast — the resolve path is reachable, so the guard (not an inert
    // resolve) is what protects the own read: a *prior event* read of the same
    // uncommitted marker resolves it (verdict: not committed → rolled back, the
    // marker deleted).
    raw.get(&id, &cell_at(0), probe(999)).await?;
    assert!(
        fx.cells.unsettled_marker_of(&id).is_none(),
        "a prior event read resolves the uncommitted marker away",
    );
    Ok(())
}

// ---- Batched read-committed staging: query-count + prev-pairing tests ----
//
// These test the batch-read staging change: a `ReadCommitted` stage reads its
// survivors' committed bases through one `get_many` per section chunk (never a
// point read per cell), and each chunk's bases pair with EXACTLY the dirty
// records that produced it. The query-count property derives its expected batch
// count from the stage's dirty *input* (independent of the code under test);
// the pairing tests seed distinct per-cell bases and abort, so a reversed
// pairing diverges.

/// The counting cell store the query-count fixture mints: a
/// [`CountingCellStore`] over shared in-memory cells, so `batch_reads` /
/// `visible_point_reads` count exactly the stage's `get_many` / `get`.
type CountingCell = CountingCellStore<MemoryCellStore<ScriptedOracle>>;
type CountingBackend =
    PartitionBackend<ScriptedOracle, MemoryDescriptorIdentityStore, CountingCell>;
type CountingSession = KeyedStateSession<CountingBackend, ()>;

/// The cell at `(section, coord)` — a single-byte coordinate, so byte order is
/// numeric order. Lets one Value session buffer across many sections.
fn cell_in(section: i8, coord: u8) -> CellKey {
    CellKey {
        section: Section::new(section),
        coordinate: Coordinate::from_bytes(vec![coord]),
    }
}

/// A single-collection session over a fresh key (row isolation) whose lower
/// store counts every read the stage issues. Keeps the counting-store handle
/// (shares the session's op counters), the shared dirty workspace (read for the
/// stage's input truth before `finalize` drains it), the oracle (records the
/// message marker so a promoted stage's committed projection resolves), and the
/// collection id / name.
struct CountingFixture {
    session: CountingSession,
    counting: CountingCell,
    dirty: Arc<DirtyStore>,
    oracle: ScriptedOracle,
    id: CollectionId,
    name: StateName,
    state_key: StateKey,
    dedup_id: Uuid,
    _shutdown_tx: watch::Sender<ShutdownPhase>,
    _cancel_tx: watch::Sender<bool>,
}

impl CountingFixture {
    /// One Value collection named `name`; `read_uncommitted` selects its commit
    /// mode (the registry reads the mode from the `CollectionDef`, not the
    /// descriptor).
    fn new(read_uncommitted: bool, name: &str) -> Result<Self> {
        let state_name = StateName::try_new(name)?;
        let commit_mode = if read_uncommitted {
            CommitMode::ReadUncommitted
        } else {
            CommitMode::ReadCommitted
        };
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &value_state::<JsonCodec>(name),
            CollectionDef {
                commit_mode,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let oracle = ScriptedOracle::default();
        let counting = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            oracle.clone(),
            registry.clone(),
        ));
        let dirty = Arc::new(DirtyStore::new());
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let dedup_id = Uuid::new_v4();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let session = KeyedStateSession::new(SessionParts {
            cell: counting.clone(),
            dirty: dirty.clone(),
            oracle: oracle.clone(),
            loader: (),
            registry,
            state_key: state_key.clone(),
            event: EventRef::Message { dedup_id },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        });
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            state_name.clone(),
        );
        Ok(Self {
            session,
            counting,
            dirty,
            oracle,
            id,
            name: state_name,
            state_key,
            dedup_id,
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// The `get_many` batches the RC stage will issue, derived the way
    /// `stage_collection` groups its input: survivors (dirty cells not subsumed
    /// by a section clear) per section, ceil-divided by `CELL_BATCH`. Zero when
    /// nothing survives. Read from the live dirty store before `finalize`
    /// drains it, so it is the stage's input, never its output.
    fn expected_batches(&self) -> usize {
        let touched = self.dirty.touched(&self.state_key.key);
        let Some((_, cleared, cells)) = touched.iter().find(|((_, n), ..)| *n == self.name) else {
            return 0;
        };
        let mut per_section: HashMap<Section, usize> = HashMap::new();
        for (cell, value) in cells {
            let subsumed = *value == DirtyVal::Cleared && cleared.contains(&cell.section);
            if !subsumed {
                *per_section.entry(cell.section).or_default() += 1;
            }
        }
        per_section.values().map(|n| n.div_ceil(CELL_BATCH)).sum()
    }

    /// Settles a finalized receipt so the committed projection is readable: a
    /// `Staged` receipt records the message marker (the boundary's post-stage
    /// order) then promotes. `Clean` (RU direct writes, or nothing staged)
    /// needs no settle.
    async fn settle(&self, finalized: Finalized<CountingCell>) -> Result<()> {
        if let Finalized::Staged(staged) = finalized {
            self.oracle.record_message(self.dedup_id).await?;
            assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
        }
        Ok(())
    }

    /// The committed value of `cell`, read through a prior event probe so the
    /// read resolves raw committed truth.
    async fn committed(&self, cell: &CellKey) -> Result<Option<Bytes>> {
        Ok(self
            .counting
            .get(&self.id, cell, probe(u128::MAX))
            .await?
            .into_inner())
    }
}

/// One staging op: a set / clear at `(section, coord)` or a whole-section
/// clear. `section` is already reduced to a small index so a handful of
/// sections actually collide (multi-section grouping and section-clear
/// subsumption both need repeats).
#[derive(Clone, Copy, Debug)]
enum StageOp {
    Set { section: u8, coord: u8, byte: u8 },
    Clear { section: u8, coord: u8 },
    ClearSection { section: u8 },
}

impl Arbitrary for StageOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 | 1 => Self::Set {
                section: u8::arbitrary(g) % 3,
                coord: u8::arbitrary(g),
                byte: u8::arbitrary(g),
            },
            2 | 3 => Self::Clear {
                section: u8::arbitrary(g) % 3,
                coord: u8::arbitrary(g),
            },
            _ => Self::ClearSection {
                section: u8::arbitrary(g) % 3,
            },
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set {
                section,
                coord,
                byte,
            } => Box::new((coord, byte).shrink().map(move |(coord, byte)| Self::Set {
                section,
                coord,
                byte,
            })),
            Self::Clear { section, coord } => Box::new(
                coord
                    .shrink()
                    .map(move |coord| Self::Clear { section, coord }),
            ),
            Self::ClearSection { .. } => quickcheck::empty_shrinker(),
        }
    }
}

/// Replays `ops` into a plain model mirroring the dirty store's
/// last-writer-wins semantics: `set`/`clear` upsert a cell, `clear_section`
/// marks the section and drops its buffered cells (exactly
/// `DirtyStore::clear_section`). Returns the per-cell outcomes and the cleared
/// sections — the stage's input, modeled independently of `stage_collection`.
fn replay_dirty(ops: &[StageOp]) -> (HashMap<CellKey, DirtyVal>, HashSet<Section>) {
    let mut cells: HashMap<CellKey, DirtyVal> = HashMap::new();
    let mut cleared: HashSet<Section> = HashSet::new();
    for op in ops {
        match *op {
            StageOp::Set {
                section,
                coord,
                byte,
            } => {
                cells.insert(
                    cell_in(section as i8, coord),
                    DirtyVal::Set(Bytes::copy_from_slice(&[byte])),
                );
            }
            StageOp::Clear { section, coord } => {
                cells.insert(cell_in(section as i8, coord), DirtyVal::Cleared);
            }
            StageOp::ClearSection { section } => {
                let s = Section::new(section as i8);
                cleared.insert(s);
                cells.retain(|c, _| c.section != s);
            }
        }
    }
    (cells, cleared)
}

/// Applies `ops` to `session` in order — the same sequence [`replay_dirty`]
/// models. Generic over the backend so both fixtures drive it.
async fn apply_stage_ops<B: StateBackend>(
    session: &KeyedStateSession<B, ()>,
    name: &StateName,
    ops: &[StageOp],
) {
    for op in ops {
        match *op {
            StageOp::Set {
                section,
                coord,
                byte,
            } => {
                session
                    .seed(
                        StateType::Application,
                        name,
                        &cell_in(section as i8, coord),
                        Some(&[byte]),
                    )
                    .await;
            }
            StageOp::Clear { section, coord } => {
                session
                    .seed(
                        StateType::Application,
                        name,
                        &cell_in(section as i8, coord),
                        None,
                    )
                    .await;
            }
            StageOp::ClearSection { section } => {
                session
                    .seed_section_clear(StateType::Application, name, Section::new(section as i8))
                    .await;
            }
        }
    }
}

/// Every cell a set/clear op names (a section-clear names no cell).
fn touched_cells(ops: &[StageOp]) -> HashSet<CellKey> {
    ops.iter()
        .filter_map(|op| match *op {
            StageOp::Set { section, coord, .. } | StageOp::Clear { section, coord } => {
                Some(cell_in(section as i8, coord))
            }
            StageOp::ClearSection { .. } => None,
        })
        .collect()
}

/// A staging population: one op sequence under a chosen commit mode.
#[derive(Clone, Debug)]
struct StagePop {
    ru: bool,
    ops: Vec<StageOp>,
}

impl Arbitrary for StagePop {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ru: bool::arbitrary(g),
            ops: Vec::<StageOp>::arbitrary(g).into_iter().take(40).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ru = self.ru;
        Box::new(self.ops.shrink().map(move |ops| Self { ru, ops }))
    }
}

/// Drives one staging population through the real `finalize` and checks the
/// query-count law plus the committed projection.
async fn run_stage_query_counts(pop: StagePop) -> Result<()> {
    let fx = CountingFixture::new(pop.ru, "qc")?;
    apply_stage_ops(&fx.session, &fx.name, &pop.ops).await;

    // The expected batch count, derived from the stage's dirty input.
    let expected_batches = fx.expected_batches();
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;

    // Query-count law: never a visible point read; RC issues exactly the
    // per-section batch count, RU reads no bases at all.
    if fx.counting.visible_point_reads() != 0 {
        bail!(
            "stage issued {} visible point reads, must be 0",
            fx.counting.visible_point_reads()
        );
    }
    let expected_reads = if pop.ru { 0 } else { expected_batches };
    if fx.counting.batch_reads() != expected_reads {
        bail!(
            "stage issued {} batch reads, expected {expected_reads}",
            fx.counting.batch_reads()
        );
    }

    // Committed projection parity: settle, then every touched cell equals the
    // model's `into_data` (Set → its bytes, Clear / cleared-section / untouched
    // → absent).
    fx.settle(finalized).await?;
    let (cells, _cleared) = replay_dirty(&pop.ops);
    for cell in &touched_cells(&pop.ops) {
        let expected = match cells.get(cell) {
            Some(DirtyVal::Set(b)) => Some(b.clone()),
            Some(DirtyVal::Cleared) | None => None,
        };
        let committed = fx.committed(cell).await?;
        if committed != expected {
            bail!("committed {cell:?} = {committed:?}, expected {expected:?}");
        }
    }
    Ok(())
}

/// The query-count law over random op sequences in both commit modes: the RC
/// stage reads committed bases in exactly `Σ_section ceil(survivors /
/// CELL_BATCH)` batches and zero point reads, RU reads no bases, and the
/// committed projection tracks the model regardless.
#[test]
fn prop_stage_query_counts() {
    fn prop(pop: StagePop) -> TestResult {
        match executor::block_on(run_stage_query_counts(pop)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(StagePop) -> TestResult);
}

/// `n` distinct cells in one section stage in `ceil(n / CELL_BATCH)` batches
/// and zero point reads — the batch-boundary law at the 127/128/129 edges.
async fn stage_query_count_section_size(n: usize, expected_batches: usize) -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    for c in 0..n {
        fx.session
            .seed(
                StateType::Application,
                &fx.name,
                &cell_in(0, c as u8),
                Some(b"v"),
            )
            .await;
    }
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(fx.counting.visible_point_reads(), 0);
    assert_eq!(fx.counting.batch_reads(), expected_batches);
    fx.settle(finalized).await
}

#[tokio::test]
async fn stage_query_count_section_size_127() -> Result<()> {
    stage_query_count_section_size(127, 1).await
}

#[tokio::test]
async fn stage_query_count_section_size_128() -> Result<()> {
    stage_query_count_section_size(128, 1).await
}

#[tokio::test]
async fn stage_query_count_section_size_129() -> Result<()> {
    stage_query_count_section_size(129, 2).await
}

/// Batches split PER SECTION, never across: 130 cells in one section plus 5 in
/// another read in `ceil(130/128) + ceil(5/128) = 3` batches — a global count
/// split would be 2 and would feed `get_many` a cross-section batch.
#[tokio::test]
async fn stage_query_count_splits_per_section() -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    for c in 0..130u16 {
        fx.session
            .seed(
                StateType::Application,
                &fx.name,
                &cell_in(0, c as u8),
                Some(b"v"),
            )
            .await;
    }
    for c in 0..5u8 {
        fx.session
            .seed(StateType::Application, &fx.name, &cell_in(1, c), Some(b"v"))
            .await;
    }
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(fx.counting.visible_point_reads(), 0);
    assert_eq!(
        fx.counting.batch_reads(),
        3,
        "ceil(130/128) + ceil(5/128) = 2 + 1",
    );
    fx.settle(finalized).await
}

/// A stage with no surviving cells builds no batch and issues no read: a
/// section clear followed by a clear of one of its cells leaves only a
/// `Cleared` cell subsumed by the unsettled section marker. The clears-only
/// stage still writes its marker (a separate counter), but reads nothing.
#[tokio::test]
async fn stage_clears_only_issues_no_read() -> Result<()> {
    let fx = CountingFixture::new(false, "qc")?;
    fx.session
        .seed_section_clear(StateType::Application, &fx.name, Section::new(0))
        .await;
    fx.session
        .seed(StateType::Application, &fx.name, &cell_in(0, 7), None)
        .await;
    fx.counting.reset();
    let finalized = fx.session.finalize().await?;
    assert_eq!(
        fx.counting.batch_reads(),
        0,
        "a subsumed Cleared cell must build no batch",
    );
    assert_eq!(fx.counting.visible_point_reads(), 0);
    fx.settle(finalized).await
}

/// Each staged cell pairs with its OWN committed base: two same-section cells
/// with distinct committed bases `A`/`B`, overwritten and then aborted, restore
/// to `A`/`B` — a reversed base pairing would restore them swapped. The
/// deterministic pairing falsifier (query counts stay green under a swap).
#[tokio::test]
async fn stage_restores_distinct_bases_on_abort() -> Result<()> {
    let fx = Fixture::new()?;
    let c0 = cell_at(0);
    let c1 = cell_at(1);

    // Seed distinct committed bases A / B for the two same-section cells.
    let (event, dedup) = message(1);
    let session = fx.session(event).handle();
    session
        .seed(StateType::Application, &fx.value_name, &c0, Some(b"A"))
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &c1, Some(b"B"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the seeding event must stage");
    };
    fx.oracle.record_message(dedup).await?;
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);

    // Overwrite both, then abort: each cell rolls back to its own base.
    let (event, _dedup) = message(2);
    let session = fx.session(event).handle();
    session
        .seed(StateType::Application, &fx.value_name, &c0, Some(b"X"))
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &c1, Some(b"Y"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the overwriting event must stage");
    };
    staged.rollback().await;

    let probe = probe(u128::MAX);
    assert_eq!(
        fx.cell_store()
            .get(&fx.value_id(), &c0, probe)
            .await?
            .into_inner(),
        Some(Bytes::from_static(b"A")),
        "c0 restored to its own base",
    );
    assert_eq!(
        fx.cell_store()
            .get(&fx.value_id(), &c1, probe)
            .await?
            .into_inner(),
        Some(Bytes::from_static(b"B")),
        "c1 restored to its own base",
    );
    Ok(())
}

/// One event's outcome in the multi-section equivalence trace.
#[derive(Clone, Copy, Debug)]
enum MultiOutcome {
    /// Commit and promote; `fail_promote` poisons the promote so it reports
    /// `Incomplete` and the committed projection heals through the resolving
    /// read window.
    Commit { fail_promote: bool },
    /// Stage then roll back — the committed projection is unchanged.
    Abort,
    /// Drop the receipt, discard the dirty buffer, then re-apply and commit —
    /// the retry re-stages over its own unsettled provisional cells' base.
    Retry,
}

impl Arbitrary for MultiOutcome {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Abort,
            1 => Self::Retry,
            2 => Self::Commit { fail_promote: true },
            _ => Self::Commit {
                fail_promote: false,
            },
        }
    }
}

/// One event: a short op list and its outcome.
#[derive(Clone, Debug)]
struct MultiEvent {
    ops: Vec<StageOp>,
    outcome: MultiOutcome,
}

impl Arbitrary for MultiEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: Vec::<StageOp>::arbitrary(g).into_iter().take(6).collect(),
            outcome: MultiOutcome::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        Box::new(self.ops.shrink().map(move |ops| Self { ops, outcome }))
    }
}

/// A trace of multi-section RC events over one key.
#[derive(Clone, Debug)]
struct MultiTrace {
    events: Vec<MultiEvent>,
}

impl Arbitrary for MultiTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: Vec::<MultiEvent>::arbitrary(g)
                .into_iter()
                .take(6)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// A staging op with its byte resolved to a trace-unique value, so every cell
/// with an unsettled committed base has a DISTINCT base — the condition that
/// makes a reversed base pairing observable.
#[derive(Clone, Debug)]
enum ConcreteOp {
    Set(CellKey, u8),
    Clear(CellKey),
    ClearSection(Section),
}

/// Assigns each `Set` a trace-unique byte (from `next`), so re-applying the
/// same list on a retry reproduces the identical bases.
fn concrete_ops(ops: &[StageOp], next: &mut u8) -> Vec<ConcreteOp> {
    ops.iter()
        .map(|op| match *op {
            StageOp::Set { section, coord, .. } => {
                let byte = *next;
                *next = next.wrapping_add(1);
                ConcreteOp::Set(cell_in(section as i8, coord), byte)
            }
            StageOp::Clear { section, coord } => ConcreteOp::Clear(cell_in(section as i8, coord)),
            StageOp::ClearSection { section } => {
                ConcreteOp::ClearSection(Section::new(section as i8))
            }
        })
        .collect()
}

/// Applies concrete ops to the session.
async fn apply_concrete(session: &Session, name: &StateName, ops: &[ConcreteOp]) {
    for op in ops {
        match op {
            ConcreteOp::Set(cell, byte) => {
                session
                    .seed(StateType::Application, name, cell, Some(&[*byte]))
                    .await;
            }
            ConcreteOp::Clear(cell) => {
                session.seed(StateType::Application, name, cell, None).await;
            }
            ConcreteOp::ClearSection(section) => {
                session
                    .seed_section_clear(StateType::Application, name, *section)
                    .await;
            }
        }
    }
}

/// The event's net surviving `Set` cells and cleared sections (mirroring
/// [`replay_dirty`] over concrete ops): a `Clear` removes its cell, a
/// `ClearSection` drops the whole section's buffered cells.
fn replay_concrete(ops: &[ConcreteOp]) -> (HashMap<CellKey, Bytes>, HashSet<Section>) {
    let mut cells: HashMap<CellKey, Bytes> = HashMap::new();
    let mut cleared: HashSet<Section> = HashSet::new();
    for op in ops {
        match op {
            ConcreteOp::Set(cell, byte) => {
                cells.insert(cell.clone(), Bytes::copy_from_slice(&[*byte]));
            }
            ConcreteOp::Clear(cell) => {
                cells.remove(cell);
            }
            ConcreteOp::ClearSection(section) => {
                cleared.insert(*section);
                cells.retain(|c, _| c.section != *section);
            }
        }
    }
    (cells, cleared)
}

/// Advances the committed-projection model by one committed event: erase every
/// cleared section's committed rows, delete each surviving `Clear`'s committed
/// row, then write each surviving `Set`'s bytes.
fn commit_into_model(
    model: &mut HashMap<CellKey, Bytes>,
    cells: &HashMap<CellKey, Bytes>,
    cleared: &HashSet<Section>,
    surviving_clears: &HashSet<CellKey>,
) {
    for section in cleared {
        model.retain(|c, _| c.section != *section);
    }
    for cell in surviving_clears {
        model.remove(cell);
    }
    for (cell, bytes) in cells {
        model.insert(cell.clone(), bytes.clone());
    }
}

/// Drives a multi-section RC trace through the real lifecycle, asserting the
/// committed projection tracks the model after every event across commit /
/// abort / retry outcomes and multiple sections. Distinct per-cell bases make a
/// reversed base pairing diverge here.
async fn run_multi_section(trace: MultiTrace) -> Result<()> {
    let fx = Fixture::new()?;
    let name = fx.value_name.clone();
    let mut model: HashMap<CellKey, Bytes> = HashMap::new();
    let mut all: HashSet<CellKey> = HashSet::new();
    let mut next_byte: u8 = 1;

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup) = message(index as u128 + 1);
        let concrete = concrete_ops(&ev.ops, &mut next_byte);
        let (cells, cleared) = replay_concrete(&concrete);
        // Cells a surviving `Clear` deletes (present as an explicit absence in
        // the net state: named by a clear, not re-set, in a non-cleared
        // section). Reconstruct from the concrete ops for the commit model.
        let mut surviving_clears: HashSet<CellKey> = HashSet::new();
        for op in &concrete {
            match op {
                ConcreteOp::Clear(cell) if !cleared.contains(&cell.section) => {
                    surviving_clears.insert(cell.clone());
                }
                ConcreteOp::Set(cell, _) => {
                    surviving_clears.remove(cell);
                }
                _ => {}
            }
        }
        for op in &concrete {
            if let ConcreteOp::Set(cell, _) | ConcreteOp::Clear(cell) = op {
                all.insert(cell.clone());
            }
        }

        {
            let scope = fx.session(event);
            let session = scope.handle();
            apply_concrete(&session, &name, &concrete).await;

            match ev.outcome {
                MultiOutcome::Commit { fail_promote } => {
                    let finalized = session.finalize().await?;
                    fx.oracle.record_message(dedup).await?;
                    if let Finalized::Staged(staged) = finalized {
                        if fail_promote {
                            fx.set_poison(Some(Poison::Collection(
                                name.clone(),
                                ErrorCategory::Transient,
                            )));
                        }
                        let outcome = staged.certify().promote().await;
                        fx.set_poison(None);
                        match (fail_promote, outcome) {
                            (false, ApplyOutcome::Resolved) | (true, ApplyOutcome::Incomplete) => {}
                            (false, ApplyOutcome::Incomplete) => {
                                bail!("a healthy promote reported Incomplete")
                            }
                            (true, ApplyOutcome::Resolved) => {
                                bail!("a poisoned promote reported Resolved")
                            }
                        }
                    }
                    commit_into_model(&mut model, &cells, &cleared, &surviving_clears);
                }
                MultiOutcome::Abort => {
                    if let Finalized::Staged(staged) = session.finalize().await? {
                        staged.rollback().await;
                    }
                }
                MultiOutcome::Retry => {
                    drop(session.finalize().await?);
                    session.discard_dirty();
                    apply_concrete(&session, &name, &concrete).await;
                    let finalized = session.finalize().await?;
                    fx.oracle.record_message(dedup).await?;
                    if let Finalized::Staged(staged) = finalized {
                        assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
                    }
                    commit_into_model(&mut model, &cells, &cleared, &surviving_clears);
                }
            }
        }

        for cell in &all {
            let probe = probe(u128::MAX);
            let committed = fx
                .cell_store()
                .get(&fx.value_id(), cell, probe)
                .await?
                .into_inner();
            let expected = model.get(cell).cloned();
            if committed != expected {
                bail!(
                    "after event {index}, committed {cell:?} = {committed:?}, expected \
                     {expected:?}"
                );
            }
        }
    }
    Ok(())
}

/// The multi-section RC equivalence property: the committed projection
/// converges to the model across commit / abort / retry over several sections,
/// with distinct per-cell bases so a reversed base pairing diverges.
#[test]
fn prop_multi_section_rc_equivalence() {
    fn prop(trace: MultiTrace) -> TestResult {
        match executor::block_on(run_multi_section(trace)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MultiTrace) -> TestResult);
}
