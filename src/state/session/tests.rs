//! Session lifecycle property + the marker-ordering pin.
//!
//! [`prop_value_lifecycle_equivalence`] drives a random sequence of events —
//! each a short op list (set/clear/mid-handler commit/rollback) plus a
//! commit/abort/reset/fail outcome — through the **real** production session
//! lifecycle (the `finalize`-minted receipt's `promote`/`rollback` plus
//! `reset`) over **one
//! partition-shared [`DirtyStore`]**, minting each event's session as an
//! [`EventStateScope`] that drops at event-end (the production lifecycle). It
//! asserts, after every operation, that a plain `Option<Bytes>` model equals
//! the session's own overlay read, and after every event that the model equals
//! both the committed projection and a fresh **overlay read** (the dirty
//! short-circuit a `committed_value` probe bypasses), and that the shared
//! dirty buffer is empty for the key — so a failed event's buffered write can
//! neither linger nor be read as uncommitted. A `commit()` advances the
//! model's durable snapshot immediately, so the property also proves the
//! at-least-once `commit()` contract: `commit()`-landed writes survive
//! abort/reset/failure while post-commit ops still roll back; a `Rollback`
//! reverts reads to the commit floor (or the pre-event committed value) and the
//! buffer to empty. Two focused examples pin ordering a trace cannot observe:
//! the single marker flushes **exactly once, strictly after the stage** (the
//! [`ScriptedOracle`] `record_message` counter); another pins the `commit()`
//! drain to its own collection. Four deterministic pins nail the `rollback()`
//! contract, including that a terminated session's rollback is a `NoOp` so a
//! stale clone cannot drain a later same-key event's buffer.

use super::sealed::{ApplyOutcome, StateLifecycle};
use super::{CellSession, Finalized, KeyedStateSession, SessionParts, TerminationWatch};
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
use crate::state::tests::support::{assert_no_settlement_residue, probe};
use crate::state::{
    CollectionId, CollectionRef, EventRef, PartitionBackend, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
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
        Self::with_collections(&[VALUE_NAME])
    }

    /// A fixture whose registry holds one `ReadCommitted` value collection per
    /// name; `value_name` is `names[0]`, so single-collection callers reach it
    /// through the shared helpers unchanged.
    fn with_collections(names: &[&str]) -> Result<Self> {
        let value_name = names
            .first()
            .ok_or_else(|| eyre!("with_collections needs at least one collection"))?;
        let mut registry = CollectionDefRegistry::new(None);
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

    // The receipt is deliberately dropped: the pin is about marker ordering,
    // and a dropped receipt is safe (recovery never reads the in-memory
    // record).
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));
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
        .set(StateType::Application, &cart, &value_cell(), b"a")
        .await?;
    session
        .set(StateType::Application, &wishlist, &value_cell(), b"b")
        .await?;

    assert_eq!(
        session.commit(StateType::Application, &cart).await?,
        StoreOutcome::Applied,
    );

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
        .set(StateType::Application, &cart, &value_cell(), b"V")
        .await?;
    assert_eq!(
        session.commit(StateType::Application, &cart).await?,
        StoreOutcome::Applied,
    );

    // Buffer W over cart, and X over the sibling.
    session
        .set(StateType::Application, &cart, &value_cell(), b"W")
        .await?;
    session
        .set(StateType::Application, &wishlist, &value_cell(), b"X")
        .await?;

    // Rollback cart: the buffered W vanishes.
    assert_eq!(
        session.rollback(StateType::Application, &cart),
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
    assert!(fx.cells.standing_marker_of(&cart_id).is_none());

    // Sibling isolation: the rollback drained only cart; wishlist stands.
    let touched = fx.dirty.touched(&fx.state_key.key);
    assert_eq!(touched.len(), 1, "the rollback drained only its collection");
    assert_eq!(touched[0].0.1, wishlist);
    Ok(())
}

/// A `rollback()` on an empty buffer is a `NoOp` — at event start (nothing
/// buffered) and right after a `commit()` drained the buffer (the floor is not
/// discardable). The committed value is unaffected.
#[tokio::test]
async fn rollback_on_an_empty_buffer_is_noop() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, _dedup) = message(1);
    let session = fx.session(event).handle();

    // Event start: nothing buffered.
    assert_eq!(
        session.rollback(StateType::Application, &fx.value_name),
        StoreOutcome::NoOp,
    );

    // Commit drains the buffer, so the follow-up rollback finds nothing.
    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"V")
        .await?;
    assert_eq!(
        session
            .commit(StateType::Application, &fx.value_name)
            .await?,
        StoreOutcome::Applied,
    );
    assert_eq!(
        session.rollback(StateType::Application, &fx.value_name),
        StoreOutcome::NoOp,
    );

    // The committed floor stands.
    assert_eq!(fx.committed_value().await?, Some(Bytes::from_static(b"V")));
    Ok(())
}

/// With no prior `commit()`, `rollback()` reverts to the **pre-event**
/// committed value. Event 1 durably commits `V`; event 2 sets `W`, clears the
/// section (the marker leg — a surviving dirty clear marker would mask the read
/// to `None`), then rolls back: the read is the pre-event `V`, and `finalize`
/// finds nothing to stage.
#[tokio::test]
async fn rollback_without_a_commit_restores_the_pre_event_value() -> Result<()> {
    let fx = Fixture::new()?;
    let key = fx.state_key.key.clone();

    // Event 1: durably commit V as the pre-event committed value. The scope is
    // dropped explicitly after the settle so the shared dirty buffer is clear
    // before event 2.
    let scope1 = fx.session(message(1).0);
    let s1 = scope1.handle();
    let (_event1, dedup) = message(1);
    s1.set(StateType::Application, &fx.value_name, &value_cell(), b"V")
        .await?;
    let finalized = s1.finalize().await?;
    s1.register_marker(dedup);
    s1.flush_marker().await?;
    if let Finalized::Staged(staged) = finalized {
        assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
    }
    drop(scope1);
    // Raw residue probe before the resolving read below, which would heal a
    // skipped promote to the same bytes and mask it.
    assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
    assert_eq!(fx.committed_value().await?, Some(Bytes::from_static(b"V")));

    // Event 2: set W and clear the section, then roll back with no commit().
    let scope = fx.session(message(2).0);
    let session = scope.handle();
    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"W")
        .await?;
    session
        .clear_section(StateType::Application, &fx.value_name, Section::new(0))
        .await?;
    assert_eq!(
        session.rollback(StateType::Application, &fx.value_name),
        StoreOutcome::Applied,
    );

    // The read is the pre-event committed value — a surviving clear marker
    // would mask it to None.
    assert_eq!(
        session
            .get(StateType::Application, &fx.value_name, &value_cell())
            .await?,
        Some(Bytes::from_static(b"V")),
    );

    // Nothing left to stage, and the buffer is empty for the key.
    assert!(matches!(session.finalize().await?, Finalized::Clean));
    assert!(fx.dirty.touched(&key).is_empty());
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
    // what `EventContext::invalidate` does after every dispatch) and its scope
    // drops, clearing event 1's dirty range.
    cancel_tx.send(true)?;
    assert!(stale.is_terminated());
    drop(scope1);

    // Event 2 for the same key on a fresh, live cancel watch buffers W.
    let (_cancel_tx2, cancel_rx2) = watch::channel(false);
    let scope2 = fx.session_with_cancel(message(2).0, cancel_rx2);
    let session = scope2.handle();
    session
        .set(StateType::Application, &fx.value_name, &value_cell(), b"W")
        .await?;

    // The stale clone's rollback finds a terminated session: NoOp, no drain.
    assert_eq!(
        stale.rollback(StateType::Application, &fx.value_name),
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
    /// The mid-handler write-through: everything buffered so far becomes
    /// durable immediately and survives every non-commit outcome.
    Commit,
    /// The mid-handler discard: everything buffered since the last `Commit`
    /// (or event start) vanishes; reads revert to the commit floor.
    Rollback,
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
        // Sets weighted up so state actually accumulates between commits.
        match u8::arbitrary(g) % 6 {
            0..=2 => Self::Set(u8::arbitrary(g)),
            3 => Self::Clear,
            4 => Self::Commit,
            _ => Self::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set(b) => Box::new(b.shrink().map(Self::Set)),
            Self::Clear | Self::Commit | Self::Rollback => quickcheck::empty_shrinker(),
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
    /// The durable value this floor pins, or `pre_event` when no `commit()`
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
                .set(StateType::Application, name, &value_cell(), &[byte])
                .await?;
            model.scratch = Some(Bytes::copy_from_slice(&[byte]));
            model.buffered = true;
        }
        ValueOp::Clear => {
            session
                .clear(StateType::Application, name, &value_cell())
                .await?;
            model.scratch = None;
            model.buffered = true;
        }
        ValueOp::Commit => {
            let outcome = session.commit(StateType::Application, name).await?;
            if outcome != expected_outcome(model.buffered) {
                return Ok(false);
            }
            model.floor = Floor::Committed(model.scratch.clone());
            model.buffered = false;
        }
        ValueOp::Rollback => {
            let outcome = session.rollback(StateType::Application, name);
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

/// Drives the trace through the real session lifecycle, asserting the session's
/// own read equals a plain `Option<Bytes>` model after every operation, and the
/// committed projection equals the model after every event.
///
/// A mid-event `commit()` snapshots the scratch model as immediately durable:
/// on a commit the full scratch wins, on every other outcome the durable state
/// must equal the last `commit()`-landed snapshot (post-commit ops roll back;
/// `commit()`-landed ops survive) — the at-least-once `commit()` contract. A
/// `Rollback` reverts the scratch to the commit floor (or the pre-event
/// committed value) and must report `Applied` iff anything was buffered.
async fn run(trace: Trace) -> Result<bool> {
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
                    return Ok(false);
                }
            }

            match ev.outcome {
                Outcome::Commit => {
                    let finalized = session.finalize().await?;
                    session.register_marker(dedup_id);
                    session.flush_marker().await?;
                    if let Finalized::Staged(staged) = finalized {
                        // The memory store never fails, so a healthy promote
                        // must fully resolve — and leave no residue, checked
                        // raw before the loop-tail resolving reads heal a
                        // skipped settle to identical bytes and mask it. A
                        // stage overwrites any marker a prior Reset/Failed
                        // event abandoned, so the check is exact here (and
                        // only here: those outcomes leave residue by design).
                        if staged.certify().promote().await != ApplyOutcome::Resolved {
                            return Ok(false);
                        }
                        assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
                    }
                    // Commit advances the model (last-writer-wins).
                    model = ev_model.scratch;
                }
                Outcome::Abort => {
                    if let Finalized::Staged(staged) = session.finalize().await? {
                        staged.rollback().await;
                        // Same raw probe as the Commit arm: a rollback that
                        // skipped its store call would be healed to identical
                        // bytes by the loop-tail resolving reads and masked.
                        assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
                    }
                    // Post-commit ops roll back to their `prev`, which
                    // finalize captured *after* the `commit()` landed — the
                    // `commit()`-landed snapshot.
                    model = ev_model.floor.resolve(model);
                }
                Outcome::Reset => {
                    // Dropping the receipt leaves any provisional written by
                    // `finalize` standing, projecting its `prev` (the
                    // `commit()`-landed snapshot, or the unchanged committed
                    // base) — exactly the discarded-stage behavior `reset`
                    // pairs with.
                    let _finalized = session.finalize().await?;
                    session.reset();
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
/// clears-bearing marker), the staged entry makes `finalize` return
/// [`Finalized::Staged`] — the backstop-arming rule — and the receipt's
/// `promote` resolves that entry (the receipt exists to consume).
///
/// An example because the [`Finalized::Staged`] backstop-arming signal
/// and the [`ApplyOutcome::Resolved`] apply signal are protocol outcomes
/// [`prop_value_lifecycle_equivalence`] cannot observe: that property asserts
/// the committed value projection, which a clears-only event leaves empty
/// whether or not the backstop armed, so only the finalize/apply outcomes and
/// the standing event marker expose the staging.
#[tokio::test]
async fn clears_only_event_finalizes_staged() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, _dedup_id) = message(1);
    let session = fx.session(event).handle();

    session
        .clear_section(StateType::Application, &fx.value_name, Section::new(0))
        .await?;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("a clears-only collection must stage so the backstop is armed");
    };
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
        staged.certify().promote().await,
        ApplyOutcome::Resolved,
        "the staged entry exists to resolve"
    );
    Ok(())
}

/// The clears-only stage runs the stage-boundary foreign-marker resolve: a
/// standing **foreign** committed marker (seeded crash-style through a raw
/// store handle) is resolved — its cells settle per its verdict — rather than
/// blind-deleted by the clears-only event's own settle, and the session's own
/// clears-bearing marker is written by `finalize` then deleted by the
/// receipt's `promote` (which also applies the clear's gap erase).
///
/// The generated crash/reassignment alphabet (the crash-trace generator's
/// clears dimension) subsumes this shape; these two pins are kept as the fast,
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
        .clear_section(StateType::Application, &fx.value_name, Section::new(0))
        .await?;
    // The receipt is held across the raw probes below, then consumed.
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the clears-only event must stage");
    };

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
    assert_eq!(staged.certify().promote().await, ApplyOutcome::Resolved);
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
        .set(StateType::Application, &fx.value_name, &value_cell(), b"v1")
        .await?;
    // Attempt one's receipt is deliberately dropped — the discarded stage the
    // retry boundary pairs with `reset`.
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));

    // The retry boundary: discard dirty + marker, then re-dispatch the same
    // event.
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
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the retry re-stage must mint a receipt");
    };

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
        fx.cells.standing_marker_of(&fx.value_id()).is_none(),
        "the settle deleted the single (overwritten) event marker"
    );
    Ok(())
}

/// The own-event read-window guard: the staging event's own reads between stage
/// and settle must NOT resolve their own in-flight event marker
/// (`help_read_window`'s `marker.event() == own` disjunct). Resolving it would
/// settle the event mid-flight — the reachable shape is a retry attempt reading
/// its own predecessor attempt's still-standing clears-bearing marker. This is
/// an example because the value-projection lifecycle property is structurally
/// blind to it: a mid-flight self-resolution rolls the uncommitted stage back
/// and the retry re-stages the same values, so the committed projection still
/// converges — only the marker's standing state, observed here at the read
/// path, exposes the premature settle.
#[tokio::test]
async fn own_event_read_does_not_resolve_its_own_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let raw = fx.cell_store();
    let id = fx.value_id();
    let collection = CollectionRef::new(id.clone(), None);

    // Stage event E's clears-bearing marker directly (one survivor cell in the
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

    // E's own read hits the standing marker with `own == E`: the guard keeps
    // the marker standing (E must not settle itself mid-flight) and E's staged
    // cell stays provisional.
    raw.get(&id, &cell_at(0), e).await?;
    let standing = fx
        .cells
        .standing_marker_of(&id)
        .ok_or_else(|| eyre!("an own-event read must leave the in-flight marker standing"))?;
    assert_eq!(
        standing.event(),
        e,
        "the own read left E's marker untouched"
    );
    assert!(
        !fx.cells.provisional_coordinates(&id).is_empty(),
        "the own read settled nothing — E's staged cell is still provisional",
    );

    // Contrast — the resolve path is reachable, so the guard (not an inert
    // resolve) is what protects the own read: a *foreign* read of the same
    // uncommitted marker resolves it (verdict: not committed → rolled back, the
    // marker deleted).
    raw.get(&id, &cell_at(0), probe(999)).await?;
    assert!(
        fx.cells.standing_marker_of(&id).is_none(),
        "a foreign read resolves the uncommitted marker away",
    );
    Ok(())
}
