//! Session operations must match the value model.
//!
//! The property uses one shared dirty store across successive event scopes.
//! It checks reads, collection drains, reset, finalize, and admission after
//! failed promotion. Mid-handler commits form an irreversible committed base.
//! A successful finalize drains the overlay and returns one frozen stage.
//!
//! Focused tests cover collection isolation, terminated handles, stage retry,
//! and the durable marker. Middleware tests exercise the settlement boundary.

use super::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::codec::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ErrorCategory;
use crate::state::StateAccessError;
use crate::state::StateBackend;
use crate::state::cell::Values;
use crate::state::cell_key::{CellKey, Section};
use crate::state::descriptor::value_state;
use crate::state::dirty::DirtyStore;
use crate::state::manager::EventStateScope;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Promoted;
use crate::state::store::CellRead;
use crate::state::tests::cell_suite::{
    FailingCellStore, MemoryDeduplicationStore, Poison, PoisonHandle, value_cell,
};
use crate::state::tests::support::{assert_no_settlement_residue, probe};
use crate::state::{
    CollectionId, EventRef, PartitionBackend, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

mod counts;
mod lifecycle;
mod multi;
mod retry;
mod stage;

const VALUE_NAME: &str = "cart";

/// The poison-armable cell store the fixture mints: a [`FailingCellStore`]
/// over the shared in-memory cells, disarmed (`None`) by default so it
/// delegates cleanly — the property arms it to schedule stage and promote
/// failures at runtime.
type TestStore = FailingCellStore<MemoryCellStore>;
type TestBackend =
    PartitionBackend<MemoryDeduplicationStore, MemoryDescriptorIdentityStore, TestStore, ()>;
/// The per-event session type the fixture mints (loader slot unused, so `()`).
type Session = KeyedStateSession<TestBackend, ()>;

/// Fixture sharing the partition-lifetime cell store across the per-event
/// sessions it mints, so a second event reads the first's committed values.
struct Fixture {
    cells: MemoryCells,
    dedup: MemoryDeduplicationStore,
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
            dedup: MemoryDeduplicationStore::default(),
            registry: Arc::new(registry),
            state_key: StateKey::new(Uuid::from_u128(0x00C0_FFEE), Arc::from("key")),
            value_name: StateName::try_new(value_name)?,
            dirty: Arc::new(DirtyStore::new()),
            poison: Arc::default(),
            shutdown_rx,
            cancel_rx,
            _shutdown_tx: shutdown_tx,
            _cancel_tx: cancel_tx,
        })
    }

    /// The partition-lifetime cell store (a clone sharing the durable cells
    /// and the runtime poison slot).
    fn cell_store(&self) -> TestStore {
        FailingCellStore::with_handle(
            MemoryCellStore::new(self.cells.clone()),
            self.poison.clone(),
        )
    }

    /// Arms (`Some`) or disarms (`None`) the poison slot shared by every
    /// store clone the fixture minted.
    fn set_poison(&self, poison: Option<Poison>) {
        *self.poison.lock() = poison;
    }

    /// Mints the per-event scope for `event` over clones of the shared store,
    /// dedup store, and the one partition-shared dirty workspace.
    fn session(&self, event: EventRef) -> EventStateScope<Session> {
        EventStateScope::new(KeyedStateSession::new(SessionParts {
            cell: self.cell_store(),
            dirty: self.dirty.clone(),
            dedup: self.dedup.clone(),
            loader: (),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event,
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(self.shutdown_rx.clone(), self.cancel_rx.clone()),
        }))
    }

    /// Mints a session for `event` over a caller-owned cancel watch (sharing
    /// the fixture's store, dedup store, dirty workspace, and key). Production
    /// gives each event its own per-event cancel signal; the fixture's
    /// shared channel cannot terminate one event alone, which the
    /// stale-clone containment test needs.
    fn session_with_cancel(
        &self,
        event: EventRef,
        cancel_rx: watch::Receiver<bool>,
    ) -> EventStateScope<Session> {
        EventStateScope::new(KeyedStateSession::new(SessionParts {
            cell: self.cell_store(),
            dirty: self.dirty.clone(),
            dedup: self.dedup.clone(),
            loader: (),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event,
            dedup_ttl: CompactDuration::new(30),
            checks: (),
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
            .get::<Values>(
                StateType::Application,
                &self.value_name,
                value_cell().as_ref(),
            )
            .await?)
    }

    fn value_id(&self) -> CollectionId {
        CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            self.value_name.clone(),
        )
    }

    /// Returns the durable committed Value bytes.
    async fn committed_value(&self) -> Result<Option<Bytes>> {
        Ok(
            CellRead::<Values>::read(&self.cell_store(), &self.value_id(), value_cell().as_ref())
                .await?
                .0
                .into_inner(),
        )
    }
}

/// `probe(n)` plus its dedup id, for asserting against the marker store.
fn message(n: u128) -> (EventRef, Uuid) {
    (probe(n), Uuid::from_u128(n))
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
    let cart_id = CollectionId::new(fx.state_key.clone(), StateType::Application, cart);
    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &cart_id, value_cell().as_ref())
            .await?
            .0
            .into_inner(),
        Some(Bytes::from_static(b"a")),
    );
    let wishlist_id = CollectionId::new(
        fx.state_key.clone(),
        StateType::Application,
        wishlist.clone(),
    );
    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &wishlist_id, value_cell().as_ref())
            .await?
            .0
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
            .get::<Values>(StateType::Application, &cart, value_cell().as_ref())
            .await?,
        Some(Bytes::from_static(b"V")),
    );

    // Zero durable writes by the rollback: the committed row is still V, and
    // no provisional cell or event marker was created.
    let cart_id = CollectionId::new(fx.state_key.clone(), StateType::Application, cart);
    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &cart_id, value_cell().as_ref())
            .await?
            .0
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
            .get::<Values>(
                StateType::Application,
                &fx.value_name,
                value_cell().as_ref()
            )
            .await?,
        Some(Bytes::from_static(b"W")),
    );
    Ok(())
}

/// A permanent promote failure restores the committed base.
async fn promote_receipt(
    fx: &Fixture,
    staged: super::sealed::Staged<TestStore, ()>,
    fail_promote: bool,
) -> Result<()> {
    if fail_promote {
        fx.set_poison(Some(Poison::Collection(
            fx.value_name.clone(),
            ErrorCategory::Permanent,
        )));
    }
    match staged.promote(|| false).await {
        Promoted::Complete => assert!(!fail_promote),
        Promoted::Rejected(rejected) => {
            assert!(fail_promote);
            assert!(rejected.abort(|| false).await);
        }
        _ => bail!("unexpected promote result"),
    }
    fx.set_poison(None);
    assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
    Ok(())
}

/// Seeding and inspection seams for the suites, so a test can arrange overlay
/// state without a collection handle.
///
/// The seams **arrange** state; they never model a mutation. Each seeding seam
/// takes the operation gate — so a seed serializes against a concurrent scoped
/// operation rather than tearing one — but skips the mutator admission order
/// (stale pin, closed session, termination), which is what the real handles
/// exercise in the gate suite. A seed therefore succeeds on a session a handle
/// would refuse, which is the point: a test can seed the state a refusal is
/// asserted against. Consecutive seeds are not one atomic unit; each releases
/// the gate.
#[cfg(test)]
impl<B, L> KeyedStateSession<B, L>
where
    B: StateBackend,
{
    /// Stages one cell (or an absence) into this event's dirty overlay. Must
    /// not be called while a permit is already held: the gate is not reentrant.
    pub(crate) async fn seed(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: Option<&[u8]>,
    ) {
        let permit = self.permit().await;
        self.stage_cell(
            &permit,
            state_type,
            name,
            cell,
            value.map(Bytes::copy_from_slice),
        );
    }

    /// [`Self::seed`]'s section-clear twin.
    pub(crate) async fn seed_section_clear(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) {
        let permit = self.permit().await;
        self.stage_section_clear(&permit, state_type, name, section);
    }

    /// The visible committed bytes of one cell, gate-free — the read seam for
    /// suites outside `crate::state`.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    pub(crate) async fn peek(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        self.get::<Values>(state_type, name, cell.as_ref()).await
    }
}
