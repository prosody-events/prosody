use super::*;
use crate::codec::JsonCodec;
use crate::consumer::middleware::tests::test_support::RecordingOracle;
use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
use crate::consumer::middleware::tests::test_support::{Ctx, buffered, is_provisional};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::collection::sealed::{ReadEngine, Session};
use crate::state::descriptor::value_state;
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{FailingCellStore, value_cell};
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, EventRef, PartitionBackend, StateKey, StateName,
    StateType,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::Result;
use tokio::sync::watch;
use uuid::Uuid;

/// Which apply hook fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hook {
    Commit,
    Abort,
}

/// Per hook firing, the bytes each probed collection's Value cell
/// answered — read through the event's own session
/// (`context.test_lifecycle()`), exactly the view a user hook gets. Read
/// errors are captured as strings so an erroring read fails the exact
/// assertion instead of vanishing.
type HookReads = Vec<(Hook, Vec<Result<Option<Bytes>, String>>)>;

/// Probe handler recording a raw read of each named collection inside
/// every apply hook. `on_message`/`on_timer` are unused — the settle
/// boundary is driven directly.
#[derive(Clone)]
struct HookProbe {
    names: Vec<StateName>,
    reads: Arc<Mutex<HookReads>>,
}

impl HookProbe {
    fn new(names: Vec<StateName>) -> Self {
        Self {
            names,
            reads: Arc::default(),
        }
    }

    fn reads(&self) -> HookReads {
        self.reads.lock().clone()
    }

    async fn record<C>(&self, hook: Hook, context: &C)
    where
        C: EventContext,
    {
        type Engine<S> = <S as Session>::Engine;
        let mut values = Vec::with_capacity(self.names.len());
        match context.test_lifecycle() {
            Ok(session) => {
                // One raw point read per probed collection, through the
                // session's own engine — the same command a user hook's
                // typed handle runs, minus the decode.
                let mut inner = Engine::<C::State>::begin_read(&session).await;
                for name in &self.names {
                    values.push(
                        Engine::<C::State>::read_point(
                            &session,
                            &mut inner,
                            StateType::Application,
                            name,
                            &value_cell(),
                        )
                        .await
                        .map_err(|e| e.to_string()),
                    );
                }
            }
            Err(e) => values.push(Err(format!("lifecycle bind failed: {e}"))),
        }
        self.reads.lock().push((hook, values));
    }
}

impl FallibleHandler for HookProbe {
    type Error = TestError;
    type Output = u64;
    type Payload = serde_json::Value;

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
    }

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(0)
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(0)
    }

    async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.record(Hook::Commit, &context).await;
    }

    async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.record(Hook::Abort, &context).await;
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for HookProbe {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// Window 1 — `after_abort` after the arm-shutdown rollback reads the
/// **restored committed base**: the drain removed the dirty overlay and
/// the receipt's rollback settled the staged cell back to `prev` before
/// the hook fires, so the hook sees the truthful base, not the aborted
/// write's bytes.
#[tokio::test]
async fn arm_shutdown_after_abort_reads_the_restored_committed_base() -> Result<()> {
    let (context, cell_store, cart_id) = buffered(Ctx::with_shutdown_on_timer_read).await?;
    // Seed the committed base the rollback restores. Safe after the
    // buffered set: finalize captures `prev` later, inside settle.
    cell_store
        .write_resolved(
            &CollectionRef::new(cart_id.clone(), None),
            &[(value_cell(), Some(Bytes::from_static(b"base")))],
            &[],
        )
        .await?;
    let handler = HookProbe::new(vec![StateName::try_new("cart")?]);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(aborted.load(Ordering::SeqCst), 1, "arm-shutdown aborts");
    assert_eq!(committed.load(Ordering::SeqCst), 0);
    assert!(
        !is_provisional(&cell_store, &cart_id).await?,
        "the receipt's rollback settled the staged cell before the hook",
    );
    assert_eq!(
        handler.reads(),
        vec![(Hook::Abort, vec![Ok(Some(Bytes::from_static(b"base")))])],
        "after_abort reads the restored committed base, not the aborted write",
    );
    Ok(())
}

/// Oracle whose every `record_message` flips shutdown on the stored
/// context and fails Transient, so the record loop's next top sees
/// shutdown — the ambiguous marker-record window (the marker MAY be
/// durable, so nothing may roll back). `resolve` answers `NotCommitted`
/// but is never consulted here: the hook's own-event read short-circuits
/// to `prev` without an oracle read.
#[derive(Clone)]
struct FlushTripOracle {
    trip: MockEventContext,
    attempts: Arc<AtomicUsize>,
}

impl FlushTripOracle {
    fn new(trip: MockEventContext) -> Self {
        Self {
            trip,
            attempts: Arc::default(),
        }
    }
}

impl CommitOracle for FlushTripOracle {
    type Error = TestError;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.trip.request_shutdown();
        Err(TestError(ErrorCategory::Transient, "record"))
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::NotCommitted)
    }
}

/// Window 2 — `after_abort` in the ambiguous marker-record shutdown window
/// reads `prev`: a record attempt was made, so the staged cells are
/// deliberately left provisional (`certify` consumed the receipt — no
/// rollback compiles), and the hook's own-event read short-circuits to
/// the committed base without settling anything.
#[tokio::test(start_paused = true)]
async fn ambiguous_record_shutdown_after_abort_reads_prev() -> Result<()> {
    type TripBackend = PartitionBackend<
        FlushTripOracle,
        MemoryDescriptorIdentityStore,
        MemoryCellStore<FlushTripOracle>,
    >;

    let cart = StateName::try_new("cart")?;
    let mut registry = CollectionDefRegistry::default();
    registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    // The stored clone shares the Arc'd shutdown watch with the typed
    // context below, so the oracle's flip is visible to settle's polls.
    let base: MockEventContext = MockEventContext::new();
    let oracle = FlushTripOracle::new(base.clone());
    let cells = MemoryCells::new();
    let cell_store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let state_key = StateKey::new(Uuid::from_u128(0xF1), Arc::from("user-1"));
    let cart_id = CollectionId::new(state_key.clone(), StateType::Application, cart.clone());

    cell_store
        .write_resolved(
            &CollectionRef::new(cart_id.clone(), None),
            &[(value_cell(), Some(Bytes::from_static(b"prev")))],
            &[],
        )
        .await?;

    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session: KeyedStateSession<TripBackend, MemoryLoader<serde_json::Value>> =
        KeyedStateSession::new(SessionParts {
            cell: cell_store,
            dirty: Arc::new(DirtyStore::new()),
            oracle: oracle.clone(),
            loader: MemoryLoader::new(),
            registry,
            state_key,
            event: EventRef::Message {
                dedup_id: Uuid::from_u128(0xF1),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
            publisher: None,
        });
    session
        .seed(
            StateType::Application,
            &cart,
            &value_cell(),
            Some(b"staged"),
        )
        .await;
    let context = base.with_session(session);

    let handler = HookProbe::new(vec![cart]);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(
        aborted.load(Ordering::SeqCst),
        1,
        "the ambiguous record abandons"
    );
    assert_eq!(committed.load(Ordering::SeqCst), 0);
    assert_eq!(
        oracle.attempts.load(Ordering::SeqCst),
        1,
        "exactly one record attempt preceded the shutdown — the ambiguity trigger",
    );
    assert_eq!(
        handler.reads(),
        vec![(Hook::Abort, vec![Ok(Some(Bytes::from_static(b"prev")))])],
        "after_abort reads prev through the own-event short-circuit",
    );
    assert!(
        !cells.provisional_coordinates(&cart_id).is_empty(),
        "the staged cell stays provisional for the armed sweep — the hook read settled nothing",
    );
    Ok(())
}

/// Window 3 — the `Incomplete`-promote `after_commit` reads the **mixed
/// per-cell committed projection**: the promoted collection answers its
/// new value, the un-promoted one answers `prev` through the own-event
/// short-circuit — never uncommitted bytes, never a durable write from
/// the hook read itself.
#[tokio::test]
async fn incomplete_promote_after_commit_reads_the_mixed_per_cell_view() -> Result<()> {
    type SplitStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
    type SplitBackend =
        PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SplitStore>;

    let cart = StateName::try_new("cart")?;
    let wishlist = StateName::try_new("wishlist")?;
    let mut registry = CollectionDefRegistry::default();
    for name in ["cart", "wishlist"] {
        registry.register(&value_state::<JsonCodec>(name), CollectionDef::new(None))?;
    }
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    let recorded = oracle.recorded();
    let cells = MemoryCells::new();
    // Poison cart's PROMOTE path only (`commit_provisional` fails
    // Permanent); the stage and the seeding writes stay healthy.
    let store = FailingCellStore::new(
        MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone()),
        cart.clone(),
    );
    let state_key = StateKey::new(Uuid::from_u128(0xF2), Arc::from("user-1"));
    let cart_id = CollectionId::new(state_key.clone(), StateType::Application, cart.clone());
    let wishlist_id =
        CollectionId::new(state_key.clone(), StateType::Application, wishlist.clone());

    for (id, base) in [(&cart_id, b"A0"), (&wishlist_id, b"B0")] {
        store
            .write_resolved(
                &CollectionRef::new(id.clone(), None),
                &[(value_cell(), Some(Bytes::from_static(base)))],
                &[],
            )
            .await?;
    }

    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session: KeyedStateSession<SplitBackend, MemoryLoader<serde_json::Value>> =
        KeyedStateSession::new(SessionParts {
            cell: store,
            dirty: Arc::new(DirtyStore::new()),
            oracle,
            loader: MemoryLoader::new(),
            registry,
            state_key,
            event: EventRef::Message {
                dedup_id: Uuid::from_u128(0xF2),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
            publisher: None,
        });
    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"A1"))
        .await;
    session
        .seed(
            StateType::Application,
            &wishlist,
            &value_cell(),
            Some(b"B1"),
        )
        .await;
    let context = MockEventContext::new().with_session(session);

    let handler = HookProbe::new(vec![cart, wishlist]);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(committed.load(Ordering::SeqCst), 1, "the event committed");
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    assert_eq!(
        recorded.lock().as_slice(),
        [Uuid::from_u128(0xF2)],
        "the session's own message marker recorded before the commit",
    );
    assert_eq!(
        handler.reads(),
        vec![(
            Hook::Commit,
            vec![
                // Un-promoted: own-event provisional short-circuits to prev.
                Ok(Some(Bytes::from_static(b"A0"))),
                // Promoted: the new committed value.
                Ok(Some(Bytes::from_static(b"B1"))),
            ],
        )],
        "after_commit reads the mixed per-cell committed projection",
    );
    // Raw residue probes: the hook read issued no durable write — cart is
    // still provisional for the armed sweep, wishlist promoted clean.
    assert!(
        !cells.provisional_coordinates(&cart_id).is_empty(),
        "cart stays provisional after the Incomplete promote",
    );
    assert!(
        cells.provisional_coordinates(&wishlist_id).is_empty(),
        "wishlist promoted clean",
    );
    Ok(())
}
