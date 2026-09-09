use super::*;
use crate::codec::JsonCodec;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::tests::test_support::RecordingDedup;
use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::collection::sealed::{ReadEngine, Session};
use crate::state::descriptor::value_state;
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{FailingCellStore, value_cell};
use crate::state::{
    CollectionId, CollectionRef, EventRef, PartitionBackend, StateKey, StateName, StateType,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::Result;
use std::future::ready;
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

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(0))
    }

    fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(0))
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(0))
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

/// Every dedup write requests shutdown and returns a transient error.
#[derive(Clone)]
struct FlushTripDedup {
    trip: MockEventContext,
    attempts: Arc<AtomicUsize>,
}

impl FlushTripDedup {
    fn new(trip: MockEventContext) -> Self {
        Self {
            trip,
            attempts: Arc::default(),
        }
    }
}

impl DeduplicationStore for FlushTripDedup {
    type Error = TestError;

    fn insert(&self, _dedup_id: Uuid) -> impl Future<Output = Result<(), Self::Error>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.trip.request_shutdown();
        ready(Err(TestError(ErrorCategory::Transient, "record")))
    }

    fn exists(&self, _id: Uuid) -> impl Future<Output = Result<bool, Self::Error>> {
        ready(Ok(false))
    }
}

/// A dedup failure after promote aborts the source.
/// The abort hook reads the promoted value.
#[tokio::test(start_paused = true)]
async fn dedup_shutdown_hook_reads_the_committed_value() -> Result<()> {
    type TripBackend =
        PartitionBackend<FlushTripDedup, MemoryDescriptorIdentityStore, MemoryCellStore, ()>;

    let cart = StateName::try_new("cart")?;
    let mut registry = CollectionDefRegistry::default();
    registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    // The stored clone shares the Arc'd shutdown watch with the typed
    // context below, so the dedup failure reaches settle's shutdown check.
    let base: MockEventContext = MockEventContext::new();
    let dedup = FlushTripDedup::new(base.clone());
    let cells = MemoryCells::new();
    let cell_store = MemoryCellStore::new(cells.clone());
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
            dedup: dedup.clone(),
            loader: MemoryLoader::new(),
            registry,
            state_key,
            event: EventRef::Message {
                dedup_id: Uuid::from_u128(0xF1),
            },
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
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
        dedup.attempts.load(Ordering::SeqCst),
        1,
        "exactly one record attempt preceded the shutdown — the ambiguity trigger",
    );
    assert_eq!(
        handler.reads(),
        vec![(Hook::Abort, vec![Ok(Some(Bytes::from_static(b"staged")))])],
        "after_abort reads the promoted value",
    );
    assert!(
        cells.provisional_coordinates(&cart_id).is_empty(),
        "the promote completed before the dedup write",
    );
    Ok(())
}

/// The commit hook reads certified values after an incomplete promote.
/// Its reads leave the unpromoted collection provisional.
#[tokio::test]
async fn incomplete_promote_hook_reads_all_committed_collections() -> Result<()> {
    type SplitStore = FailingCellStore<MemoryCellStore>;
    type SplitBackend =
        PartitionBackend<RecordingDedup, MemoryDescriptorIdentityStore, SplitStore, ()>;

    let cart = StateName::try_new("cart")?;
    let wishlist = StateName::try_new("wishlist")?;
    let mut registry = CollectionDefRegistry::default();
    for name in ["cart", "wishlist"] {
        registry.register(&value_state::<JsonCodec>(name), CollectionDef::new(None))?;
    }
    let registry = Arc::new(registry);
    let dedup = RecordingDedup::new();
    let recorded = dedup.recorded();
    let cells = MemoryCells::new();
    // Poison cart's PROMOTE path only (`commit_provisional` fails
    // Permanent); the stage and the seeding writes stay healthy.
    let store = FailingCellStore::new(MemoryCellStore::new(cells.clone()), cart.clone());
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
            dedup,
            loader: MemoryLoader::new(),
            registry,
            state_key,
            event: EventRef::Message {
                dedup_id: Uuid::from_u128(0xF2),
            },
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
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
                // Unpromoted: the sibling certificate exposes the new value.
                Ok(Some(Bytes::from_static(b"A1"))),
                // Promoted: the new committed value.
                Ok(Some(Bytes::from_static(b"B1"))),
            ],
        )],
        "after_commit reads both committed values",
    );
    // Raw residue probes: the hook read issued no durable write — cart is
    // still provisional for admission; wishlist has promoted.
    assert!(
        !cells.provisional_coordinates(&cart_id).is_empty(),
        "cart stays provisional after the failed promote",
    );
    assert!(
        cells.provisional_coordinates(&wishlist_id).is_empty(),
        "wishlist promoted clean",
    );
    Ok(())
}
