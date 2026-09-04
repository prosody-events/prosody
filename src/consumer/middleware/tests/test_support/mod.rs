//! Shared scaffolding for middleware tests: the mock event context, the
//! scripted handler double and error, message/trigger fixtures, the defer
//! outcome trio, and the recording-session harness.

use std::convert::Infallible;
use std::future::{self, Future};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use educe::Educe;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot, watch};
use tracing::Span;
use uuid::Uuid;

use crate::Key;
use crate::consumer::event_context::{EventContext, StateAccessError, TerminationSignals};
use crate::consumer::handler::EventHandler;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue, UncommittedMessage};
use crate::consumer::middleware::{
    DemandType, FallibleHandler, RepinProof, Settlement, SettlementHandler,
};
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::receipted_sealed;
use crate::consumer::{Keyed, Receipted, Redelivery, Uncommitted};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::{MemoryLoader, MessageLoader};
use crate::state::cell::Committed;
use crate::state::descriptor::{Registered, StateDescriptor, ValueDescriptor, value_state};
use crate::state::dirty::DirtyStore;
use crate::state::manager::ArmedKeys;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{
    EventSession, KeyedStateSession, LifecycleAccess, SessionParts, TerminationWatch,
};
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{FailingCellStore, value_cell};
use crate::state::tests::support::UnavailableState;
use crate::state::{
    CollectionId, CommitDecision, EventRef, PartitionBackend, StateKey, StateName, StateType,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};

/// Session whose sweep fails for the `cart` collection.
pub type FailingSweepSession = KeyedStateSession<
    PartitionBackend<
        RecordingOracle,
        MemoryDescriptorIdentityStore,
        FailingCellStore<MemoryCellStore<RecordingOracle>>,
    >,
    MemoryLoader<Value>,
>;

/// Event context for a session whose sweep fails.
pub type FailingSweepContext = MockEventContext<Value, FailingSweepSession>;

/// A context backed by a real keyed-state test session.
pub type Ctx = FailingSweepContext;

/// A commit guard that reports when durability reaches the commit.
///
/// The guard then waits for test release. This pause makes the order between
/// commit and a later apply hook observable. The report is a one-shot because
/// a guard commits at most once.
pub struct GatedGuard {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    committed: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
}

impl GatedGuard {
    /// Returns a guard, both gates, and its terminal counters.
    pub fn new() -> (
        Self,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let committed: Arc<AtomicUsize> = Arc::default();
        let aborted: Arc<AtomicUsize> = Arc::default();
        (
            Self {
                entered: entered_tx,
                release: release_rx,
                committed: Arc::clone(&committed),
                aborted: Arc::clone(&aborted),
            },
            entered_rx,
            release_tx,
            committed,
            aborted,
        )
    }
}

impl Uncommitted for GatedGuard {
    async fn commit(self) {
        let _send_result = self.entered.send(());
        drop(self.release.await);
        self.committed.fetch_add(1, Ordering::SeqCst);
    }

    async fn abort(self) {
        self.aborted.fetch_add(1, Ordering::SeqCst);
        drop(self.entered);
    }
}

impl receipted_sealed::Sealed for GatedGuard {}

impl Receipted for GatedGuard {
    fn redelivery(&self) -> impl Future<Output = Redelivery> + Send {
        future::ready(Redelivery::Sweeps)
    }

    fn receipt(&mut self) -> impl Future<Output = ()> + Send {
        future::ready(())
    }
}

/// Returns the `cart` value descriptor.
pub fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Buffers one `cart` write through a real session.
///
/// The settle boundary owns the only stage. `configure` changes the context
/// before the write.
pub async fn buffered(
    configure: impl FnOnce(Ctx) -> Ctx,
) -> color_eyre::Result<(Ctx, MemoryCellStore<RecordingOracle>, CollectionId)> {
    buffered_with(Arc::default(), None, configure).await
}

/// Buffers one `cart` write with a finite optional sweep failure.
pub async fn buffered_with(
    armed: ArmedKeys,
    sweep_failure: Option<(ErrorCategory, usize)>,
    configure: impl FnOnce(Ctx) -> Ctx,
) -> color_eyre::Result<(Ctx, MemoryCellStore<RecordingOracle>, CollectionId)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
    let oracle = RecordingOracle::new();
    let cell_store = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let cart_name = StateName::try_new("cart")?;
    let store = sweep_failure.map_or_else(
        || FailingCellStore::with_handle(cell_store.clone(), Arc::default()),
        |(category, budget)| {
            FailingCellStore::failing_promote(cell_store.clone(), cart_name, category, budget)
        },
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session = KeyedStateSession::new(SessionParts {
        cell: store,
        dirty: Arc::new(DirtyStore::new()),
        oracle,
        loader: MemoryLoader::new(),
        registry,
        state_key: state_key.clone(),
        event: EventRef::Message {
            dedup_id: Uuid::from_u128(0x7),
        },
        recovery_delay: CompactDuration::new(30),
        armed,
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    });
    let context = configure(MockEventContext::new().with_session(session));
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|error| color_eyre::eyre::eyre!("bind cart: {error}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;
    let cart_id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("cart")?,
    );
    Ok((context, cell_store, cart_id))
}

/// Reports whether `id` still has a provisional cell.
pub async fn is_provisional(
    cell_store: &MemoryCellStore<RecordingOracle>,
    id: &CollectionId,
) -> color_eyre::Result<bool> {
    let stream = cell_store.provisional_cells(id);
    futures::pin_mut!(stream);
    Ok(stream.next().await.transpose()?.is_some())
}

/// Returns the resolved value from a settled `cart` cell.
pub async fn committed_value(
    cell_store: &MemoryCellStore<RecordingOracle>,
    id: &CollectionId,
) -> color_eyre::Result<Option<Bytes>> {
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    cell_store
        .get(id, &value_cell(), probe)
        .await
        .map(Committed::into_inner)
        .map_err(|error| color_eyre::eyre::eyre!("read committed: {error}"))
}

/// Timer-operation error the mock injects on demand, carrying the category to
/// classify as. The backstop arm is must-succeed (invariant 8), so it retries
/// **every** category forever — a `with_timer_failures(k, category)` context
/// exercises the retry-forever self-heal for each, including `Terminal` (which
/// `retry_step` retries rather than abandons) and `Permanent` (which the arm's
/// own loop retries past `retry_step`'s `Skip`).
mod context;
pub use context::*;
mod handlers;
pub use handlers::*;
mod recording_state;
pub use recording_state::*;
