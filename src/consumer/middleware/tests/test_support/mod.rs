//! Shared scaffolding for middleware tests: the mock event context, the
//! scripted handler double and error, message/trigger fixtures, the defer
//! outcome trio, and the recording-session harness.

use crate::state::cell::Values;
use crate::state::store::CellRead;
use crate::state::tests::support::StageInspection;
use std::convert::Infallible;
use std::future::{self, Future};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use educe::Educe;
use futures::StreamExt;
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot, watch};
use tracing::Span;
use uuid::Uuid;

use crate::Key;
use crate::consumer::event_context::{EventContext, StateAccessError, TerminationSignals};
use crate::consumer::handler::EventHandler;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue, UncommittedMessage};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::{
    DemandType, FallibleHandler, RepinProof, Settlement, SettlementHandler,
};
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::{Keyed, Uncommitted};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::{MemoryLoader, MessageLoader};
use crate::state::cell::Committed;
use crate::state::descriptor::tests::{TestSession, test_session_parts};
use crate::state::descriptor::{Registered, StateDescriptor, ValueDescriptor, value_state};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{
    EventSession, KeyedStateSession, LifecycleAccess, SessionParts, TerminationWatch,
};
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::UnavailableState;
use crate::state::{CollectionId, EventRef, PartitionBackend, StateKey, StateName, StateType};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};

/// A context backed by a real keyed-state test session.
pub type Ctx = MockEventContext<Value, TestSession>;

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

impl Arbitrary for DemandType {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 10 {
            0 => Self::Normal,
            9 => Self::Failure { retry: u32::MAX },
            retry => Self::Failure {
                retry: u32::from(retry),
            },
        }
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
) -> color_eyre::Result<(Ctx, MemoryCellStore, CollectionId)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let state_key = StateKey::new(Uuid::from_u128(0x7), Arc::from("user-1"));
    let (session, cell_store) =
        test_session_parts(MemoryLoader::new(), registry, state_key.clone());
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
    cell_store: &MemoryCellStore,
    id: &CollectionId,
) -> color_eyre::Result<bool> {
    let stream = cell_store.staged_cells(id);
    futures::pin_mut!(stream);
    Ok(stream.next().await.transpose()?.is_some())
}

/// Returns the resolved value from a settled `cart` cell.
pub async fn committed_value(
    cell_store: &MemoryCellStore,
    id: &CollectionId,
) -> color_eyre::Result<Option<Bytes>> {
    CellRead::<Values>::read(cell_store, id, &value_cell())
        .await
        .map(|(committed, _)| committed)
        .map(Committed::into_inner)
        .map_err(|error| color_eyre::eyre::eyre!("read committed: {error}"))
}

/// Mock context with timer, shutdown, and state controls.
mod context;
/// Fault slots, error categories, and settlement records.
pub(crate) mod faults;
pub use context::*;
mod handlers;
pub use handlers::*;
mod recording_state;
pub use recording_state::*;
