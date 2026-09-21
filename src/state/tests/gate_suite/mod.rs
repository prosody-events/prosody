//! Deterministic KV4 pins for the per-event session operation gate.
//!
//! The sequential transparency property cannot exercise `join!`-shaped
//! schedules, so these pins force them: a real [`KeyedStateSession`] over
//! `Cached<HoldingCellStore<CountingCellStore<Memory>>>`, with the holding
//! seam withholding one lower response so the racing op is **forced** into
//! the bad interleaving (a post-race assert alone proves nothing). Each pin
//! asserts the outcome equals *some* serial order of the two ops; each goes
//! red by removing the relevant op's admission — making
//! `OwnerEngine::begin_read` or `OwnerEngine::begin_write` hand back a witness
//! over an already-released permit. A
//! cancel-safety pin covers the futurelock posture's safe half (dropping a
//! holding or queued session-op future releases the gate), and the closure
//! pin proves settlement fences mutators while post-settle reads still
//! answer.
//!
//! Per-test `current_thread` runtimes keep the schedules deterministic: a
//! spawned op only progresses while the test body awaits, so "parked on the
//! gate" and "parked in the hold" are stable states the test observes via the
//! hold's `entered` signal, never via timing.

use super::super::cached::Cached;
use super::super::descriptor::{
    CellStateError, MapStateError, StateDescriptor, deque, deque_state, map, map_state, value_state,
};
use super::super::dirty::DirtyStore;
use super::super::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use super::super::order_codec::{I64KeyCodec, OrderedKeyCodec};
use super::super::registry::{CollectionDef, CollectionDefRegistry};
use super::super::session::sealed::StateLifecycle;
use super::super::session::{KeyedStateSession, SessionParts, TerminationWatch};
use super::super::store::{CELL_BATCH, CellStore};
use super::super::{
    CollectionId, CollectionRef, Direction, PartitionBackend, StateAccessError, StateKey,
    StateName, StateType, StoreOutcome,
};
use super::cell_suite::{MemoryDeduplicationStore, value_cell};
use super::collection_suite::finalize_and_promote;
use super::support::{CountingCellStore, HoldingCellStore, Holds, probe};
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::middleware::RepinProof;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::cell::Values;
use crate::state::store::CellRead;
use crate::state::{DequeQuery, KeyQuery};

use super::super::fjall::test_db;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::timeout;
use uuid::Uuid;

/// The hang-guard for acquisitions that must proceed — never the assertion.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// The stream error tests put a valid item before a corrupt item in one chunk.
/// A failing chunk yields only its error. Both items must fit in one chunk.
const _: () = assert!(
    CELL_BATCH.get() >= 2,
    "the stream error tests require two items in one chunk"
);

/// Yields until a just-spawned task has reached its park point (the gate
/// acquire); 8 yields covers the deepest spawn → acquire chain, and the
/// pins stay correct regardless (the gate serializes either way).
async fn let_task_park() {
    for _ in 0..8_u8 {
        yield_now().await;
    }
}

/// The gate suite's lower store: holds beneath counters beneath memory.
type GateStore = HoldingCellStore<CountingCellStore<MemoryCellStore>>;

/// The per-partition backend the gate-suite sessions run over.
type GateBackend = PartitionBackend<
    MemoryDeduplicationStore,
    MemoryDescriptorIdentityStore,
    Cached<GateStore>,
    (),
>;

/// One test's fixture: the composed cache, its seams, and session minting.
struct GateFixture {
    cached: Cached<GateStore>,
    counting: CountingCellStore<MemoryCellStore>,
    holds: Arc<Holds>,
    cells: MemoryCells,
    dedup: MemoryDeduplicationStore,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
}

impl GateFixture {
    /// Builds the fixture over the shared fjall database keyspace `name`,
    /// registering the suite's value/map/deque collections.
    fn new(name: &str) -> Result<Self> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>("v"), CollectionDef::new(None))?;
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("m"),
            CollectionDef::new(None),
        )?;
        registry.register(&deque_state::<JsonCodec>("d"), CollectionDef::new(None))?;
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("ks"),
            CollectionDef {
                keyset_limit: 3,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        let holding = HoldingCellStore::new(counting.clone());
        let holds = holding.holds();
        let cached = Cached::new(test_db::cache(name)?, holding);
        Ok(Self {
            cached,
            counting,
            holds,
            cells,
            dedup,
            registry,
            state_key: StateKey::new(Uuid::new_v4(), Arc::from("key")),
        })
    }

    /// Mints a session for dedup id `n`. Dropped senders are fine —
    /// `watch::Receiver::borrow` keeps returning the last value.
    fn session(&self, n: u128) -> KeyedStateSession<GateBackend, MemoryLoader<Value>> {
        self.session_with_dirty(n, Arc::default())
    }

    /// [`Self::session`] over a caller-owned dirty workspace, so a pin can read
    /// exactly what an invocation staged.
    fn session_with_dirty(
        &self,
        n: u128,
        dirty: Arc<DirtyStore>,
    ) -> KeyedStateSession<GateBackend, MemoryLoader<Value>> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        KeyedStateSession::new(SessionParts::<GateBackend, _> {
            cell: self.cached.clone(),
            dirty,
            dedup: self.dedup.clone(),
            loader: MemoryLoader::new(),
            registry: self.registry.clone(),
            state_key: self.state_key.clone(),
            event: probe(n),
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        })
    }

    /// The [`CollectionId`] of the registered collection `name`.
    fn id(&self, name: &str) -> Result<CollectionId> {
        Ok(CollectionId::new(
            self.state_key.clone(),
            StateType::Application,
            StateName::try_new(name)?,
        ))
    }
}

/// A fresh single-thread runtime per pin, so spawned ops progress only while
/// the test awaits — the deterministic-schedule requirement.
fn runtime() -> Result<Runtime> {
    Ok(Builder::new_current_thread().enable_all().build()?)
}

mod keyset;
mod transactions;
use keyset::*;
mod reads;
mod release;
mod reset;
mod writes;
use reset::*;
mod streams;
use streams::*;
mod conforming;
