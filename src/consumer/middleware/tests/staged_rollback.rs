use super::*;
use crate::consumer::middleware::tests::test_support::cart;
use crate::loader::MemoryLoader;
use crate::state::descriptor::Registered;
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{EventRef, StateKey, StateName};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::json;
use std::future::ready;
use uuid::Uuid;

/// Records whether its `after_commit` typed-handle read answered or hit the
/// stale-pin fence — witnessing that the permanent-`Skip` arm re-stamps
/// the hook context.
#[derive(Clone)]
struct SkipReadProbe {
    read: Arc<Mutex<Option<Result<(), String>>>>,
}

impl FallibleHandler for SkipReadProbe {
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
        let outcome = match context.state(Registered::new(cart())) {
            Ok(handle) => handle.get().await.map(|_| ()).map_err(|e| e.to_string()),
            Err(e) => Err(format!("bind: {e}")),
        };
        *self.read.lock() = Some(outcome);
    }

    async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
    }

    async fn shutdown(self) {}
}

/// A permanent stage rejection re-pins the hook context before its state read.
/// Without the re-pin, a stale context fails with `Terminated`.
#[tokio::test]
async fn permanent_skip_hook_reads_through_the_stamp() -> Result<()> {
    use crate::consumer::middleware::tests::test_support::RecordingDedup;
    use crate::consumer::partition::ShutdownPhase;
    use crate::state::PartitionBackend;
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::session::sealed::StateLifecycle;
    use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::tests::cell_suite::FailingCellStore;
    use crate::timers::duration::CompactDuration;
    use tokio::sync::watch;

    type SkipStore = FailingCellStore<MemoryCellStore>;
    type SkipBackend =
        PartitionBackend<RecordingDedup, MemoryDescriptorIdentityStore, SkipStore, ()>;

    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let dedup = RecordingDedup::new();
    // Poison the STAGE path so `settle`'s own `finalize` hits Skip.
    let cell_store = FailingCellStore::failing_write_provisional(
        MemoryCellStore::new(MemoryCells::new()),
        StateName::try_new("cart")?,
        ErrorCategory::Permanent,
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session: KeyedStateSession<SkipBackend, MemoryLoader<serde_json::Value>> =
        KeyedStateSession::new(SessionParts {
            cell: cell_store,
            dirty: Arc::new(DirtyStore::new()),
            dedup,
            loader: MemoryLoader::new(),
            registry,
            state_key: StateKey::new(Uuid::from_u128(0x5C2), Arc::from("user-1")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            dedup_ttl: CompactDuration::new(30),
            checks: (),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        });

    // A nested retry's epoch bump: `reset` discards the (empty) dirty and
    // bumps the shared epoch, leaving THIS clone pinned stale. Buffer the
    // poisoned write through a live re-pinned clone (shared dirty overlay),
    // so `finalize` stages it and hits the poison — while the settle still
    // receives the stale clone.
    session.reset(RepinProof::for_test()).await;
    let live = session.repin(RepinProof::for_test());
    let live_ctx = MockEventContext::new().with_session(live);
    let live_handle = live_ctx
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind live: {e}"))?;
    live_handle.set(json!({ "x": 1_i32 })).await?;

    let context = MockEventContext::new().with_session(session);
    let read = Arc::new(Mutex::new(None));
    let handler = SkipReadProbe { read: read.clone() };
    let (guard, committed, _aborted) = RecordingGuard::new();

    settle(&LeafHandler::new(handler.clone()), context, guard, Ok(0)).await;

    assert_eq!(
        committed.load(Ordering::SeqCst),
        1,
        "the permanent-skip arm still commits",
    );
    match read.lock().clone() {
        Some(Ok(())) => {}
        Some(Err(e)) => bail!("the Skip-arm hook read was fenced: {e}"),
        None => bail!("after_commit never fired"),
    }
    Ok(())
}
