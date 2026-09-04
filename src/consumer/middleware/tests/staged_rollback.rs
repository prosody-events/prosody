use super::*;
use crate::consumer::middleware::tests::test_support::{
    Ctx, DuplicateHandler, TestLifecycleAccess, buffered, buffered_with, cart, committed_value,
    is_provisional,
};
use crate::loader::MemoryLoader;
use crate::state::descriptor::Registered;
use crate::state::manager::ArmedKeys;
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Finalized;
use crate::state::session::sealed::StateLifecycle;
use crate::state::{EventRef, StateKey, StateName};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, bail, eyre};
use quickcheck::{QuickCheck, TestResult};
use serde_json::json;
use std::future::ready;
use tokio::runtime::Builder;
use uuid::Uuid;

/// Shutdown between a successful stage and the backstop arm drives
/// settle's ONE reachable rollback: the guard aborts, the staged cell
/// rolls back to its committed base (here absent, so the cell resolves
/// away), and `after_abort` fires. The abort itself proves the staged arm
/// ran — a `Clean` finalize would have committed instead.
#[tokio::test]
async fn arm_shutdown_rolls_the_staged_cells_back() -> Result<()> {
    let (context, cell_store, cart_id) = buffered(Ctx::with_shutdown_on_timer_read).await?;
    let handler = ProbeHandler::ok(0);
    let log = handler.log.clone();
    let (guard, committed, aborted) = RecordingGuard::new_reruns();

    settle(&handler, context, guard, Ok(0)).await;

    assert_eq!(
        aborted.load(Ordering::SeqCst),
        1,
        "arm-shutdown must abort the guard"
    );
    assert_eq!(committed.load(Ordering::SeqCst), 0);
    assert!(
        !is_provisional(&cell_store, &cart_id).await?,
        "the receipt's rollback must settle the staged cell",
    );
    assert_eq!(
        committed_value(&cell_store, &cart_id).await?,
        None,
        "rollback restores the absent committed base — not the staged value",
    );
    assert_eq!(
        log.lock().clone(),
        vec![HookEvent::AfterAbort(Ok(0))],
        "the arm-shutdown abort fires after_abort exactly once",
    );
    Ok(())
}

/// The permanent-finalize-skip arm of `settle_committed`: a **Permanent**
/// stage failure is the one durability step the sequence skips rather than
/// retries (a genuine data rejection cannot self-heal). The documented
/// posture is commit-defensively: the offset still commits (no livelock on
/// an unstageable event), the marker record is skipped (a present marker
/// must certify a durable stage — invariant: marker present ⇒ stage
/// durable), and the backstop is armed defensively so the sweep resolves
/// whatever partial stage may have landed.
#[tokio::test]
async fn permanent_finalize_skip_commits_unmarked_with_backstop_armed() -> Result<()> {
    use crate::consumer::middleware::tests::test_support::RecordingOracle;
    use crate::consumer::partition::ShutdownPhase;
    use crate::state::EventRef;
    use crate::state::PartitionBackend;
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::tests::cell_suite::FailingCellStore;
    use crate::timers::duration::CompactDuration;
    use tokio::sync::watch;

    type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
    type SkipBackend = PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;

    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    let recorded = oracle.recorded();
    // Poison the STAGE path: `write_provisional` on `cart` fails
    // Permanent, so `finalize` inside `settle` hits `StepOutcome::Skip`.
    let cell_store = FailingCellStore::failing_write_provisional(
        MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
        StateName::try_new("cart")?,
        ErrorCategory::Permanent,
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session: KeyedStateSession<SkipBackend, MemoryLoader<serde_json::Value>> =
        KeyedStateSession::new(SessionParts {
            cell: cell_store,
            dirty: Arc::new(DirtyStore::new()),
            oracle,
            loader: MemoryLoader::new(),
            registry,
            state_key: StateKey::new(Uuid::from_u128(0x5C1), Arc::from("user-1")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        });
    let context = MockEventContext::new()
        .with_session(session)
        .with_timer_tracking();

    // Buffer a write; do NOT finalize — `settle` owns the stage and must
    // hit the poison itself. The session's message EventRef carries the
    // marker the skip path must NOT record.
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;

    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&handler, context.clone(), guard, Ok(0)).await;

    assert_eq!(
        committed.load(Ordering::SeqCst),
        1,
        "a permanently-unstageable event still commits (no livelock)",
    );
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    assert!(
        recorded.lock().is_empty(),
        "the marker must NOT record over an uncertain stage",
    );
    assert_eq!(
        context.count_scheduled(TimerType::StateRecovery),
        1,
        "the backstop must be armed defensively for the sweep",
    );
    Ok(())
}

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

impl SettlementHandler for SkipReadProbe {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// The permanent-finalize-`Skip` arm must fire `after_commit` through
/// `fire_apply_hook`, not a direct call: a settle context left STALE by a
/// nested retry's epoch bump is re-stamped current before the hook reads.
/// Without the stamp the hook's typed read errors `Terminated` — the one
/// hook-fire site that used to bypass the stamp. Red-proven by reverting
/// the arm to `handler.after_commit(context, result)`: the read then
/// reports `Terminated`.
#[tokio::test]
async fn permanent_skip_hook_reads_through_the_stamp() -> Result<()> {
    use crate::consumer::middleware::tests::test_support::RecordingOracle;
    use crate::consumer::partition::ShutdownPhase;
    use crate::state::PartitionBackend;
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::session::sealed::StateLifecycle;
    use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::tests::cell_suite::FailingCellStore;
    use crate::timers::duration::CompactDuration;
    use tokio::sync::watch;

    type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
    type SkipBackend = PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;

    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    // Poison the STAGE path so `settle`'s own `finalize` hits Skip.
    let cell_store = FailingCellStore::failing_write_provisional(
        MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
        StateName::try_new("cart")?,
        ErrorCategory::Permanent,
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session: KeyedStateSession<SkipBackend, MemoryLoader<serde_json::Value>> =
        KeyedStateSession::new(SessionParts {
            cell: cell_store,
            dirty: Arc::new(DirtyStore::new()),
            oracle,
            loader: MemoryLoader::new(),
            registry,
            state_key: StateKey::new(Uuid::from_u128(0x5C2), Arc::from("user-1")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
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

    settle(&handler, context, guard, Ok(0)).await;

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

/// Shutdown can abort before receipt. After receipt it keeps the committed
/// source. Each failed timer write must retry. Each collection bound limits
/// recovery delay.
#[test]
fn prop_settle_aborts_iff_shutdown() {
    fn property(
        posture: u8,
        failure: u8,
        shutdown: bool,
        late_shutdown: bool,
        timer_failure: (u8, u8),
    ) -> TestResult {
        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build();
        let Ok(runtime) = runtime else {
            return TestResult::error("failed to build paused runtime");
        };
        runtime.block_on(async move {
            match settle_case(posture, failure, shutdown, late_shutdown, timer_failure).await {
                Ok(result) => result,
                Err(error) => TestResult::error(error.to_string()),
            }
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, bool, bool, (u8, u8)) -> TestResult);
}

async fn settle_case(
    posture: u8,
    failure: u8,
    shutdown: bool,
    late_shutdown: bool,
    timer_failure: (u8, u8),
) -> Result<TestResult> {
    let sweep_failure = match failure % 3 {
        0 => None,
        1 => Some((ErrorCategory::Permanent, 8)),
        _ => Some((ErrorCategory::Transient, 1)),
    };
    let armed: ArmedKeys = Arc::default();
    let count = usize::from(timer_failure.0 % 5);
    let (mut context, cell_store, cart_id) = buffered_with(
        armed.clone(),
        sweep_failure,
        Some(CompactDuration::new(1)),
        |context| fail_timer_writes(context, timer_failure),
    )
    .await?;
    let posture = posture % 3;
    let late_shutdown = late_shutdown
        && !shutdown
        && posture == 1
        && matches!(sweep_failure, Some((ErrorCategory::Transient, _)));
    if late_shutdown {
        context = context.with_shutdown_on_timer_read();
    }
    if posture == 0 {
        let lifecycle = context.test_lifecycle()?;
        assert!(matches!(lifecycle.finalize().await?, Finalized::Staged(_)));
    }
    if shutdown {
        context = context.with_shutdown();
    }
    let (guard, committed, aborted) = if posture == 2 {
        RecordingGuard::new_reruns()
    } else {
        RecordingGuard::new()
    };
    let receipts = guard.receipts.clone();
    let kept = guard.kept.clone();
    let receipt_saw_provisional = Arc::new(AtomicBool::new(false));
    let commit_saw_resolved = Arc::new(AtomicBool::new(false));
    let guard = guard.with_order(
        cell_store.clone(),
        cart_id.clone(),
        receipt_saw_provisional.clone(),
        commit_saw_resolved.clone(),
    );
    let handler = ProbeHandler::ok(0);
    let before = CompactDateTime::now()?;
    if posture == 0 {
        settle(&DuplicateHandler, context.clone(), guard, Ok(())).await;
    } else {
        settle(&handler, context.clone(), guard, Ok(0)).await;
    }

    let after = CompactDateTime::now()?;
    let kept = kept.load(Ordering::SeqCst);
    let committed = committed.load(Ordering::SeqCst);
    let aborted = aborted.load(Ordering::SeqCst);
    let receipts = receipts.load(Ordering::SeqCst);
    let still_provisional = is_provisional(&cell_store, &cart_id).await?;
    let scheduled = context.count_scheduled(TimerType::StateRecovery);
    let transient = matches!(sweep_failure, Some((ErrorCategory::Transient, _)));
    let failing = sweep_failure.is_some();

    let expected_abort = shutdown && (posture != 0 || transient);
    let expected_receipts = usize::from(posture == 1 && !shutdown);
    // The Duplicate setup stages the cell before `settle`. A Duplicate
    // abort leaves that cell for the next sweep. A Final abort occurs
    // before the stage, so nothing is provisional.
    let expected_provisional = failing && (posture == 0 || !expected_abort);
    let expected_armed = usize::from(
        !expected_abort
            && !late_shutdown
            && (posture == 2 || (posture == 1 && failing) || (posture == 0 && transient)),
    );
    if aborted != usize::from(expected_abort)
        || committed != usize::from(!expected_abort && !late_shutdown)
        || kept != usize::from(late_shutdown)
        || receipts != expected_receipts
        || still_provisional != expected_provisional
        || scheduled != expected_armed * (count + 1)
        || armed.len() != expected_armed
        || !context
            .durable_scheduled(TimerType::StateRecovery)
            .iter()
            .all(|fire| {
                (before.epoch_seconds() + 1..=after.epoch_seconds() + 1)
                    .contains(&fire.epoch_seconds())
            })
        || (late_shutdown && handler.log.lock().as_slice() != [HookEvent::AfterCommit(Ok(0))])
        || (posture == 1
            && !shutdown
            && (!receipt_saw_provisional.load(Ordering::SeqCst)
                || commit_saw_resolved.load(Ordering::SeqCst) == failing))
    {
        return Ok(TestResult::error(format!(
            "posture={posture} failure={sweep_failure:?} shutdown={shutdown} late={late_shutdown} \
             timer={timer_failure:?} committed={committed} aborted={aborted} receipts={receipts} \
             kept={kept} provisional={still_provisional} armed={scheduled} durable={:?}",
            context.durable_scheduled(TimerType::StateRecovery)
        )));
    }
    Ok(TestResult::passed())
}

fn fail_timer_writes(context: Ctx, failure: (u8, u8)) -> Ctx {
    let category = match failure.1 % 3 {
        0 => ErrorCategory::Transient,
        1 => ErrorCategory::Permanent,
        _ => ErrorCategory::Terminal,
    };
    context
        .with_timer_tracking()
        .with_timer_failures(usize::from(failure.0 % 5), category)
}
