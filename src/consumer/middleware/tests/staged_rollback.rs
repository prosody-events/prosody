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
            publisher: None,
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
            publisher: None,
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

/// Every settle posture commits unless shutdown interrupts it, and a failed
/// promote never blocks the commit.
///
/// The property varies the posture (Duplicate, Final sweeps, Final reruns),
/// an optional promote failure (category and budget), and the shutdown flag.
/// A Final posture aborts if and only if shutdown is active. That check runs
/// before the stage, so the abort leaves no provisional cell. A Duplicate
/// sweeps a pre-staged cell. A clean sweep commits even under shutdown. A
/// Permanent sweep failure logs and commits. A Transient sweep failure aborts
/// under shutdown; otherwise it arms one backstop and commits.
///
/// A failed promote leaves the cell provisional in every posture. The sweep
/// posture arms one backstop for it; the rerun posture armed before the
/// marker. The sweep posture records its receipt while the cell is
/// provisional and commits after the promote. Each case runs on a paused
/// runtime.
#[test]
fn prop_settle_aborts_iff_shutdown() {
    fn property(posture: u8, failure: u8, shutdown: bool) -> TestResult {
        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build();
        let Ok(runtime) = runtime else {
            return TestResult::error("failed to build paused runtime");
        };
        runtime.block_on(async move {
            let sweep_failure = match failure % 3 {
                0 => None,
                1 => Some((ErrorCategory::Permanent, 8)),
                _ => Some((ErrorCategory::Transient, 1)),
            };
            let armed: ArmedKeys = Arc::default();
            let configure = MockEventContext::with_timer_tracking;
            let Ok((mut context, cell_store, cart_id)) =
                buffered_with(armed, sweep_failure, configure).await
            else {
                return TestResult::error("failed to buffer the write");
            };
            let posture = posture % 3;
            if posture == 0 {
                let Ok(lifecycle) = context.test_lifecycle() else {
                    return TestResult::error("failed to get the lifecycle");
                };
                if !matches!(lifecycle.finalize().await, Ok(Finalized::Staged(_))) {
                    return TestResult::error("duplicate setup did not stage the cell");
                }
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
            let receipt_saw_provisional = Arc::new(AtomicBool::new(false));
            let commit_saw_resolved = Arc::new(AtomicBool::new(false));
            let guard = if posture == 1 && !shutdown {
                guard.with_order(
                    cell_store.clone(),
                    cart_id.clone(),
                    receipt_saw_provisional.clone(),
                    commit_saw_resolved.clone(),
                )
            } else {
                guard
            };
            if posture == 0 {
                settle(&DuplicateHandler, context.clone(), guard, Ok(())).await;
            } else {
                settle(&ProbeHandler::ok(0), context.clone(), guard, Ok(0)).await;
            }

            let committed = committed.load(Ordering::SeqCst);
            let aborted = aborted.load(Ordering::SeqCst);
            let receipts = receipts.load(Ordering::SeqCst);
            let Ok(still_provisional) = is_provisional(&cell_store, &cart_id).await else {
                return TestResult::error("failed to inspect the provisional cell");
            };
            let scheduled = context.count_scheduled(TimerType::StateRecovery);
            let transient = matches!(sweep_failure, Some((ErrorCategory::Transient, _)));
            let failing = sweep_failure.is_some();

            let expected_abort = if posture == 0 {
                shutdown && transient
            } else {
                shutdown
            };
            let expected_receipts = usize::from(posture == 1 && !shutdown);
            // The Duplicate setup stages the cell before `settle`. A Duplicate
            // abort leaves that cell for the next sweep. A Final abort occurs
            // before the stage, so nothing is provisional.
            let expected_provisional = failing && (posture == 0 || !expected_abort);
            let expected_armed = usize::from(
                !expected_abort
                    && (posture == 2 || (posture == 1 && failing) || (posture == 0 && transient)),
            );
            if aborted != usize::from(expected_abort)
                || committed != usize::from(!expected_abort)
                || receipts != expected_receipts
                || still_provisional != expected_provisional
                || scheduled != expected_armed
                || (posture == 1
                    && !shutdown
                    && (!receipt_saw_provisional.load(Ordering::SeqCst)
                        || commit_saw_resolved.load(Ordering::SeqCst) == failing))
            {
                return TestResult::error(format!(
                    "posture={posture} failure={sweep_failure:?} shutdown={shutdown} \
                     committed={committed} aborted={aborted} receipts={receipts} \
                     provisional={still_provisional} armed={scheduled}"
                ));
            }
            TestResult::passed()
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, bool) -> TestResult);
}
