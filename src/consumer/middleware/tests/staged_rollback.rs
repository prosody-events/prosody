use super::*;
use crate::consumer::middleware::tests::test_support::{
    Ctx, buffered, cart, committed_value, is_provisional,
};
use crate::loader::MemoryLoader;
use crate::state::descriptor::Registered;
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::{EventRef, StateKey, StateName};
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
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
    let (guard, committed, aborted) = RecordingGuard::new();

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

/// Abort-only-on-shutdown, end to end through `settle`'s success path: a
/// shutdown is the *sole* thing that stops the durability sequence short
/// of a commit — a shutdown seen before the finalize stage abandons with
/// nothing ever staged, so nothing is left provisional; the offset aborts
/// either way (the event redelivers and re-runs). Every store failure
/// instead retries forever.
///
/// The never-abort-except-shutdown invariant, as a property over a
/// generated leading-failure count, the **category** those failures
/// classify as, and a shutdown flag: `settle`'s success path aborts the
/// offset **iff** shutdown (leaving nothing provisional), and
/// otherwise self-heals to a commit **no matter how many** failures —
/// of **any** category — the arm hits first (the arm is must-succeed,
/// invariant 8) — then records the marker, commits, and promotes the
/// cell. Generating the category is what exercises the retry-forever
/// fold in `retry_step`: `Terminal` retries rather than abandons, and
/// `Permanent` is retried by the arm's own loop past `retry_step`'s
/// `Skip`. Each iteration runs on its own paused single-thread
/// runtime so the retry backoff advances instantly and never blocks.
#[test]
fn prop_settle_aborts_iff_shutdown() {
    fn property(fail_count: u8, category_sel: u8, shutdown: bool) -> TestResult {
        // A small bound keeps each iteration's paused-clock retry loop fast
        // while still crossing the zero / non-zero boundary.
        let fail_count = usize::from(fail_count % 6);
        let category = match category_sel % 3 {
            0 => ErrorCategory::Transient,
            1 => ErrorCategory::Permanent,
            _ => ErrorCategory::Terminal,
        };
        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build();
        let Ok(runtime) = runtime else {
            return TestResult::error("failed to build paused runtime");
        };
        runtime.block_on(async move {
            let configure = |c: Ctx| {
                let c = c.with_timer_failures(fail_count, category);
                if shutdown { c.with_shutdown() } else { c }
            };
            let Ok((context, cell_store, cart_id)) = buffered(configure).await else {
                return TestResult::error("failed to buffer the write");
            };
            let handler = ProbeHandler::ok(0);
            let (guard, committed, aborted) = RecordingGuard::new();

            settle(&handler, context, guard, Ok(0)).await;

            let committed = committed.load(Ordering::SeqCst);
            let aborted = aborted.load(Ordering::SeqCst);
            let provisional = cell_store.provisional_cells(&cart_id);
            futures::pin_mut!(provisional);
            let still_provisional = matches!(provisional.next().await, Some(Ok(_)));

            if shutdown {
                // Abort iff shutdown: the offset aborts, and nothing was
                // ever staged (settle's finalize step sees shutdown first).
                if aborted != 1 || committed != 0 || still_provisional {
                    return TestResult::error(format!(
                        "shutdown must abort with nothing staged: committed={committed} \
                         aborted={aborted} provisional={still_provisional}"
                    ));
                }
            } else {
                // No shutdown: self-heal to a commit however many failures first.
                if committed != 1 || aborted != 0 || still_provisional {
                    return TestResult::error(format!(
                        "non-shutdown must self-heal to commit: committed={committed} \
                         aborted={aborted} provisional={still_provisional}"
                    ));
                }
            }
            TestResult::passed()
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, bool) -> TestResult);
}
