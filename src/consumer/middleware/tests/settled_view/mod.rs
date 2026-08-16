//! Settled-view discard: on every settle path that did **not** successfully
//! finalize, the boundary discards this event's uncommitted overlay under the
//! held closed gate before the apply hook fires, so a hook read — and a leaked
//! hook-window read — observes fully-settled committed truth with no
//! aborted-attempt residue, while an explicit mid-handler `commit()` floor
//! survives. Also pins the apply-hook contract (a hook mutation errors
//! `SessionClosed`, `rollback()` is an effectless `NoOp`), teardown fencing
//! (post-`terminate` operations return `Terminated`), and hook-window reads.
use super::*;
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell::Committed;
use crate::state::descriptor::tests::{FixedOracle, TestSession, test_session_parts};
use crate::state::descriptor::{CellStateError, Registered, ValueHandle, value_state};
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::sealed::StateLifecycle;
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::value_cell;
use crate::state::{CollectionId, EventRef, StateKey, StateName, StateType, StoreOutcome};
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::{Value, json};
use std::marker::PhantomData;
use std::sync::Arc;
use uuid::Uuid;

type Ctx = MockEventContext<Value, TestSession>;
type Handle = ValueHandle<TestSession, JsonCodec>;

const FLOOR: &str = "floor";
const PENDING: &str = "pending";

/// The settlement classification a [`ViewProbe`] reports, as a ZST marker
/// so the associated (self-less) `settlement()` can read it.
trait Classify: Clone + Send + Sync + 'static {
    const SETTLEMENT: Settlement;
}

#[derive(Clone)]
struct AsFinal;
impl Classify for AsFinal {
    const SETTLEMENT: Settlement = Settlement::Final;
}

#[derive(Clone)]
struct AsBypassed;
impl Classify for AsBypassed {
    const SETTLEMENT: Settlement = Settlement::Bypassed;
}

/// The name of the [`StateAccessError`] variant a fenced op hit, so an
/// assertion names the exact fence rather than matching an opaque value.
fn err_tag(error: &CellStateError<JsonCodecError>) -> String {
    match error {
        CellStateError::Access(StateAccessError::SessionClosed) => "SessionClosed".into(),
        CellStateError::Access(StateAccessError::Terminated) => "Terminated".into(),
        CellStateError::Access(StateAccessError::Unavailable) => "Unavailable".into(),
        other => format!("other: {other}"),
    }
}

/// What a fired hook observed through the event's own (stamped) session.
#[derive(Clone, Default)]
struct HookObservation {
    /// The `commit()`-floored collection's read — must survive the discard.
    floor: Option<Result<Option<Value>, String>>,
    /// The uncommitted collection's read — must be the committed base
    /// (`None`), never the discarded buffered set.
    pending: Option<Result<Option<Value>, String>>,
    /// A hook mutation attempt: the error-variant tag, or `Ok` if it
    /// wrongly succeeded.
    mutation: Option<Result<(), String>>,
    /// A hook `rollback()` outcome — must be an effectless `NoOp`.
    rollback: Option<StoreOutcome>,
}

/// Probe reading `floor` + `pending` and attempting a mutation/rollback
/// through typed handles inside whichever apply hook fires, classifying
/// settlement by `M`.
#[derive(Clone)]
struct ViewProbe<M> {
    seen: Arc<Mutex<Option<HookObservation>>>,
    _marker: PhantomData<fn() -> M>,
}

impl<M: Classify> ViewProbe<M> {
    fn new() -> Self {
        Self {
            seen: Arc::default(),
            _marker: PhantomData,
        }
    }

    fn observation(&self) -> Option<HookObservation> {
        self.seen.lock().clone()
    }

    async fn observe<C>(&self, context: &C)
    where
        C: EventContext<Payload = Value>,
    {
        let mut obs = HookObservation::default();
        match (handle(context, FLOOR), handle(context, PENDING)) {
            (Ok(floor), Ok(pending)) => {
                obs.floor = Some(floor.get().await.map_err(|e| e.to_string()));
                obs.pending = Some(pending.get().await.map_err(|e| e.to_string()));
                // A hook mutation must be fenced: the gate is Closed.
                obs.mutation = Some(pending.set(json!("hook")).await.map_err(|e| err_tag(&e)));
                // rollback() on a closed session is an effectless NoOp.
                obs.rollback = Some(pending.rollback().await);
            }
            _ => obs.pending = Some(Err("handle bind failed".into())),
        }
        *self.seen.lock() = Some(obs);
    }
}

impl<M: Classify> FallibleHandler for ViewProbe<M> {
    type Error = TestError;
    type Output = u64;
    type Payload = Value;

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
        self.observe(&context).await;
    }

    async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.observe(&context).await;
    }

    async fn shutdown(self) {}
}

impl<M: Classify> SettlementHandler for ViewProbe<M> {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        M::SETTLEMENT
    }
}

/// Binds `name`'s typed Value handle off `context` — exactly what a user
/// hook does.
fn handle<C>(context: &C, name: &str) -> Result<ValueHandle<C::State, JsonCodec>>
where
    C: EventContext,
{
    context
        .state(Registered::new(value_state::<JsonCodec>(name)))
        .map_err(|e| eyre!("bind {name}: {e}"))
}

/// Builds a two-collection session: `floor` is set **and** `commit()`ed
/// (durable, drained from the buffer — the commit-now floor), `pending` is
/// set but left buffered (uncommitted). Returns the ready context, the
/// durable store, and both collection ids.
async fn two_collections() -> Result<(
    Ctx,
    MemoryCellStore<FixedOracle>,
    CollectionId,
    CollectionId,
)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&value_state::<JsonCodec>(FLOOR), CollectionDef::new(None))?;
    registry.register(&value_state::<JsonCodec>(PENDING), CollectionDef::new(None))?;
    let state_key = StateKey::new(Uuid::from_u128(0x5D), Arc::from("user-1"));
    let (session, cell_store) =
        test_session_parts(MemoryLoader::new(), registry, state_key.clone());
    let context: Ctx = MockEventContext::new().with_session(session);

    let floor: Handle = handle(&context, FLOOR)?;
    floor.set(json!("floor")).await?;
    assert_eq!(
        floor.commit().await?,
        StoreOutcome::Applied,
        "the floor commit() must land a durable write",
    );
    let pending: Handle = handle(&context, PENDING)?;
    pending.set(json!("pending")).await?;

    let floor_id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new(FLOOR)?,
    );
    let pending_id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new(PENDING)?,
    );
    Ok((context, cell_store, floor_id, pending_id))
}

/// Whether a collection's Value cell holds any committed bytes on the
/// durable store — read through a foreign probe event, so a still-buffered
/// write is invisible.
async fn durably_present(
    cell_store: &MemoryCellStore<FixedOracle>,
    id: &CollectionId,
) -> Result<bool> {
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    cell_store
        .get(id, &value_cell(), probe)
        .await
        .map(|c| Committed::into_inner(c).is_some())
        .map_err(|e| eyre!("committed read: {e}"))
}

fn guard() -> (RecordingGuard, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let committed = Arc::new(AtomicUsize::new(0));
    let aborted = Arc::new(AtomicUsize::new(0));
    let g = RecordingGuard {
        committed: committed.clone(),
        aborted: aborted.clone(),
    };
    (g, committed, aborted)
}

async fn run_and_observe<M: Classify>(
    probe: &ViewProbe<M>,
    context: Ctx,
    g: RecordingGuard,
    result: Result<u64, TestError>,
) -> Result<HookObservation> {
    settle(probe, context, g, result).await;
    probe.observation().ok_or_else(|| eyre!("a hook must fire"))
}

/// Drives one non-finalized arm end to end: buffers the floor+pending,
/// settles the probe with `result`, and asserts the settled-view discard,
/// the commit-now floor, and the apply-hook contract.
async fn run_arm<M: Classify>(
    arm: &str,
    result: Result<u64, TestError>,
    commits: bool,
) -> Result<()> {
    let (context, cell_store, floor_id, pending_id) = two_collections().await?;
    let (g, committed, aborted) = guard();
    let probe = ViewProbe::<M>::new();
    let obs = run_and_observe(&probe, context, g, result).await?;
    assert_arm(arm, &obs, &committed, &aborted, commits)?;
    // Durable truth: the floor is committed, the discarded pending is not.
    assert!(
        durably_present(&cell_store, &floor_id).await?,
        "{arm}: the commit-now floor is durable",
    );
    assert!(
        !durably_present(&cell_store, &pending_id).await?,
        "{arm}: the discarded pending write never became durable",
    );
    Ok(())
}

fn assert_arm(
    arm: &str,
    obs: &HookObservation,
    committed: &Arc<AtomicUsize>,
    aborted: &Arc<AtomicUsize>,
    commits: bool,
) -> Result<()> {
    if commits {
        assert_eq!(committed.load(Ordering::SeqCst), 1, "{arm}: commits");
        assert_eq!(aborted.load(Ordering::SeqCst), 0, "{arm}: not aborted");
    } else {
        assert_eq!(aborted.load(Ordering::SeqCst), 1, "{arm}: aborts");
        assert_eq!(committed.load(Ordering::SeqCst), 0, "{arm}: not committed");
    }
    // Settled-view discard: no aborted-overlay residue.
    match &obs.pending {
        Some(Ok(None)) => {}
        other => bail!("{arm}: pending must read the committed base None, got {other:?}"),
    }
    // Commit-now floor survives the discard.
    match &obs.floor {
        Some(Ok(Some(v))) if *v == json!("floor") => {}
        other => bail!("{arm}: floor must survive as \"floor\", got {other:?}"),
    }
    // Apply-hook contract: a mutation is fenced SessionClosed.
    match &obs.mutation {
        Some(Err(tag)) if tag == "SessionClosed" => {}
        other => bail!("{arm}: a hook mutation must error SessionClosed, got {other:?}"),
    }
    // rollback() stays an effectless NoOp.
    assert_eq!(
        obs.rollback,
        Some(StoreOutcome::NoOp),
        "{arm}: hook rollback() must be a NoOp",
    );
    Ok(())
}

/// Non-finalized arms — no aborted-overlay residue and the apply-hook
/// contract, over final Permanent, final Transient,
/// `Bypassed`, and the direct `abandon` (Terminal). Each fires its hook
/// after the boundary discards the uncommitted overlay, so the hook sees
/// `pending == None` (base, residue gone) and `floor == "floor"`
/// (commit-now floor survived); a hook mutation is fenced `SessionClosed`
/// and `rollback()` is a `NoOp`; and `pending` is not durable while `floor`
/// is. Falsify by deleting the `discard_uncommitted` line on the arm under
/// test in `settle.rs`: `pending` then reads the buffered `"pending"`.
#[tokio::test]
async fn non_finalized_arms_discard_overlay_keeping_commit_floor() -> Result<()> {
    // Final Permanent, final Transient, and Bypassed all commit the guard;
    // the direct abandon (Terminal) aborts it. Each fires a hook.
    run_arm::<AsFinal>(
        "final-permanent",
        Err(TestError(ErrorCategory::Permanent, "final")),
        true,
    )
    .await?;
    run_arm::<AsFinal>(
        "final-transient",
        Err(TestError(ErrorCategory::Transient, "final")),
        true,
    )
    .await?;
    run_arm::<AsBypassed>("bypassed", Ok(0), true).await?;
    run_arm::<AsFinal>(
        "terminal-abandon",
        Err(TestError(ErrorCategory::Terminal, "final")),
        false,
    )
    .await?;
    Ok(())
}

/// The permanent finalize-**failure** arm (settled-view cleanup plus the
/// commit-now floor): a Permanent stage
/// failure hits `StepOutcome::Skip`, which commits defensively but is NOT a
/// successful finalize — so `finalize` never drained the buffer and the
/// boundary must discard it under the held permit before the hook. The hook
/// then sees `pending == None` (residue gone) and the commit-now `floor`
/// survives. Falsify by deleting the `discard_uncommitted` line in the Skip
/// arm of `settle_committed`: `pending` reads the buffered `"pending"`.
#[tokio::test]
async fn permanent_finalize_failure_discards_overlay_keeping_floor() -> Result<()> {
    use crate::consumer::middleware::tests::test_support::RecordingOracle;
    use crate::consumer::partition::ShutdownPhase;
    use crate::state::PartitionBackend;
    use crate::state::dirty::DirtyStore;
    use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
    use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
    use crate::state::tests::cell_suite::FailingCellStore;
    use crate::timers::duration::CompactDuration;
    use tokio::sync::watch;

    type SkipStore = FailingCellStore<MemoryCellStore<RecordingOracle>>;
    type SkipBackend = PartitionBackend<RecordingOracle, MemoryDescriptorIdentityStore, SkipStore>;
    type SkipSession = KeyedStateSession<SkipBackend, MemoryLoader<Value>>;

    let mut registry = CollectionDefRegistry::default();
    registry.register(&value_state::<JsonCodec>(FLOOR), CollectionDef::new(None))?;
    registry.register(&value_state::<JsonCodec>(PENDING), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    // Poison PENDING's stage so `settle`'s own `finalize` hits Skip; FLOOR's
    // mid-handler `commit()` uses `write_resolved` and is untouched.
    let cell_store = FailingCellStore::failing_write_provisional(
        MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone()),
        StateName::try_new(PENDING)?,
        ErrorCategory::Permanent,
    );
    let (_s, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_c, cancel_rx) = watch::channel(false);
    let session: SkipSession = KeyedStateSession::new(SessionParts {
        cell: cell_store,
        dirty: Arc::new(DirtyStore::new()),
        oracle,
        loader: MemoryLoader::new(),
        registry,
        state_key: StateKey::new(Uuid::from_u128(0x5D5), Arc::from("user-1")),
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

    // FLOOR: durable commit-now floor. PENDING: buffered, to be discarded.
    let floor: ValueHandle<SkipSession, JsonCodec> = context
        .state(Registered::new(value_state::<JsonCodec>(FLOOR)))
        .map_err(|e| eyre!("bind floor: {e}"))?;
    floor.set(json!("floor")).await?;
    assert_eq!(floor.commit().await?, StoreOutcome::Applied);
    context
        .state(Registered::new(value_state::<JsonCodec>(PENDING)))
        .map_err(|e| eyre!("bind pending: {e}"))?
        .set(json!("pending"))
        .await?;

    let (g, committed, aborted) = guard();
    let probe = ViewProbe::<AsFinal>::new();
    settle(&probe, context, g, Ok(0)).await;

    let obs = probe.observation().ok_or_else(|| eyre!("hook fires"))?;
    // The permanent-skip arm commits defensively.
    assert_arm(
        "permanent-finalize-failure",
        &obs,
        &committed,
        &aborted,
        true,
    )?;
    Ok(())
}

/// Teardown fence plus graceful hook-window read: a current-pin handle
/// captured before settle reads committed data during the hook window (the
/// gate is Closed, reads allowed — graceful completion), then errors
/// `Terminated` once the session is torn down (`terminate`, the sync half
/// the scope's `Drop` runs). Falsify by dropping the `is_terminated()` term
/// in `ensure_live`: the post-teardown read returns `Ok` instead.
mod teardown;
