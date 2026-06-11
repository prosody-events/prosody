//! Tests for the keyed-state durability boundary (`settle`).
//!
//! Every test drives the **real** blanket `EventHandler` impl
//! (`EventHandler::on_message`), so the `settle` sequence runs end-to-end:
//! the inner `FallibleHandler` buffers state ops and registers the message's
//! commit marker exactly as the deduplication middleware does, then `settle`
//! seals, arms the `StateRecovery` backstop, flushes the marker through the
//! commit oracle (strictly after the seal), commits the offset, and resolves.
//!
//! The session is minted directly (the manager just calls
//! [`ValueStateSession::new`]) over a [`MemoryDurableValueStore`] and a
//! [`RecordingOracle`] whose `record_message` writes an inspectable
//! [`MemoryDeduplicationStore`] — the same shared store a dedup filter would
//! read. Inspecting that store is how the seal-before-marker invariant is
//! pinned: the marker row can never appear over an unsealed WAL.
//!
//! Marker registration is done by the leaf handler here (mirroring the
//! dedup middleware's `register_marker` on `Ok` / `Permanent`); the dedup
//! middleware's own filter/register behavior is covered in
//! `deduplication::tests::handler`. The seal/apply/rollback resolution of the
//! recorded sealed set is covered at the session level in
//! `state::session::tests`; here the focus is the boundary sequence and its
//! ordering.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_utils::CachePadded;
use parking_lot::Mutex as SyncMutex;
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;
use tokio::sync::watch;
use uuid::Uuid;

use crate::codec::{Codec, JsonCodec};
use crate::consumer::DemandType;
use crate::consumer::EventHandler;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::deduplication::{DeduplicationStore, MemoryDeduplicationStore};
use crate::consumer::middleware::tests::test_support::{MockEventContext, TimerOperation};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleEventHandler, FallibleHandler,
};
use crate::consumer::partition::ShutdownPhase;
use crate::consumer::partition::offsets::OffsetTracker;
use crate::loader::MemoryLoader;
use crate::state::descriptor::{ValueDescriptor, ValueStateError, value_state};
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{LifecycleAccess, SessionParts, TerminationWatch, ValueStateSession};
use crate::state::value::{DurableWalStore, ValueKind};
use crate::state::{
    CollectionId, CommitDecision, CommitMode, DurableState, EventRef, StateKey, StateName,
    StateType,
};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::{Key, Offset, Topic};

use crate::test_util::TEST_RUNTIME;
use color_eyre::eyre::{Result, eyre};

const CART: ValueDescriptor = value_state("cart");
const WISHLIST: ValueDescriptor = value_state("wishlist");
const SEGMENT: Uuid = Uuid::from_u128(0x5E6);

type Session = ValueStateSession<
    FailingSealDurable,
    RecordingOracle,
    MemoryDirtyValueStoreProvider,
    MemoryLoader<Value>,
>;
type TestContext = MockEventContext<Value, Session>;

/// Commit oracle whose `record_message` writes a shared, inspectable dedup
/// store — the message half of `CommitManager`. `resolve` answers
/// committed-iff-present, used only on the recovery path (not by `settle`).
#[derive(Clone)]
struct RecordingOracle {
    store: MemoryDeduplicationStore,
}

impl CommitOracle for RecordingOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.store.insert(dedup_id).await
    }

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.store.exists(dedup_id).await?,
            EventRef::Timer(_) => true,
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

/// Durable Value bundle that delegates to [`MemoryDurableValueStore`] but
/// fails the first `fail_seals` `seal` calls with an injected error of the
/// configured classification: `Permanent` drives the boundary's skip arm,
/// `Transient` drives `retry_step`'s retry-in-place loop. Independently, it
/// fails the first `fail_applies` post-commit `apply_sealed` calls, driving
/// `commit_apply` to [`ApplyOutcome::Incomplete`] so the backstop must stay
/// armed for the sweep.
#[derive(Clone, Debug)]
struct FailingSealDurable {
    inner: MemoryDurableValueStore,
    fail_seals: Arc<AtomicUsize>,
    fail_applies: Arc<AtomicUsize>,
    category: ErrorCategory,
}

impl FailingSealDurable {
    fn never_fails() -> Self {
        Self {
            inner: MemoryDurableValueStore::for_tests(),
            fail_seals: Arc::new(AtomicUsize::new(0)),
            fail_applies: Arc::new(AtomicUsize::new(0)),
            category: ErrorCategory::Permanent,
        }
    }

    fn failing_seals(count: usize, category: ErrorCategory) -> Self {
        Self {
            inner: MemoryDurableValueStore::for_tests(),
            fail_seals: Arc::new(AtomicUsize::new(count)),
            fail_applies: Arc::new(AtomicUsize::new(0)),
            category,
        }
    }

    /// Seals succeed, but the first `count` post-commit `apply_sealed` calls
    /// fail permanently — the seal commits but never resolves, so
    /// `commit_apply` returns [`ApplyOutcome::Incomplete`].
    fn failing_applies(count: usize) -> Self {
        Self {
            inner: MemoryDurableValueStore::for_tests(),
            fail_seals: Arc::new(AtomicUsize::new(0)),
            fail_applies: Arc::new(AtomicUsize::new(count)),
            category: ErrorCategory::Permanent,
        }
    }

    /// Sets whether the *next* `apply_sealed` fails — per-event control over a
    /// shared store, safe because per-key serialization runs one event's
    /// settle to completion before the next dispatches.
    fn arm_apply_failure(&self, fail: bool) {
        self.fail_applies.store(usize::from(fail), Ordering::SeqCst);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("injected seal failure ({category:?})")]
struct InjectedSealError {
    category: ErrorCategory,
}

impl ClassifyError for InjectedSealError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

mod failing_seal_impls {
    use super::{
        Bytes, DurableState, EventRef, FailingSealDurable, InjectedSealError, Ordering, ValueKind,
    };
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::memory::MemoryStateError;
    use crate::state::value::{DirectApplyStore, DurableWalStore, ValueOp, ValueStore};
    use crate::state::{CollectionId, CollectionRef, Read, SealedCollection, StoreOutcome};
    use thiserror::Error;

    /// Error of [`FailingSealDurable`]: either the injected seal failure or a
    /// pass-through of the backing store error.
    #[derive(Debug, Error)]
    pub(super) enum FailingSealError {
        #[error(transparent)]
        Injected(#[from] InjectedSealError),
        #[error(transparent)]
        Inner(#[from] MemoryStateError),
    }

    impl ClassifyError for FailingSealError {
        fn classify_error(&self) -> ErrorCategory {
            match self {
                Self::Injected(e) => e.classify_error(),
                Self::Inner(e) => e.classify_error(),
            }
        }
    }

    impl ValueStore for FailingSealDurable {
        type Error = FailingSealError;

        async fn get<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<Read<Bytes>, Self::Error> {
            Ok(self.inner.get(collection).await?)
        }

        async fn set<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
            payload: Bytes,
        ) -> Result<(), Self::Error> {
            Ok(self.inner.set(collection, payload).await?)
        }

        async fn clear<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<(), Self::Error> {
            Ok(self.inner.clear(collection).await?)
        }
    }

    impl DurableWalStore<ValueKind> for FailingSealDurable {
        type Error = FailingSealError;

        async fn read_partition<'a>(
            &'a self,
            collection: &'a CollectionId<ValueKind>,
        ) -> Result<DurableState<ValueKind>, Self::Error> {
            Ok(DurableWalStore::read_partition(&self.inner, collection).await?)
        }

        async fn seal<'a, I>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            event: EventRef,
            ops: I,
        ) -> Result<SealedCollection<ValueKind>, Self::Error>
        where
            I: IntoIterator<Item = ValueOp> + Send + 'a,
        {
            if self.fail_seals.load(Ordering::SeqCst) > 0 {
                self.fail_seals.fetch_sub(1, Ordering::SeqCst);
                return Err(InjectedSealError {
                    category: self.category,
                }
                .into());
            }
            Ok(self.inner.seal(collection, event, ops).await?)
        }

        async fn apply_sealed<'a>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            event: EventRef,
        ) -> Result<StoreOutcome, Self::Error> {
            if self.fail_applies.load(Ordering::SeqCst) > 0 {
                self.fail_applies.fetch_sub(1, Ordering::SeqCst);
                return Err(InjectedSealError {
                    category: ErrorCategory::Permanent,
                }
                .into());
            }
            Ok(self.inner.apply_sealed(collection, event).await?)
        }

        async fn rollback_sealed<'a>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            event: EventRef,
        ) -> Result<StoreOutcome, Self::Error> {
            Ok(self.inner.rollback_sealed(collection, event).await?)
        }
    }

    impl DirectApplyStore<ValueKind> for FailingSealDurable {
        type Error = FailingSealError;

        async fn direct_apply<'a, I>(
            &'a self,
            collection: &'a CollectionRef<ValueKind>,
            ops: I,
        ) -> Result<StoreOutcome, Self::Error>
        where
            I: IntoIterator<Item = ValueOp> + Send + 'a,
        {
            Ok(self.inner.direct_apply(collection, ops).await?)
        }
    }
}

/// Leaf handler driven through the blanket `EventHandler` impl. Writes its
/// target collection (`CART` by default; override with [`Self::on_collection`])
/// per the scripted op, then registers the message commit marker on a final
/// outcome (`Ok` / `Permanent`) — mirroring the deduplication middleware — and
/// returns the scripted result.
#[derive(Clone)]
struct BoundaryHandler {
    descriptor: ValueDescriptor,
    set_byte: u8,
    marker: Uuid,
    result: Result<(), ErrorCategory>,
    observed: Arc<SyncMutex<Vec<Option<Value>>>>,
}

impl BoundaryHandler {
    fn set_ok(byte: u8, marker: Uuid) -> Self {
        Self {
            descriptor: CART,
            set_byte: byte,
            marker,
            result: Ok(()),
            observed: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    fn set_err(byte: u8, marker: Uuid, category: ErrorCategory) -> Self {
        Self {
            descriptor: CART,
            set_byte: byte,
            marker,
            result: Err(category),
            observed: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    /// Targets a specific registered collection instead of the default `CART`.
    fn on_collection(mut self, descriptor: ValueDescriptor) -> Self {
        self.descriptor = descriptor;
        self
    }

    async fn run<C>(&self, ctx: &C) -> Result<(), BoundaryError>
    where
        C: EventContext<Payload = Value>,
    {
        let cart = ctx.state(self.descriptor).map_err(ValueStateError::from)?;
        let before = cart.get().await?;
        self.observed.lock().push(before);
        cart.set(Value::from(self.set_byte)).await?;

        let result = match self.result {
            Ok(()) => Ok(()),
            Err(category) => Err(BoundaryError::Scripted(category)),
        };

        // Mirror the dedup middleware: register the marker on a final outcome.
        let register = match &result {
            Ok(()) => true,
            Err(e) => matches!(e.classify_error(), ErrorCategory::Permanent),
        };
        if register && let Ok(lifecycle) = ctx.state(LifecycleAccess) {
            lifecycle.register_marker(self.marker);
        }
        result
    }
}

#[derive(Debug, thiserror::Error)]
enum BoundaryError {
    #[error(transparent)]
    Cart(#[from] ValueStateError<<JsonCodec as Codec>::Error>),
    #[error("scripted failure")]
    Scripted(ErrorCategory),
}

impl ClassifyError for BoundaryError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cart(e) => e.classify_error(),
            Self::Scripted(category) => *category,
        }
    }
}

impl FallibleHandler for BoundaryHandler {
    type Error = BoundaryError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        _msg: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run(&ctx).await
    }

    async fn on_timer<C>(
        &self,
        ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext<Payload = Value>,
    {
        self.run(&ctx).await
    }

    async fn shutdown(self) {}
}

impl FallibleEventHandler for BoundaryHandler {}

fn registry() -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    let def =
        CollectionDef::new(Some(CompactDuration::new(3_600))).with_commit_mode(CommitMode::Wal);
    registry.register(&CART, def.clone())?;
    registry.register(&WISHLIST, def)?;
    Ok(Arc::new(registry))
}

fn termination() -> TerminationWatch {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    TerminationWatch::new(shutdown_rx, cancel_rx)
}

fn session(
    durable: FailingSealDurable,
    store: MemoryDeduplicationStore,
    key: &Key,
) -> Result<Session> {
    Ok(ValueStateSession::new(SessionParts {
        durable,
        oracle: RecordingOracle { store },
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::new(),
        registry: registry()?,
        state_key: StateKey::new(SEGMENT, key.clone()),
        event: EventRef::Message {
            dedup_id: Uuid::from_u128(0xE0E0),
        },
        recovery_delay: CompactDuration::new(30),
        termination: termination(),
    }))
}

fn cart_id(key: &Key) -> Result<CollectionId<ValueKind>> {
    collection_id(key, "cart")
}

fn collection_id(key: &Key, name: &str) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(SEGMENT, key.clone()),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

async fn read_applied(
    durable: &FailingSealDurable,
    id: &CollectionId<ValueKind>,
) -> Result<Option<Value>> {
    match DurableWalStore::read_partition(durable, id).await? {
        DurableState::Idle { applied } => applied
            .map(|cell| serde_json::from_slice::<Value>(&cell))
            .transpose()
            .map_err(|e| eyre!("applied cell is not codec JSON: {e}")),
        DurableState::Sealed { .. } => Err(eyre!("expected Idle, found a sealed WAL")),
    }
}

fn make_tracker() -> OffsetTracker {
    let version = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    OffsetTracker::new("t".into(), 0, 64, Duration::from_secs(5), version)
}

/// Drives one message dispatch through the blanket `EventHandler` impl
/// (hence `settle`), against `context`'s session.
async fn dispatch_message(
    handler: &BoundaryHandler,
    context: TestContext,
    offset: Offset,
    key: &Key,
) -> Result<()> {
    let tracker = make_tracker();
    let uncommitted = tracker.take(offset).await?;
    let message =
        ConsumerMessage::for_testing(Topic::from("t"), 0, offset, key.clone(), Value::Null)
            .map_err(|e| eyre!("for_testing: {e}"))?
            .into_uncommitted(uncommitted);
    EventHandler::on_message(handler, context, message, DemandType::Normal).await;
    Ok(())
}

fn schedules<S>(context: &MockEventContext<Value, S>) -> usize {
    context.count_scheduled(TimerType::StateRecovery)
}

/// The backstop is armed via `clear_and_schedule` (a per-key singleton
/// overwrite), never a bare `schedule`.
fn clear_and_schedules<S>(context: &MockEventContext<Value, S>) -> usize {
    context
        .timer_operations()
        .iter()
        .filter(|op| {
            matches!(
                op,
                TimerOperation::ClearAndSchedule(_, TimerType::StateRecovery)
            )
        })
        .count()
}

fn unschedules<S>(context: &MockEventContext<Value, S>) -> usize {
    context
        .timer_operations()
        .iter()
        .filter(|op| matches!(op, TimerOperation::Unschedule(_, TimerType::StateRecovery)))
        .count()
}

/// On a successful dispatch the boundary seals, flushes the marker, and
/// applies the sealed set: the durable cell holds the value, the WAL is
/// resolved (`Idle`), and the marker row is present. The backstop is armed as
/// a per-key singleton (`clear_and_schedule`) and is **never** point-cleared
/// — the sweep self-clears once the key goes quiet. So exactly one
/// `ClearAndSchedule` and *zero* `Unschedule`s of `StateRecovery` are
/// recorded (finding F2: the boundary cannot disturb a backstop).
#[tokio::test]
async fn commit_seals_flushes_applies_and_arms_singleton_backstop() -> Result<()> {
    let durable = FailingSealDurable::never_fails();
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let marker = Uuid::from_u128(0xA11CE);

    let context = MockEventContext::new()
        .with_timer_tracking()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler = BoundaryHandler::set_ok(7, marker);

    dispatch_message(&handler, context.clone(), 0, &key).await?;

    let id = cart_id(&key)?;
    assert_eq!(
        read_applied(&durable, &id).await?,
        Some(Value::from(7_i32)),
        "the committed value is applied to durable state"
    );
    assert!(
        store.exists(marker).await?,
        "the marker row is present after commit"
    );
    assert_eq!(
        clear_and_schedules(&context),
        1,
        "the seal armed exactly one singleton backstop via clear_and_schedule"
    );
    assert_eq!(
        unschedules(&context),
        0,
        "the boundary never point-clears a backstop"
    );
    Ok(())
}

/// Seal-before-marker regression pin: with the seal failing permanently, the
/// boundary must NOT flush the marker — the dedup row stays absent over the
/// unsealed WAL — yet it arms the backstop defensively and the handler ran
/// exactly once. With the pre-rework ordering (marker written before the
/// seal) the row would be present here.
#[tokio::test]
async fn seal_failure_writes_no_marker() -> Result<()> {
    let durable = FailingSealDurable::failing_seals(1, ErrorCategory::Permanent);
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let marker = Uuid::from_u128(0xBEEF);

    let context = MockEventContext::new()
        .with_timer_tracking()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler = BoundaryHandler::set_ok(9, marker);

    dispatch_message(&handler, context.clone(), 0, &key).await?;

    assert!(
        !store.exists(marker).await?,
        "no marker may exist while the WAL is unsealed"
    );
    assert_eq!(
        handler.observed.lock().len(),
        1,
        "the handler runs exactly once per dispatch — seal retries don't re-run it"
    );
    assert_eq!(
        clear_and_schedules(&context),
        1,
        "a permanent seal failure still arms the singleton backstop defensively"
    );
    assert_eq!(
        unschedules(&context),
        0,
        "the boundary never point-clears a backstop"
    );
    Ok(())
}

/// The literal finding-1 interleaving, transient variant: handler `Ok`, then
/// the seal fails transiently. Pre-rework, the dedup row was already written
/// before the seal, so the retry re-dispatch dedup-short-circuited and the
/// handler's writes were silently lost. Now `retry_step` retries the seal in
/// place — the handler runs exactly once, the marker flushes only after the
/// seal eventually succeeds, and the value lands durably. Paused tokio time
/// auto-advances the boundary's retry sleeps.
#[tokio::test(start_paused = true)]
async fn transient_seal_failure_retries_in_place_without_rerunning_handler() -> Result<()> {
    let durable = FailingSealDurable::failing_seals(2, ErrorCategory::Transient);
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let marker = Uuid::from_u128(0xF1);

    let context = MockEventContext::new()
        .with_timer_tracking()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler = BoundaryHandler::set_ok(5, marker);

    dispatch_message(&handler, context.clone(), 0, &key).await?;

    assert_eq!(
        handler.observed.lock().len(),
        1,
        "transient seal retries never re-run the handler"
    );
    assert!(
        store.exists(marker).await?,
        "the marker flushes once the seal eventually succeeds"
    );
    let id = cart_id(&key)?;
    assert_eq!(
        read_applied(&durable, &id).await?,
        Some(Value::from(5_i32)),
        "the handler's writes survive the transient seal failures"
    );
    assert_eq!(
        clear_and_schedules(&context),
        1,
        "the eventual seal arms one singleton backstop"
    );
    assert_eq!(
        unschedules(&context),
        0,
        "the boundary never point-clears a backstop"
    );
    Ok(())
}

/// A permanent handler error flushes the registered marker (so the
/// failed-but-final message deduplicates on redelivery), commits, and seals
/// nothing — no backstop is armed.
#[tokio::test]
async fn permanent_error_flushes_marker_without_sealing() -> Result<()> {
    let durable = FailingSealDurable::never_fails();
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let marker = Uuid::from_u128(0xDEAD);

    let context = MockEventContext::new()
        .with_timer_tracking()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler = BoundaryHandler::set_err(3, marker, ErrorCategory::Permanent);

    dispatch_message(&handler, context.clone(), 0, &key).await?;

    assert!(
        store.exists(marker).await?,
        "a permanent error flushes the registered marker"
    );
    assert_eq!(schedules(&context), 0, "a non-Ok outcome seals nothing");
    let id = cart_id(&key)?;
    assert_eq!(
        read_applied(&durable, &id).await?,
        None,
        "the failed dispatch's write never reaches durable state"
    );
    Ok(())
}

/// F2 cross-event regression — the interleaving the single-event harness
/// could not see. Two events on the **same key** share one timer state:
///
/// * Event A writes `CART`; its post-commit apply fails, so `CART` stays
///   `Sealed` (pending) with a `StateRecovery` backstop armed.
/// * Event B writes a *different* collection (`WISHLIST`) and resolves cleanly
///   in the same fire-time second.
///
/// Pre-fix, B's `Resolved` point-clear `unschedule`d the shared backstop slot,
/// stripping the only backstop covering A's still-pending seal. The singleton
/// `clear_and_schedule` design never unschedules, so after B settles the key
/// **still has** a `StateRecovery` timer. The `unschedules == 0` assertion is
/// the deterministic teeth (it fails pre-fix the moment B resolves, regardless
/// of the wall-clock second); the non-empty net-schedule assertion states the
/// consequence the plan calls for.
#[tokio::test]
async fn cross_event_resolved_commit_keeps_a_pending_backstop() -> Result<()> {
    let durable = FailingSealDurable::failing_applies(1);
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    // One shared timer-op log across both events: cloning the base context
    // shares the underlying `Arc`, so A's and B's ops accumulate together.
    let base = MockEventContext::<Value>::new().with_timer_tracking();

    // Event A on CART: apply fails (the one armed failure), CART stays Sealed.
    let ctx_a = base
        .clone()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler_a = BoundaryHandler::set_ok(1, Uuid::from_u128(0xA)).on_collection(CART);
    dispatch_message(&handler_a, ctx_a, 0, &key).await?;

    // Event B on WISHLIST: resolves cleanly (the failure budget is spent).
    let ctx_b = base
        .clone()
        .with_session(session(durable.clone(), store.clone(), &key)?);
    let handler_b = BoundaryHandler::set_ok(2, Uuid::from_u128(0xB)).on_collection(WISHLIST);
    dispatch_message(&handler_b, ctx_b, 1, &key).await?;

    assert!(
        matches!(
            DurableWalStore::read_partition(&durable, &collection_id(&key, "cart")?).await?,
            DurableState::Sealed { .. }
        ),
        "A's apply failed, so CART is still a pending seal after B settles"
    );
    assert_eq!(
        unschedules(&base),
        0,
        "the boundary never point-clears, so B cannot strip A's backstop"
    );
    assert!(
        !net_scheduled(&base, TimerType::StateRecovery).is_empty(),
        "a StateRecovery backstop still covers CART's pending seal after B"
    );
    Ok(())
}

/// Property (multi-event-per-key, shared timer state): for any sequence of
/// committing events on one key — each targeting `CART` or `WISHLIST` and
/// either resolving cleanly or failing its apply — the boundary issues **zero**
/// `Unschedule`s, and whenever the key has any pending (unresolved) seal a
/// `StateRecovery` timer is scheduled. This is the plan's invariant "a key with
/// any pending entry has a scheduled `StateRecovery` timer", exercised over
/// real interleavings the old point-clear could corrupt.
#[test]
fn prop_pending_key_always_has_a_backstop() {
    fn property(steps: Vec<(bool, bool)>) -> TestResult {
        if steps.is_empty() {
            return TestResult::discard();
        }
        let input = format!("{steps:?}");
        match TEST_RUNTIME.block_on(run_backstop_sequence(steps)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("backstop invariant violated for {input}")),
            Err(e) => TestResult::error(format!("{input}: {e}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<(bool, bool)>) -> TestResult);
}

/// Drives `steps` (`(use_wishlist, fail_apply)` per event) on one key against a
/// shared durable and a shared timer-op log, checking the F2 invariant after
/// every settle. Returns `Ok(false)` on the first violation.
async fn run_backstop_sequence(steps: Vec<(bool, bool)>) -> Result<bool> {
    let durable = FailingSealDurable::never_fails();
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let base = MockEventContext::<Value>::new().with_timer_tracking();

    for (idx, (use_wishlist, fail_apply)) in steps.into_iter().enumerate() {
        durable.arm_apply_failure(fail_apply);
        let descriptor = if use_wishlist { WISHLIST } else { CART };
        let marker = Uuid::from_u128(idx as u128 + 1);
        let handler = BoundaryHandler::set_ok(idx as u8, marker).on_collection(descriptor);
        let context = base
            .clone()
            .with_session(session(durable.clone(), store.clone(), &key)?);

        dispatch_message(&handler, context, idx as Offset, &key).await?;

        // Invariant 1: the boundary never point-clears a backstop.
        if unschedules(&base) != 0 {
            return Ok(false);
        }
        // Invariant 2: any pending (Sealed) collection ⇒ a backstop is armed.
        let pending = is_sealed(&durable, &key, "cart").await?
            || is_sealed(&durable, &key, "wishlist").await?;
        if pending && net_scheduled(&base, TimerType::StateRecovery).is_empty() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Replays the recorded timer ops (all for one key, since each test uses a
/// single key) into the net set of scheduled times for `timer_type`.
fn net_scheduled<S>(
    context: &MockEventContext<Value, S>,
    timer_type: TimerType,
) -> BTreeSet<CompactDateTime> {
    let mut set = BTreeSet::new();
    for op in context.timer_operations() {
        match op {
            TimerOperation::Schedule(time, t) if t == timer_type => {
                set.insert(time);
            }
            TimerOperation::ClearAndSchedule(time, t) if t == timer_type => {
                set.clear();
                set.insert(time);
            }
            TimerOperation::Unschedule(time, t) if t == timer_type => {
                set.remove(&time);
            }
            TimerOperation::ClearScheduled(t) if t == timer_type => set.clear(),
            _ => {}
        }
    }
    set
}

async fn is_sealed(durable: &FailingSealDurable, key: &Key, name: &str) -> Result<bool> {
    Ok(matches!(
        DurableWalStore::read_partition(durable, &collection_id(key, name)?).await?,
        DurableState::Sealed { .. }
    ))
}

/// Property: a sequence of committing messages on one key reads its own
/// committed writes, lands each value durably (WAL always resolved to
/// `Idle`), and records each message's marker.
#[test]
fn prop_committed_messages_read_their_own_writes() {
    fn property(bytes: Vec<u8>) -> TestResult {
        if bytes.is_empty() {
            return TestResult::discard();
        }
        let input = format!("{bytes:?}");
        match TEST_RUNTIME.block_on(run_commit_sequence(bytes)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("model divergence for {input}")),
            Err(e) => TestResult::error(format!("{input}: {e}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>) -> TestResult);
}

async fn run_commit_sequence(bytes: Vec<u8>) -> Result<bool> {
    let durable = FailingSealDurable::never_fails();
    let store = MemoryDeduplicationStore::new();
    let key: Key = Arc::from("k");
    let id = cart_id(&key)?;

    let mut model: Option<Value> = None;
    for (idx, byte) in bytes.into_iter().enumerate() {
        let marker = Uuid::from_u128(idx as u128 + 1);
        let handler = BoundaryHandler::set_ok(byte, marker);
        let observed = handler.observed.clone();
        let context = MockEventContext::new()
            .with_timer_tracking()
            .with_session(session(durable.clone(), store.clone(), &key)?);

        dispatch_message(&handler, context, idx as Offset, &key).await?;

        // The handler observed the prior committed value.
        if observed.lock().as_slice() != [model.clone()] {
            return Ok(false);
        }
        model = Some(Value::from(byte));
        // The value landed and the WAL resolved to Idle.
        if read_applied(&durable, &id).await? != model {
            return Ok(false);
        }
        // The marker for this message is present.
        if !store.exists(marker).await? {
            return Ok(false);
        }
    }
    Ok(true)
}
