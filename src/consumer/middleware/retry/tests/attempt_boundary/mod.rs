use super::*;
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::event_context::StateAccessError;
use crate::consumer::middleware::tests::test_support::{
    RecordingOracle, RecordingSession, committed_json_value, recording_session,
};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::state::descriptor::{CellStateError, Registered, ValueDescriptor, value_state};
use crate::state::memory::MemoryCellStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::{EventRef, StateKey, StoreOutcome};
use color_eyre::eyre::Result;
use parking_lot::Mutex;
use serde_json::{Value, json};
use uuid::Uuid;

/// What a keyed-state read observed inside an apply hook.
#[derive(Debug, Clone, PartialEq)]
enum ReadObs {
    /// The read succeeded, carrying the visible value.
    Value(Option<Value>),
    /// The read was fenced (`StateAccessError::Terminated`).
    Terminated,
    /// Any other error (fails the assertions that expect the two above).
    Other(String),
}

/// What a mid-handler `commit()` observed inside an apply hook.
#[derive(Debug, Clone, PartialEq)]
enum CommitObs {
    /// The commit ran, carrying its outcome.
    Outcome(StoreOutcome),
    /// The commit was fenced (`StateAccessError::Terminated`).
    Terminated,
    /// Any other error.
    Other(String),
}

fn classify_read(r: Result<Option<Value>, CellStateError<JsonCodecError>>) -> ReadObs {
    match r {
        Ok(value) => ReadObs::Value(value),
        Err(CellStateError::Access(StateAccessError::Terminated)) => ReadObs::Terminated,
        Err(other) => ReadObs::Other(format!("{other}")),
    }
}

fn classify_commit(r: Result<StoreOutcome, CellStateError<JsonCodecError>>) -> CommitObs {
    match r {
        Ok(outcome) => CommitObs::Outcome(outcome),
        Err(CellStateError::Access(StateAccessError::Terminated)) => CommitObs::Terminated,
        Err(other) => CommitObs::Other(format!("{other}")),
    }
}

/// The message's dedup id on the session `EventRef` — the one marker the
/// boundary may record, once, for the final attempt.
const MSG_DEDUP_ID: Uuid = Uuid::from_u128(0xA11);

fn cart() -> ValueDescriptor {
    value_state::<JsonCodec>("cart")
}

fn wishlist() -> ValueDescriptor {
    value_state::<JsonCodec>("wishlist")
}

/// Fails attempt 1 (`Normal`) after staging `cart`; succeeds attempt 2
/// (`Failure`) after staging `wishlist`. The two attempts touch
/// **disjoint** collections, so a leaked attempt-1 write would surface
/// as a committed `cart`.
#[derive(Clone)]
struct AttemptAwareHandler {
    calls: Arc<AtomicUsize>,
}

impl AttemptAwareHandler {
    async fn handle<C>(&self, context: C, demand_type: DemandType) -> Result<(), TestError>
    where
        C: EventContext<Payload = Value>,
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match demand_type {
            DemandType::Normal => {
                let handle = context
                    .state(Registered::new(cart()))
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                handle
                    .set(json!({ "attempt": 1_i32 }))
                    .await
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                Err(TestError(ErrorCategory::Transient))
            }
            DemandType::Failure => {
                let handle = context
                    .state(Registered::new(wishlist()))
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                handle
                    .set(json!({ "attempt": 2_i32 }))
                    .await
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                Ok(())
            }
        }
    }
}

impl FallibleHandler for AttemptAwareHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_excise<C>(
        &self,
        context: C,
        _message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context, demand_type).await
    }

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context, demand_type).await
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
        Ok(())
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for AttemptAwareHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// Retry over a real session: attempt 1 stages `cart` then fails
/// Transient; the `next_attempt` verb's `reset` discards it (and bumps the
/// epoch); attempt 2 stages `wishlist` and succeeds; `settle` then
/// certifies only attempt 2's work and records the event's marker
/// **exactly once** (a retried event yields one marker — the final
/// attempt's). The committed state must show `wishlist` present and
/// `cart` **absent**.
///
/// Removing the reset's discard fails this: attempt 1's `cart` write would
/// survive in the dirty overlay, `finalize` would stage it alongside
/// `wishlist`, and the read-back would find `cart` committed to attempt 1's
/// value — precisely the leak the reset exists to prevent.
#[tokio::test]
async fn retry_isolates_dirty_between_attempts_and_records_one_marker() -> Result<()> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    registry.register(&wishlist(), CollectionDef::new(None))?;
    let state_key = StateKey::new(Uuid::from_u128(0xE), Arc::from("user-1"));
    let (session, cell_store, _dirty, recorded) = recording_session(
        registry,
        state_key.clone(),
        EventRef::Message {
            dedup_id: MSG_DEDUP_ID,
        },
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = AttemptAwareHandler {
        calls: calls.clone(),
    };
    let retry_handler = create_retry_handler(handler, 10);
    let context = MockEventContext::new().with_session(session);

    let tracker = create_offset_tracker();
    let uncommitted_offset = tracker.take(0).await?;
    let message = create_test_message()?;
    let uncommitted_message = message.into_uncommitted(uncommitted_offset);

    EventHandler::on_message(
        &retry_handler,
        context,
        uncommitted_message,
        DemandType::Normal,
    )
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one failed then one ok attempt"
    );
    assert_eq!(
        committed_json_value(&cell_store, state_key.clone(), "wishlist").await?,
        Some(json!({ "attempt": 2_i32 })),
        "attempt 2's write must be committed",
    );
    assert_eq!(
        committed_json_value(&cell_store, state_key.clone(), "cart").await?,
        None,
        "attempt 1's discarded write must NOT leak into the committed state",
    );
    assert_eq!(
        recorded.lock().clone(),
        vec![MSG_DEDUP_ID],
        "a retried event records exactly one marker — the final attempt's",
    );
    assert_eq!(
        tracker.shutdown().await,
        Some(0),
        "the offset commits after the successful retry",
    );
    Ok(())
}

// --- the settle-stamp hook-view arms: the final stamp and the expired
// intermediate ---

/// Fails attempt 1 (`Normal`) Transient; on attempt 2 (`Failure`) stages
/// `wishlist = {attempt:2}` and succeeds. Its `after_commit` re-reads
/// `wishlist` through the (stamped) hook context and records the outcome.
#[derive(Clone)]
struct FinalHookReadHandler {
    read: Arc<Mutex<Option<ReadObs>>>,
}

impl FinalHookReadHandler {
    async fn handle<C>(&self, context: C, demand_type: DemandType) -> Result<(), TestError>
    where
        C: EventContext<Payload = Value>,
    {
        match demand_type {
            DemandType::Normal => Err(TestError(ErrorCategory::Transient)),
            DemandType::Failure => {
                let handle = context
                    .state(Registered::new(wishlist()))
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                handle
                    .set(json!({ "attempt": 2_i32 }))
                    .await
                    .map_err(|_| TestError(ErrorCategory::Terminal))?;
                Ok(())
            }
        }
    }
}

impl FallibleHandler for FinalHookReadHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_excise<C>(
        &self,
        context: C,
        _message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context, demand_type).await
    }

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context, demand_type).await
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
        Ok(())
    }

    async fn after_commit<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let obs = match context.state(Registered::new(wishlist())) {
            Ok(handle) => classify_read(handle.get().await),
            Err(e) => ReadObs::Other(format!("bind: {e}")),
        };
        *self.read.lock() = Some(obs);
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for FinalHookReadHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// Builds the recording-session fixture and mock context the hook-view
/// pins share; returns the pieces they assert on.
fn hook_fixture(
    handler_registry: impl FnOnce(&mut CollectionDefRegistry) -> Result<()>,
) -> Result<(
    MockEventContext<Value, RecordingSession>,
    MemoryCellStore<RecordingOracle>,
    StateKey,
)> {
    let mut registry = CollectionDefRegistry::default();
    handler_registry(&mut registry)?;
    let state_key = StateKey::new(Uuid::from_u128(0xF00), Arc::from("user-1"));
    let (session, cell_store, _dirty, _recorded) = recording_session(
        registry,
        state_key.clone(),
        EventRef::Message {
            dedup_id: MSG_DEDUP_ID,
        },
    );
    Ok((
        MockEventContext::new().with_session(session),
        cell_store,
        state_key,
    ))
}

/// Final hook reads settled state — simple: after one retry the final
/// `after_commit` reads attempt 2's `wishlist` — the post-settle read
/// contract survives a retry. (The final context is already current here,
/// so this arm is the green baseline; the stamp's load-bearing case is
/// the nested arm.)
#[tokio::test]
async fn final_hook_reads_settled_state_after_retry() -> Result<()> {
    let read = Arc::new(Mutex::new(None));
    let handler = FinalHookReadHandler { read: read.clone() };
    let retry_handler = create_retry_handler(handler, 10);
    let (context, cell_store, state_key) =
        hook_fixture(|r| Ok(r.register(&wishlist(), CollectionDef::new(None))?))?;

    let tracker = create_offset_tracker();
    let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
    EventHandler::on_message(&retry_handler, context, uncommitted, DemandType::Normal).await;

    assert_eq!(
        read.lock().clone(),
        Some(ReadObs::Value(Some(json!({ "attempt": 2_i32 })))),
        "the final after_commit reads attempt 2's committed wishlist",
    );
    assert_eq!(
        committed_json_value(&cell_store, state_key, "wishlist").await?,
        Some(json!({ "attempt": 2_i32 })),
        "wishlist committed",
    );
    let _ = tracker.shutdown().await;
    Ok(())
}

/// Final hook reads settled state — nested: an inner retry bumps the epoch
/// during the outer attempt, leaving the outer's final context pinned
/// stale; the settle
/// stamp re-pins it current so `after_commit`'s read still sees the
/// settled `wishlist`. Dropping the `redispatch` stamp in `fire_apply_hook`
/// makes this read `Terminated` (the stale pin no longer matches the
/// inner-bumped epoch).
#[tokio::test]
async fn final_hook_reads_settled_state_under_nested_retry() -> Result<()> {
    let read = Arc::new(Mutex::new(None));
    let handler = FinalHookReadHandler { read: read.clone() };
    // Inner retry (FallibleHandler) drives the attempts and bumps the
    // shared epoch; the outer retry (EventHandler) settles on its own
    // attempt-1 context, now stale relative to the inner's bumps.
    let nested = create_retry_handler(create_retry_handler(handler, 10), 10);
    let (context, cell_store, state_key) =
        hook_fixture(|r| Ok(r.register(&wishlist(), CollectionDef::new(None))?))?;

    let tracker = create_offset_tracker();
    let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
    EventHandler::on_message(&nested, context, uncommitted, DemandType::Normal).await;

    assert_eq!(
        read.lock().clone(),
        Some(ReadObs::Value(Some(json!({ "attempt": 2_i32 })))),
        "the stamp re-pins the final hook current, so its read sees the settled wishlist despite \
         the inner retry's epoch bump",
    );
    assert_eq!(
        committed_json_value(&cell_store, state_key, "wishlist").await?,
        Some(json!({ "attempt": 2_i32 })),
        "wishlist committed",
    );
    let _ = tracker.shutdown().await;
    Ok(())
}

/// Fails attempts until `succeed_on`, staging `cart = {attempt:n}` each
/// attempt; its intermediate `after_abort` (fired between attempts with the
/// EXPIRED pre-verb context) tries to read and `commit()` `cart`, recording
/// both observations.
mod intermediate_hooks;
