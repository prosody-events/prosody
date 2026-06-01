//! Directed tests for [`super::KeyedStateMiddleware`].
//!
//! These tests exercise the middleware's hook routing, the recovery
//! handler, the `CollectionDef` registry, and the apply hook lifecycle.
//! All tests are broker-free; they construct mock contexts, stub
//! oracles, and operate against `MemoryDurableValueStore`.

#![allow(clippy::wildcard_imports, clippy::match_wildcard_for_single_variants)]

use super::handler::recover_pending_entries;
use super::*;
use crate::Key;
use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::test_support::{MockEventContext, TimerOperation};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::DurableState;
use crate::state::StoreOutcome;
use crate::state::memory::{
    MemoryDirtyValueStore, MemoryDirtyValueStoreFactory, MemoryDirtyValueStoreProvider,
    MemoryDurableValueStore,
};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::state::value::{
    DirectApplyStore, DurableWalStore, StoredPayload, ValueKind, ValueOp, ValueStore,
};
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, CommitMode, EventRef, StateKey, StateName,
    StateType,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::fmt::Debug;
use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use uuid::Uuid;

/// Tiny test-only oracle whose verdict is fixed at construction.
#[derive(Clone, Debug)]
struct FixedOracle {
    decision: CommitDecision,
}

impl FixedOracle {
    fn committed() -> Self {
        Self {
            decision: CommitDecision::Committed,
        }
    }

    fn not_committed() -> Self {
        Self {
            decision: CommitDecision::NotCommitted,
        }
    }
}

#[derive(Debug, Error)]
#[error("fixed oracle error")]
struct FixedOracleError;

impl ClassifyError for FixedOracleError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

impl CommitOracle for FixedOracle {
    type Error = FixedOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(self.decision)
    }
}

/// Oracle that counts `resolve` calls so a test can assert it was never
/// consulted (the stale-pending sweep arm must not touch the oracle).
#[derive(Clone)]
struct CountingOracle {
    decision: CommitDecision,
    calls: Arc<AtomicUsize>,
}

impl CountingOracle {
    fn new(decision: CommitDecision) -> Self {
        Self {
            decision,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl CommitOracle for CountingOracle {
    type Error = FixedOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.decision)
    }
}

fn make_state_key() -> StateKey {
    StateKey::new(Uuid::new_v4(), Arc::from("test-key"))
}

fn make_collection_id(state_key: &StateKey, name: &str) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

fn inline_bytes(b: u8) -> StoredPayload {
    StoredPayload::Inline(Bytes::from(vec![b]))
}

fn registry() -> CollectionDefRegistry {
    CollectionDefRegistry::new(Some(CompactDuration::new(3_600)))
}

fn build_context<C, D>(
    inner: C,
    durable: D,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
) -> KeyedStateContext<C, D, MemoryDirtyValueStore>
where
    C: EventContext + Clone + Send + Sync + 'static,
    D: ValueStore<Error = <D as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <D as DurableWalStore<ValueKind>>::Error>
        + Debug
        + Clone
        + Send
        + Sync
        + 'static,
{
    KeyedStateContext::new(
        inner,
        durable,
        MemoryDirtyValueStore::new(),
        registry,
        state_key,
        event,
    )
}

/// Registry that pins `name` to `mode`. Used by tests that exercise a
/// non-default commit mode without rebuilding the full Builder.
fn registry_with_mode(name: &str, mode: CommitMode) -> Result<CollectionDefRegistry> {
    let mut r = CollectionDefRegistry::new(Some(CompactDuration::new(3_600)));
    let def = CollectionDef::new(Some(CompactDuration::new(3_600))).with_commit_mode(mode);
    r.insert(StateName::try_new(name)?, def);
    Ok(r)
}

/// `ctx.value(name).set(...)` should accumulate dirty ops and
/// `seal_all` should write them to the durable WAL.
#[tokio::test]
async fn value_set_then_seal_persists_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let registry = Arc::new(registry());
    let state_key = make_state_key();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let ctx = build_context(
        MockEventContext::new(),
        durable.clone(),
        registry,
        state_key.clone(),
        event,
    );

    let handle = ctx.value("counter")?;
    handle.set(inline_bytes(7)).await?;

    let sealed = ctx.resolve_per_collection().await?;
    assert_eq!(sealed.len(), 1, "exactly one collection sealed");

    let id = make_collection_id(&state_key, "counter")?;
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Sealed { wal, .. } => {
            assert_eq!(wal.event(), event);
            Ok(())
        }
        other => Err(eyre!("expected Sealed, got {other:?}")),
    }
}

/// `flush()` on a `Dirty` transaction drains the dirty ops directly to
/// durable applied state and returns the transaction to `Clean`.
#[tokio::test]
async fn flush_drains_dirty_and_returns_clean() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let registry = Arc::new(registry());
    let state_key = make_state_key();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(42),
    };
    let ctx = build_context(
        MockEventContext::new(),
        durable.clone(),
        registry,
        state_key.clone(),
        event,
    );

    let handle = ctx.value("counter")?;
    handle.set(inline_bytes(13)).await?;
    let outcome = handle.flush().await?;
    assert_eq!(
        outcome,
        StoreOutcome::Applied,
        "flush of Dirty must report Applied"
    );

    let id = make_collection_id(&state_key, "counter")?;
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => {
            assert_eq!(applied, Some(inline_bytes(13)));
        }
        other => return Err(eyre!("expected Idle after flush, got {other:?}")),
    }

    // Second flush is a no-op on Clean.
    let outcome = handle.flush().await?;
    assert_eq!(outcome, StoreOutcome::NoOp);
    Ok(())
}

/// Two `value(name)` calls return handles that share the same
/// transaction so a set on one is visible to the other.
#[tokio::test]
async fn repeat_value_call_returns_same_transaction() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let registry = Arc::new(registry());
    let state_key = make_state_key();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let ctx = build_context(
        MockEventContext::new(),
        durable.clone(),
        registry,
        state_key.clone(),
        event,
    );

    let first = ctx.value("counter")?;
    first.set(inline_bytes(5)).await?;

    let second = ctx.value("counter")?;
    assert_eq!(second.get().await?, Some(inline_bytes(5)));
    Ok(())
}

/// Direct-mode collections direct-apply via `resolve_per_collection`
/// without producing a sealed WAL.
#[tokio::test]
async fn direct_apply_all_skips_seal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let registry = Arc::new(registry_with_mode("counter", CommitMode::Direct)?);
    let state_key = make_state_key();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(3),
    };
    let ctx = build_context(
        MockEventContext::new(),
        durable.clone(),
        registry,
        state_key.clone(),
        event,
    );

    let handle = ctx.value("counter")?;
    handle.set(inline_bytes(9)).await?;
    let sealed = ctx.resolve_per_collection().await?;
    assert!(
        sealed.is_empty(),
        "direct-mode must not surface sealed entries"
    );

    let id = make_collection_id(&state_key, "counter")?;
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => {
            assert_eq!(applied, Some(inline_bytes(9)));
            Ok(())
        }
        other => Err(eyre!("expected Idle, got {other:?}")),
    }
}

/// The collection-def registry returns the per-collection TTL when
/// registered, otherwise the default.
#[test]
fn registry_lookup_falls_back_to_default() -> Result<()> {
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(7_200)));
    let bounded = StateName::try_new("bounded")?;
    let unbounded = StateName::try_new("unbounded")?;
    registry.insert(
        bounded.clone(),
        CollectionDef::new(Some(CompactDuration::new(60))),
    );
    registry.insert(unbounded.clone(), CollectionDef::new(None));

    assert_eq!(registry.ttl_for(&bounded), Some(CompactDuration::new(60)));
    assert_eq!(registry.ttl_for(&unbounded), None);
    let missing = StateName::try_new("missing")?;
    assert_eq!(
        registry.ttl_for(&missing),
        Some(CompactDuration::new(7_200)),
        "unregistered collection uses default ttl"
    );
    Ok(())
}

/// State recovery resolves a sealed partition via the oracle.
#[tokio::test]
async fn state_recovery_applies_sealed_when_oracle_says_committed() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let id = make_collection_id(&state_key, "snapshot")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(42),
    };

    // Seal a WAL out-of-band; the recovery handler should apply it.
    durable
        .seal(
            &collection_ref,
            event,
            vec![ValueOp::Set {
                payload: inline_bytes(11),
            }],
        )
        .await?;

    let context = MockEventContext::new().with_timer_tracking();
    let registry = registry();
    recover_pending_entries(
        &context,
        &durable,
        &durable,
        &FixedOracle::committed(),
        &registry,
        state_key.clone(),
    )
    .await
    .map_err(|e| eyre!("recovery failed: {e}"))?;

    let applied = match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => applied,
        other => return Err(eyre!("expected Idle, got {other:?}")),
    };
    assert_eq!(applied, Some(inline_bytes(11)));
    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery))),
        "recovery should clear the StateRecovery timer when done"
    );
    Ok(())
}

/// F4: the stale-pending sweep arm. A crash between `insert_pending` and the
/// WAL write leaves a pending row over an *Idle* partition. Recovery must
/// delete that row, leave the partition Idle, clear the timer, and **never
/// consult the oracle** (there is no sealed event to resolve). Before the
/// real memory pending index (F1), the index was derived from
/// `entry.wal.is_some()`, so this crash state was unrepresentable and this
/// branch was dead in every memory-backed test.
#[tokio::test]
async fn state_recovery_deletes_stale_pending_over_idle_partition() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let id = make_collection_id(&state_key, "snapshot")?;

    // Seed pending without ever sealing a WAL: the partition is Idle.
    PendingIndexStore::insert_pending::<ValueKind>(&durable, &id).await?;
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Idle { .. }
    ));
    let pending_before: Vec<_> = durable.scan_pending(&state_key).collect().await;
    assert_eq!(pending_before.len(), 1, "the stale pending row is present");

    let context = MockEventContext::new().with_timer_tracking();
    let oracle = CountingOracle::new(CommitDecision::Committed);
    recover_pending_entries(
        &context,
        &durable,
        &durable,
        &oracle,
        &registry(),
        state_key.clone(),
    )
    .await
    .map_err(|e| eyre!("recovery failed: {e}"))?;

    // The stale row is gone, the partition is still Idle, the oracle was
    // never consulted, and the recovery timer was cleared.
    let pending_after: Vec<_> = durable.scan_pending(&state_key).collect().await;
    assert!(
        pending_after.is_empty(),
        "stale pending row must be deleted"
    );
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Idle { .. }
    ));
    assert_eq!(
        oracle.call_count(),
        0,
        "oracle must not be consulted for a stale pending row"
    );
    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery))),
        "recovery should clear the StateRecovery timer when done"
    );
    Ok(())
}

/// State recovery rolls a sealed partition back when oracle says not
/// committed.
#[tokio::test]
async fn state_recovery_rolls_back_when_oracle_says_not_committed() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let id = make_collection_id(&state_key, "snapshot")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(43),
    };

    durable
        .seal(
            &collection_ref,
            event,
            vec![ValueOp::Set {
                payload: inline_bytes(99),
            }],
        )
        .await?;

    let context = MockEventContext::new();
    let registry = registry();
    recover_pending_entries(
        &context,
        &durable,
        &durable,
        &FixedOracle::not_committed(),
        &registry,
        state_key.clone(),
    )
    .await
    .map_err(|e| eyre!("recovery failed: {e}"))?;

    let applied = match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => applied,
        other => return Err(eyre!("expected Idle, got {other:?}")),
    };
    assert_eq!(applied, None, "rollback restored pre-seal state");
    Ok(())
}

/// State recovery skips entries whose kind the middleware does not yet
/// implement. (Memory scanner never emits non-Value entries, so this
/// case is exercised at compile time on the dispatch; we verify the
/// no-Value flow as a sanity check.)
#[tokio::test]
async fn state_recovery_with_empty_partition_clears_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let context = MockEventContext::new().with_timer_tracking();
    let registry = registry();

    recover_pending_entries(
        &context,
        &durable,
        &durable,
        &FixedOracle::committed(),
        &registry,
        state_key.clone(),
    )
    .await
    .map_err(|e| eyre!("recovery failed: {e}"))?;

    // No collections sealed; recovery is a no-op aside from clearing
    // the timer.
    assert_eq!(
        context.count_scheduled(TimerType::StateRecovery),
        0,
        "no schedule fires; only clear"
    );
    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery))),
        "recovery must clear timer even on empty partitions"
    );
    Ok(())
}

/// `KeyedStateAccessError::Name` is a permanent classification.
#[test]
fn keyed_state_access_error_classifies_permanent() -> Result<()> {
    let err = match StateName::try_new("") {
        Ok(_) => return Err(eyre!("empty state name should fail")),
        Err(e) => KeyedStateAccessError::Name(e),
    };
    assert!(
        matches!(err.classify_error(), ErrorCategory::Permanent),
        "permanent classification"
    );
    Ok(())
}

/// Builder validation: `build()` fails fast when required fields are
/// missing.
#[test]
fn builder_rejects_missing_fields() -> Result<()> {
    type ProbeOutput = ();
    let builder: KeyedStateMiddlewareBuilder<
        MemoryDurableValueStore,
        MemoryDurableValueStore,
        FixedOracle,
        MemoryDirtyValueStoreFactory,
        ProbeOutput,
    > = KeyedStateMiddleware::builder();
    let err = builder
        .build()
        .err()
        .ok_or_else(|| eyre!("expected missing field"))?;
    assert!(matches!(
        err,
        KeyedStateMiddlewareBuildError::Missing("durable")
    ));
    Ok(())
}

// --- KeyedStateHandler<FallibleHandler> directed tests ---

use crate::consumer::DemandType;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::timers::Trigger;

/// Tiny user handler that always returns an error so handler-level
/// tests can exercise the `Inner` error propagation path.
#[derive(Clone)]
struct RecordingHandler;

#[derive(Debug, Error)]
#[error("recording handler failed")]
struct RecordingHandlerError;

impl ClassifyError for RecordingHandlerError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

impl FallibleHandler for RecordingHandler {
    type Error = RecordingHandlerError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        _msg: ConsumerMessage<Self::Payload>,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let _ = ctx; // suppress unused warning when handler skips value
        Err(RecordingHandlerError)
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

/// No-op handler used to drive `KeyedStateHandler::on_message` in WAL
/// and Direct mode. The handler does not touch any collection, so no
/// seals fire — which is exactly the property we want to assert for
/// the "no seals → no schedule" cases.
#[derive(Clone)]
struct ValueWritingHandler<H> {
    _h: PhantomData<fn() -> H>,
}

impl<H> ValueWritingHandler<H> {
    fn new() -> Self {
        Self { _h: PhantomData }
    }
}

/// The middleware injects a `KeyedStateContext<...>` as the context the
/// handler sees. To exercise it in tests, the handler downcasts via the
/// `KeyedStateAccess` extension trait.
impl<H: Send + Sync + 'static> FallibleHandler for ValueWritingHandler<H> {
    type Error = RecordingHandlerError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        _msg: ConsumerMessage<Self::Payload>,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        let _ = ctx; // The blanket FallibleHandler bound erases keyed-state access.
        // Real handlers add `+ KeyedStateAccess` to their bound to recover it.
        // The non-recovery message arm of the test exercises the handler-shape
        // contract; the seal-recording branch is exercised by manually
        // invoking the wrapped context.
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

/// Build a `KeyedStateHandler` over a `ValueWritingHandler` so we can
/// drive `on_message` / `on_timer` directly in tests.
fn build_handler(
    durable: MemoryDurableValueStore,
    oracle: FixedOracle,
    commit_mode: CommitMode,
) -> KeyedStateHandler<
    ValueWritingHandler<MemoryDurableValueStore>,
    MemoryDurableValueStore,
    MemoryDurableValueStore,
    FixedOracle,
    MemoryDirtyValueStoreProvider,
> {
    let registry = registry().with_default_commit_mode(commit_mode);
    KeyedStateHandler {
        inner: ValueWritingHandler::new(),
        durable: durable.clone(),
        scanner: durable,
        oracle,
        provider: Ok(MemoryDirtyValueStoreProvider),
        consumer_group: Arc::from("test-group"),
        version: Arc::from("1"),
        registry: Arc::new(registry),
        segment_id: Uuid::new_v4(),
        recovery_delay: CompactDuration::new(30),
    }
}

fn test_message() -> Result<ConsumerMessage<serde_json::Value>> {
    ConsumerMessage::for_testing(
        crate::Topic::from("t"),
        0,
        0,
        Arc::from("k"),
        serde_json::Value::Null,
    )
    .map_err(|e| eyre!("for_testing: {e}"))
}

/// T1 (reader side): the keyed-state handler derives a message's dedup id
/// through the canonical [`dedup_uuid_for_message`] the deduplication
/// *writer* uses, threading its configured `version` and `consumer_group`.
/// A regression to the previous hardcoded `version = ""` / `event_id = None`
/// form would diverge from the writer, making the recovery oracle read
/// `NotCommitted` for committed messages and roll their state back. Covered
/// for payloads with and without an `event_id` so both `dedup_uuid` hash
/// branches are exercised.
#[tokio::test]
async fn derive_dedup_id_matches_canonical_writer_derivation() -> Result<()> {
    use crate::consumer::middleware::deduplication::dedup_uuid_for_message;

    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable, FixedOracle::committed(), CommitMode::Wal);

    for payload in [serde_json::json!({ "id": "evt-1" }), serde_json::json!({})] {
        let msg =
            ConsumerMessage::for_testing(crate::Topic::from("t"), 0, 7, Arc::from("k"), payload)
                .map_err(|e| eyre!("for_testing: {e}"))?;
        let derived = handler.derive_dedup_id_for_message(&msg);
        let expected = dedup_uuid_for_message("1", "test-group", "t", 0, &msg);
        assert_eq!(
            derived, expected,
            "handler derivation must match the canonical writer derivation"
        );
    }
    Ok(())
}

/// In WAL mode, a `StateRecovery` timer fire returns
/// `KeyedStateOutput::Recovery` (no error, no inner dispatch).
#[tokio::test]
async fn on_timer_state_recovery_returns_recovery_variant() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();
    let key: Key = Arc::from("user-1");
    let trigger = Trigger::for_testing(key, CompactDateTime::from(1_u32), TimerType::StateRecovery);

    let result = handler
        .on_timer(context.clone(), trigger, DemandType::Normal)
        .await;
    let output = result.map_err(|e| eyre!("recovery on_timer should succeed: {e:#}"))?;
    assert!(matches!(output, KeyedStateOutput::Recovery));
    // The recovery handler also clears any StateRecovery timer at the end.
    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery)))
    );
    Ok(())
}

/// `after_commit(Ok(Recovery))` and `after_abort(Ok(Recovery))` suppress
/// both apply hooks on the inner.
#[tokio::test]
async fn after_commit_and_after_abort_suppress_on_recovery_output() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();

    // No panic, no inner mutation.
    handler
        .after_commit(context.clone(), Ok(KeyedStateOutput::Recovery))
        .await;
    handler
        .after_abort(context, Ok(KeyedStateOutput::Recovery))
        .await;
    Ok(())
}

/// `after_commit` with an `Inner` payload carrying sealed collections
/// applies each one and clears the `StateRecovery` timer exactly once.
#[tokio::test]
async fn after_commit_with_sealed_list_clears_state_recovery_timer() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();

    let state_key = StateKey::new(handler.segment_id, Arc::from("u"));
    let id = make_collection_id(&state_key, "x")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(100),
    };
    // Seal a WAL out-of-band so apply_sealed has work to do.
    durable
        .seal(
            &collection_ref,
            event,
            vec![ValueOp::Set {
                payload: inline_bytes(7),
            }],
        )
        .await?;

    handler
        .after_commit(
            context.clone(),
            Ok(KeyedStateOutput::Inner {
                inner: (),
                sealed_event: Some(event),
                sealed_collections: vec![collection_ref],
            }),
        )
        .await;

    let cleared = context
        .timer_operations()
        .iter()
        .filter(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery)))
        .count();
    assert_eq!(
        cleared, 1,
        "exactly one clear_scheduled(StateRecovery) fires"
    );

    let applied = match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => applied,
        other => return Err(eyre!("expected Idle, got {other:?}")),
    };
    assert_eq!(applied, Some(inline_bytes(7)));
    Ok(())
}

/// C1: when an `apply_sealed` fails, `after_commit` must leave the one-shot
/// `StateRecovery` timer armed so the sweep retries — clearing it would tear
/// down the only backstop on the exact path where apply just failed, risking
/// a silently lost committed write once the sealed WAL row's TTL expires. The
/// failure is induced via an event mismatch: the WAL is sealed under one
/// event but the apply hook resolves a different one.
#[tokio::test]
async fn after_commit_apply_error_leaves_state_recovery_timer_armed() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();

    let state_key = StateKey::new(handler.segment_id, Arc::from("u"));
    let id = make_collection_id(&state_key, "x")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let sealed_event = EventRef::Message {
        dedup_id: Uuid::from_u128(200),
    };
    durable
        .seal(
            &collection_ref,
            sealed_event,
            vec![ValueOp::Set {
                payload: inline_bytes(7),
            }],
        )
        .await?;

    // Resolve a *different* event than the one sealed: apply_sealed returns
    // EventMismatch, so the hook must not clear the timer.
    let mismatched_event = EventRef::Message {
        dedup_id: Uuid::from_u128(201),
    };
    handler
        .after_commit(
            context.clone(),
            Ok(KeyedStateOutput::Inner {
                inner: (),
                sealed_event: Some(mismatched_event),
                sealed_collections: vec![collection_ref],
            }),
        )
        .await;

    let cleared = context
        .timer_operations()
        .iter()
        .filter(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery)))
        .count();
    assert_eq!(
        cleared, 0,
        "apply failure must leave the recovery timer armed"
    );

    // The WAL is untouched: the partition stays Sealed for the sweep to retry.
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Sealed { .. }
    ));
    Ok(())
}

/// `after_abort` with a sealed list rolls back each collection and
/// clears the timer.
#[tokio::test]
async fn after_abort_with_sealed_list_rolls_back() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();

    let state_key = StateKey::new(handler.segment_id, Arc::from("u"));
    let id = make_collection_id(&state_key, "x")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(101),
    };
    durable
        .seal(
            &collection_ref,
            event,
            vec![ValueOp::Set {
                payload: inline_bytes(13),
            }],
        )
        .await?;

    handler
        .after_abort(
            context.clone(),
            Ok(KeyedStateOutput::Inner {
                inner: (),
                sealed_event: Some(event),
                sealed_collections: vec![collection_ref],
            }),
        )
        .await;

    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery)))
    );

    let applied = match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Idle { applied } => applied,
        other => return Err(eyre!("expected Idle, got {other:?}")),
    };
    assert_eq!(applied, None, "rollback restored pre-seal state");
    Ok(())
}

/// `on_message` in Direct mode never schedules a `StateRecovery` timer.
#[tokio::test]
async fn on_message_direct_mode_never_schedules_state_recovery() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(
        durable.clone(),
        FixedOracle::committed(),
        CommitMode::Direct,
    );
    let context = MockEventContext::new().with_timer_tracking();
    let msg = test_message()?;
    let _result = handler
        .on_message(context.clone(), msg, DemandType::Normal)
        .await;

    let scheduled = context.count_scheduled(TimerType::StateRecovery);
    assert_eq!(scheduled, 0, "direct mode must not schedule recovery");
    Ok(())
}

/// `on_message` in WAL mode with zero seals also does not schedule.
#[tokio::test]
async fn on_message_wal_mode_with_no_seals_does_not_schedule() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let context = MockEventContext::new().with_timer_tracking();
    let msg = test_message()?;
    let _result = handler
        .on_message(context.clone(), msg, DemandType::Normal)
        .await;

    let scheduled = context.count_scheduled(TimerType::StateRecovery);
    assert_eq!(
        scheduled, 0,
        "WAL mode with no dirty collections must not schedule recovery"
    );
    Ok(())
}

/// Sanity: the recording handler proves we can drive `on_message` in WAL
/// mode and observe the inner error propagating through `Inner(_)`.
#[tokio::test]
async fn on_message_inner_error_propagates_as_inner_variant() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = KeyedStateHandler {
        inner: RecordingHandler,
        durable: durable.clone(),
        scanner: durable,
        oracle: FixedOracle::committed(),
        provider: Ok(MemoryDirtyValueStoreProvider),
        consumer_group: Arc::from("test-group"),
        version: Arc::from("1"),
        registry: Arc::new(registry()),
        segment_id: Uuid::new_v4(),
        recovery_delay: CompactDuration::new(30),
    };
    let context = MockEventContext::new().with_timer_tracking();
    let msg = test_message()?;

    let err = handler
        .on_message(context.clone(), msg, DemandType::Normal)
        .await
        .err()
        .ok_or_else(|| eyre!("recording handler must surface its error"))?;
    assert!(matches!(err, KeyedStateMiddlewareError::Inner(_)));
    // Inner errored → no seal call → no schedule.
    assert_eq!(context.count_scheduled(TimerType::StateRecovery), 0);
    Ok(())
}

// --- Property tests ---

/// One step of a middleware dispatch trace: a value op plus the event
/// outcome the apply hook receives.
#[derive(Clone, Debug)]
enum MiddlewareStep {
    Set(u8),
    Clear,
}

#[derive(Clone, Debug)]
struct MiddlewareEvent {
    step: MiddlewareStep,
    commit: bool,
}

#[derive(Clone, Debug)]
struct MiddlewareTrace(Vec<MiddlewareEvent>);

impl Arbitrary for MiddlewareStep {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Set(u8::arbitrary(g))
        } else {
            Self::Clear
        }
    }
}

impl Arbitrary for MiddlewareEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            step: MiddlewareStep::arbitrary(g),
            commit: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let commit = self.commit;
        Box::new(
            self.step
                .clone()
                .shrink_step()
                .map(move |step| Self { step, commit }),
        )
    }
}

impl MiddlewareStep {
    /// Minimal shrink: a `Set` shrinks toward `Clear` and toward smaller bytes.
    fn shrink_step(self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Clear => Box::new(iter::empty()),
            Self::Set(b) => Box::new(iter::once(Self::Clear).chain(b.shrink().map(Self::Set))),
        }
    }
}

impl Arbitrary for MiddlewareTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 12;
        let events = (0..len).map(|_| MiddlewareEvent::arbitrary(g)).collect();
        Self(events)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(MiddlewareTrace))
    }
}

/// T4 — middleware dispatch property. Drives a sequence of value ops through
/// the **actual** keyed-state machinery — a real [`KeyedStateContext`], its
/// `resolve_per_collection` seal, and the `KeyedStateHandler::after_commit` /
/// `after_abort` apply hooks (the path the C1 fix lives in) — and asserts the
/// durable visible state tracks a model after each event: a committed event
/// applies the WAL, an aborted event rolls it back. The previous version of
/// this test ran `value_test_suite::run_trace` against raw stores and never
/// constructed a context or handler, so the dispatch path had no coverage.
///
/// (A generic `FallibleHandler::on_message<C: EventContext>` cannot statically
/// recover `KeyedStateAccess`, so the op is performed on the context directly,
/// exactly as the directed seal tests do; the seal + apply-hook dispatch is
/// the middleware-specific logic under test.)
#[test]
fn prop_middleware_dispatch_matches_durable_model() {
    fn property(trace: MiddlewareTrace) -> bool {
        executor::block_on(run_middleware_dispatch(trace)).unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(MiddlewareTrace) -> bool);
}

async fn run_middleware_dispatch(trace: MiddlewareTrace) -> Result<bool> {
    let durable = MemoryDurableValueStore::for_tests();
    let handler = build_handler(durable.clone(), FixedOracle::committed(), CommitMode::Wal);
    let state_key = make_state_key();
    let id = make_collection_id(&state_key, "v")?;
    let registry = Arc::new(registry());

    let mut model: Option<StoredPayload> = None;

    for (idx, event) in trace.0.into_iter().enumerate() {
        let event_ref = EventRef::Message {
            dedup_id: Uuid::from_u128(idx as u128 + 1),
        };
        // Drive the real per-event context + seal.
        let ctx = build_context(
            MockEventContext::new(),
            durable.clone(),
            registry.clone(),
            state_key.clone(),
            event_ref,
        );
        let handle = ctx.value("v")?;
        let applied_if_committed = match &event.step {
            MiddlewareStep::Set(byte) => {
                handle.set(inline_bytes(*byte)).await?;
                Some(inline_bytes(*byte))
            }
            MiddlewareStep::Clear => {
                handle.clear().await?;
                None
            }
        };
        let sealed = ctx.resolve_per_collection().await?;

        // Route through the real apply hooks.
        let output = KeyedStateOutput::Inner {
            inner: (),
            sealed_event: Some(event_ref),
            sealed_collections: sealed,
        };
        let mock = MockEventContext::new();
        if event.commit {
            handler.after_commit(mock, Ok(output)).await;
            model = applied_if_committed;
        } else {
            handler.after_abort(mock, Ok(output)).await;
            // Rollback leaves pre-event applied state unchanged.
        }

        // The WAL must be resolved (Idle) and the applied state must match.
        match DurableWalStore::read_partition(&durable, &id).await? {
            DurableState::Idle { applied } if applied == model => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}
