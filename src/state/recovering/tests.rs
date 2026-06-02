//! Tests for [`super::RecoveringValueStore`].
//!
//! The directed tests cover the recovery contract on `get` (Idle vs
//! Sealed, `Committed` vs `NotCommitted`, error propagation, pass-through
//! invariants). The property tests reuse the shared
//! [`crate::state::value_test_suite`] runners against
//! `RecoveringValueStore<MemoryDurableValueStore, MockOracle>`.

use super::{CollectionTtl, CommitOracle, RecoveringValueStore, RecoveringValueStoreError};
use crate::consumer::middleware::test_support::MockEventContext;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::memory::{MemoryDirtyValueStore, MemoryDurableValueStore, MemoryStateError};
use crate::state::middleware::{CollectionDef, CollectionDefRegistry, recover_pending_entries};
use crate::state::pending::{PendingEntry, PendingIndexScanner, PendingIndexStore};
use crate::state::value::{DirectApplyStore, DurableWalStore, StoredPayload, ValueOp, ValueStore};
use crate::state::value_test_suite::{
    self, DirectTrace, OraclePolicy, TEST_TTL, Trace, collection_ref, inline,
};
use crate::state::{
    CollectionId, CollectionKind, CollectionRef, CommitDecision, DurableState, EventRef, Read,
    SealedCollection, StateKey, StateName, StoreOutcome, ValueKind,
};
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{self, Result};
use futures::{Stream, executor};
use parking_lot::Mutex;
use quickcheck::{QuickCheck, TestResult};
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// One-hour `TEST_TTL` value reused as the constructor-supplied default
/// for the directed and property tests in this module.
const TEST_TTL_DURATION: Option<CompactDuration> = TEST_TTL;

// ---- MockOracle -------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct MockOracle {
    policy: MockPolicy,
}

#[derive(Clone, Debug, Default)]
enum MockPolicy {
    #[default]
    AlwaysCommitted,
    AlwaysNotCommitted,
    Failing,
    Recording(Arc<Mutex<Vec<EventRef>>>),
}

impl MockOracle {
    fn always_committed() -> Self {
        Self {
            policy: MockPolicy::AlwaysCommitted,
        }
    }

    fn always_not_committed() -> Self {
        Self {
            policy: MockPolicy::AlwaysNotCommitted,
        }
    }

    fn failing() -> Self {
        Self {
            policy: MockPolicy::Failing,
        }
    }

    fn recording() -> (Self, Arc<Mutex<Vec<EventRef>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                policy: MockPolicy::Recording(Arc::clone(&log)),
            },
            log,
        )
    }
}

impl CommitOracle for MockOracle {
    type Error = MockOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        match &self.policy {
            MockPolicy::AlwaysCommitted => Ok(CommitDecision::Committed),
            MockPolicy::AlwaysNotCommitted => Ok(CommitDecision::NotCommitted),
            MockPolicy::Failing => Err(MockOracleError::Injected),
            MockPolicy::Recording(log) => {
                log.lock().push(event);
                Ok(CommitDecision::Committed)
            }
        }
    }
}

#[derive(Debug, Error)]
enum MockOracleError {
    #[error("injected mock oracle failure")]
    Injected,
}

impl ClassifyError for MockOracleError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

// ---- helpers ---------------------------------------------------------------

fn event(id: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(id),
    }
}

/// A `RecoveringValueStore` over a fresh in-memory backing store with the
/// shared one-hour TTL, driven by `oracle`. Shared by the property runners,
/// each of which differs only in the oracle policy and the suite runner it
/// drives.
fn recovering_memory(
    oracle: MockOracle,
) -> RecoveringValueStore<MemoryDurableValueStore, MockOracle> {
    RecoveringValueStore::with_default_ttl(
        MemoryDurableValueStore::for_tests(),
        oracle,
        TEST_TTL_DURATION,
    )
}

fn into_eyre<E>(e: E) -> eyre::Report
where
    E: Error + Send + Sync + 'static,
{
    eyre::eyre!(e)
}

// ---- directed tests --------------------------------------------------------

#[tokio::test]
async fn get_on_idle_partition_does_not_call_oracle() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let (oracle, log) = MockOracle::recording();
    let store = RecoveringValueStore::with_default_ttl(inner, oracle, TEST_TTL_DURATION);
    let id = collection_ref()?.id().clone();

    assert_eq!(store.get(&id).await.map_err(into_eyre)?, Read::Absent);
    assert!(
        log.lock().is_empty(),
        "oracle should not be consulted for Idle"
    );
    Ok(())
}

#[tokio::test]
async fn get_on_sealed_partition_committed_applies_wal() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let event = event(1);
    let payload = inline(7);
    inner
        .seal(
            &collection,
            event,
            vec![ValueOp::Set {
                payload: payload.clone(),
            }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::with_default_ttl(
        inner.clone(),
        MockOracle::always_committed(),
        TEST_TTL_DURATION,
    );
    assert_eq!(
        store.get(&id).await.map_err(into_eyre)?,
        Read::Present(payload)
    );
    match inner.read_partition(&id).await.map_err(into_eyre)? {
        DurableState::Idle { .. } => Ok(()),
        other @ DurableState::Sealed { .. } => {
            Err(eyre::eyre!("expected Idle post-recovery, got {other:?}"))
        }
    }
}

#[tokio::test]
async fn get_on_sealed_partition_not_committed_rolls_back() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let event = event(2);
    inner
        .seal(
            &collection,
            event,
            vec![ValueOp::Set { payload: inline(9) }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::with_default_ttl(
        inner.clone(),
        MockOracle::always_not_committed(),
        TEST_TTL_DURATION,
    );
    assert_eq!(store.get(&id).await.map_err(into_eyre)?, Read::Absent);
    match inner.read_partition(&id).await.map_err(into_eyre)? {
        DurableState::Idle { applied: None } => Ok(()),
        DurableState::Idle {
            applied: Some(other),
        } => Err(eyre::eyre!(
            "expected Idle{{None}}, got Idle{{Some({other:?})}}"
        )),
        DurableState::Sealed { .. } => Err(eyre::eyre!("expected Idle, got Sealed")),
    }
}

#[tokio::test]
async fn get_on_sealed_partition_not_committed_preserves_prior_applied() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let prior = inline(3);
    inner
        .direct_apply(
            &collection,
            vec![ValueOp::Set {
                payload: prior.clone(),
            }],
        )
        .await
        .map_err(into_eyre)?;
    let event = event(4);
    inner
        .seal(
            &collection,
            event,
            vec![ValueOp::Set { payload: inline(5) }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::with_default_ttl(
        inner.clone(),
        MockOracle::always_not_committed(),
        TEST_TTL_DURATION,
    );
    assert_eq!(
        store.get(&id).await.map_err(into_eyre)?,
        Read::Present(prior)
    );
    Ok(())
}

#[tokio::test]
async fn get_idempotent_after_recovery() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    inner
        .seal(
            &collection,
            event(6),
            vec![ValueOp::Set { payload: inline(1) }],
        )
        .await
        .map_err(into_eyre)?;

    let (oracle, log) = MockOracle::recording();
    let store = RecoveringValueStore::with_default_ttl(inner, oracle, TEST_TTL_DURATION);
    let first = store.get(&id).await.map_err(into_eyre)?;
    let second = store.get(&id).await.map_err(into_eyre)?;
    assert_eq!(first, second);
    assert_eq!(
        log.lock().len(),
        1,
        "oracle should only be consulted once: subsequent gets see Idle"
    );
    Ok(())
}

#[tokio::test]
async fn oracle_error_propagates() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    inner
        .seal(&collection, event(7), vec![ValueOp::Clear])
        .await
        .map_err(into_eyre)?;

    let store =
        RecoveringValueStore::with_default_ttl(inner, MockOracle::failing(), TEST_TTL_DURATION);
    let err = store
        .get(&id)
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected oracle error to propagate"))?;
    assert!(matches!(err, RecoveringValueStoreError::Oracle(_)));
    Ok(())
}

#[tokio::test]
async fn read_partition_returns_sealed_unchanged() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let payload = inline(8);
    inner
        .seal(
            &collection,
            event(8),
            vec![ValueOp::Set {
                payload: payload.clone(),
            }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::with_default_ttl(
        inner.clone(),
        MockOracle::always_committed(),
        TEST_TTL_DURATION,
    );
    match DurableWalStore::read_partition(&store, &id)
        .await
        .map_err(into_eyre)?
    {
        DurableState::Sealed { .. } => Ok(()),
        other @ DurableState::Idle { .. } => Err(eyre::eyre!(
            "read_partition must pass through unchanged, got {other:?}"
        )),
    }
}

#[tokio::test]
async fn set_and_clear_pass_through() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let store = RecoveringValueStore::with_default_ttl(
        inner.clone(),
        MockOracle::always_committed(),
        TEST_TTL_DURATION,
    );
    let collection = collection_ref()?;
    let id = collection.id().clone();

    store.set(&id, inline(1)).await.map_err(into_eyre)?;
    assert_eq!(
        store.get(&id).await.map_err(into_eyre)?,
        Read::Present(inline(1))
    );

    store.clear(&id).await.map_err(into_eyre)?;
    assert_eq!(store.get(&id).await.map_err(into_eyre)?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn recovery_writes_use_default_ttl() -> Result<()> {
    let (recording, captures) = TtlRecordingDurable::new();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    recording
        .seal(
            &collection,
            event(9),
            vec![ValueOp::Set { payload: inline(2) }],
        )
        .await
        .map_err(into_eyre)?;

    let some_ttl = TEST_TTL_DURATION;
    let store_some = RecoveringValueStore::with_default_ttl(
        recording.clone(),
        MockOracle::always_committed(),
        some_ttl,
    );
    store_some.get(&id).await.map_err(into_eyre)?;
    assert_eq!(captures.last_apply(), Some(TtlCapture { ttl: some_ttl }));

    // Re-seal and recover under TTL=None to exercise the other arm.
    recording
        .seal(
            &collection,
            event(10),
            vec![ValueOp::Set { payload: inline(3) }],
        )
        .await
        .map_err(into_eyre)?;
    captures.reset();
    let store_none = RecoveringValueStore::with_default_ttl(
        recording.clone(),
        MockOracle::always_not_committed(),
        None,
    );
    store_none.get(&id).await.map_err(into_eyre)?;
    assert_eq!(captures.last_rollback(), Some(TtlCapture { ttl: None }));
    Ok(())
}

/// C2: when the resolver is the shared `Arc<CollectionDefRegistry>`, a
/// first-touch recovery write binds the collection's **per-collection** TTL
/// override — not the middleware-wide default. This is the same value the
/// timer-sweep recovery (`recover_pending_entries`) reads from
/// `registry.ttl_for(name)`, so the two recovery paths can no longer bind
/// divergent TTLs for the same collection.
#[tokio::test]
async fn recovery_writes_use_registry_per_collection_ttl() -> Result<()> {
    let override_ttl = CompactDuration::new(7_200);
    let default_ttl = CompactDuration::new(60);

    // `collection_ref()` names the collection "profile"; register an
    // override distinct from the registry-wide default.
    let mut registry = CollectionDefRegistry::new(Some(default_ttl));
    registry.insert(
        StateName::try_new("profile")?,
        CollectionDef::new(Some(override_ttl)),
    );
    let registry = Arc::new(registry);

    let (recording, captures) = TtlRecordingDurable::new();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    recording
        .seal(
            &collection,
            event(20),
            vec![ValueOp::Set { payload: inline(5) }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::new(
        recording.clone(),
        MockOracle::always_committed(),
        registry.clone(),
    );
    store.get(&id).await.map_err(into_eyre)?;

    // The recovery write bound the per-collection override, and it equals
    // what the resolver reports for this collection.
    let resolved = CollectionTtl::ttl_for(&registry, &id);
    assert_eq!(resolved, Some(override_ttl));
    assert_eq!(captures.last_apply(), Some(TtlCapture { ttl: resolved }));
    Ok(())
}

/// T2: the two recovery entry points must agree. First-touch
/// ([`RecoveringValueStore::get`]) and the timer-sweep
/// ([`recover_pending_entries`]) are driven over the *same* `Sealed`
/// partition (same pre-seal applied state, same WAL ops, same commit
/// decision, same per-collection TTL resolver). The property asserts both
/// produce **equivalent recovered visible state** and bind the **same TTL**
/// on the recovery write — the equivalence the C2 resolver guarantees and
/// that `value_test_suite` previously only example-tested by driving
/// `apply_sealed`/`rollback_sealed` directly, bypassing both real entry
/// points. Iteration count comes from `QUICKCHECK_TESTS`.
#[test]
fn prop_recovery_entry_points_equivalent() {
    fn property(seed: Vec<u8>, committed: bool) -> TestResult {
        match executor::block_on(run_entry_point_equivalence(seed, committed)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(e) => TestResult::error(format!("entry-point equivalence errored: {e}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>, bool) -> TestResult);
}

/// Seals one partition (twice, on two recording stores), recovers one via
/// first-touch and the other via the sweep, and reports whether the
/// recovered state and the bound TTL agree across the two entry points.
async fn run_entry_point_equivalence(seed: Vec<u8>, committed: bool) -> Result<bool> {
    // WAL must be non-empty; derive a deterministic op list from the seed.
    let ops: Vec<ValueOp> = if seed.is_empty() {
        vec![ValueOp::Set { payload: inline(0) }]
    } else {
        seed.iter()
            .map(|b| {
                if b % 5 == 0 {
                    ValueOp::Clear
                } else {
                    ValueOp::Set {
                        payload: inline(*b),
                    }
                }
            })
            .collect()
    };
    let event = event(0x7202);

    // A per-collection TTL override distinct from the registry-wide default,
    // so a divergence between the two entry points' TTL binding would show.
    let override_ttl = CompactDuration::new(4_242);
    let mut registry = CollectionDefRegistry::new(Some(CompactDuration::new(60)));
    registry.insert(
        StateName::try_new("profile")?,
        CollectionDef::new(Some(override_ttl)),
    );
    let registry = Arc::new(registry);
    let expected_ttl = Some(TtlCapture {
        ttl: Some(override_ttl),
    });

    let oracle = if committed {
        MockOracle::always_committed()
    } else {
        MockOracle::always_not_committed()
    };

    // `collection_ref()` names the collection "profile" (matching the
    // registry override). A pre-seal applied value lets the not-committed
    // (rollback) arm assert the prior state is preserved.
    let collection = collection_ref()?;
    let id = collection.id().clone();

    // First-touch entry point.
    let (store_a, caps_a) = TtlRecordingDurable::new();
    store_a.set(&id, inline(7)).await.map_err(into_eyre)?;
    store_a
        .seal(&collection, event, ops.clone())
        .await
        .map_err(into_eyre)?;
    let recovering = RecoveringValueStore::new(store_a, oracle.clone(), registry.clone());
    let state_a = recovering.get(&id).await.map_err(into_eyre)?;
    let ttl_a = if committed {
        caps_a.last_apply()
    } else {
        caps_a.last_rollback()
    };

    // Timer-sweep entry point over an identically-sealed partition.
    let (store_b, caps_b) = TtlRecordingDurable::new();
    store_b.set(&id, inline(7)).await.map_err(into_eyre)?;
    store_b
        .seal(&collection, event, ops.clone())
        .await
        .map_err(into_eyre)?;
    let context = MockEventContext::new().with_timer_tracking();
    recover_pending_entries(
        &context,
        &store_b,
        &store_b,
        &oracle,
        registry.as_ref(),
        id.state_key().clone(),
    )
    .await
    .map_err(|e| eyre::eyre!("sweep failed: {e}"))?;
    let state_b = ValueStore::get(&store_b, &id).await.map_err(into_eyre)?;
    let ttl_b = if committed {
        caps_b.last_apply()
    } else {
        caps_b.last_rollback()
    };

    Ok(state_a == state_b && ttl_a == ttl_b && ttl_a == expected_ttl)
}

#[tokio::test]
async fn inner_error_during_apply_propagates() -> Result<()> {
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let failing = FailingApplyDurable::new(MemoryDurableValueStore::for_tests());
    failing
        .inner
        .seal(
            &collection,
            event(11),
            vec![ValueOp::Set { payload: inline(4) }],
        )
        .await
        .map_err(into_eyre)?;

    let store = RecoveringValueStore::with_default_ttl(
        failing,
        MockOracle::always_committed(),
        TEST_TTL_DURATION,
    );
    let err = store
        .get(&id)
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected inner error to propagate"))?;
    assert!(matches!(err, RecoveringValueStoreError::Inner(_)));
    Ok(())
}

// ---- property runners ------------------------------------------------------
//
// **Coverage split.** First-touch recovery via `ValueStore::get` is verified
// by the directed tests above (`get_on_sealed_partition_committed_applies_wal`,
// `get_on_sealed_partition_not_committed_rolls_back`,
// `get_on_sealed_partition_not_committed_preserves_prior_applied`,
// `get_idempotent_after_recovery`, `oracle_error_propagates`,
// `inner_error_during_apply_propagates`). The property runners below verify
// `RecoveringValueStore`'s pass-through behavior under random
// trace/crash patterns: every non-`get` method delegates unchanged to the
// inner store, and the combinator does not perturb the
// [`value_test_suite`] model invariants (durable applied, pending ops,
// dirty/clean visibility). After a `TraceOp::Crash` the shared runner
// drives recovery explicitly via `durable.apply_sealed` /
// `rollback_sealed` (per `OraclePolicy`) — the Recovering wrapper sees
// these as plain pass-through calls. This split keeps the get-time
// recovery path and the pass-through path both covered without
// re-architecting the runner around two visibility models.

#[test]
fn prop_recovering_memory_trace() {
    fn property(trace: Trace) -> bool {
        executor::block_on(value_test_suite::run_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> bool);
}

#[test]
fn prop_recovering_memory_idempotence_trace() {
    fn property(trace: Trace) -> bool {
        executor::block_on(value_test_suite::run_idempotence_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> bool);
}

#[test]
fn prop_recovering_memory_direct_trace() {
    fn property(trace: DirectTrace) -> bool {
        executor::block_on(value_test_suite::run_direct_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(DirectTrace) -> bool);
}

#[test]
fn prop_recovering_memory_crash_committed() {
    fn property(trace: Trace) -> bool {
        executor::block_on(value_test_suite::run_trace_with_policy(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
            OraclePolicy::AlwaysCommitted,
        ))
        .unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> bool);
}

#[test]
fn prop_recovering_memory_crash_not_committed() {
    fn property(trace: Trace) -> bool {
        executor::block_on(value_test_suite::run_trace_with_policy(
            recovering_memory(MockOracle::always_not_committed()),
            MemoryDirtyValueStore::new,
            trace,
            OraclePolicy::AlwaysNotCommitted,
        ))
        .unwrap_or(false)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> bool);
}

// ---- TTL-recording test double ---------------------------------------------

#[derive(Clone, Debug)]
struct TtlRecordingDurable {
    inner: MemoryDurableValueStore,
    captures: TtlCaptureHandle,
}

#[derive(Clone, Debug, Default)]
struct TtlCaptureHandle {
    inner: Arc<Mutex<TtlCaptureInner>>,
}

/// Records the TTL bound onto recovery writes. A `None` entry means
/// "not yet captured for this op"; `Some(TtlCapture { ttl })` records
/// the value seen, where `ttl` itself may be `Some(_)` (with-TTL arm)
/// or `None` (no-TTL arm). The wrapping avoids `Option<Option<T>>`.
#[derive(Debug, Default)]
struct TtlCaptureInner {
    last_apply: Option<TtlCapture>,
    last_rollback: Option<TtlCapture>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TtlCapture {
    ttl: Option<CompactDuration>,
}

impl TtlRecordingDurable {
    fn new() -> (Self, TtlCaptureHandle) {
        let captures = TtlCaptureHandle::default();
        (
            Self {
                inner: MemoryDurableValueStore::for_tests(),
                captures: captures.clone(),
            },
            captures,
        )
    }
}

impl TtlCaptureHandle {
    fn last_apply(&self) -> Option<TtlCapture> {
        self.inner.lock().last_apply
    }

    fn last_rollback(&self) -> Option<TtlCapture> {
        self.inner.lock().last_rollback
    }

    fn reset(&self) {
        let mut inner = self.inner.lock();
        inner.last_apply = None;
        inner.last_rollback = None;
    }
}

impl ValueStore for TtlRecordingDurable {
    type Error = <MemoryDurableValueStore as ValueStore>::Error;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        self.inner.get(collection).await
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        self.inner.set(collection, payload).await
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.inner.clear(collection).await
    }
}

impl DurableWalStore<ValueKind> for TtlRecordingDurable {
    type Error = <MemoryDurableValueStore as DurableWalStore<ValueKind>>::Error;

    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        DurableWalStore::read_partition(&self.inner, collection).await
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
        self.inner.seal(collection, event, ops).await
    }

    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.captures.inner.lock().last_apply = Some(TtlCapture {
            ttl: collection.ttl(),
        });
        self.inner.apply_sealed(collection, expected_event).await
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.captures.inner.lock().last_rollback = Some(TtlCapture {
            ttl: collection.ttl(),
        });
        self.inner.rollback_sealed(collection, expected_event).await
    }
}

impl DirectApplyStore<ValueKind> for TtlRecordingDurable {
    type Error = <MemoryDurableValueStore as DirectApplyStore<ValueKind>>::Error;

    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        self.inner.direct_apply(collection, ops).await
    }
}

// Delegated so `TtlRecordingDurable` can drive the timer-sweep entry point
// (`recover_pending_entries`) as well as the first-touch one, letting T2
// compare both against the same recording store.
impl PendingIndexStore for TtlRecordingDurable {
    type Error = <MemoryDurableValueStore as DurableWalStore<ValueKind>>::Error;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner.insert_pending(id).await
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.inner.delete_pending(id).await
    }
}

impl PendingIndexScanner for TtlRecordingDurable {
    type Error = <MemoryDurableValueStore as DurableWalStore<ValueKind>>::Error;

    fn scan_pending(
        &self,
        state_key: &StateKey,
    ) -> impl Stream<Item = Result<PendingEntry, Self::Error>> + Send {
        self.inner.scan_pending(state_key)
    }
}

// ---- FailingApplyDurable: returns Err from apply_sealed --------------------

#[derive(Clone, Debug)]
struct FailingApplyDurable {
    inner: MemoryDurableValueStore,
}

impl FailingApplyDurable {
    fn new(inner: MemoryDurableValueStore) -> Self {
        Self { inner }
    }
}

impl ValueStore for FailingApplyDurable {
    type Error = FailingDurableError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        self.inner
            .get(collection)
            .await
            .map_err(FailingDurableError::Memory)
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        self.inner
            .set(collection, payload)
            .await
            .map_err(FailingDurableError::Memory)
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.inner
            .clear(collection)
            .await
            .map_err(FailingDurableError::Memory)
    }
}

impl DurableWalStore<ValueKind> for FailingApplyDurable {
    type Error = FailingDurableError;

    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        DurableWalStore::read_partition(&self.inner, collection)
            .await
            .map_err(FailingDurableError::Memory)
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
        self.inner
            .seal(collection, event, ops)
            .await
            .map_err(FailingDurableError::Memory)
    }

    async fn apply_sealed<'a>(
        &'a self,
        _collection: &'a CollectionRef<ValueKind>,
        _expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        Err(FailingDurableError::InjectedApply)
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.inner
            .rollback_sealed(collection, expected_event)
            .await
            .map_err(FailingDurableError::Memory)
    }
}

impl DirectApplyStore<ValueKind> for FailingApplyDurable {
    type Error = FailingDurableError;

    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        self.inner
            .direct_apply(collection, ops)
            .await
            .map_err(FailingDurableError::Memory)
    }
}

#[derive(Debug, Error)]
enum FailingDurableError {
    #[error("injected apply_sealed failure")]
    InjectedApply,
    #[error(transparent)]
    Memory(#[from] MemoryStateError),
}

impl ClassifyError for FailingDurableError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
