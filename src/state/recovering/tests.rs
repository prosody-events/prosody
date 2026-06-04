//! Tests for [`super::RecoveringValueStore`].
//!
//! The directed tests cover the recovery contract on `get` (Idle vs
//! Sealed, `Committed` vs `NotCommitted`, error propagation, pass-through
//! invariants). The property tests reuse the shared
//! [`crate::state::value_test_suite`] runners against
//! `RecoveringValueStore<MemoryDurableValueStore, MockOracle>`.

use super::{CollectionTtl, CommitOracle, RecoveringValueStore, RecoveringValueStoreError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::manager::sweep_pending;
use crate::state::memory::{MemoryDirtyValueStore, MemoryDurableValueStore, MemoryStateError};
use crate::state::pending::{PendingEntry, PendingIndexScanner, PendingIndexStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::value::{DirectApplyStore, DurableWalStore, ValueOp, ValueStore};
use crate::state::value_test_suite::{
    self, DirectTrace, OraclePolicy, TEST_TTL, Trace, bytes, collection_ref, finish_trace,
};
use crate::state::{
    CollectionId, CollectionKind, CollectionRef, CommitDecision, DurableState, EventRef, Read,
    SealedCollection, StateKey, StoreOutcome, ValueKind,
};
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use futures::{Stream, executor};
use parking_lot::Mutex;
use quickcheck::{QuickCheck, TestResult};
use std::collections::HashMap;
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
    /// Records every resolved event and returns the carried decision, so a
    /// test can assert which events were consulted while still steering the
    /// recover-before-seal outcome (commit vs roll back).
    Recording(Arc<Mutex<Vec<EventRef>>>, CommitDecision),
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

    /// Records consulted events and resolves each as `Committed`.
    fn recording() -> (Self, Arc<Mutex<Vec<EventRef>>>) {
        Self::recording_with(CommitDecision::Committed)
    }

    /// Records consulted events and resolves each as `NotCommitted`, so a
    /// rollback test can still assert the resolve path ran.
    fn recording_not_committed() -> (Self, Arc<Mutex<Vec<EventRef>>>) {
        Self::recording_with(CommitDecision::NotCommitted)
    }

    fn recording_with(decision: CommitDecision) -> (Self, Arc<Mutex<Vec<EventRef>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                policy: MockPolicy::Recording(Arc::clone(&log), decision),
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
            MockPolicy::Recording(log, decision) => {
                log.lock().push(event);
                Ok(*decision)
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

// ---- ScriptedOracle: per-event commit decisions ----------------------------

/// Oracle that returns a per-event decision from a script, defaulting to
/// `NotCommitted` for unscripted events. Drives an arbitrary
/// committed/not-committed pattern across a sequence of events for the
/// seal-chain equivalence property.
#[derive(Clone, Default)]
struct ScriptedOracle {
    decisions: Arc<Mutex<HashMap<Uuid, CommitDecision>>>,
}

impl ScriptedOracle {
    fn set_decision(&self, event: EventRef, committed: bool) {
        if let EventRef::Message { dedup_id } = event {
            let decision = if committed {
                CommitDecision::Committed
            } else {
                CommitDecision::NotCommitted
            };
            self.decisions.lock().insert(dedup_id, decision);
        }
    }
}

impl CommitOracle for ScriptedOracle {
    type Error = MockOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let decision = match event {
            EventRef::Message { dedup_id } => self
                .decisions
                .lock()
                .get(&dedup_id)
                .copied()
                .unwrap_or(CommitDecision::NotCommitted),
            EventRef::Timer(_) => CommitDecision::NotCommitted,
        };
        Ok(decision)
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

/// Reads `id` and returns the `Sealed` state's `(applied, wal event)`,
/// erroring if the partition is `Idle`. Callers keep their own
/// `assert_eq!`s — and their bespoke failure messages — on the returned
/// values; this only unwraps the expected `Sealed` shape.
async fn expect_sealed(
    inner: &MemoryDurableValueStore,
    id: &CollectionId<ValueKind>,
) -> Result<(Option<Bytes>, EventRef)> {
    match inner.read_partition(id).await.map_err(into_eyre)? {
        DurableState::Sealed { applied, wal } => Ok((applied, wal.event())),
        DurableState::Idle { applied } => {
            Err(eyre::eyre!("expected Sealed, got Idle{{{applied:?}}}"))
        }
    }
}

/// Registry whose `default_ttl` covers unregistered collections and whose
/// `"profile"` collection (the name `collection_ref()` uses) carries an
/// `override_ttl` distinct from that default. The recovery-write TTL tests
/// assert the per-collection override is bound, not the registry-wide
/// default, so the two values must differ.
fn profile_registry(
    default_ttl: CompactDuration,
    override_ttl: CompactDuration,
) -> Result<Arc<CollectionDefRegistry>> {
    let mut registry = CollectionDefRegistry::new(Some(default_ttl));
    let profile: ValueDescriptor = value_state("profile");
    registry.register(&profile, CollectionDef::new(Some(override_ttl)))?;
    Ok(Arc::new(registry))
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
    let payload = bytes(7);
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
        .seal(&collection, event, vec![ValueOp::Set { payload: bytes(9) }])
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
    let prior = bytes(3);
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
        .seal(&collection, event, vec![ValueOp::Set { payload: bytes(5) }])
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
            vec![ValueOp::Set { payload: bytes(1) }],
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
    let payload = bytes(8);
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

    store.set(&id, bytes(1)).await.map_err(into_eyre)?;
    assert_eq!(
        store.get(&id).await.map_err(into_eyre)?,
        Read::Present(bytes(1))
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
            vec![ValueOp::Set { payload: bytes(2) }],
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
            vec![ValueOp::Set { payload: bytes(3) }],
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
/// timer-sweep recovery (`sweep_pending`) reads from
/// `registry.ttl_for(name)`, so the two recovery paths can no longer bind
/// divergent TTLs for the same collection.
#[tokio::test]
async fn recovery_writes_use_registry_per_collection_ttl() -> Result<()> {
    let override_ttl = CompactDuration::new(7_200);
    let default_ttl = CompactDuration::new(60);
    let registry = profile_registry(default_ttl, override_ttl)?;

    let (recording, captures) = TtlRecordingDurable::new();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    recording
        .seal(
            &collection,
            event(20),
            vec![ValueOp::Set { payload: bytes(5) }],
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
/// ([`sweep_pending`]) are driven over the *same* `Sealed`
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
        vec![ValueOp::Set { payload: bytes(0) }]
    } else {
        seed.iter()
            .map(|b| {
                if b % 5 == 0 {
                    ValueOp::Clear
                } else {
                    ValueOp::Set { payload: bytes(*b) }
                }
            })
            .collect()
    };
    let event = event(0x7202);

    // A per-collection TTL override distinct from the registry-wide default,
    // so a divergence between the two entry points' TTL binding would show.
    let override_ttl = CompactDuration::new(4_242);
    let registry = profile_registry(CompactDuration::new(60), override_ttl)?;
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
    store_a.set(&id, bytes(7)).await.map_err(into_eyre)?;
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
    store_b.set(&id, bytes(7)).await.map_err(into_eyre)?;
    store_b
        .seal(&collection, event, ops.clone())
        .await
        .map_err(into_eyre)?;
    sweep_pending(
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
            vec![ValueOp::Set { payload: bytes(4) }],
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

// ---- recover-before-seal directed tests ------------------------------------
//
// These target the keystone fix: a WAL-mode `seal` must resolve a prior
// crashed-but-sealed WAL on the same partition *before* overwriting it, or
// that prior event's commit decision is lost permanently.

/// Committed prior. A prior event sealed and crashed before resolution
/// (`Sealed{E1}`); the oracle says E1 committed. Sealing a *new* event E2
/// must apply E1 first — folding its ops into authoritative state — before
/// E2's WAL overwrites the partition. Asserts the oracle resolved E1 exactly
/// once and that E1's committed payload survives under E2's WAL.
#[tokio::test]
async fn seal_recovers_committed_prior_before_overwrite() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let e1 = event(1);
    let e2 = event(2);
    let p1 = bytes(11);

    // Crash post-seal / pre-resolve on E1.
    inner
        .seal(
            &collection,
            e1,
            vec![ValueOp::Set {
                payload: p1.clone(),
            }],
        )
        .await
        .map_err(into_eyre)?;

    let (oracle, log) = MockOracle::recording();
    let store = RecoveringValueStore::with_default_ttl(inner.clone(), oracle, TEST_TTL_DURATION);
    store
        .seal(&collection, e2, vec![ValueOp::Set { payload: bytes(22) }])
        .await
        .map_err(into_eyre)?;

    assert_eq!(
        *log.lock(),
        vec![e1],
        "the prior event E1 must be resolved exactly once before E2 seals"
    );
    let (applied, wal_event) = expect_sealed(&inner, &id).await?;
    assert_eq!(
        applied,
        Some(p1),
        "E1's committed payload must be folded into applied — not lost"
    );
    assert_eq!(wal_event, e2, "the new seal must own the WAL");
    Ok(())
}

/// Not-committed prior. The oracle says the prior crashed E1 did not commit;
/// sealing E2 must roll E1 back — leaving the pre-seal applied untouched —
/// before E2's WAL lands. The end state (`applied == prior`) is reachable by
/// a naive overwrite that never resolves E1, since rolling back a
/// not-committed event is a no-op on `applied`; the recording oracle asserts
/// the resolve path actually ran (E1 consulted exactly once), so this test
/// discriminates the recover-before-seal fix rather than its absence.
#[tokio::test]
async fn seal_rolls_back_not_committed_prior_before_overwrite() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let prior = bytes(3);
    inner
        .direct_apply(
            &collection,
            vec![ValueOp::Set {
                payload: prior.clone(),
            }],
        )
        .await
        .map_err(into_eyre)?;
    let e1 = event(1);
    let e2 = event(2);
    inner
        .seal(&collection, e1, vec![ValueOp::Set { payload: bytes(99) }])
        .await
        .map_err(into_eyre)?;

    let (oracle, log) = MockOracle::recording_not_committed();
    let store = RecoveringValueStore::with_default_ttl(inner.clone(), oracle, TEST_TTL_DURATION);
    store
        .seal(&collection, e2, vec![ValueOp::Set { payload: bytes(44) }])
        .await
        .map_err(into_eyre)?;

    assert_eq!(
        *log.lock(),
        vec![e1],
        "the prior event E1 must be resolved exactly once before E2 seals"
    );
    let (applied, wal_event) = expect_sealed(&inner, &id).await?;
    assert_eq!(
        applied,
        Some(prior),
        "rolled-back E1 must leave the pre-seal applied untouched"
    );
    assert_eq!(wal_event, e2);
    Ok(())
}

/// Same-event guard. Re-sealing the *same* event (redelivery of our own WAL)
/// must skip recovery entirely — the oracle is consulted zero times — and the
/// partition stays sealed for that event.
#[tokio::test]
async fn seal_same_event_redelivery_skips_oracle() -> Result<()> {
    let inner = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let id = collection.id().clone();
    let e1 = event(1);
    inner
        .seal(&collection, e1, vec![ValueOp::Set { payload: bytes(7) }])
        .await
        .map_err(into_eyre)?;

    let (oracle, log) = MockOracle::recording();
    let store = RecoveringValueStore::with_default_ttl(inner.clone(), oracle, TEST_TTL_DURATION);
    store
        .seal(&collection, e1, vec![ValueOp::Set { payload: bytes(8) }])
        .await
        .map_err(into_eyre)?;

    assert!(
        log.lock().is_empty(),
        "a same-event reseal must not consult the oracle"
    );
    let (_, wal_event) = expect_sealed(&inner, &id).await?;
    assert_eq!(wal_event, e1, "the partition must stay sealed for E1");
    Ok(())
}

/// Crash-recovery equivalence for the seal-chain. Sealing a sequence of
/// events `E0..En` through the wrapper resolves each prior event before
/// overwriting its WAL (recover-before-seal), and the final `get` resolves
/// `En` (read-before-use). The final visible state must equal a reference
/// model that folds every *committed* event's ops in order — so no committed
/// event's decision is lost across the chain, and no not-committed event
/// bleeds through. Every recovery write must also bind the shared resolver's
/// per-collection TTL. The sweep entry point's equivalence to `get` is
/// covered separately by [`prop_recovery_entry_points_equivalent`]. Iteration
/// count comes from `QUICKCHECK_TESTS`.
#[test]
fn prop_seal_chain_recovers_every_committed_event() {
    fn property(decisions: Vec<bool>) -> TestResult {
        match executor::block_on(run_seal_chain_equivalence(decisions)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(e) => TestResult::error(format!("seal-chain equivalence errored: {e}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<bool>) -> TestResult);
}

/// Drives a seal-chain with per-event decisions through the recovery wrapper
/// and reports whether the final visible state matches the reference fold and
/// every recovery write bound the resolver's per-collection TTL.
async fn run_seal_chain_equivalence(decisions: Vec<bool>) -> Result<bool> {
    let override_ttl = CompactDuration::new(4_242);
    let registry = profile_registry(CompactDuration::new(60), override_ttl)?;

    let collection = collection_ref()?;
    let id = collection.id().clone();

    let oracle = ScriptedOracle::default();
    let (store, caps) = TtlRecordingDurable::new();
    let wrapper = RecoveringValueStore::new(store, oracle.clone(), registry);

    let mut reference: Option<Bytes> = None;
    for (i, &committed) in decisions.iter().enumerate() {
        let ev = event(i as u128);
        oracle.set_decision(ev, committed);
        let payload = bytes(i as u8);
        if committed {
            reference = Some(payload.clone());
        }
        wrapper
            .seal(&collection, ev, vec![ValueOp::Set { payload }])
            .await
            .map_err(into_eyre)?;
    }
    // Final read-before-use resolves the last sealed event.
    let final_state = wrapper.get(&id).await.map_err(into_eyre)?;
    let expected = reference.map_or(Read::Absent, Read::Present);

    let apply_ttl_ok = caps
        .last_apply()
        .is_none_or(|c| c.ttl == Some(override_ttl));
    let rollback_ttl_ok = caps
        .last_rollback()
        .is_none_or(|c| c.ttl == Some(override_ttl));

    Ok(final_state == expected && apply_ttl_ok && rollback_ttl_ok)
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
    fn property(trace: Trace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(value_test_suite::run_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ));
        finish_trace(result, "model mismatch", &input_dbg)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> TestResult);
}

#[test]
fn prop_recovering_memory_idempotence_trace() {
    fn property(trace: Trace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(value_test_suite::run_idempotence_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ));
        finish_trace(result, "idempotence violated", &input_dbg)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> TestResult);
}

#[test]
fn prop_recovering_memory_direct_trace() {
    fn property(trace: DirectTrace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(value_test_suite::run_direct_trace(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
        ));
        finish_trace(result, "direct-mode invariant violated", &input_dbg)
    }
    QuickCheck::new().quickcheck(property as fn(DirectTrace) -> TestResult);
}

#[test]
fn prop_recovering_memory_crash_committed() {
    fn property(trace: Trace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(value_test_suite::run_trace_with_policy(
            recovering_memory(MockOracle::always_committed()),
            MemoryDirtyValueStore::new,
            trace,
            OraclePolicy::AlwaysCommitted,
        ));
        finish_trace(result, "model mismatch", &input_dbg)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> TestResult);
}

#[test]
fn prop_recovering_memory_crash_not_committed() {
    fn property(trace: Trace) -> TestResult {
        let input_dbg = format!("{trace:#?}");
        let result = executor::block_on(value_test_suite::run_trace_with_policy(
            recovering_memory(MockOracle::always_not_committed()),
            MemoryDirtyValueStore::new,
            trace,
            OraclePolicy::AlwaysNotCommitted,
        ));
        finish_trace(result, "model mismatch", &input_dbg)
    }
    QuickCheck::new().quickcheck(property as fn(Trace) -> TestResult);
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
    ) -> Result<Read<Bytes>, Self::Error> {
        self.inner.get(collection).await
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
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
// (`sweep_pending`) as well as the first-touch one, letting T2
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
    ) -> Result<Read<Bytes>, Self::Error> {
        self.inner
            .get(collection)
            .await
            .map_err(FailingDurableError::Memory)
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
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
