//! The success-path marker record is **must-succeed**: `settle` retries a
//! failed record of ANY category — Transient, Terminal, and Permanent alike —
//! until the marker lands. The marker is framework bookkeeping, never a data
//! rejection: skipping a Permanent failure would commit the offset with the
//! stage uncertified, and the armed sweep would then silently roll a
//! successful handler's writes back with no redelivery to replay them. The
//! marker itself is the session's boundary-readable event identity
//! (`message_marker()`), so these pins also prove the identity sources: a
//! message session records its `EventRef` dedup id; a pure timer session
//! records nothing.
use super::*;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CommitDecision, EventRef, PartitionBackend, StateKey, StateName, StateType,
    TimerEventRef,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use quickcheck::{QuickCheck, TestResult};
use serde_json::json;
use std::future::ready;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::watch;
use uuid::Uuid;

/// Marker-store failure with a configured classification.
#[derive(Debug, Error)]
#[error("mock marker store failed ({0:?})")]
struct MockMarkerError(ErrorCategory);

impl ClassifyError for MockMarkerError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// Oracle whose `record_message` fails a configured number of times with a
/// configured category before succeeding, logging every recorded id;
/// `resolve` always answers Committed.
#[derive(Clone)]
struct FlakyMarkerOracle {
    remaining: Arc<AtomicUsize>,
    category: ErrorCategory,
    recorded: Arc<Mutex<Vec<Uuid>>>,
}

impl FlakyMarkerOracle {
    fn new(fail_count: usize, category: ErrorCategory) -> Self {
        Self {
            remaining: Arc::new(AtomicUsize::new(fail_count)),
            category,
            recorded: Arc::default(),
        }
    }

    fn recorded(&self) -> Vec<Uuid> {
        self.recorded.lock().clone()
    }
}

impl CommitOracle for FlakyMarkerOracle {
    type Error = MockMarkerError;

    fn record_message(&self, dedup_id: Uuid) -> impl Future<Output = Result<(), Self::Error>> {
        // While the countdown is positive, decrement it and inject one
        // more failure; once exhausted, record the marker.
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return ready(Err(MockMarkerError(self.category)));
        }
        self.recorded.lock().push(dedup_id);
        ready(Ok(()))
    }

    fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> impl Future<Output = Result<CommitDecision, Self::Error>> {
        ready(Ok(CommitDecision::Committed))
    }
}

type FlakyBackend = PartitionBackend<
    FlakyMarkerOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<FlakyMarkerOracle>,
>;
type FlakySession = KeyedStateSession<FlakyBackend, MemoryLoader<serde_json::Value>>;

/// The fixed message dedup id the sessions below carry on their
/// `EventRef` — the identity the boundary reads and records.
const DEDUP_ID: Uuid = Uuid::from_u128(0xFEE1);

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// A real session for `event` whose marker record routes through
/// `oracle`, plus the shared durable cell store and the `cart`
/// collection id for post-settle inspection.
fn flaky_session(
    oracle: FlakyMarkerOracle,
    event: EventRef,
) -> Result<(
    FlakySession,
    MemoryCellStore<FlakyMarkerOracle>,
    CollectionId,
)> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let cell_store = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let state_key = StateKey::new(Uuid::from_u128(0xD), Arc::from("user-1"));
    let session = KeyedStateSession::new(SessionParts {
        cell: cell_store.clone(),
        dirty: Arc::new(DirtyStore::new()),
        oracle,
        loader: MemoryLoader::new(),
        registry,
        state_key: state_key.clone(),
        event,
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    });
    let cart_id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("cart")?,
    );
    Ok((session, cell_store, cart_id))
}

/// Asserts the `cart` cell has no durable residue — neither a
/// provisional cell nor a committed value — via raw probes that no
/// resolving read can heal.
async fn assert_no_durable_cart(
    cell_store: &MemoryCellStore<FlakyMarkerOracle>,
    cart_id: &CollectionId,
) -> Result<()> {
    let provisional = cell_store.provisional_cells(cart_id);
    futures::pin_mut!(provisional);
    assert!(
        provisional.next().await.transpose()?.is_none(),
        "no provisional cell may exist",
    );
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    let cell = CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    };
    assert_eq!(
        Committed::into_inner(cell_store.get(cart_id, &cell, probe).await?),
        None,
        "no committed value may exist",
    );
    Ok(())
}

/// The error-path marker gate, Permanent direction: a **final** Permanent
/// error records the session's message marker best-effort — with NO stage
/// (the buffered write must leave no durable residue) — so the
/// failed-but-final message deduplicates instead of re-running its
/// failure on every redelivery.
#[tokio::test]
async fn err_permanent_records_the_marker_with_no_stage() -> Result<()> {
    let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
    let (session, cell_store, cart_id) =
        flaky_session(oracle.clone(), EventRef::Message { dedup_id: DEDUP_ID })?;
    let context = MockEventContext::new().with_session(session);
    // Stage a dirty write so "no stage" is a real claim, not vacuous.
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;

    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(
        &handler,
        context,
        guard,
        Err(TestError(ErrorCategory::Permanent, "final")),
    )
    .await;

    assert_eq!(
        oracle.recorded(),
        vec![DEDUP_ID],
        "a final Permanent error must record the message marker so the failure deduplicates",
    );
    assert_no_durable_cart(&cell_store, &cart_id).await?;
    assert_eq!(committed.load(Ordering::SeqCst), 1, "final errors commit");
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    Ok(())
}

/// The error-path marker gate, Transient direction: the record is gated
/// to Permanent. A Transient final (no retry layer below took it) is not
/// handled, so its marker must not certify anything.
#[tokio::test]
async fn err_transient_never_records_the_marker() -> Result<()> {
    let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
    let (session, cell_store, cart_id) =
        flaky_session(oracle.clone(), EventRef::Message { dedup_id: DEDUP_ID })?;
    let context = MockEventContext::new().with_session(session);
    let handle = context
        .state(Registered::new(cart()))
        .map_err(|e| eyre!("bind cart: {e}"))?;
    handle.set(json!({ "x": 1_i32 })).await?;

    let handler = ProbeHandler::ok(0);
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(
        &handler,
        context,
        guard,
        Err(TestError(ErrorCategory::Transient, "final")),
    )
    .await;

    assert!(
        oracle.recorded().is_empty(),
        "a Transient final error must NOT record a marker over never-staged state",
    );
    assert_no_durable_cart(&cell_store, &cart_id).await?;
    assert_eq!(committed.load(Ordering::SeqCst), 1, "final errors commit");
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    Ok(())
}

/// A pure timer never records a message marker on any path:
/// `message_marker()` is `None` on a timer session with no reload
/// override, so the Ok, Permanent, and Transient finals all settle
/// marker-free (the trigger commit is the timer's dedup).
#[tokio::test]
async fn pure_timer_never_records_a_message_marker() -> Result<()> {
    let finals: [Result<u64, TestError>; 3] = [
        Ok(0),
        Err(TestError(ErrorCategory::Permanent, "final")),
        Err(TestError(ErrorCategory::Transient, "final")),
    ];
    for result in finals {
        let oracle = FlakyMarkerOracle::new(0, ErrorCategory::Transient);
        let timer = EventRef::Timer(TimerEventRef::new(
            TimerType::Application,
            CompactDateTime::from(1000_u32),
            0,
        ));
        let (session, _cell_store, _cart_id) = flaky_session(oracle.clone(), timer)?;
        let context = MockEventContext::new().with_session(session);
        let handler = ProbeHandler::ok(0);
        let (guard, committed, aborted) = RecordingGuard::new();

        settle(&handler, context, guard, result).await;

        assert!(
            oracle.recorded().is_empty(),
            "a pure timer must never record a message marker",
        );
        assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

/// However many leading marker-record failures of whatever category the
/// oracle throws, `settle`'s success path self-heals: the offset commits
/// exactly once, the marker is recorded exactly once (the stage is
/// certified), and the staged cell is promoted — never left provisional
/// for the sweep to roll back. Each iteration runs on its own paused
/// runtime so the retry backoff advances instantly.
#[test]
fn prop_marker_record_self_heals_to_certified_commit() {
    fn property(fail_count: u8, category_sel: u8) -> TestResult {
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
            let oracle = FlakyMarkerOracle::new(fail_count, category);
            let event = EventRef::Message { dedup_id: DEDUP_ID };
            let (session, cell_store, cart_id) = match flaky_session(oracle.clone(), event) {
                Ok(parts) => parts,
                Err(e) => return TestResult::error(format!("setup: {e}")),
            };
            let context = MockEventContext::new().with_session(session);
            let Ok(handle) = context.state(Registered::new(cart())) else {
                return TestResult::error("bind failed");
            };
            if let Err(e) = handle.set(json!({ "x": 1_i32 })).await {
                return TestResult::error(format!("set: {e}"));
            }

            let handler = ProbeHandler::ok(0);
            let (guard, committed, aborted) = RecordingGuard::new();

            settle(&handler, context, guard, Ok(0)).await;

            let committed = committed.load(Ordering::SeqCst);
            let aborted = aborted.load(Ordering::SeqCst);
            let recorded = oracle.recorded();
            let provisional = cell_store.provisional_cells(&cart_id);
            futures::pin_mut!(provisional);
            let still_provisional = matches!(provisional.next().await, Some(Ok(_)));
            let probe = EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX),
            };
            let value = match cell_store
                .get(
                    &cart_id,
                    &CellKey {
                        section: Section::new(0),
                        coordinate: Coordinate::empty(),
                    },
                    probe,
                )
                .await
            {
                Ok(committed) => Committed::into_inner(committed),
                Err(e) => return TestResult::error(format!("read back: {e}")),
            };

            if committed != 1
                || aborted != 0
                || recorded != vec![DEDUP_ID]
                || still_provisional
                || value.is_none()
            {
                return TestResult::error(format!(
                    "category={category:?} fail_count={fail_count}: committed={committed} \
                     aborted={aborted} recorded={recorded:?} provisional={still_provisional} \
                     promoted={}",
                    value.is_some()
                ));
            }
            TestResult::passed()
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8) -> TestResult);
}
