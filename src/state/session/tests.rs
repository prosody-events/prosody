//! Directed tests for [`ValueStateSession`] and [`UnavailableState`].
//!
//! These exercise the session's byte-cell operations, the per-event
//! transaction sharing, the sealed lifecycle (`finalize` →
//! `commit_apply` / `rollback_aborted`), and the attempt-boundary
//! `reset`. All tests are broker-free against `MemoryDurableValueStore`.

use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::*;
use crate::codec::JsonBinaryCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::descriptor::{DescriptorIdentity, ValueDescriptor, value_state};
use crate::state::memory::{
    MemoryDirtyValueStoreProvider, MemoryDurableValueStore, MemoryStateError,
};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::tests::value_suite::{FixedOracle, FixedOracleError, bytes};
use crate::state::value::ValueOp;
use crate::state::{CommitDecision, DurableState, SealedCollection, StateType};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::iter;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use uuid::Uuid;

type TestSession = ValueStateSession<
    MemoryDurableValueStore,
    FixedOracle,
    MemoryDirtyValueStoreProvider,
    MemoryLoader<serde_json::Value>,
>;

/// Commit oracle that records every `record_message` so the marker-flush
/// tests can assert what the session wrote through the oracle.
#[derive(Clone, Default)]
struct RecordingOracle {
    recorded: Arc<SyncMutex<Vec<Uuid>>>,
}

impl CommitOracle for RecordingOracle {
    type Error = FixedOracleError;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.recorded.lock().push(dedup_id);
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::Committed)
    }
}

type RecordingSession = ValueStateSession<
    MemoryDurableValueStore,
    RecordingOracle,
    MemoryDirtyValueStoreProvider,
    MemoryLoader<serde_json::Value>,
>;

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

/// A `Message` event keyed by a fixed dedup id; the tests only need
/// distinct, reproducible events.
fn msg_event(dedup_id: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(dedup_id),
    }
}

fn registry() -> CollectionDefRegistry {
    CollectionDefRegistry::new(Some(CompactDuration::new(3_600)))
}

/// Fixed pool of static collection names the multi-collection lifecycle
/// tests register; only the first `modes.len()` are ever used.
const COLLECTION_NAMES: [&str; 3] = ["c0", "c1", "c2"];

/// Registry pinning `COLLECTION_NAMES[i]` to `modes[i]`, every collection
/// carrying the shared test TTL.
fn registry_with_modes(modes: &[CommitMode]) -> Result<CollectionDefRegistry> {
    let mut r = registry();
    for (name, &mode) in COLLECTION_NAMES.iter().zip(modes) {
        let def = CollectionDef::new(Some(CompactDuration::new(3_600))).with_commit_mode(mode);
        let descriptor: ValueDescriptor = value_state(name);
        r.register(&descriptor, def)?;
    }
    Ok(r)
}

/// Termination watch whose signals never fire (senders kept alive so the
/// receivers keep reporting live values).
fn live_termination() -> (
    watch::Sender<ShutdownPhase>,
    watch::Sender<bool>,
    TerminationWatch,
) {
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (cancel_tx, cancel_rx) = watch::channel(false);
    (
        shutdown_tx,
        cancel_tx,
        TerminationWatch::new(shutdown_rx, cancel_rx),
    )
}

fn build_session(
    durable: MemoryDurableValueStore,
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
) -> TestSession {
    let (_shutdown_tx, _cancel_tx, termination) = live_termination();
    // The senders may drop: a watch receiver keeps reporting the last
    // value, which stays "live" for these tests.
    ValueStateSession::new(SessionParts {
        durable,
        oracle: FixedOracle::committed(),
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::new(),
        registry: Arc::new(registry),
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        termination,
    })
}

/// Builds a session over a [`RecordingOracle`] so the marker-flush tests can
/// inspect what the session wrote through the oracle.
fn build_recording_session(
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
) -> (RecordingSession, Arc<SyncMutex<Vec<Uuid>>>) {
    let (_shutdown_tx, _cancel_tx, termination) = live_termination();
    let oracle = RecordingOracle::default();
    let recorded = oracle.recorded.clone();
    let session = ValueStateSession::new(SessionParts {
        durable: MemoryDurableValueStore::for_tests(),
        oracle,
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::new(),
        registry: Arc::new(registry),
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        termination,
    });
    (session, recorded)
}

/// Read a partition expected to be `Idle`, returning its applied payload.
async fn read_idle_applied(
    durable: &MemoryDurableValueStore,
    id: &CollectionId<ValueKind>,
) -> Result<Option<Bytes>> {
    match DurableWalStore::read_partition(durable, id).await? {
        DurableState::Idle { applied } => Ok(applied),
        other @ DurableState::Sealed { .. } => Err(eyre!("expected Idle, got {other:?}")),
    }
}

// ---- multi-collection lifecycle property -----------------------------------

/// 1–3 collections with arbitrary commit modes, plus whether the event
/// ultimately commits (apply) or aborts (rollback). Drives the full session
/// lifecycle: the `finalize` fan-out and the `commit_apply` /
/// `rollback_aborted` resolution of the recorded sealed set.
#[derive(Clone, Debug)]
struct SessionShape {
    modes: Vec<CommitMode>,
    commit: bool,
}

impl Arbitrary for SessionShape {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = usize::arbitrary(g) % COLLECTION_NAMES.len() + 1;
        let modes = (0..count)
            .map(|_| {
                if bool::arbitrary(g) {
                    CommitMode::Wal
                } else {
                    CommitMode::Direct
                }
            })
            .collect();
        Self {
            modes,
            commit: bool::arbitrary(g),
        }
    }
}

/// Mints a multi-collection session, sets one distinct cell per collection,
/// finalizes, then resolves under `commit` (apply) or `!commit` (rollback),
/// checking the full lifecycle for `modes`:
///
/// * `finalize` records exactly the `Wal` collections in the sealed set, each
///   `Wal` partition is `Sealed` under the session event, and each `Direct`
///   collection is already `Idle` with its applied cell (never sealed).
/// * the resolution reports `Resolved` (or `NothingSealed` with no `Wal`
///   collection), and afterward every partition is `Idle` with: a committed
///   `Wal` collection's op folded into `applied`, an aborted `Wal` collection
///   rolled back to its (empty) pre-seal `applied`, and a `Direct` collection
///   unchanged (resolution never touches it).
async fn run_session_shape(modes: &[CommitMode], commit: bool) -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let event = msg_event(0x5E_A1ED);
    let session = build_session(
        durable.clone(),
        registry_with_modes(modes)?,
        state_key.clone(),
        event,
    );

    for (i, name) in COLLECTION_NAMES.iter().take(modes.len()).enumerate() {
        session
            .set_state_cell(&StateName::try_new(name)?, bytes(i as u8))
            .await?;
    }

    let wal_count = modes
        .iter()
        .filter(|m| matches!(m, CommitMode::Wal))
        .count();
    let outcome = session.finalize().await?;
    let expected_outcome = if wal_count == 0 {
        FinalizeOutcome::Clean
    } else {
        FinalizeOutcome::Sealed
    };
    if outcome != expected_outcome {
        return Err(eyre!(
            "finalize returned {outcome:?}, expected {expected_outcome:?} for modes {modes:?}"
        ));
    }

    // The recorded sealed set holds exactly the Wal collections.
    let recorded = session
        .inner
        .sealed
        .lock()
        .as_ref()
        .map_or(0, |s| s.collections.len());
    if recorded != wal_count {
        return Err(eyre!(
            "recorded {recorded} sealed collections, expected {wal_count} (modes {modes:?})"
        ));
    }

    // Pre-resolution: each Wal partition is Sealed under the event, each
    // Direct partition already applied during finalize.
    for (i, (name, &mode)) in COLLECTION_NAMES.iter().zip(modes).enumerate() {
        let id = make_collection_id(&state_key, name)?;
        match mode {
            CommitMode::Wal => match DurableWalStore::read_partition(&durable, &id).await? {
                DurableState::Sealed { wal, .. } if wal.event() == event => {}
                other => {
                    return Err(eyre!(
                        "{name}: expected Sealed under {event:?}, got {other:?}"
                    ));
                }
            },
            CommitMode::Direct => {
                let applied = read_idle_applied(&durable, &id).await?;
                if applied != Some(bytes(i as u8)) {
                    return Err(eyre!(
                        "{name}: Direct collection must be Idle with its applied cell, got \
                         {applied:?}"
                    ));
                }
            }
        }
    }

    // Resolve the recorded sealed set and check the post-resolution state.
    let resolution = if commit {
        session.commit_apply().await
    } else {
        session.rollback_aborted().await
    };
    let expected_resolution = if wal_count == 0 {
        ApplyOutcome::NothingSealed
    } else {
        ApplyOutcome::Resolved
    };
    if resolution != expected_resolution {
        return Err(eyre!(
            "resolution {resolution:?}, expected {expected_resolution:?} (commit={commit}, modes \
             {modes:?})"
        ));
    }

    for (i, (name, &mode)) in COLLECTION_NAMES.iter().zip(modes).enumerate() {
        let id = make_collection_id(&state_key, name)?;
        // A committed Wal folds its op into applied; an aborted Wal rolls
        // back to the empty pre-seal applied; a Direct collection is
        // untouched by resolution (it applied during finalize).
        let expected = match (mode, commit) {
            (CommitMode::Direct, _) | (CommitMode::Wal, true) => Some(bytes(i as u8)),
            (CommitMode::Wal, false) => None,
        };
        let applied = read_idle_applied(&durable, &id).await?;
        if applied != expected {
            return Err(eyre!(
                "{name}: post-resolution applied {applied:?}, expected {expected:?} \
                 (commit={commit})"
            ));
        }
    }
    Ok(())
}

/// Multi-collection lifecycle property. Subsumes the per-collection
/// finalize/seal/direct/commit/rollback walks: one property over
/// (collection-count ∈ {1,2,3} × per-collection commit mode × commit/abort)
/// proves the `finalize` fan-out records exactly the `Wal` seals and
/// direct-applies the rest, and that `commit_apply` / `rollback_aborted`
/// then resolve the sealed set to the right authoritative `applied` state.
/// Iteration count comes from `QUICKCHECK_TESTS`.
#[test]
fn prop_session_lifecycle_resolves_to_expected_applied() {
    fn property(shape: SessionShape) -> TestResult {
        let SessionShape { modes, commit } = shape;
        let dbg = format!("modes={modes:?} commit={commit}");
        match executor::block_on(run_session_shape(&modes, commit)) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(format!("{e}\n{dbg}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(SessionShape) -> TestResult);
}

/// Repeated cell accesses by the same name join the same per-event
/// transaction, so a set is visible to a later read.
#[tokio::test]
async fn repeat_access_joins_same_transaction() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable, registry(), state_key, msg_event(2));

    let counter = StateName::try_new("counter")?;
    session.set_state_cell(&counter, bytes(5)).await?;
    assert_eq!(session.state_cell(&counter).await?, Some(bytes(5)));
    Ok(())
}

/// Clones share the per-event transaction map: a set through one clone is
/// visible through another.
#[tokio::test]
async fn clones_share_transaction_map() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable, registry(), state_key, msg_event(3));
    let clone = session.clone();

    let counter = StateName::try_new("counter")?;
    session.set_state_cell(&counter, bytes(9)).await?;
    assert_eq!(clone.state_cell(&counter).await?, Some(bytes(9)));
    Ok(())
}

/// `finalize` with no dirty collections is `Clean`, and the subsequent
/// `commit_apply` reports nothing sealed.
#[tokio::test]
async fn finalize_without_ops_is_clean() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let session = build_session(durable, registry(), make_state_key(), msg_event(5));

    assert_eq!(session.finalize().await?, FinalizeOutcome::Clean);
    assert_eq!(session.commit_apply().await, ApplyOutcome::NothingSealed);
    Ok(())
}

/// `commit_apply` reports `Incomplete` when a resolution fails, leaving
/// the WAL in place for the recovery sweep. Failure is induced by
/// rolling the WAL back out-of-band and re-sealing it under a different
/// event before the apply runs.
#[tokio::test]
async fn commit_apply_failure_reports_incomplete() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(
        durable.clone(),
        registry(),
        state_key.clone(),
        msg_event(10),
    );

    let counter = StateName::try_new("counter")?;
    session.set_state_cell(&counter, bytes(60)).await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);

    // Replace the WAL out-of-band with one sealed under a different
    // event; the session's apply then mismatches and fails.
    let id = make_collection_id(&state_key, "counter")?;
    let collection_ref = CollectionRef::new(id.clone(), None);
    durable
        .rollback_sealed(&collection_ref, msg_event(10))
        .await?;
    durable
        .seal(
            &collection_ref,
            msg_event(11),
            vec![ValueOp::Set { payload: bytes(61) }],
        )
        .await?;

    assert_eq!(session.commit_apply().await, ApplyOutcome::Incomplete);
    assert!(matches!(
        DurableWalStore::read_partition(&durable, &id).await?,
        DurableState::Sealed { .. }
    ));
    Ok(())
}

/// `resolve_sealed` attempts *every* recorded sealed collection even when
/// one fails partway through: with three `Wal` collections and the durable
/// rigged to fail the second `apply_sealed`, `commit_apply` reports
/// `Incomplete` and still calls `apply_sealed` for all three — it never
/// breaks the loop on the first failure (which would silently strand the
/// later collections' WALs).
#[tokio::test]
async fn commit_apply_continues_past_a_failed_collection() -> Result<()> {
    let durable = CountingFailDurable::new(2);
    let state_key = make_state_key();
    let modes = [CommitMode::Wal, CommitMode::Wal, CommitMode::Wal];
    let (_shutdown_tx, _cancel_tx, termination) = live_termination();
    let session = ValueStateSession::new(SessionParts {
        durable: durable.clone(),
        oracle: FixedOracle::committed(),
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::<serde_json::Value>::new(),
        registry: Arc::new(registry_with_modes(&modes)?),
        state_key,
        event: msg_event(77),
        recovery_delay: CompactDuration::new(30),
        termination,
    });

    for (i, name) in COLLECTION_NAMES.iter().enumerate() {
        session
            .set_state_cell(&StateName::try_new(name)?, bytes(i as u8))
            .await?;
    }
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);

    assert_eq!(
        session.commit_apply().await,
        ApplyOutcome::Incomplete,
        "a failed resolution must surface Incomplete"
    );
    assert_eq!(
        durable.apply_count(),
        3,
        "resolve_sealed must attempt every sealed collection, not break on the first failure"
    );
    Ok(())
}

/// Registration verification routes through the registry: unknown names
/// are `Unregistered`, identity mismatches are rejected.
#[tokio::test]
async fn verify_state_registration_checks_identity() -> Result<()> {
    let cart: ValueDescriptor = value_state("cart");
    let mut reg = registry();
    reg.register(&cart, CollectionDef::new(None))?;
    let durable = MemoryDurableValueStore::for_tests();
    let session = build_session(durable, reg, make_state_key(), msg_event(12));

    let name = session.verify_state_registration("cart", &cart.structural_identity())?;
    assert_eq!(name.as_str(), "cart");

    assert!(matches!(
        session.verify_state_registration("missing", &cart.structural_identity()),
        Err(StateAccessError::Unregistered { name: "missing" })
    ));

    let mismatched = value_state::<JsonBinaryCodec>("cart").structural_identity();
    assert!(matches!(
        session.verify_state_registration("cart", &mismatched),
        Err(StateAccessError::IdentityMismatch { .. })
    ));
    Ok(())
}

/// The termination watch trips on partition shutdown and on event
/// cancellation.
#[tokio::test]
async fn termination_watch_trips_on_either_signal() -> Result<()> {
    use crate::consumer::partition::ShutdownPhase;

    let durable = MemoryDurableValueStore::for_tests();
    let (shutdown_tx, cancel_tx, termination) = {
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::Running);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        (
            shutdown_tx,
            cancel_tx,
            TerminationWatch::new(shutdown_rx, cancel_rx),
        )
    };
    let session = ValueStateSession::new(SessionParts {
        durable,
        oracle: FixedOracle::committed(),
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::<serde_json::Value>::new(),
        registry: Arc::new(registry()),
        state_key: make_state_key(),
        event: msg_event(13),
        recovery_delay: CompactDuration::new(30),
        termination,
    });

    assert!(!session.is_terminated());
    cancel_tx.send(true)?;
    assert!(session.is_terminated());
    cancel_tx.send(false)?;
    assert!(!session.is_terminated());
    shutdown_tx.send(ShutdownPhase::Cancelling)?;
    assert!(session.is_terminated());
    Ok(())
}

/// `UnavailableState` refuses every operation and its lifecycle is inert.
#[tokio::test]
async fn unavailable_state_refuses_everything() -> Result<()> {
    let session: UnavailableState<serde_json::Value> = UnavailableState::new();
    let name = StateName::try_new("anything")?;

    assert!(matches!(
        session.state_cell(&name).await,
        Err(StateAccessError::Unavailable)
    ));
    assert!(matches!(
        session.set_state_cell(&name, bytes(1)).await,
        Err(StateAccessError::Unavailable)
    ));
    assert!(matches!(
        session.clear_state_cell(&name).await,
        Err(StateAccessError::Unavailable)
    ));
    assert!(matches!(
        session.flush_state_cell(&name).await,
        Err(StateAccessError::Unavailable)
    ));
    assert!(session.is_terminated());

    assert_eq!(session.finalize().await?, FinalizeOutcome::Clean);
    assert_eq!(session.commit_apply().await, ApplyOutcome::NothingSealed);
    assert_eq!(
        session.rollback_aborted().await,
        ApplyOutcome::NothingSealed
    );

    // The marker still routes through the stub (so the boundary's flush is
    // uniform across stateful and stateless sessions), but flush is inert
    // and reset discards the recording.
    let id = Uuid::from_u128(1);
    session.register_marker(id);
    assert_eq!(session.registered_markers(), vec![id]);
    session.flush_marker().await?;
    session.reset();
    assert!(session.registered_markers().is_empty());
    Ok(())
}

/// Marker slot is last-wins, flushes through the oracle exactly once, and
/// clears on flush success and on `reset`. Property: replaying a sequence of
/// `Register`/`Flush`/`Reset` ops against the session matches a 3-line model
/// of the slot, and every flushed id reaches the oracle.
#[test]
fn prop_marker_slot_matches_model() {
    /// One op against the marker slot.
    #[derive(Clone, Debug)]
    enum MarkerOp {
        Register(u8),
        Flush,
        Reset,
    }

    impl Arbitrary for MarkerOp {
        fn arbitrary(g: &mut Gen) -> Self {
            match u8::arbitrary(g) % 3 {
                0 => Self::Register(u8::arbitrary(g)),
                1 => Self::Flush,
                _ => Self::Reset,
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match self {
                Self::Register(b) => Box::new(b.shrink().map(Self::Register)),
                Self::Flush | Self::Reset => Box::new(iter::empty()),
            }
        }
    }

    fn property(ops: Vec<MarkerOp>) -> TestResult {
        let result: Result<bool> = executor::block_on(async {
            let (session, recorded) =
                build_recording_session(registry(), make_state_key(), msg_event(1));

            // Model: the slot is `Option<Uuid>`; flush writes it through and
            // clears it; reset clears it.
            let mut slot: Option<Uuid> = None;
            let mut expected_flushed: Vec<Uuid> = Vec::new();

            for op in ops {
                match op {
                    MarkerOp::Register(b) => {
                        let id = Uuid::from_u128(u128::from(b));
                        session.register_marker(id);
                        slot = Some(id);
                    }
                    MarkerOp::Flush => {
                        session.flush_marker().await?;
                        if let Some(id) = slot.take() {
                            expected_flushed.push(id);
                        }
                    }
                    MarkerOp::Reset => {
                        session.reset();
                        slot = None;
                    }
                }
                if session.registered_marker() != slot {
                    return Ok(false);
                }
            }
            Ok(*recorded.lock() == expected_flushed)
        });
        match result {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(e) => TestResult::error(format!("{e}")),
        }
    }

    QuickCheck::new().quickcheck(property as fn(Vec<MarkerOp>) -> TestResult);
}

// ---- CountingFailDurable: fails the Nth apply_sealed -----------------------

/// Durable Value bundle that delegates to [`MemoryDurableValueStore`] but
/// fails the `fail_at`-th `apply_sealed` call (1-based), counting every
/// `apply_sealed`. Drives the "no early break" assertion in
/// [`commit_apply_continues_past_a_failed_collection`].
#[derive(Clone, Debug)]
struct CountingFailDurable {
    inner: MemoryDurableValueStore,
    fail_at: usize,
    applies: Arc<AtomicUsize>,
}

impl CountingFailDurable {
    fn new(fail_at: usize) -> Self {
        Self {
            inner: MemoryDurableValueStore::for_tests(),
            fail_at,
            applies: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn apply_count(&self) -> usize {
        self.applies.load(Ordering::SeqCst)
    }
}

impl ValueStore for CountingFailDurable {
    type Error = CountingFailError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        self.inner.get(collection).await.map_err(Into::into)
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        self.inner
            .set(collection, payload)
            .await
            .map_err(Into::into)
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        self.inner.clear(collection).await.map_err(Into::into)
    }
}

impl DurableWalStore<ValueKind> for CountingFailDurable {
    type Error = CountingFailError;

    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        DurableWalStore::read_partition(&self.inner, collection)
            .await
            .map_err(Into::into)
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
            .map_err(Into::into)
    }

    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        if self.applies.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
            return Err(CountingFailError::InjectedApply);
        }
        self.inner
            .apply_sealed(collection, expected_event)
            .await
            .map_err(Into::into)
    }

    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        self.inner
            .rollback_sealed(collection, expected_event)
            .await
            .map_err(Into::into)
    }
}

impl DirectApplyStore<ValueKind> for CountingFailDurable {
    type Error = CountingFailError;

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
            .map_err(Into::into)
    }
}

#[derive(Debug, Error)]
enum CountingFailError {
    #[error("injected apply_sealed failure")]
    InjectedApply,
    #[error(transparent)]
    Memory(#[from] MemoryStateError),
}

impl ClassifyError for CountingFailError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
