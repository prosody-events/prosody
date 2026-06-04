//! Directed tests for [`ValueStateSession`] and [`UnavailableState`].
//!
//! These exercise the session's byte-cell operations, the per-event
//! transaction sharing, the sealed lifecycle (`finalize` →
//! `commit_apply` / `rollback_aborted`), and the attempt-boundary
//! `reset`. All tests are broker-free against `MemoryDurableValueStore`.

use super::sealed::{ApplyOutcome, FinalizeOutcome, StateLifecycle};
use super::*;
use crate::codec::JsonCodec;
use crate::consumer::middleware::defer::message::loader::MemoryLoader;
use crate::consumer::partition::ShutdownPhase;
use crate::state::descriptor::{DescriptorIdentity, ValueDescriptor, value_state};
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::middleware::{CollectionDef, CollectionDefRegistry};
use crate::state::value_test_suite::bytes;
use crate::state::{DurableState, StateType};
use color_eyre::eyre::{Result, eyre};
use uuid::Uuid;

type TestSession = ValueStateSession<
    MemoryDurableValueStore,
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

/// Registry that pins `name` to `mode`.
fn registry_with_mode(name: &'static str, mode: CommitMode) -> Result<CollectionDefRegistry> {
    let mut r = registry();
    let def = CollectionDef::new(Some(CompactDuration::new(3_600))).with_commit_mode(mode);
    let descriptor: ValueDescriptor = value_state(name);
    r.register(&descriptor, def)?;
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
        dirty: MemoryDirtyValueStoreProvider,
        loader: MemoryLoader::new(),
        registry: Arc::new(registry),
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        termination,
    })
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

/// `set_state_cell` accumulates dirty ops and `finalize` seals them to
/// the durable WAL under the session's event.
#[tokio::test]
async fn value_set_then_finalize_persists_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let event = msg_event(1);
    let session = build_session(durable.clone(), registry(), state_key.clone(), event);

    session
        .set_state_cell(&StateName::try_new("counter")?, bytes(7))
        .await?;

    let outcome = session.finalize().await?;
    assert_eq!(outcome, FinalizeOutcome::Sealed);

    let id = make_collection_id(&state_key, "counter")?;
    match DurableWalStore::read_partition(&durable, &id).await? {
        DurableState::Sealed { wal, .. } => {
            assert_eq!(wal.event(), event);
            Ok(())
        }
        other @ DurableState::Idle { .. } => Err(eyre!("expected Sealed, got {other:?}")),
    }
}

/// `flush_state_cell` on a dirty transaction drains the ops directly to
/// durable applied state and returns the transaction to `Clean`.
#[tokio::test]
async fn flush_drains_dirty_and_returns_clean() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(
        durable.clone(),
        registry(),
        state_key.clone(),
        msg_event(42),
    );

    let counter = StateName::try_new("counter")?;
    session.set_state_cell(&counter, bytes(13)).await?;
    let outcome = session.flush_state_cell(&counter).await?;
    assert_eq!(
        outcome,
        StoreOutcome::Applied,
        "flush of Dirty must report Applied"
    );

    let id = make_collection_id(&state_key, "counter")?;
    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(13)));

    // Second flush is a no-op on Clean.
    let outcome = session.flush_state_cell(&counter).await?;
    assert_eq!(outcome, StoreOutcome::NoOp);
    Ok(())
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

/// Direct-mode collections direct-apply during `finalize` without
/// producing a sealed WAL.
#[tokio::test]
async fn direct_mode_finalize_skips_seal() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(
        durable.clone(),
        registry_with_mode("counter", CommitMode::Direct)?,
        state_key.clone(),
        msg_event(4),
    );

    session
        .set_state_cell(&StateName::try_new("counter")?, bytes(9))
        .await?;
    let outcome = session.finalize().await?;
    assert_eq!(
        outcome,
        FinalizeOutcome::Clean,
        "direct-mode must not surface sealed entries"
    );

    let id = make_collection_id(&state_key, "counter")?;
    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(9)));
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

/// The full commit lifecycle: set → finalize (seals) → `commit_apply`
/// resolves the WAL into applied state.
#[tokio::test]
async fn commit_apply_resolves_recorded_seals() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable.clone(), registry(), state_key.clone(), msg_event(6));

    session
        .set_state_cell(&StateName::try_new("counter")?, bytes(21))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);

    let id = make_collection_id(&state_key, "counter")?;
    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(21)));

    // The sealed set is consumed: a second apply reports nothing sealed.
    assert_eq!(session.commit_apply().await, ApplyOutcome::NothingSealed);
    Ok(())
}

/// The abort lifecycle: set → finalize (seals) → `rollback_aborted`
/// clears the WAL without applying.
#[tokio::test]
async fn rollback_aborted_rolls_recorded_seals_back() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable.clone(), registry(), state_key.clone(), msg_event(7));

    session
        .set_state_cell(&StateName::try_new("counter")?, bytes(33))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);
    assert_eq!(session.rollback_aborted().await, ApplyOutcome::Resolved);

    let id = make_collection_id(&state_key, "counter")?;
    assert_eq!(
        read_idle_applied(&durable, &id).await?,
        None,
        "rollback restored pre-seal state"
    );
    Ok(())
}

/// `reset` discards buffered dirty ops, the transaction map, and the
/// recorded sealed set: a failed attempt's writes never reach a later
/// finalize.
#[tokio::test]
async fn reset_discards_dirty_ops_and_sealed_set() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable.clone(), registry(), state_key.clone(), msg_event(8));

    let counter = StateName::try_new("counter")?;
    session.set_state_cell(&counter, bytes(99)).await?;
    session.reset();

    // The buffered set is gone: finalize seals nothing.
    assert_eq!(session.finalize().await?, FinalizeOutcome::Clean);
    let id = make_collection_id(&state_key, "counter")?;
    assert_eq!(read_idle_applied(&durable, &id).await?, None);

    // The next attempt's ops work over the fresh scope.
    session.set_state_cell(&counter, bytes(1)).await?;
    assert_eq!(session.state_cell(&counter).await?, Some(bytes(1)));
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);
    assert_eq!(session.commit_apply().await, ApplyOutcome::Resolved);
    assert_eq!(read_idle_applied(&durable, &id).await?, Some(bytes(1)));
    Ok(())
}

/// `reset` after a finalize clears the recorded sealed set, so the apply
/// hooks have nothing to resolve (the defer-swallow contract).
#[tokio::test]
async fn reset_after_finalize_clears_sealed_set() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let state_key = make_state_key();
    let session = build_session(durable.clone(), registry(), state_key.clone(), msg_event(9));

    session
        .set_state_cell(&StateName::try_new("counter")?, bytes(50))
        .await?;
    assert_eq!(session.finalize().await?, FinalizeOutcome::Sealed);
    session.reset();
    assert_eq!(session.commit_apply().await, ApplyOutcome::NothingSealed);
    Ok(())
}

/// `commit_apply` reports `Incomplete` when a resolution fails, leaving
/// the WAL in place for the recovery sweep. Failure is induced by
/// rolling the WAL back out-of-band and re-sealing it under a different
/// event before the apply runs.
#[tokio::test]
async fn commit_apply_failure_reports_incomplete() -> Result<()> {
    use crate::state::value::ValueOp;

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

/// Registration verification routes through the registry: unknown names
/// are `Unregistered`, identity mismatches are rejected.
#[tokio::test]
async fn verify_state_registration_checks_identity() -> Result<()> {
    const CART: ValueDescriptor = value_state("cart");
    let mut reg = registry();
    reg.register(&CART, CollectionDef::new(None))?;
    let durable = MemoryDurableValueStore::for_tests();
    let session = build_session(durable, reg, make_state_key(), msg_event(12));

    let name = session.verify_state_registration("cart", &CART.structural_identity())?;
    assert_eq!(name.as_str(), "cart");

    assert!(matches!(
        session.verify_state_registration("missing", &CART.structural_identity()),
        Err(StateAccessError::Unregistered { name: "missing" })
    ));

    let mismatched = value_state::<JsonCodec>("cart")
        .with_schema_label("v2")
        .structural_identity();
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
    session.reset();
    Ok(())
}
