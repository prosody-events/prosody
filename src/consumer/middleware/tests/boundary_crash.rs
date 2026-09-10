//! Admission and the duplicate filter preserve one committed handler effect.

use super::*;
use crate::consumer::middleware::deduplication::{
    DeduplicationHandler, DeduplicationStore, MemoryDeduplicationStore,
};
use crate::consumer::middleware::tests::test_support::{GatedGuard, cart};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::{MemoryLoader, MessageLoader};
use crate::otel::SpanRelation;
use crate::state::backend::AdmissionChecks;
use crate::state::descriptor::Registered;
use crate::state::fjall::test_db::cold_marker_checks;
use crate::state::manager::{Admission, PartitionStateManager, test_manager};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::MessageMarker;
use crate::state::session::Promoted;
use crate::state::session::sealed::{MarkerIdentity, StateLifecycle};
use crate::state::session::{EventSession, Finalized, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::tests::support::seed_commit_evidence;
use crate::state::{
    CollectionId, CollectionRef, EventRef, StateKey, StateName, StateType, TimerEventRef,
};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use crate::timers::test_support::setup_timer_manager;
use color_eyre::eyre::{Result, ensure, eyre};
use quickcheck::QuickCheck;
use serde_json::{Value, json};
use std::future::Future;
use tokio::sync::watch::channel;
use uuid::Uuid;

/// Replays a source at a selected durable cut, with a new event on either side.
async fn run(kind: u8, cut: u8, value: u8) -> Result<bool> {
    let cell = MemoryCellStore::new(MemoryCells::new());
    let dedup = MemoryDeduplicationStore::new();
    let key = StateKey::new(Uuid::new_v4(), Arc::from("user-1"));
    let collection = CollectionRef::new(
        CollectionId::new(
            key.clone(),
            StateType::Application,
            StateName::try_new("cart")?,
        ),
        None,
    );
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let manager = test_manager(
        cell.clone(),
        dedup.clone(),
        Arc::new(registry),
        key.segment_id,
        cold_marker_checks()?,
        MemoryLoader::<Value>::new(),
    );
    let (_stream, timers, shutdown) = setup_timer_manager().await?;
    let id = Uuid::new_v4();
    let kind = kind % 3;
    let cut = cut % 6;
    let trigger = Trigger::new(
        key.key.clone(),
        CompactDateTime::now()?.add_duration(CompactDuration::new(30))?,
        TimerType::Application,
        tracing::Span::current(),
    );
    let event = if kind == 0 {
        EventRef::Message { dedup_id: id }
    } else {
        timers.schedule_trigger(trigger.clone()).await?;
        EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag,
        ))
    };
    let (_cancel, cancel) = channel(false);
    let termination = TerminationWatch::new(shutdown.subscribe(), cancel);
    let scope = manager.session(key.key.clone(), event, termination.clone());
    let session = scope.handle();
    if kind == 2 {
        session.set_reload_marker(MessageMarker::new(id));
    }
    let context = MockEventContext::new().with_session(session.clone());
    crash(&session, context, &cell, &collection, cut, value).await?;
    drop(session);
    drop(scope);
    let committed = cut >= 2;
    ensure!(
        manager
            .admit(key.key.clone(), &timers, &shutdown.subscribe())
            .await
            == Admission::Fresh
    );
    let live = match event {
        EventRef::Message { .. } => true,
        EventRef::Timer(_) => {
            timers
                .current_timer_tag(&key.key, trigger.time, trigger.timer_type)
                .await?
                == Some(trigger.tag)
        }
    };
    ensure!(
        live == (kind == 0 || !committed),
        "admit did not retire the timer source"
    );
    if kind != 1 {
        ensure!(
            dedup.exists(id).await? == committed,
            "admit did not restore the source dedup id"
        );
    }
    redeliver(
        &manager,
        &key.key,
        &termination,
        &dedup,
        Delivery {
            event,
            id,
            kind,
            value,
            committed,
            live,
        },
    )
    .await?;
    ensure!(cell.marker_state(collection.id()).await?.staged.is_none());
    shutdown.send_replace(ShutdownPhase::Cancelling);
    Ok(true)
}

struct Delivery {
    event: EventRef,
    id: Uuid,
    kind: u8,
    value: u8,
    committed: bool,
    live: bool,
}

async fn crash<S: EventSession<Loader: MessageLoader<Payload = Value>>>(
    session: &S,
    context: MockEventContext<Value, S>,
    cell: &MemoryCellStore,
    collection: &CollectionRef,
    cut: u8,
    value: u8,
) -> Result<()> {
    context
        .state(Registered::new(cart()))?
        .set(json!(value))
        .await?;
    let handler = ProbeHandler::ok(0);
    match cut {
        0 => {}
        1..=3 => {
            let finalized = session.finalize().await?;
            if cut == 2 {
                seed_commit_evidence(cell, collection).await?;
            }
            if cut == 3 {
                let Finalized::Staged(staged) = finalized else {
                    return Err(eyre!("the handler did not stage"));
                };
                ensure!(matches!(staged.promote(|| false).await, Promoted::Complete));
            }
        }
        4 => {
            let (guard, entered, _release, ..) = GatedGuard::new();
            let task = tokio::spawn(async move {
                settle(&handler, context, guard, Ok(0)).await;
            });
            entered.await?;
            task.abort();
            ensure!(task.await.is_err());
        }
        _ => {
            let (guard, committed, _) = RecordingGuard::new();
            settle(&handler, context, guard, Ok(0)).await;
            ensure!(committed.load(Ordering::SeqCst) == 1);
        }
    }
    Ok(())
}

async fn write_value<S: EventSession<Loader: MessageLoader<Payload = Value>>>(
    context: MockEventContext<Value, S>,
    value: u8,
) -> Result<()> {
    context
        .state(Registered::new(cart()))?
        .set(json!(value))
        .await?;
    let (guard, ..) = RecordingGuard::new();
    settle(&ProbeHandler::ok(0), context, guard, Ok(0)).await;
    Ok(())
}

async fn replay<S: EventSession<Loader: MessageLoader<Payload = Value>>>(
    context: MockEventContext<Value, S>,
    dedup: &MemoryDeduplicationStore,
    value: u8,
) -> Result<usize> {
    let handler = ProbeHandler::ok(1);
    let calls = handler.log.clone();
    let filter = DeduplicationHandler {
        inner: handler,
        store: dedup.clone(),
    };
    let result = FallibleHandler::on_message(
        &filter,
        context.clone(),
        create_test_message()?,
        DemandType::Normal,
    )
    .await;
    if matches!(result, Ok(Some(_))) {
        context
            .state(Registered::new(cart()))?
            .set(json!(value))
            .await?;
    }
    let (guard, ..) = RecordingGuard::new();
    settle(&filter, context, guard, result).await;
    Ok(calls
        .lock()
        .iter()
        .filter(|event| matches!(event, HookEvent::Handler))
        .count())
}

async fn redeliver<
    M: PartitionStateManager<Session: EventSession<Loader: MessageLoader<Payload = Value>>>,
>(
    manager: &M,
    key: &crate::Key,
    termination: &TerminationWatch,
    dedup: &MemoryDeduplicationStore,
    delivery: Delivery,
) -> Result<()> {
    let Delivery {
        event,
        id,
        kind,
        value,
        committed,
        live,
    } = delivery;
    let newer = value.wrapping_add(1);
    let mut expected = value;
    for phase in 0..2_u8 {
        if phase == value % 2 {
            let scope = manager.session(
                key.clone(),
                EventRef::Message {
                    dedup_id: Uuid::new_v4(),
                },
                termination.clone(),
            );
            write_value(MockEventContext::new().with_session(scope.handle()), newer).await?;
            expected = newer;
        }
        if phase == 0 && live {
            let scope = manager.session(key.clone(), event, termination.clone());
            let session = scope.handle();
            if kind == 2 {
                session.set_reload_marker(MessageMarker::new(id));
            }
            let calls = replay(MockEventContext::new().with_session(session), dedup, value).await?;
            ensure!(
                calls == usize::from(!committed),
                "a committed source reached the handler again"
            );
            if !committed {
                expected = value;
            }
        }
    }
    let scope = manager.session(
        key.clone(),
        EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        termination.clone(),
    );
    let context = MockEventContext::new().with_session(scope.handle());
    ensure!(context.state(Registered::new(cart()))?.get().await? == Some(json!(expected)));
    if kind != 1 {
        // Dedup expiry permits a new delivery while an older certificate can still
        // exist.
        dedup.expire(id).await;
        let scope = manager.session(
            key.clone(),
            EventRef::Message { dedup_id: id },
            termination.clone(),
        );
        let calls = replay(
            MockEventContext::new().with_session(scope.handle()),
            dedup,
            value.wrapping_add(2),
        )
        .await?;
        ensure!(calls == 1, "expiry did not permit a new delivery");
    }
    Ok(())
}

#[test]
fn prop_boundary_crash_exactly_once() {
    fn property(kind: u8, cut: u8, value: u8) -> Result<bool> {
        TEST_RUNTIME.block_on(run(kind, cut, value))
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, u8) -> Result<bool>);
}

/// Admission repairs corrupt markers and dispatches the next event.
async fn repaired_dispatch(poisoned: bool) -> Result<()> {
    use crate::consumer::Keyed;
    use crate::consumer::message::UncommittedEvent;
    use crate::consumer::middleware::deduplication::{DedupIdentity, dedup_uuid_for_message};
    use crate::consumer::partition::dispatch::process_event;
    use crate::state::tests::cell_suite::{FailingCellStore, Poison};

    let cell =
        FailingCellStore::with_handle(MemoryCellStore::new(MemoryCells::new()), Arc::default());
    if poisoned {
        cell.set_poison(Some(Poison::MarkerRead(ErrorCategory::Permanent)));
    }
    let dedup = MemoryDeduplicationStore::new();
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let checks = cold_marker_checks()?;
    let manager = test_manager(
        cell,
        dedup.clone(),
        Arc::new(registry),
        Uuid::new_v4(),
        checks.clone(),
        MemoryLoader::<Value>::new(),
    );
    let (_stream, timers, shutdown) = setup_timer_manager().await?;
    let tracker = make_offset_tracker();
    let message = create_test_message()?;
    let key = message.key().clone();
    let identity = DedupIdentity {
        version: "1",
        group_id: "test",
        topic: "test-topic",
        partition: 0,
    };
    let id = dedup_uuid_for_message(identity, &message);
    let message = message.into_uncommitted(tracker.take(0).await?);
    let handler = ProbeHandler::ok(0);
    process_event(
        UncommittedEvent::Message(message),
        &handler,
        &shutdown.subscribe(),
        &timers,
        &manager,
        identity,
        SpanRelation::default(),
    )
    .await;
    ensure!(
        handler.log.lock().len() == 2,
        "admission blocked the next event"
    );
    ensure!(dedup.exists(id).await?);
    ensure!(checks.contains(&key).await?);
    ensure!(
        tracker.shutdown().await.is_some(),
        "admission did not commit the source"
    );
    shutdown.send_replace(ShutdownPhase::Cancelling);
    Ok(())
}

#[test]
fn prop_admit_dispatch_soundness() {
    use crate::test_util::GlobalMetrics;
    use std::sync::LazyLock;
    static METRICS: LazyLock<GlobalMetrics> = LazyLock::new(GlobalMetrics::install_global);
    fn property(poisoned: bool) -> Result<bool> {
        TEST_RUNTIME.block_on(async {
            let count = || -> Result<i64> {
                Ok(METRICS
                    .points("keyed_state.admission.corrupt_marker")?
                    .iter()
                    .map(|(_, count)| count)
                    .sum())
            };
            let before = count()?;
            repaired_dispatch(poisoned).await?;
            ensure!(
                count()? - before == i64::from(poisoned),
                "corrupt marker repair did not increment its counter"
            );
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(bool) -> Result<bool>);
}

/// Rejects proof removal so the stage error must retain its classification.
#[derive(Clone)]
struct RejectedUnmark;

impl AdmissionChecks for RejectedUnmark {
    type Error = TestError;

    fn contains(&self, _key: &crate::Key) -> impl Future<Output = Result<bool, Self::Error>> {
        ready(Ok(false))
    }

    fn mark(&self, _key: &crate::Key) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }

    fn unmark(&self, _key: &crate::Key) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(TestError(ErrorCategory::Transient, "unmark")))
    }
}

/// A rejected stage or promote leaves no residue and records no skipped event.
#[test]
fn prop_boundary_permanent_rejection() {
    async fn run(mode: u8, value: u8) -> Result<bool> {
        use crate::codec::JsonCodec;
        use crate::state::descriptor::value_state;
        use crate::state::tests::cell_suite::{FailingCellStore, Poison, value_cell};

        let mode = mode % 3;
        let raw = MemoryCellStore::new(MemoryCells::new());
        let cell = FailingCellStore::with_handle(raw.clone(), Arc::default());
        let dedup = MemoryDeduplicationStore::new();
        let mut registry = CollectionDefRegistry::default();
        let names = [StateName::try_new("cart")?, StateName::try_new("sibling")?];
        for name in &names {
            registry.register(
                &value_state::<JsonCodec>(name.as_str()),
                CollectionDef::new(None),
            )?;
        }
        let key = StateKey::new(Uuid::new_v4(), Arc::from("user-1"));
        let manager = test_manager(
            cell.clone(),
            dedup.clone(),
            Arc::new(registry),
            key.segment_id,
            RejectedUnmark,
            MemoryLoader::<Value>::new(),
        );
        let (_stream, timers, shutdown) = setup_timer_manager().await?;
        let (_cancel, cancel) = channel(false);
        let id = Uuid::new_v4();
        let scope = manager.session(
            key.key.clone(),
            EventRef::Message { dedup_id: id },
            TerminationWatch::new(shutdown.subscribe(), cancel),
        );
        let session = scope.handle();
        let context = MockEventContext::new().with_session(session.clone());
        let count = if mode == 1 { 1 } else { 2 };
        for name in &names[..count] {
            context
                .state(Registered::new(value_state::<JsonCodec>(name.as_str())))?
                .set(json!(value))
                .await?;
        }
        cell.set_poison(Some(if mode == 0 {
            Poison::WriteProvisional(names[0].clone(), ErrorCategory::Permanent)
        } else {
            Poison::Collection(names[0].clone(), ErrorCategory::Permanent)
        }));
        if mode == 0 {
            let Err(error) = session.finalize().await else {
                return Err(eyre!("the stage must reject"));
            };
            ensure!(
                error.classify_error() == ErrorCategory::Permanent,
                "proof removal replaced the stage error"
            );
        }
        let (guard, committed, aborted) = RecordingGuard::new();
        settle(&ProbeHandler::ok(0), context, guard, Ok(0)).await;
        ensure!(committed.load(Ordering::SeqCst) == 1);
        ensure!(aborted.load(Ordering::SeqCst) == 0);
        ensure!(
            dedup.exists(id).await? == (mode == 2),
            "a skip recorded dedup evidence"
        );
        for (index, name) in names[..count].iter().enumerate() {
            let collection = CollectionId::new(key.clone(), StateType::Application, name.clone());
            ensure!(
                raw.marker_state(&collection).await?.staged.is_none(),
                "rejected state retained residue"
            );
            ensure!(
                raw.get(&collection, &value_cell()).await?.get().is_some()
                    == (mode == 2 && index == 1)
            );
        }
        cell.set_poison(None);
        ensure!(
            manager
                .admit(key.key.clone(), &timers, &shutdown.subscribe())
                .await
                == Admission::Fresh
        );
        shutdown.send_replace(ShutdownPhase::Cancelling);
        Ok(true)
    }
    fn property(mode: u8, value: u8) -> Result<bool> {
        TEST_RUNTIME.block_on(run(mode, value))
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8) -> Result<bool>);
}
