//! Admission preserves committed values across crashes, registry changes, and
//! replays.

use super::*;
use crate::codec::JsonCodec;
use crate::state::backend::AdmissionChecks;
use crate::state::fjall::test_db::cold_marker_checks;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::tests::support::{MemoryDeduplicationStore, run_admit_soundness};
use crate::test_util::TEST_RUNTIME;
use crate::timers::Trigger;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::test_support::retirement_trace;
use color_eyre::Result;
use quickcheck::QuickCheck;

use crate::state::TimerEventRef;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::descriptor::value_state;
use crate::state::marker::decode_marker_payload;
use crate::state::registry::CollectionDef;
use crate::state::tests::cell_suite::{bytes, value_cell};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::memory::memory_store;
use crate::timers::test_support::{setup_timer_manager_with_store, test_segment};
use color_eyre::eyre::ensure;

use tracing::Span;
use uuid::Uuid;

#[test]
fn prop_admit_soundness() {
    fn property(value: u8, committed: bool) -> Result<bool> {
        TEST_RUNTIME.block_on(run_admit_soundness(
            MemoryCellStore::new(MemoryCells::new()),
            MemoryDeduplicationStore::new(),
            value,
            committed,
        ))
    }
    QuickCheck::new().quickcheck(property as fn(u8, bool) -> Result<bool>);
}

/// Legacy admission follows the old source evidence. Retirement repairs both
/// timer indexes.
async fn legacy_and_timer_residue(value: u8, mode: u8) -> Result<bool> {
    let store = MemoryCellStore::new(MemoryCells::new());
    let dedup = MemoryDeduplicationStore::new();
    let mut registry = CollectionDefRegistry::default();
    registry.register(&value_state::<JsonCodec>("a"), CollectionDef::new(None))?;
    let checks = cold_marker_checks()?;
    let key = StateKey::new(Uuid::new_v4(), Arc::from("legacy"));
    let collection = CollectionRef::new(
        CollectionId::new(
            key.clone(),
            StateType::Application,
            StateName::try_new("a")?,
        ),
        None,
    );
    let manager = test_manager(
        store.clone(),
        dedup.clone(),
        Arc::new(registry),
        key.segment_id,
        checks.clone(),
        (),
    );
    let timer_store = memory_store(test_segment("admit", 300_u32));
    let (stream, timers, shutdown) =
        setup_timer_manager_with_store(timer_store.clone(), ShutdownPhase::default()).await?;
    let _stream = stream;
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(30))?;
    let trigger = Trigger::with_tag(
        key.key.clone(),
        time,
        TimerType::Application,
        i32::from(value),
        Span::current(),
    );
    let variant = mode % 5;
    let event = if variant < 2 {
        EventRef::Message {
            dedup_id: Uuid::new_v4(),
        }
    } else {
        EventRef::Timer(TimerEventRef::new(trigger.timer_type, time, trigger.tag))
    };
    let committed = variant != 0 && variant != 2;
    if let EventRef::Message { dedup_id } = event {
        if committed {
            dedup.insert(dedup_id).await?;
        }
    } else {
        seed_legacy_timer(&timers, &timer_store, &trigger, variant).await?;
    }

    let writes = [(
        value_cell(),
        ProvisionalWrite::new(
            Some(bytes(value)),
            Committed::new(Some(bytes(value.wrapping_add(1)))),
            event,
        ),
    )];
    // Frozen V1 payload: one empty coordinate in section 0, no clears.
    let legacy = decode_marker_payload(
        event,
        &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        MarkerVersion::V1,
        None,
    )?;
    store
        .write_provisional(&collection, &writes, Some(&legacy))
        .await?;
    ensure!(
        manager
            .admit(key.key.clone(), &timers, &shutdown.subscribe())
            .await
            == Admission::Fresh
    );
    ensure!(checks.contains(&key.key).await?);
    ensure!(store.marker_state(collection.id()).await?.staged.is_none());
    let expected = bytes(if committed {
        value
    } else {
        value.wrapping_add(1)
    });
    ensure!(
        store
            .get(collection.id(), &value_cell(), event)
            .await?
            .get()
            == Some(&expected),
        "legacy admission changed the commit decision"
    );
    ensure!(
        manager
            .admit(key.key.clone(), &timers, &shutdown.subscribe())
            .await
            == Admission::Fresh
    );
    shutdown.send_replace(ShutdownPhase::Cancelling);
    legacy_deregistration(value).await?;
    retire_timer_residue(value, mode, store, dedup).await?;
    Ok(true)
}

/// Seeds the old timer source, including a partial replacement.
async fn seed_legacy_timer(
    timers: &TimerManager<TableAdapter<InMemoryTriggerStore>>,
    store: &TableAdapter<InMemoryTriggerStore>,
    trigger: &Trigger,
    variant: u8,
) -> Result<()> {
    use crate::timers::store::operations::TriggerOperations;
    if variant != 4 {
        timers.schedule_trigger(trigger.clone()).await?;
        if variant >= 3 {
            store
                .operations()
                .delete_key_trigger(trigger.timer_type, &trigger.key, trigger.time)
                .await?;
        }
        if variant == 3 {
            let mut replacement = trigger.clone();
            replacement.tag += 1_i32;
            store.operations().upsert_key_trigger(replacement).await?;
        }
    }
    Ok(())
}

#[test]
fn prop_legacy_and_timer_admit_soundness() {
    fn property(value: u8, mode: u8) -> Result<bool> {
        TEST_RUNTIME.block_on(legacy_and_timer_residue(value, mode))
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8) -> Result<bool>);
}

/// Admission removes discovered V1 markers before a later delivery can record
/// their dedup id.
async fn legacy_deregistration(value: u8) -> Result<()> {
    use crate::state::cell::{Committed, ProvisionalWrite};
    use crate::state::marker::{AttemptId, decode_marker_payload};
    use crate::state::tests::cell_suite::{bytes, value_cell};
    use crate::state::tests::support::admit_registered;
    use color_eyre::eyre::ensure;
    use uuid::Uuid;

    let store = MemoryCellStore::new(MemoryCells::new());
    let dedup = MemoryDeduplicationStore::new();
    let key = StateKey::new(Uuid::new_v4(), Arc::from("legacy-deregistered"));
    let a = CollectionRef::new(
        CollectionId::new(
            key.clone(),
            StateType::Application,
            StateName::try_new("a")?,
        ),
        None,
    );
    let b = CollectionRef::new(
        CollectionId::new(key, StateType::Application, StateName::try_new("b")?),
        None,
    );
    let collections = [a, b];
    let touched: Arc<[_]> = collections
        .iter()
        .map(|c| (c.id().state_type(), c.id().name().clone()))
        .collect();
    let dedup_id = Uuid::new_v4();
    let event = EventRef::Message { dedup_id };
    let older = EventRef::Message {
        dedup_id: Uuid::new_v4(),
    };
    let evidence = EventMarker::frozen(older, &[], &[], &touched, None, None, AttemptId::new());
    store
        .commit_provisional(&collections[0], &evidence, &[])
        .await?;
    let writes = [(
        value_cell(),
        ProvisionalWrite::new(
            Some(bytes(value)),
            Committed::new(Some(bytes(value.wrapping_add(1)))),
            event,
        ),
    )];
    let marker = decode_marker_payload(
        event,
        &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        MarkerVersion::V1,
        None,
    )?;
    for collection in &collections {
        store
            .write_provisional(collection, &writes, Some(&marker))
            .await?;
    }
    ensure!(admit_registered(&store, &dedup, &collections[..1]).await? == Admission::Fresh);
    ensure!(
        store
            .marker_state(collections[1].id())
            .await?
            .staged
            .is_none(),
        "admit kept the uncommitted legacy marker"
    );
    ensure!(
        store
            .provisional_cell_at(collections[1].id(), &value_cell())
            .await?
            .is_some(),
        "the legacy cell must remain orphaned"
    );
    dedup.insert(dedup_id).await?;
    ensure!(admit_registered(&store, &dedup, &collections).await? == Admission::Fresh);
    ensure!(
        store
            .get(collections[1].id(), &value_cell(), older)
            .await?
            .get()
            == Some(&bytes(value.wrapping_add(1))),
        "an orphan cell exposed uncommitted data"
    );
    Ok(())
}

/// Failed stage and promote exits remove the proof before the key can dispatch
/// again.
#[test]
fn prop_settle_preserves_admission_proof() {
    async fn run(mode: u8, value: u8) -> Result<bool> {
        use crate::codec::JsonCodec;
        use crate::state::descriptor::value_state;
        use crate::state::registry::CollectionDef;
        use crate::state::session::{Finalized, sealed::StateLifecycle};
        use crate::state::tests::cell_suite::{FailingCellStore, Poison, value_cell};
        use crate::timers::test_support::setup_timer_manager;
        use color_eyre::eyre::ensure;
        use uuid::Uuid;

        let raw = MemoryCellStore::new(MemoryCells::new());
        let cell = FailingCellStore::with_handle(raw.clone(), Arc::default());
        let checks = cold_marker_checks()?;
        let key = StateKey::new(Uuid::new_v4(), Arc::from("proof"));
        let mut registry = CollectionDefRegistry::default();
        let names = [StateName::try_new("a")?, StateName::try_new("b")?];
        for name in &names {
            registry.register(
                &value_state::<JsonCodec>(name.as_str()),
                CollectionDef::new(None),
            )?;
        }
        let manager = test_manager(
            cell.clone(),
            MemoryDeduplicationStore::new(),
            Arc::new(registry),
            key.segment_id,
            checks.clone(),
            (),
        );
        let (_stream, timers, shutdown) = setup_timer_manager().await?;
        for _ in 0_u8..2 {
            ensure!(
                manager
                    .admit(key.key.clone(), &timers, &shutdown.subscribe())
                    .await
                    == Admission::Fresh
            );
            ensure!(checks.contains(&key.key).await?);
            let (_cancel, cancel) = watch::channel(false);
            let scope = manager.session(
                key.key.clone(),
                EventRef::Message {
                    dedup_id: Uuid::new_v4(),
                },
                TerminationWatch::new(shutdown.subscribe(), cancel),
            );
            let session = scope.handle();
            for name in &names {
                session
                    .seed(StateType::Application, name, &value_cell(), Some(&[value]))
                    .await;
            }
            if mode.is_multiple_of(4) {
                cell.set_poison(Some(Poison::WriteProvisional(
                    names[0].clone(),
                    ErrorCategory::Permanent,
                )));
            }
            let finalized = session.finalize().await;
            if mode.is_multiple_of(4) {
                ensure!(finalized.is_err());
            } else {
                let Finalized::Staged(staged) = finalized? else {
                    return Ok(false);
                };
                if mode % 4 == 1 {
                    cell.set_poison(Some(Poison::Collection(
                        names[0].clone(),
                        ErrorCategory::Permanent,
                    )));
                }
                ensure!(staged.promote(|| mode % 4 == 2).await == (mode % 4 != 2));
            }
            cell.set_poison(None);
            ensure!(
                checks.contains(&key.key).await? == (mode % 4 == 3),
                "a failed settle retained its admission proof"
            );
            ensure!(
                manager
                    .admit(key.key.clone(), &timers, &shutdown.subscribe())
                    .await
                    == Admission::Fresh
            );
            for name in &names {
                let id = CollectionId::new(key.clone(), StateType::Application, name.clone());
                ensure!(raw.marker_state(&id).await?.staged.is_none());
            }
        }
        shutdown.send_replace(ShutdownPhase::Cancelling);
        Ok(true)
    }
    fn property(mode: u8, value: u8) -> Result<bool> {
        TEST_RUNTIME.block_on(run(mode, value))
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8) -> Result<bool>);
}

async fn retire_timer_residue(
    value: u8,
    mode: u8,
    store: MemoryCellStore,
    dedup: MemoryDeduplicationStore,
) -> Result<()> {
    use crate::state::TimerEventRef;
    use crate::state::descriptor::value_state;
    use crate::state::marker::AttemptId;
    use crate::state::registry::CollectionDef;
    use color_eyre::eyre::ensure;
    use uuid::Uuid;
    retirement_trace(mode, i32::from(value), |timers, trigger| async move {
        let key = StateKey::new(Uuid::new_v4(), trigger.key.clone());
        let collection = CollectionRef::new(
            CollectionId::new(
                key.clone(),
                StateType::Application,
                StateName::try_new("a")?,
            ),
            None,
        );
        let event = EventRef::Timer(TimerEventRef::new(
            trigger.timer_type,
            trigger.time,
            trigger.tag,
        ));
        let marker = EventMarker::frozen(
            event,
            &[],
            &[],
            &vec![(StateType::Application, collection.id().name().clone())].into(),
            None,
            None,
            AttemptId::new(),
        );
        store.commit_provisional(&collection, &marker, &[]).await?;
        let mut registry = CollectionDefRegistry::default();
        registry.register(&value_state::<JsonCodec>("a"), CollectionDef::new(None))?;
        let manager = test_manager(store, dedup, Arc::new(registry), key.segment_id, (), ());
        let (_tx, shutdown) = watch::channel(ShutdownPhase::default());
        ensure!(manager.admit(key.key, &timers, &shutdown).await == Admission::Fresh);
        Ok(())
    })
    .await?;
    Ok(())
}
