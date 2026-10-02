//! Admission repairs residue before a dispatch.

use super::*;

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
                    .points("prosody.state.admission.corrupt_marker")?
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
