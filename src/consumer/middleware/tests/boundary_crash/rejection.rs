//! Permanent state rejection removes residue before the source commits.

use super::*;

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
        settle(
            &LeafHandler::new(ProbeHandler::ok(0)),
            context,
            guard,
            Ok(0),
        )
        .await;
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
                CellRead::<Values>::read(&raw, &collection, &value_cell())
                    .await?
                    .0
                    .get()
                    .is_some()
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
