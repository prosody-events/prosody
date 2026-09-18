//! Set membership follows the model through each event and query.

use super::*;
use crate::state::descriptor::{SetHandle, set_state};
use crate::test_util::TEST_RUNTIME;
use quickcheck::QuickCheck;
use std::collections::BTreeSet;

async fn run_set_trace(
    trace: MapTrace,
    constraints: StreamConstraints,
    batch: MapGetManyInput,
) -> Result<bool> {
    assert_batch(&batch, constraints).await?;

    for commit_mode in [CommitMode::ReadCommitted, CommitMode::ReadUncommitted] {
        for keyset_limit in [0, 3, 8] {
            let passed = run_collection_trace(
                trace.clone(),
                set_state::<I64KeyCodec>("st"),
                "st",
                CollectionDef {
                    commit_mode,
                    keyset_limit,
                    ..CollectionDef::new(None)
                },
                async |handle, op, model: &mut BTreeSet<i64>| {
                    let outcome = match op {
                        MapOp::Set(key, _) => {
                            handle.insert(&key).await?;
                            model.insert(key);
                            OpOutcome::Continue
                        }
                        MapOp::Remove(key) => {
                            handle.remove(&key).await?;
                            model.remove(&key);
                            OpOutcome::Continue
                        }
                        MapOp::Get(key) => {
                            mismatch_unless(handle.contains(&key).await? == model.contains(&key))
                        }
                        MapOp::IsEmpty => {
                            mismatch_unless(handle.is_empty().await? == model.is_empty())
                        }
                        MapOp::Clear => {
                            handle.clear().await?;
                            model.clear();
                            OpOutcome::Continue
                        }
                        MapOp::Commit => {
                            handle.commit().await?;
                            OpOutcome::Committed
                        }
                    };
                    assert_set(handle, model, constraints, &batch.queries).await?;
                    Ok(outcome)
                },
                async |handle, model, backing: &Backing<'_>| {
                    assert_set(handle, model, constraints, &batch.queries).await?;
                    if keyset_limit == 8 {
                        let id = CollectionId::new(
                            backing.state_key.clone(),
                            StateType::Application,
                            StateName::try_new("st")?,
                        );
                        let store = MemoryCellStore::new(backing.cells.clone());
                        let stored = CellRead::<Values>::read(&store, &id, &keyset_cell())
                            .await?
                            .0
                            .into_inner();
                        let live: Vec<_> = model.iter().copied().collect();
                        assert!(
                            match stored {
                                Some(bytes) => bytes[..] == tracked_frame(&live)[..],
                                None => live.is_empty(),
                            },
                            "keyset bytes must equal committed membership"
                        );
                    }
                    Ok(true)
                },
            )
            .await?;
            if !passed {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Exercises arbitrary populations and batches across the storage batch
/// boundary.
async fn assert_batch(input: &MapGetManyInput, constraints: StreamConstraints) -> Result<()> {
    let cells = MemoryCells::new();
    let dedup = MemoryDeduplicationStore::default();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("batch"));
    let descriptor = set_state::<I64KeyCodec>("st");
    let (registry, collection) =
        registry_and_ref(&descriptor, "st", &state_key, CollectionDef::new(None))?;
    let event = read_event(0);
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session)?;
    let mut model = BTreeSet::new();
    for &(member, _) in &input.entries {
        handle.insert(&member).await?;
        model.insert(member);
    }
    if input.commit {
        finalize_and_promote(
            &session,
            &dedup,
            event_dedup(event),
            &cells,
            collection.id(),
        )
        .await?;
    }
    let read = make_session(&cells, &dedup, &registry, &state_key, read_event(1));
    let handle = if input.commit {
        descriptor.bind(&read)?
    } else {
        handle
    };
    assert_set(&handle, &model, constraints, &input.queries).await
}

async fn assert_set<S: StateSession>(
    handle: &SetHandle<S, I64KeyCodec>,
    model: &BTreeSet<i64>,
    constraints: StreamConstraints,
    queries: &[i64],
) -> Result<()> {
    for key in KEY_POOL {
        assert_eq!(handle.contains(&key).await?, model.contains(&key));
    }
    assert_eq!(handle.is_empty().await?, model.is_empty());
    assert_eq!(
        handle.contains_many(queries).await?,
        queries
            .iter()
            .map(|key| model.contains(key))
            .collect::<Vec<_>>()
    );
    for dir in [Direction::Forward, Direction::Backward] {
        let mut expected: Vec<_> = model.iter().copied().collect();
        if dir == Direction::Backward {
            expected.reverse();
        }
        assert_eq!(drain(handle.keys(dir)).await?, expected);
        expected.retain(|key| constraints.contains(*key, dir));
        expected.truncate(constraints.limit.map_or(usize::MAX, NonZeroUsize::get));
        assert_eq!(
            drain(constraints.apply(handle.query(dir)).keys()).await?,
            expected
        );
    }
    Ok(())
}

/// Set membership and keyset bytes agree through queries, commits, and
/// recovery.
#[test]
fn prop_set_collection() {
    fn property(
        trace: MapTrace,
        constraints: StreamConstraints,
        batch: MapGetManyInput,
    ) -> Result<bool> {
        TEST_RUNTIME.block_on(run_set_trace(trace, constraints, batch))
    }
    QuickCheck::new()
        .quickcheck(property as fn(MapTrace, StreamConstraints, MapGetManyInput) -> Result<bool>);
}
