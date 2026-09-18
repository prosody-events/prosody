//! Committed set reads use the same model on memory and Cassandra.

use super::*;
use crate::state::descriptor::{SetDescriptor, SetHandle};
use std::collections::BTreeSet;

type OwnerSetHandle<B> = SetHandle<OwnerSession<<B as ReaderBackend>::OwnerCell>, I64KeyCodec>;

/// Drives committed set writes and checks every reader surface.
pub(in crate::state_reader::tests) async fn run_reader_set_trace<B: ReaderBackend>(
    backend: &B,
    descriptor: SetDescriptor<I64KeyCodec>,
    case: &ReaderCase<'_>,
    trace: Trace<MapOp>,
) -> Result<bool> {
    let registry = backend.registry();
    let state_key = seed_source(backend, descriptor, case).await?;
    let mut model = BTreeSet::new();
    for (index, ops) in trace.events_ops().enumerate() {
        let staged = ops.to_vec();
        let for_handle = staged.clone();
        owner_commit_cell(
            backend.owner_cell(),
            &registry,
            &state_key,
            descriptor,
            index as u128,
            move |handle: OwnerSetHandle<B>| async move {
                for op in for_handle {
                    match op {
                        MapOp::Set(key, _) => handle
                            .insert(&key)
                            .await
                            .map_err(|error| eyre!("insert: {error}"))?,
                        MapOp::Remove(key) => handle
                            .remove(&key)
                            .await
                            .map_err(|error| eyre!("remove: {error}"))?,
                        MapOp::Clear => handle
                            .clear()
                            .await
                            .map_err(|error| eyre!("clear: {error}"))?,
                        MapOp::Get(_) | MapOp::IsEmpty | MapOp::Commit => {}
                    }
                }
                Ok(())
            },
        )
        .await?;
        for op in staged {
            match op {
                MapOp::Set(key, _) => {
                    model.insert(key);
                }
                MapOp::Remove(key) => {
                    model.remove(&key);
                }
                MapOp::Clear => model.clear(),
                MapOp::Get(_) | MapOp::IsEmpty | MapOp::Commit => {}
            }
        }
        let deps = backend.deps();
        let reader = StateReader::new(&deps, case.sub.clone(), descriptor)?;
        if reader.is_empty(case.key.clone()).await? != model.is_empty() {
            return Ok(false);
        }
        for member in KEY_POOL {
            assert_eq!(
                reader.contains(case.key.clone(), &member).await?,
                model.contains(&member)
            );
        }
        let expected = KEY_POOL.map(|member| model.contains(&member)).to_vec();
        if reader.contains_many(case.key.clone(), &KEY_POOL).await? != expected {
            return Ok(false);
        }
        let forward =
            collect_stream(reader.keys(case.key.clone(), Direction::Forward).await?).await?;
        if forward != model.iter().copied().collect::<Vec<_>>() {
            return Ok(false);
        }
        assert_eq!(
            collect_stream(
                reader
                    .query(case.key.clone(), Direction::Forward)
                    .after(&-2)
                    .to(&1)
                    .limit(NonZeroUsize::MIN)
                    .keys()
                    .await?
            )
            .await?,
            model.range(-1..=1).take(1).copied().collect::<Vec<_>>()
        );
        let backward =
            collect_stream(reader.keys(case.key.clone(), Direction::Backward).await?).await?;
        if backward != model.iter().rev().copied().collect::<Vec<_>>() {
            return Ok(false);
        }
    }
    Ok(true)
}
