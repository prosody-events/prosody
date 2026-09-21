//! Committed set reads use the same model on memory and Cassandra.

use super::*;
use crate::state::KeyQuery;
use crate::state::descriptor::{SetDescriptor, SetHandle};
use color_eyre::eyre::WrapErr;
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
        if !check(backend, descriptor, case, &model)
            .await
            .wrap_err_with(|| format!("event {index}: {ops:?}; trace: {trace:?}"))?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn check<B: ReaderBackend>(
    backend: &B,
    descriptor: SetDescriptor<I64KeyCodec>,
    case: &ReaderCase<'_>,
    model: &BTreeSet<i64>,
) -> Result<bool> {
    let deps = backend.deps();
    let reader = &StateReader::new(&deps, case.sub.clone(), descriptor)?;
    // The first read warms the publication snapshot. The rest share it.
    let empty = reader.is_empty(case.key.clone()).await?;
    let (points, presence, forward, bounded, backward) = join!(
        all_match(KEY_POOL.iter(), |member| async move {
            Ok(reader.contains(case.key.clone(), member).await? == model.contains(member))
        }),
        reader.contains_many(case.key.clone(), &KEY_POOL),
        collect_query(reader.keys(case.key.clone(), KeyQuery::new(Direction::Forward))),
        collect_query(
            reader.keys(
                case.key.clone(),
                KeyQuery::new(Direction::Forward)
                    .after(&-2)
                    .to(&1)
                    .limit(NonZeroUsize::MIN)
            )
        ),
        collect_query(reader.keys(case.key.clone(), KeyQuery::new(Direction::Backward))),
    );
    let points = points?;
    let presence = presence?;
    let forward = forward?;
    let bounded = bounded?;
    let backward = backward?;
    let expected = KEY_POOL.map(|member| model.contains(&member));
    Ok(empty == model.is_empty()
        && points
        && presence == expected
        && forward == model.iter().copied().collect::<Vec<_>>()
        && bounded == model.range(-1..=1).take(1).copied().collect::<Vec<_>>()
        && backward == model.iter().rev().copied().collect::<Vec<_>>())
}
