use crate::state::cell::{Committed, Presence, ProvisionalWrite, Values};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::store::{CellStore, CoordinateBatch};
use crate::state::tests::support::listed;
use crate::state::tests::support::{evidence, evidence_only, probe};
use crate::state::{CollectionId, CollectionRef, StateKey, StateName, StateType};
use crate::state_reader::CommittedCellSource;
use bytes::Bytes;
use color_eyre::{Report, Result};
use futures::future::try_join_all;
use futures::{TryStreamExt, join, try_join};
use std::ops::Bound;

/// Checks value and presence projections before the owner removes residue.
pub(crate) async fn reader_residue<
    S: CellStore,
    R: CommittedCellSource<Values> + CommittedCellSource<Presence>,
>(
    store: S,
    source: &R,
    state_key: &StateKey,
    event_id: u128,
    value: u8,
    mode: u8,
) -> Result<bool> {
    let committed = mode & 1 != 0;
    let clear = mode & 2 != 0;
    let other = mode & 4 != 0;
    let id = residue_id(state_key, "residue")?;
    let remote = residue_id(state_key, "evidence")?;
    let collection = CollectionRef::new(id.clone(), None);
    let section = Section::new(7);
    let cells = [0, 1, 2].map(|i| CellKey {
        section,
        coordinate: Coordinate::from_bytes(vec![i]),
    });
    let base = Bytes::from(vec![value]);
    let next = Bytes::from(vec![value.wrapping_add(1)]);
    let resolved = cells
        .each_ref()
        .map(|cell| (cell.clone(), Some(base.clone())));
    store.write_resolved(&collection, &resolved, &[]).await?;
    let event = probe(event_id);
    let writes = [
        (
            cells[0].clone(),
            ProvisionalWrite::new(
                Some(next.clone()),
                Committed::new(Some(base.clone())),
                event,
            ),
        ),
        (
            cells[1].clone(),
            ProvisionalWrite::new(None, Committed::new(Some(base.clone())), event),
        ),
    ];
    let clears: Vec<_> = clear
        .then(|| SectionClear::frozen(section, &writes))
        .into_iter()
        .collect();
    let mut touched = [&id, &remote]
        .map(|id| (id.state_type(), id.name().clone()))
        .to_vec();
    touched.sort_unstable();
    let marker = EventMarker::frozen(
        event,
        &writes,
        clears.clone(),
        &evidence(touched.into(), None),
    );
    if committed {
        let anchor = CollectionRef::new(if other { remote } else { id.clone() }, None);
        let evidence = evidence_only(&marker);
        store.commit_provisional(&anchor, &evidence, &[]).await?;
    }
    store
        .write_provisional(&collection, listed(&marker, &writes)?)
        .await?;
    let expected = if committed {
        [Some(next), None, (!clear).then_some(base.clone())]
    } else {
        [Some(base.clone()), Some(base.clone()), Some(base)]
    };
    let matches = check_residue(source, &id, &cells, &expected, section).await?;
    store.abort_provisional(&collection, &writes).await?;
    Ok(matches)
}

/// Checks every projection before the owner can remove the staged cells.
async fn check_residue<R: CommittedCellSource<Values> + CommittedCellSource<Presence>>(
    source: &R,
    id: &CollectionId,
    cells: &[CellKey; 3],
    expected: &[Option<Bytes>; 3],
    section: Section,
) -> Result<bool> {
    // All reads precede cleanup, so the projections can run concurrently.
    let points = async {
        let matches = try_join_all(cells.iter().zip(expected).map(|(cell, expected)| async {
            Ok::<_, Report>(
                CommittedCellSource::<Values>::load(source, id, cell.as_ref()).await? == *expected,
            )
        }))
        .await?;
        Ok::<_, Report>(matches.into_iter().all(|matched| matched))
    };
    let batches = async {
        let expected_presence = expected.each_ref().map(|value| value.as_ref().map(|_| ()));
        for batch in CoordinateBatch::chunks(cells.iter().map(|cell| cell.coordinate.clone())) {
            let borrowed = batch.as_ref();
            let (values, presence) = join!(
                CommittedCellSource::<Values>::load_many(source, id, section, &borrowed),
                CommittedCellSource::<Presence>::load_many(source, id, section, &borrowed),
            );
            let (values, presence) = (values?, presence?);
            if values.as_slice() != expected.as_slice() || presence.as_slice() != expected_presence
            {
                return Ok(false);
            }
        }
        Ok::<_, Report>(true)
    };
    let scans = async {
        let matches = try_join_all([Direction::Forward, Direction::Backward].into_iter().map(
            |dir| async move {
                let scan = Scan {
                    section,
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                    dir,
                    fetch_hint: None,
                };
                let (observed, presence) = join!(
                    CommittedCellSource::<Values>::scan(source, id, scan).try_collect::<Vec<_>>(),
                    CommittedCellSource::<Presence>::scan(source, id, scan)
                        .map_ok(|(key, ())| key)
                        .try_collect::<Vec<_>>(),
                );
                let (observed, presence) = (observed?, presence?);
                let mut wanted: Vec<_> = cells
                    .iter()
                    .zip(expected)
                    .filter_map(|(cell, value)| value.clone().map(|value| (cell.clone(), value)))
                    .collect();
                if dir == Direction::Backward {
                    wanted.reverse();
                }
                Ok::<_, Report>(
                    observed == wanted && presence.iter().eq(wanted.iter().map(|(key, _)| key)),
                )
            },
        ))
        .await?;
        Ok::<_, Report>(matches.into_iter().all(|matched| matched))
    };
    let (points, batches, scans) = try_join!(points, batches, scans)?;
    Ok(points && batches && scans)
}

fn residue_id(key: &StateKey, name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        key.clone(),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}
