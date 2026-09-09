use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::marker::AttemptId;
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::store::{CellStore, CoordinateBatch};
use crate::state::tests::support::probe;
use crate::state::{CollectionId, CollectionRef, StateKey, StateName, StateType};
use crate::state_reader::CommittedCellSource;
use bytes::Bytes;
use color_eyre::Result;
use futures::TryStreamExt;

/// Checks point, shared batch, and scan projections before the owner removes
/// residue.
pub(crate) async fn reader_residue<S: CellStore, R: CommittedCellSource>(
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
        &clears,
        &touched.into(),
        None,
        None,
        AttemptId::new(),
    );
    if committed {
        let anchor = CollectionRef::new(if other { remote } else { id.clone() }, None);
        let evidence = marker.committed_payload();
        store.commit_provisional(&anchor, &evidence, &[]).await?;
    }
    store
        .write_provisional(&collection, &writes, Some(&marker))
        .await?;
    let expected = if committed {
        [Some(next), None, Some(base.clone())]
    } else {
        [Some(base.clone()), Some(base.clone()), Some(base)]
    };
    for (cell, expected) in cells.iter().zip(&expected) {
        if source.load(&id, cell).await? != *expected {
            return Ok(false);
        }
    }
    for batch in CoordinateBatch::chunks(cells.iter().map(|cell| cell.coordinate.clone())) {
        if source.load_many(&id, section, &batch).await?.as_slice() != expected {
            return Ok(false);
        }
    }
    for dir in [Direction::Forward, Direction::Backward] {
        let scan = Scan {
            section,
            start: ScanEdge::Unbounded,
            end: ScanEdge::Unbounded,
            dir,
            limit: None,
        };
        let observed: Vec<_> = source.scan(&id, scan).try_collect().await?;
        let mut wanted: Vec<_> = cells
            .iter()
            .zip(&expected)
            .enumerate()
            .filter(|(index, _)| !committed || !clear || *index == 0)
            .filter_map(|(_, (cell, value))| value.clone().map(|value| (cell.clone(), value)))
            .collect();
        if dir == Direction::Backward {
            wanted.reverse();
        }
        if observed != wanted {
            return Ok(false);
        }
    }
    store.abort_provisional(&collection, &writes).await?;
    Ok(true)
}

fn residue_id(key: &StateKey, name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        key.clone(),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}
