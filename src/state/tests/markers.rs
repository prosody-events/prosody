//! Committed evidence reads preserve marker identity and batching.

use super::*;

/// Batches and scans share one marker snapshot and preserve reader parity.
#[test]
pub(super) fn prop_resolve_reads_each_marker_once() {
    fn property(value: u8, siblings: u8, certificate: u8, length: u8) -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let cells = MemoryCells::new();
            let id = fresh_collection("read-budget")?;
            let count = usize::from(siblings % 8) + 1;
            let mut collections = Vec::with_capacity(count);
            collections.push(CollectionRef::new(id.clone(), None));
            for index in 1..count {
                collections.push(CollectionRef::new(
                    CollectionId::new(
                        id.state_key().clone(),
                        id.state_type(),
                        StateName::try_new(format!("sibling-{index}"))?,
                    ),
                    None,
                ));
            }
            let memory = MemoryCellStore::new(cells.clone());
            let store = CountingCellStore::new(memory.clone()).with_marker_counts(&collections);
            let touched = collections
                .iter()
                .map(CollectionRef::id)
                .map(|id| (id.state_type(), id.name().clone()))
                .collect();
            let evidence = evidence(touched, None);
            let event = support::probe(1);
            let data = bytes(value);
            let prev = bytes(value.wrapping_add(1));
            let length = length % 32 + 2;
            let writes: Vec<_> = (0..length)
                .map(|index| {
                    (
                        cell_in(0, index),
                        ProvisionalWrite::new(
                            Some(data.clone()),
                            Committed::new(Some(prev.clone())),
                            event,
                        ),
                    )
                })
                .collect();
            let marker = EventMarker::frozen(event, &writes, &[], &evidence);
            for collection in &collections {
                store
                    .write_provisional(collection, &writes, Some(&marker))
                    .await?;
            }
            let certificate = usize::from(certificate) % (count + 1);
            if let Some(collection) = collections.get(certificate) {
                support::seed_commit_evidence(&store, collection).await?;
            }
            let expected = Some(if certificate < count { data } else { prev });

            check_memory_read_parity(&memory, &id, &writes, expected.as_ref()).await?;

            // Exercise a batch and both scan directions with separate lookups.
            for direction in [None, Some(Direction::Forward), Some(Direction::Backward)] {
                store.reset();
                let mut lookup = EvidenceLookup::new(&store, &id);
                assert_eq!(
                    lookup
                        .resolve(Cell::Resolved(Committed::<Values>::new(None)))
                        .await?
                        .into_inner(),
                    None
                );
                assert_eq!(store.marker_reads(), 0, "resolved cells need no evidence");
                let mut keys: Vec<_> = writes.iter().map(|(cell, _)| cell.clone()).collect();
                if direction == Some(Direction::Backward) {
                    keys.reverse();
                }
                if direction.is_none() {
                    keys.extend_from_within(..);
                }
                let rows = stream::iter(keys);
                pin_mut!(rows);
                while let Some(cell) = rows.next().await {
                    let actual = lookup
                        .resolve(Cell::Provisional(
                            store
                                .provisional_cell_at(&id, &cell)
                                .await?
                                .ok_or_else(|| eyre!("provisional cell missing"))?,
                        ))
                        .await?
                        .into_inner();
                    assert_eq!(actual, expected);
                    assert_eq!(actual, cells.read_committed(&id, cell.as_ref()));
                }
                assert_eq!(store.marker_reads(), count, "one snapshot per call");
                for collection in &collections {
                    assert_eq!(
                        store.marker_reads_for(collection.id()),
                        1,
                        "read each marker once"
                    );
                }
                assert_eq!(store.durable_writes(), 0, "reads preserve durable state");
            }
            Ok(())
        })
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, u8, u8) -> Result<()>);
}

pub(super) async fn check_memory_read_parity(
    store: &MemoryCellStore,
    id: &CollectionId,
    writes: &[(CellKey, ProvisionalWrite)],
    expected: Option<&Bytes>,
) -> Result<()> {
    use crate::state::{Scan, ScanEdge};
    use futures::TryStreamExt;

    let batch = CoordinateBatch::chunks(writes.iter().map(|(cell, _)| cell.coordinate.clone()))
        .next()
        .ok_or_else(|| eyre!("batch missing"))?;
    let values = CellRead::<Values>::read_many(store, id, writes[0].0.section, &batch.as_ref())
        .await
        .map(|cells| {
            cells
                .into_iter()
                .map(|(committed, _)| committed)
                .collect::<CommittedBatch>()
        })?;
    assert_eq!(values.len(), batch.len());
    for value in values {
        assert_eq!(value.into_inner().as_ref(), expected);
    }

    let values =
        CellRead::<Values>::read_many(store, id, writes[0].0.section, &batch.as_ref()).await?;
    assert_eq!(values.len(), batch.len());
    for (value, ttl) in values {
        assert_eq!(value.into_inner().as_ref(), expected);
        assert_eq!(ttl, None);
    }

    for dir in [Direction::Forward, Direction::Backward] {
        let scan = Scan {
            section: writes[0].0.section,
            start: ScanEdge::Unbounded,
            end: ScanEdge::Unbounded,
            dir,
            fetch_hint: None,
        };
        let rows: Vec<_> = CellRead::<Values>::scan(store, id, scan)
            .try_collect()
            .await?;
        assert_eq!(rows.len(), writes.len());
        for (_, value) in rows {
            assert_eq!(Some(&value), expected);
        }
    }
    Ok(())
}
