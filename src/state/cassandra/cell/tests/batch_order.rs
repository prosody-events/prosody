use super::*;

/// A batch fits only when both its byte and statement counts fit.
#[test]
fn fits_one_batch_decides_on_both_budgets() {
    // Strictly under both budgets, and exactly at both boundaries.
    assert!(fits_one_batch([1, 2].into_iter(), 5, 3));
    assert!(fits_one_batch([2, 3].into_iter(), 5, 2));
    // Over the byte budget by one.
    assert!(!fits_one_batch([3, 3].into_iter(), 5, 8));
    // Over the count budget by one.
    assert!(!fits_one_batch([1, 1, 1].into_iter(), 100, 2));
    // Empty always fits.
    assert!(fits_one_batch(iter::empty(), 0, 0));
}

/// Both write paths preserve atomic batches and the required split phases.
#[test]
fn prop_marker_batch_phases() {
    fn unit(weight: u64) -> BatchUnit<()> {
        BatchUnit::new(weight, smallvec::SmallVec::new())
    }
    fn prop(weights: Vec<u16>, evidence: bool, max_bytes: u16, max_count: u8) -> bool {
        let weights: Vec<_> = weights.into_iter().map(|weight| weight % 1024).collect();
        let middle = weights
            .iter()
            .map(|&weight| unit(u64::from(weight)))
            .collect();
        let (units, phases) = settle_batches(
            evidence.then(|| unit(19)),
            middle,
            unit(23),
            u64::from(max_bytes),
            usize::from(max_count),
        );
        let fits = units.len() <= usize::from(max_count)
            && units.iter().map(BatchUnit::weight).sum::<u64>() <= u64::from(max_bytes);
        let observed: Vec<Vec<u64>> = phases
            .iter()
            .filter(|phase| !phase.is_empty())
            .map(|phase| units[phase.clone()].iter().map(BatchUnit::weight).collect())
            .collect();
        let mut expected: Vec<Vec<u64>> = Vec::new();
        if fits {
            expected.push(
                evidence
                    .then_some(19)
                    .into_iter()
                    .chain(weights.iter().map(|&w| u64::from(w)))
                    .chain([23])
                    .collect(),
            );
        } else {
            if evidence {
                expected.push(vec![19]);
            }
            if !weights.is_empty() {
                expected.push(weights.iter().map(|&w| u64::from(w)).collect());
            }
            expected.push(vec![23]);
        }
        if observed != expected {
            return false;
        }
        let marker = unit(19);
        let cells: Vec<_> = weights.iter().map(|&w| unit(u64::from(w))).collect();
        let chunks: Vec<_> = stage_batches(
            &marker,
            &cells,
            u64::from(max_bytes),
            usize::from(max_count),
        )
        .collect();
        let mut observed = Vec::with_capacity(weights.len());
        for chunk in &chunks {
            if chunk.end > cells.len() {
                return false;
            }
            let bound: Vec<_> = super::super::batch::stage_chunk(&marker, &cells, chunk.clone())
                .map(BatchUnit::weight)
                .collect();
            if bound.first() != Some(&19) || bound.len() != chunk.len() + 1 {
                return false;
            }
            let count = chunk.len() + 1;
            let bytes = 19
                + cells[chunk.clone()]
                    .iter()
                    .map(BatchUnit::weight)
                    .sum::<u64>();
            // A single oversized cell still needs one atomic marker-and-cell mutation.
            if chunk.len() > 1 && (count > usize::from(max_count) || bytes > u64::from(max_bytes)) {
                return false;
            }
            observed.extend(cells[chunk.clone()].iter().map(BatchUnit::weight));
        }
        !chunks.is_empty() && observed == weights.into_iter().map(u64::from).collect::<Vec<_>>()
    }
    assert!(prop(vec![0, 0], false, 0, 3));
    QuickCheck::new().quickcheck(prop as fn(Vec<u16>, bool, u16, u8) -> bool);
}

/// Admission cannot discover a provisional cell without a Staged row.
/// The owner reads its committed base and leaves the physical cell unchanged.
#[tokio::test]
async fn markerless_provisional_reads_its_committed_base() -> Result<()> {
    use super::{Pk, blob_weight};
    use smallvec::smallvec;

    init_test_logging();
    let fx = fixture().await?;
    let dedup = MemoryDeduplicationStore::default();
    let store = fx.bottom_store();
    let c = collection("markerless-orphan")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"committed-after-crash");
    let staging = event(0xA11CE);
    let blob = encode_cell_blobs(Some(&data), None)?;
    let unit = [BatchUnit::new(
        blob_weight(&blob),
        smallvec![CellBatchRow {
            statement: &fx.queries.cells.write_provisional,
            row: RowShape::Stage(StageRow {
                ttl: 0,
                data: blob.data(),
                prev_data: None,
                encoding: blob.encoding(),
                version: blob.version(),
                event: staging,
                addr: CellAddr::new(Pk::of(c.id()), &cell),
            }),
        }],
    )];
    fx.cassandra
        .execute_unlogged_batches(&unit, 1 << 20, 4_096, SHARD_FANOUT_CONCURRENCY)
        .await?;
    dedup.insert(Uuid::from_u128(0xA11CE)).await?;

    assert!(store.marker_state(c.id()).await?.staged.is_none());
    assert!(
        admit_collection(&store, &dedup, &c).await?,
        "admission sees no Staged row"
    );
    assert!(
        store.provisional_cell_at(c.id(), &cell).await?.is_some(),
        "admission left the unlisted provisional cell untouched"
    );
    assert_eq!(
        store.get(c.id(), &cell).await?,
        Committed::new(None),
        "a markerless legacy cell reads its committed base"
    );
    assert!(store.provisional_cell_at(c.id(), &cell).await?.is_some());
    Ok(())
}
