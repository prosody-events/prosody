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
        let stage: Vec<_> = [19]
            .into_iter()
            .chain(weights.iter().map(|&w| u64::from(w)))
            .map(unit)
            .collect();
        let phases = stage_batches(&stage, u64::from(max_bytes), usize::from(max_count));
        let observed: Vec<Vec<u64>> = phases
            .iter()
            .filter(|phase| !phase.is_empty())
            .map(|phase| stage[phase.clone()].iter().map(BatchUnit::weight).collect())
            .collect();
        if stage.len() <= usize::from(max_count)
            && stage.iter().map(BatchUnit::weight).sum::<u64>() <= u64::from(max_bytes)
        {
            observed == vec![stage.iter().map(BatchUnit::weight).collect::<Vec<_>>()]
        } else {
            let mut expected = vec![vec![19]];
            if !weights.is_empty() {
                expected.push(weights.iter().map(|&w| u64::from(w)).collect());
            }
            expected.push(vec![19]);
            observed == expected
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<u16>, bool, u16, u8) -> bool);
}

/// A raw provisional cell without its recovery marker is invisible to the
/// sweep, while a point read still repairs it through the commit oracle.
#[tokio::test]
async fn markerless_provisional_is_sweep_invisible_but_first_touch_repairs() -> Result<()> {
    use super::{Pk, blob_weight};
    use smallvec::smallvec;

    init_test_logging();
    let fx = fixture().await?;
    let oracle = ScriptedOracle::default();
    let store = fx.bottom_store(oracle.clone())?;
    let c = collection("markerless-orphan")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"committed-after-crash");
    let staging = event(0xA11CE);
    let blob = encode_cell_blobs(Some(&data), None)?;
    let unit = [BatchUnit::new(
        blob_weight(&blob),
        smallvec![CellBatchRow {
            statement: &fx.queries.write_provisional,
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
    oracle.record_message(Uuid::from_u128(0xA11CE)).await?;

    assert!(store.unsettled_marker(c.id()).await?.is_none());
    assert!(
        sweep_provisional(&store, &oracle, &c).await?,
        "a markerless sweep sees no work"
    );
    assert!(
        store.provisional_cell_at(c.id(), &cell).await?.is_some(),
        "the sweep left the unlisted provisional cell untouched"
    );
    assert_eq!(
        store.get(c.id(), &cell, event(2)).await?,
        Committed::new(Some(data)),
        "first-touch resolves the orphan through the commit oracle"
    );
    assert!(store.provisional_cell_at(c.id(), &cell).await?.is_none());
    Ok(())
}
