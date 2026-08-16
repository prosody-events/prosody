use super::*;

/// The pure single-batch packing decision both marker-ordering callers rest
/// on — `write_provisional`'s stage marker-FIRST choice and
/// `marker_last_split`'s settle marker-LAST split: a unit set fits one batch
/// iff the weight sum is within the byte budget AND the unit count is within
/// the statement budget. (The intra-call tear of an over-budget stage cannot be
/// injected through the trait; the ordering is enforced by the two sequential
/// awaits plus this decision, and the marker-completeness postcondition guards
/// the durable shape on every generated trace.)
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

/// Settle's budget decision preserves one atomic batch whenever possible and
/// otherwise isolates the final marker unit as the split tail. This pins the
/// split INDEX `issue_marker_last` relies on; the temporal ordering (prefix
/// awaited before the marker tail) is enforced structurally by that helper
/// owning both awaits, not by this pure property.
#[test]
fn prop_over_budget_settle_issues_marker_last() {
    use smallvec::SmallVec;

    fn prop(weights: Vec<u16>, marker_weight: u16, max_bytes: u16, max_count: u8) -> bool {
        let mut units: Vec<BatchUnit<()>> = Vec::with_capacity(weights.len() + 1);
        units.extend(
            weights
                .into_iter()
                .map(|weight| BatchUnit::new(u64::from(weight), SmallVec::new())),
        );
        units.push(BatchUnit::new(u64::from(marker_weight), SmallVec::new()));
        let split = marker_last_split(&units, u64::from(max_bytes), usize::from(max_count));
        let fits = fits_one_batch(
            units.iter().map(BatchUnit::weight),
            u64::from(max_bytes),
            usize::from(max_count),
        );

        if fits {
            split == units.len()
        } else {
            split + 1 == units.len()
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u16>, u16, u16, u8) -> bool);
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
    let store = fx.bottom_store(oracle.clone());
    let c = collection("markerless-orphan")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"committed-after-crash");
    let staging = event(0xA11CE);
    let blob = encode_cell_blobs(Some(&data), None)?;
    let unit = [BatchUnit::new(
        blob_weight(&blob),
        smallvec![CellBatchRow {
            statement: &fx.queries.write_provisional_no_ttl,
            row: RowShape::Stage(StageRow {
                ttl: None,
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

    assert!(store.standing_marker(c.id()).await?.is_none());
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
