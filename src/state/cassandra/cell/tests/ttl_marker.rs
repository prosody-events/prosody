use super::*;
use crate::state::marker::AttemptId;
use crate::state::marker::evidence_ttl;
use crate::timers::duration::CompactDuration;

/// An uncommitted clear reads prev with the provisional row's finite expiry.
/// The cache entry must expire no later than the durable value it contains.
#[tokio::test]
async fn rolled_back_staged_clear_reports_finite_co_expiry() -> Result<()> {
    use crate::timers::duration::CompactDuration;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store();
    let ttl = CompactDuration::new(3_600);
    let old = Bytes::from_static(b"old");

    let c = CollectionRef::new(collection("co-expiry-get")?.id().clone(), Some(ttl));
    let cell = value_cell();
    store
        .write_resolved(&c, &[(cell.clone(), Some(old.clone()))], &[])
        .await?;
    // No certificate exists for event(1), so the read projects prev.
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(1)),
    )];
    let marker = EventMarker::frozen(
        event(1),
        &writes,
        &[],
        &[].into(),
        None,
        None,
        AttemptId::new(),
    );
    store.write_provisional(&c, &writes, Some(&marker)).await?;

    let (committed, co_expiry) = store.get_for_cache(c.id(), &cell, event(2)).await?;
    assert_eq!(
        committed.into_inner().as_ref(),
        Some(&old),
        "rollback returns prev"
    );
    let co_expiry = co_expiry.ok_or_else(|| {
        eyre!("a rolled-back staged clear must report a finite co-expiry, not never")
    })?;
    assert!(
        co_expiry <= ttl,
        "co-expiry {co_expiry:?} must not exceed the stage TTL {ttl:?}"
    );
    Ok(())
}

/// Staged uses the collection TTL. Committed covers each sibling's retention.
#[test]
fn marker_rows_carry_evidence_ttl() {
    fn prop(first: u16, second: Option<u16>, clear: bool, floor: u16) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let fx = fixture().await?;
            let store = fx.bottom_store();
            let first = CompactDuration::new(u32::from(first) + 60);
            let second = second.map(|seconds| CompactDuration::new(u32::from(seconds) + 60));
            let c =
                CollectionRef::new(collection("marker-evidence-ttl")?.id().clone(), Some(first));
            let writes = [(
                value_cell(),
                ProvisionalWrite::new(
                    Some(Bytes::from_static(b"v")),
                    Committed::new(None),
                    event(1),
                ),
            )];
            let clears: Vec<_> = clear
                .then(|| SectionClear::frozen(value_cell().section, &writes))
                .into_iter()
                .collect();
            let floor = CompactDuration::new(
                first
                    .seconds()
                    .max(second.map_or(0, CompactDuration::seconds))
                    + u32::from(floor)
                    + 120,
            );
            let expected_evidence = second.map(|second| first.max(second).max(floor));
            let ttl = evidence_ttl(floor, [Some(first), second].into_iter());
            let marker = EventMarker::frozen(
                event(1),
                &writes,
                &clears,
                &[].into(),
                ttl,
                None,
                AttemptId::new(),
            );
            store.write_provisional(&c, &writes, Some(&marker)).await?;
            for coordinate in [&[][..], &[1_u8][..]] {
                if !coordinate.is_empty() {
                    store.commit_provisional(&c, &marker, &writes).await?;
                }
                let column = if coordinate.is_empty() {
                    "data"
                } else {
                    "event"
                };
                let expected = if coordinate.is_empty() {
                    Some(first)
                } else {
                    expected_evidence
                };
                let pk = Pk::of(c.id());
                let row = fx
                    .cassandra
                    .session()
                    .query_unpaged(
                        format!(
                            "SELECT TTL({column}) FROM {TEST_KEYSPACE}.keyed_state_cell WHERE \
                             segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind \
                             = ? AND section = ? AND coordinate = ?"
                        ),
                        (
                            pk.segment_id,
                            pk.key,
                            pk.state_type,
                            pk.name,
                            CellKind::Marker,
                            0_i8,
                            coordinate,
                        ),
                    )
                    .await?
                    .into_rows_result()?
                    .single_row::<(Option<i32>,)>()?;
                if let Some(expected) = expected {
                    let remaining = row.0.ok_or_else(|| eyre!("missing marker TTL"))?;
                    let expected = expected.seconds() as i32;
                    assert!(
                        remaining <= expected && remaining > expected - 60_i32,
                        "column={column}, remaining={remaining}, expected={expected}"
                    );
                } else {
                    assert_eq!(row.0, None);
                }
            }
            Ok(true)
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(u16, Option<u16>, bool, u16) -> TestResult);
}
