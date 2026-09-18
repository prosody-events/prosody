use super::*;
use crate::state::cell::Values;
use crate::state::marker::{AttemptId, EventEvidence};
use crate::state::store::CellRead;
use crate::state::tests::support::evidence;
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
    let marker = EventMarker::frozen(event(1), &writes, &[], &evidence([].into(), None));
    store.write_provisional(&c, &writes, Some(&marker)).await?;

    let (committed, co_expiry) = CellRead::<Values>::read(&store, c.id(), &cell).await?;
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

/// Staged uses the collection TTL. Committed uses only the finite dedup TTL.
#[test]
fn marker_rows_carry_evidence_ttl() {
    fn prop((first, second, clear, floor): (u16, Option<u16>, bool, u16)) -> Result<bool> {
        TEST_RUNTIME.block_on(async {
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
            let floor = CompactDuration::new(u32::from(floor) + 60);
            let sibling = CollectionRef::new(
                CollectionId::new(
                    c.id().state_key().clone(),
                    c.id().state_type(),
                    collection("marker-ttl-sibling")?.id().name().clone(),
                ),
                second,
            );
            let marker = EventMarker::frozen(
                event(1),
                &writes,
                &clears,
                &EventEvidence {
                    touched: [
                        (c.id().state_type(), c.id().name().clone()),
                        (sibling.id().state_type(), sibling.id().name().clone()),
                    ]
                    .into(),
                    evidence_ttl: floor,
                    dedup: None,
                    attempt: AttemptId::new(),
                },
            );
            for collection in [&c, &sibling] {
                store
                    .write_provisional(collection, &writes, Some(&marker))
                    .await?;
                for (coordinate, column, expected) in [
                    (&[][..], "data", collection.ttl()),
                    (&[1_u8][..], "event", Some(floor)),
                ] {
                    if !coordinate.is_empty() {
                        store
                            .commit_provisional(collection, &marker, &writes)
                            .await?;
                    }
                    let pk = Pk::of(collection.id());
                    let row = fx
                        .cassandra
                        .session()
                        .query_unpaged(
                            format!(
                                "SELECT TTL({column}) FROM {TEST_KEYSPACE}.keyed_state_cell WHERE \
                                 segment_id = ? AND key = ? AND state_type = ? AND name = ? AND \
                                 kind = ? AND section = ? AND coordinate = ?"
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
            }
            Ok(true)
        })
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ModelProperty(prop));
}
