use super::*;
use crate::state::marker::evidence_ttl;
use crate::timers::duration::CompactDuration;

/// The cache-fill co-expiry matches the value actually returned, not the
/// pre-resolution `TTL(data)`. A staged clear over a present base (`data`
/// NULL, `prev_data` present, finite stage TTL) whose event the oracle never
/// committed rolls back to `prev` on read — and the cache-fill point read must
/// report a finite co-expiry no later than the stage TTL. Reporting `None`
/// ("never expires", the old `TTL(data)`-only read) stamped the fjall entry
/// to strictly outlive the durable row, serving the value after the row died.
#[tokio::test]
async fn rolled_back_staged_clear_reports_finite_co_expiry() -> Result<()> {
    use crate::timers::duration::CompactDuration;

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default())?;
    let ttl = CompactDuration::new(3_600);
    let old = Bytes::from_static(b"old");

    let c = CollectionRef::new(collection("co-expiry-get")?.id().clone(), Some(ttl));
    let cell = value_cell();
    store
        .write_resolved(&c, &[(cell.clone(), Some(old.clone()))], &[])
        .await?;
    // `event(1)` is never recorded in the oracle, so resolution rolls the
    // staged clear back to `prev`.
    let writes = [(
        cell.clone(),
        ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(1)),
    )];
    let marker = EventMarker::frozen(event(1), &writes, &[], &[].into(), None);
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
    fn prop(first: u16, second: Option<u16>, clear: bool) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let fx = fixture().await?;
            let store = fx.bottom_store(ScriptedOracle::default())?;
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
            let ttl = evidence_ttl([Some(first), second].into_iter());
            let marker = EventMarker::frozen(event(1), &writes, &clears, &[].into(), ttl);
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
                    ttl
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
        .quickcheck(prop as fn(u16, Option<u16>, bool) -> TestResult);
}

/// The `Cached` stage-boundary marker eviction: event A stages two coordinates
/// through the cached assembly (fjall holds A's stage-time `prev`s as the
/// committed projections), A's commit marker is recorded but the settle never
/// runs (the skipped-settle window); event B then stages ONE overlapping
/// coordinate through the same assembly. The lower store's stage boundary
/// resolves A's event marker *beneath* the cache, so
/// `Cached::write_provisional` must delete A's marker-listed coordinates'
/// entries BEFORE forwarding down — without the delete, A's untouched
/// coordinate keeps serving the stale warm `prev` verbatim forever. A
/// deterministic falsifier of the delete-ordering the fault/crash alphabet
/// surfaces as model divergence against the live `Cached` composition; it
/// isolates the skipped-settle boundary window without a generated schedule.
#[tokio::test]
async fn stage_boundary_deletes_foreign_marker_entries() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let oracle = ScriptedOracle::default();
    let store = Cached::new(
        test_db::cache("cassandra_boundary_delete")?,
        fx.bottom_store(oracle.clone())?,
    );
    let c = collection("boundary-delete")?;
    let id = c.id().clone();
    let (cell0, cell1) = (cell_i(0), cell_i(1));
    let (base0, base1) = (Bytes::from_static(b"base0"), Bytes::from_static(b"base1"));

    // Committed bases, warmed by the write-through publish.
    store
        .write_resolved(
            &c,
            &[
                (cell0.clone(), Some(base0.clone())),
                (cell1.clone(), Some(base1.clone())),
            ],
            &[],
        )
        .await?;

    // Event A stages over both coordinates; its stage re-publishes the prevs
    // (the bases) as the warm committed projections. A's commit marker is
    // recorded — A is committed — but the settle is never attempted.
    let writes_a = [
        (
            cell0.clone(),
            ProvisionalWrite::new(
                Some(Bytes::from_static(b"a0")),
                Committed::new(Some(base0)),
                event(1),
            ),
        ),
        (
            cell1.clone(),
            ProvisionalWrite::new(
                Some(Bytes::from_static(b"a1")),
                Committed::new(Some(base1)),
                event(1),
            ),
        ),
    ];
    let marker_a = EventMarker::frozen(event(1), &writes_a, &[], &[].into(), None);
    store
        .write_provisional(&c, &writes_a, Some(&marker_a))
        .await?;
    oracle.record_message(Uuid::from_u128(1)).await?;

    // Event B stages the overlapping coordinate 1. Its prev-read may serve
    // the warm pre-settle value (the accepted bounded window — nothing is
    // asserted on it); the stage's boundary then deletes A's listed
    // coordinates' entries before the lower resolve beneath the cache.
    let prev_b = store.get(&id, &cell1, event(2)).await?;
    let writes_b = [(
        cell1.clone(),
        ProvisionalWrite::new(Some(Bytes::from_static(b"b1")), prev_b, event(2)),
    )];
    let marker_b = EventMarker::frozen(event(2), &writes_b, &[], &[].into(), None);
    store
        .write_provisional(&c, &writes_b, Some(&marker_b))
        .await?;

    // A's untouched coordinate 0 must read A's committed data: the boundary
    // delete evicted it, so the read falls through to the boundary-promoted
    // row instead of serving the stale warm prev.
    assert_eq!(
        store.get(&id, &cell0, event(3)).await?,
        Committed::new(Some(Bytes::from_static(b"a0"))),
        "the stage-boundary delete must drop the stale warm prev"
    );
    Ok(())
}
