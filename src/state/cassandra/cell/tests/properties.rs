use super::*;
use crate::state::cassandra::cell::TABLE_KEYED_STATE_CELL;
use crate::state::tests::support::run_admit_soundness;
use color_eyre::eyre::ensure;

/// Converts a property body's `Result<bool>` into a `TestResult`, surfacing the
/// error on failure (a store/setup error is a broken environment, not a
/// shrinkable property failure).
pub(super) fn finish(result: Result<bool>) -> TestResult {
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::failed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// Proves recovery parity for the production Cassandra cache.
///
/// Each simulated crash creates a new cache over the same durable rows.
#[test]
fn prop_cassandra_cell_crash_equivalence() {
    async fn run(trace: Trace) -> Result<bool> {
        let fx = fixture().await?;
        let dedup = MemoryDeduplicationStore::default();
        // Each `make` is a crash: a cold fjall cache over the same durable
        // Cassandra rows, with the runner's lower fault seam between them.
        // `cold_cache` clears the shared `cassandra_crash` keyspace pair (a
        // cheap journal marker, no keyspace-creation fsync) instead of
        // minting a fresh workspace per make; distinct v4 segments per
        // iteration keep the shared keyspace disjoint. The cleared index
        // keyspace also resets the bottom store's marker check — per-
        // assignment state dies with the assignment, so the marker-check handle is
        // minted from that same cold cache.
        let make = |handle: &PoisonHandle| -> Result<FaultyBottom> {
            let cache = test_db::cold_cache("cassandra_crash")?;
            Ok(Cached::new(
                cache,
                FailingCellStore::with_handle(fx.bottom_store(), handle.clone()),
            ))
        };
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        Box::pin(run_crash_equivalence_trace(
            make,
            dedup.clone(),
            trace,
            &probe,
        ))
        .await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck((|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(Trace) -> TestResult);
}

/// Posture-parity test over the bare live store: a blind `write_resolved`
/// leaves an unsettled clears-FREE marker unsettled.
#[test]
fn cassandra_blind_write_leaves_clears_free_marker() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let fx = fixture().await?;
        let store = fx.bottom_store();
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_blind_write_leaves_clears_free_marker(store, &probe).await
    })
}

/// Apply idempotence over the bare live store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// reads over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
fn prop_cassandra_apply_idempotence() {
    async fn run(input: ApplyTrace) -> Result<bool> {
        let fx = fixture().await?;
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_apply_idempotence(fx.bottom_store(), input, &probe).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|input| finish(TEST_RUNTIME.block_on(run(input)))) as fn(ApplyTrace) -> TestResult,
        );
}

/// Implicit-overwrite soundness over `Cached<CassandraStore>`: each overwrite
/// resolves prior residue through admission before the next stage.
#[test]
fn prop_cassandra_cell_implicit_overwrite() {
    async fn run(trace: OverwriteTrace) -> Result<bool> {
        let fx = fixture().await?;
        let dedup = MemoryDeduplicationStore::default();
        // Each op reads its committed base through a fresh COLD store, so
        // `make` clears the shared `cassandra_overwrite` keyspace pair (no
        // keyspace-creation fsync); distinct v4 segments per iteration keep it
        // disjoint. The cleared index keyspace resets the bottom store's
        // marker check too — a fresh cold assignment — so its marker-check handle
        // is minted from that same cold cache.
        let make = || -> Result<Bottom> {
            let cache = test_db::cold_cache("cassandra_overwrite")?;
            Ok(Cached::new(cache, fx.bottom_store()))
        };
        run_overwrite_trace(make, dedup.clone(), trace).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(OverwriteTrace) -> TestResult,
        );
}

/// A single `Cached<CassandraStore>` over the shared `cassandra_overlay`
/// fjall keyspace pair (warm-reuse; distinct v4 segments keep iterations
/// disjoint).
fn assembly(fx: &Fixture) -> Result<Bottom> {
    Ok(Cached::new(
        test_db::cache("cassandra_overlay")?,
        fx.bottom_store(),
    ))
}

/// Unified view soundness over `Overlay<Cached<CassandraStore>>`: point `get`s,
/// range `scan`s (bounds, direction, early-stop), dirty buffering, and
/// committed writes intermixed in one trace, all vs the sorted-map oracle
/// (unified-view soundness and oracle-correctness properties).
#[test]
fn prop_cassandra_overlay_view() {
    async fn run(trace: OverlayTrace) -> Result<bool> {
        let fx = fixture().await?;
        // Box the future: the assembly + trace exceed clippy's large-future
        // threshold on the stack.
        Box::pin(run_overlay_trace(assembly(&fx)?, trace)).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(OverlayTrace) -> TestResult,
        );
}

/// Both Cassandra scan projections match the committed model across bounds and
/// section clears.
#[test]
fn prop_cassandra_bottom_scan() {
    async fn run(trace: ScanTrace) -> Result<bool> {
        let fx = fixture().await?;
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        Box::pin(run_bottom_scan_trace(fx.bottom_store(), trace, &probe)).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(ScanTrace) -> TestResult,
        );
}

/// `TTL(data)` surfacing for the co-expiry stamp (no cluster needed — pure
/// boundary cases). A NULL means the cell has no TTL → never expires (`None`).
/// A present value is the whole remaining seconds and must round-trip —
/// crucially `0` (sub-second remaining) maps to an *immediate* expiry, never
/// `None`, or a fjall entry would outlive a durable row that dies within the
/// second.
#[test]
fn ttl_seconds_surfacing_distinguishes_no_ttl_from_sub_second() {
    use super::ttl_seconds_to_duration;
    use crate::timers::duration::CompactDuration;

    assert_eq!(ttl_seconds_to_duration(None), None, "NULL ⇒ no TTL (never)");
    assert_eq!(
        ttl_seconds_to_duration(Some(0_i32)),
        Some(CompactDuration::new(0)),
        "0 ⇒ sub-second remaining, an immediate expiry — never None"
    );
    assert_eq!(
        ttl_seconds_to_duration(Some(42_i32)),
        Some(CompactDuration::new(42))
    );
    assert_eq!(
        ttl_seconds_to_duration(Some(-1_i32)),
        Some(CompactDuration::new(0)),
        "a defensive negative also stamps an immediate expiry, not never"
    );
}

#[test]
fn prop_cassandra_admit_soundness() {
    fn property(value: u8, committed: bool) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let fx = fixture().await?;
            let dedup = MemoryDeduplicationStore::new();
            let store = fx.bottom_store();
            let c = collection("corrupt-admission")?;
            let id = c.id();
            let corrupt = format!(
                "INSERT INTO {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} (segment_id, key, \
                 state_type, name, kind, section, coordinate, version) VALUES (?, ?, ?, ?, 1, 0, \
                 0x02, 99)"
            );
            fx.cassandra
                .session()
                .query_unpaged(
                    corrupt,
                    (
                        id.state_key().segment_id,
                        id.state_key().key.as_ref(),
                        id.state_type(),
                        id.name().as_str(),
                    ),
                )
                .await?;
            ensure!(store.marker_state(id).await.is_err());
            ensure!(
                admit_collection(&store, &dedup, &c).await?,
                "corrupt marker blocked admission"
            );
            run_admit_soundness(store, dedup, value, committed).await
        }))
    }
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(u8, bool) -> TestResult);
}
