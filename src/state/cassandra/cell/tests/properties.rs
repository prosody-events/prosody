use super::*;

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
        let oracle = ScriptedOracle::default();
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
            let presence = cache.marker_checks();
            Ok(Cached::new(
                cache,
                FailingCellStore::with_handle(
                    fx.bottom_store_with(oracle.clone(), presence),
                    handle.clone(),
                ),
            ))
        };
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        Box::pin(run_crash_equivalence_trace(
            make,
            oracle.clone(),
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

/// Proves that a resolved write survives an earlier unsettled section clear.
#[test]
fn cassandra_blind_write_survives_stale_clear() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let cache = test_db::cold_cache("cassandra_blind_write")?;
        let presence = cache.marker_checks();
        let store = Cached::new(cache, fx.bottom_store_with(oracle.clone(), presence));
        run_blind_write_survives_stale_clear(store, oracle).await
    })
}

/// Posture-parity test over the bare live store: a blind `write_resolved`
/// leaves an unsettled clears-FREE marker unsettled.
#[test]
fn cassandra_blind_write_leaves_clears_free_marker() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_blind_write_leaves_clears_free_marker(store, &probe).await
    })
}

/// Regression test over the production `Cached<CassandraStore>` assembly: a
/// repair whose payload predates an unsettled committed clears-bearing marker
/// defers to peek semantics beneath the cache, so the marker's own resolution
/// (the committed positional clear) erases the cell instead of a stale repair
/// resurrecting it. The section-clear cache guard clear-eviction beats the
/// earlier deferred fill. Falsify by deleting the `deferred` guard in
/// `resolve_cell`.
#[test]
fn cassandra_repair_defers_beneath_stale_clear() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        // Stage under the fixture's presence (the prior assignment). The reader
        // is a fresh cold assignment: a cold cache whose own marker check is
        // cold, so `x` never warms it and the Cached read cold-seeds from
        // durable truth, reaching `resolve_cell`.
        let stage = fx.bottom_store(oracle.clone());
        let cache = test_db::cold_cache("cassandra_repair_defer")?;
        let presence = cache.marker_checks();
        let store = Cached::new(cache, fx.bottom_store_with(oracle.clone(), presence));
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_repair_defers_beneath_stale_clear(&stage, store, oracle, &probe).await
    })
}

/// Convergence test over `Cached<CassandraStore>`: the deferral wedges nothing
/// — when the unsettled marker aborts, x's committed projection stays its base.
#[test]
fn cassandra_repair_after_marker_abort_converges() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let stage = fx.bottom_store(oracle.clone());
        let cache = test_db::cold_cache("cassandra_repair_abort")?;
        let presence = cache.marker_checks();
        let store = Cached::new(cache, fx.bottom_store_with(oracle.clone(), presence));
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_repair_after_marker_abort_converges(&stage, store, oracle, &probe).await
    })
}

/// Apply idempotence over the bare live store: any generated interleaving of
/// marker resolution, verdict-matching settle re-applies, and per-cell
/// first-touches over one staged set with durable section clears converges to
/// the verdict state — no marker, no provisional residue, exact row shape.
#[test]
fn prop_cassandra_apply_idempotence() {
    async fn run(input: ApplyTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_apply_idempotence(fx.bottom_store(oracle.clone()), oracle, input, &probe).await
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(
            (|input| finish(TEST_RUNTIME.block_on(run(input)))) as fn(ApplyTrace) -> TestResult,
        );
}

/// Implicit-overwrite soundness over `Cached<CassandraStore>`: each overwrite
/// resolves its predecessor's provisional cell through the oracle on read, with
/// no explicit promote or rollback.
#[test]
fn prop_cassandra_cell_implicit_overwrite() {
    async fn run(trace: OverwriteTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        // Each op reads its committed base through a fresh COLD store, so
        // `make` clears the shared `cassandra_overwrite` keyspace pair (no
        // keyspace-creation fsync); distinct v4 segments per iteration keep it
        // disjoint. The cleared index keyspace resets the bottom store's
        // marker check too — a fresh cold assignment — so its marker-check handle
        // is minted from that same cold cache.
        let make = || -> Result<Bottom> {
            let cache = test_db::cold_cache("cassandra_overwrite")?;
            let presence = cache.marker_checks();
            Ok(Cached::new(
                cache,
                fx.bottom_store_with(oracle.clone(), presence),
            ))
        };
        run_overwrite_trace(make, oracle.clone(), trace).await
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
        fx.bottom_store(ScriptedOracle::default()),
    ))
}

/// Unified view soundness over `Overlay<Cached<CassandraStore>>`: point `get`s,
/// range `scan`s (bounds, direction, limit, early-stop), dirty buffering, and
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

/// Scan correctness directly over `CassandraStore::scan_cells` — the live
/// `ORDER BY ASC/DESC`, clustering-range bounds, and `LIMIT`/in-code `end` the
/// overlay merge delegates to.
#[test]
fn prop_cassandra_bottom_scan() {
    async fn run(trace: ScanTrace) -> Result<bool> {
        let fx = fixture().await?;
        let probe = CassandraShapeProbe {
            session: fx.cassandra.clone(),
        };
        run_bottom_scan_trace(fx.bottom_store(ScriptedOracle::default()), trace, &probe).await
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
