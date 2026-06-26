//! Live-cluster tests for the Cassandra [`CassandraStore`] cell backend.
//!
//! These run against the local Cassandra node and the shared `prosody_test`
//! keyspace (migrated on driver connect). They exercise the part the pure
//! decoder test cannot: `prepare`/`bind`/round-trip of every cell statement,
//! including the promote-of-clear residue read back live. The backend-generic
//! property suites ([`crate::state::tests::cell_suite`]) run here over the
//! production assembly `Cached<CassandraStore>` and `Overlay<Cached<…>>`, so
//! memory and Cassandra prove identical invariants. Each test mints a fresh
//! `segment_id` (and the property suites mint one per iteration) so rows never
//! collide across runs.

use super::{CassandraStore, CellQueries};
use crate::Topic;
use crate::cassandra::{CassandraConfiguration, CassandraStore as CassandraSession};
use crate::state::cached::Cached;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::fjall::{AssignmentEpoch, FjallCellCache, FjallClient, FjallConfiguration};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CellStore;
use crate::state::tests::cell_suite::{
    OverlayTrace, OverwriteTrace, ScanTrace, ScriptedOracle, Trace, run_bottom_scan_trace,
    run_crash_equivalence_trace, run_overlay_trace, run_overwrite_trace,
};
use crate::state::{CollectionId, CollectionRef, EventRef, StateKey, StateName, StateType};
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use std::slice;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const TEST_KEYSPACE: &str = "prosody_test";

/// The production committed bottom assembly: fjall write-through over the
/// resolving Cassandra cell store.
type Bottom = Cached<CassandraStore<ScriptedOracle>>;

/// Property-test iteration count for live-backend runs (default 25), from
/// `INTEGRATION_TESTS`. CI cranks it up; dev loops stay fast.
fn get_test_count() -> u64 {
    use std::env;
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25)
}

/// The shared driver session and prepared cell statements — the
/// partition-independent half both the bottom store and the property assemblies
/// are built from.
struct Fixture {
    cassandra: CassandraSession,
    queries: Arc<CellQueries>,
    registry: Arc<CollectionDefRegistry>,
}

async fn fixture() -> Result<Fixture> {
    let config = CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    };
    let cassandra = CassandraSession::new(&config).await?;
    let queries = Arc::new(CellQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(Fixture {
        cassandra,
        queries,
        registry: Arc::new(CollectionDefRegistry::default()),
    })
}

impl Fixture {
    /// The bare resolving Cassandra cell store over `oracle`.
    fn bottom_store(&self, oracle: ScriptedOracle) -> CassandraStore<ScriptedOracle> {
        CassandraStore::new(
            self.cassandra.clone(),
            self.queries.clone(),
            oracle,
            self.registry.clone(),
        )
    }
}

/// A fresh-segment Value cell address (`ValueNs::Entries`, empty coordinate).
fn value_cell() -> CellKey {
    CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    }
}

/// A fresh-segment collection so concurrent runs and iterations never collide.
fn collection(name: &str) -> Result<CollectionRef> {
    let key: crate::Key = Arc::from("k");
    let id = CollectionId::new(
        StateKey::new(Uuid::new_v4(), key),
        StateType::Application,
        StateName::try_new(name)?,
    );
    Ok(CollectionRef::new(id, None))
}

fn event(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
}

/// The still-provisional cells of a collection — the public, non-resolving way
/// to observe staged state (`get` would resolve and mutate it).
async fn provisional_cells<S>(
    store: &S,
    id: &CollectionId,
) -> Result<Vec<(CellKey, ProvisionalCell)>>
where
    S: CellStore,
{
    let stream = store.provisional_cells(id);
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Stage a set, observe it provisional, promote, read back resolved — the
/// hot-path round-trip — then a direct resolved clear reads back absent.
#[tokio::test]
async fn provisional_set_promote_and_resolved_clear_round_trip() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    let data = Bytes::from_static(b"v1");

    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(Some(data.clone()), Committed::new(None), event(1)),
            )],
        )
        .await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (key, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after stage"))?;
    assert_eq!(key, cell);
    assert_eq!(prov.data(), Some(&data));
    assert_eq!(prov.prev(), None);
    assert_eq!(prov.event(), event(1));

    store.mark_resolved(&c, slice::from_ref(&cell)).await?;
    assert_eq!(
        store.get(c.id(), &cell, event(2)).await?,
        Committed::new(Some(data))
    );
    assert!(provisional_cells(&store, c.id()).await?.is_empty());

    store.write_resolved(&c, &[(cell.clone(), None)]).await?;
    assert_eq!(
        store.get(c.id(), &cell, event(2)).await?,
        Committed::new(None)
    );
    Ok(())
}

/// Stage a clear over a present base, observe it provisional (`data` null,
/// `prev` present), promote, and read back absent — the promote-of-clear
/// residue decoded live (encoding/version linger, both blobs null).
#[tokio::test]
async fn provisional_clear_over_present_promotes_to_absent() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    let old = Bytes::from_static(b"old");

    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(None, Committed::new(Some(old.clone())), event(2)),
            )],
        )
        .await?;
    let staged = provisional_cells(&store, c.id()).await?;
    let (_, prov) = staged
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("expected a provisional cell after clear-over-present"))?;
    assert_eq!(prov.data(), None);
    assert_eq!(prov.prev(), Some(&old));

    store.mark_resolved(&c, slice::from_ref(&cell)).await?;
    assert_eq!(
        store.get(c.id(), &cell, event(3)).await?,
        Committed::new(None)
    );
    Ok(())
}

/// An absent row reads back as `Committed(None)`, and `provisional_cells`
/// yields the staged cell then nothing once resolved.
#[tokio::test]
async fn absent_row_and_provisional_cells_stream() -> Result<()> {
    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();

    assert_eq!(
        store.get(c.id(), &cell, event(1)).await?,
        Committed::new(None)
    );

    store
        .write_provisional(
            &c,
            &[(
                cell.clone(),
                ProvisionalWrite::new(
                    Some(Bytes::from_static(b"v")),
                    Committed::new(None),
                    event(3),
                ),
            )],
        )
        .await?;
    assert_eq!(provisional_cells(&store, c.id()).await?.len(), 1);

    store.mark_resolved(&c, &[cell]).await?;
    assert!(provisional_cells(&store, c.id()).await?.is_empty());
    Ok(())
}

/// The durable Cassandra `data` column stays zstd-compressed (`RawZstdV1`),
/// unlike the fjall cache which stores raw and lets fjall block-compress on
/// disk. Reads the column with a raw CQL `SELECT` so the store's transparent
/// decompression cannot mask a regression to raw storage.
#[tokio::test]
async fn cassandra_data_column_is_zstd_compressed() -> Result<()> {
    use crate::cassandra::TABLE_KEYED_STATE_CELL;
    use crate::state::encoding::{Encoding, decode_payload};

    init_test_logging();
    let fx = fixture().await?;
    let store = fx.bottom_store(ScriptedOracle::default());
    let c = collection("cart")?;
    let cell = value_cell();
    // A long, repetitive payload so the zstd frame is unmistakably smaller than
    // the raw bytes — a regression to raw storage fails both assertions.
    let payload = Bytes::from(vec![0xAB_u8; 4096]);
    store
        .write_resolved(&c, &[(cell, Some(payload.clone()))])
        .await?;

    let cql = format!(
        "SELECT data FROM {TEST_KEYSPACE}.{TABLE_KEYED_STATE_CELL} WHERE segment_id = ? AND key = \
         ? AND state_type = ? AND name = ?"
    );
    let id = c.id();
    let raw = fx
        .cassandra
        .session()
        .query_unpaged(
            cql,
            (
                id.state_key().segment_id,
                id.state_key().key.as_ref(),
                i8::from(id.state_type()),
                id.name().as_str(),
            ),
        )
        .await?
        .into_rows_result()?
        .maybe_first_row::<(Option<Vec<u8>>,)>()?
        .and_then(|(data,)| data)
        .ok_or_else(|| eyre!("data column missing"))?;

    assert_ne!(
        raw.as_slice(),
        payload.as_ref(),
        "durable data column must be compressed, not stored raw"
    );
    assert!(
        raw.len() < payload.len(),
        "zstd frame ({} bytes) should be smaller than the {} raw bytes",
        raw.len(),
        payload.len()
    );
    assert_eq!(
        decode_payload(&raw, Encoding::RawZstdV1)?,
        payload,
        "zstd frame must decompress to the payload"
    );
    Ok(())
}

/// Read-path uniqueness invariant: a present cell read back from the Cassandra
/// decode path is **uniquely owned** (`try_into_mut().is_ok()`), the production
/// fast path `StateHandle::get` relies on. Run over random non-empty payloads.
#[test]
fn prop_cassandra_present_cell_is_uniquely_owned() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::{QuickCheck, TestResult};

    async fn check(payload: Vec<u8>) -> Result<bool> {
        let fx = fixture().await?;
        let store = fx.bottom_store(ScriptedOracle::default());
        let c = collection("uniq")?;
        let cell = value_cell();
        let data = Bytes::from(payload);
        store
            .write_resolved(&c, &[(cell.clone(), Some(data))])
            .await?;
        let Some(bytes) = store.get(c.id(), &cell, event(1)).await?.into_inner() else {
            return Err(eyre!("expected a present committed value"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// Converts a property body's `Result<bool>` into a `TestResult`, surfacing the
/// error on failure (a store/setup error is a broken environment, not a
/// shrinkable property failure).
fn finish(result: Result<bool>) -> quickcheck::TestResult {
    use quickcheck::TestResult;
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::failed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// The crash-recovery-equivalence property over the production
/// `Cached<CassandraStore>` assembly (invariants 1, 5). A "crash" rebuilds the
/// cache cold over the same durable Cassandra rows and oracle set. The tempdir
/// and fjall client outlive every store the `make` closure mints (the await
/// completes before they drop).
#[test]
fn prop_cassandra_cell_crash_equivalence() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::QuickCheck;

    async fn run(trace: Trace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let dir = tempfile::tempdir()?;
        let client = FjallClient::open(&FjallConfiguration {
            cache_dir: dir.path().to_path_buf(),
        })?;
        let make = || -> Result<Bottom> {
            let workspace =
                client.workspace(Topic::from("cell-test"), 0, AssignmentEpoch::mint())?;
            Ok(Cached::new(
                FjallCellCache::for_workspace(workspace),
                fx.bottom_store(oracle.clone()),
            ))
        };
        run_crash_equivalence_trace(make, oracle.clone(), trace).await
    }

    init_test_logging();
    QuickCheck::new().tests(get_test_count()).quickcheck(
        (|trace| finish(TEST_RUNTIME.block_on(run(trace)))) as fn(Trace) -> quickcheck::TestResult,
    );
}

/// Implicit-overwrite soundness over `Cached<CassandraStore>`: each overwrite
/// resolves its predecessor's provisional cell through the oracle on read, with
/// no explicit promote or rollback.
#[test]
fn prop_cassandra_cell_implicit_overwrite() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::QuickCheck;

    async fn run(trace: OverwriteTrace) -> Result<bool> {
        let fx = fixture().await?;
        let oracle = ScriptedOracle::default();
        let dir = tempfile::tempdir()?;
        let client = FjallClient::open(&FjallConfiguration {
            cache_dir: dir.path().to_path_buf(),
        })?;
        let make = || -> Result<Bottom> {
            let workspace =
                client.workspace(Topic::from("cell-test"), 0, AssignmentEpoch::mint())?;
            Ok(Cached::new(
                FjallCellCache::for_workspace(workspace),
                fx.bottom_store(oracle.clone()),
            ))
        };
        run_overwrite_trace(make, oracle.clone(), trace).await
    }

    init_test_logging();
    QuickCheck::new().tests(get_test_count()).quickcheck(
        (|trace| finish(TEST_RUNTIME.block_on(run(trace))))
            as fn(OverwriteTrace) -> quickcheck::TestResult,
    );
}

/// A single `Cached<CassandraStore>` over a fresh fjall workspace.
/// `dir`/`client` must outlive the returned store, so callers keep them in
/// scope.
fn assembly(fx: &Fixture, client: &Arc<FjallClient>) -> Result<Bottom> {
    let workspace = client.workspace(Topic::from("cell-test"), 0, AssignmentEpoch::mint())?;
    Ok(Cached::new(
        FjallCellCache::for_workspace(workspace),
        fx.bottom_store(ScriptedOracle::default()),
    ))
}

/// Unified view soundness over `Overlay<Cached<CassandraStore>>`: point `get`s,
/// range `scan`s (bounds, direction, limit, early-stop), dirty buffering, and
/// committed writes intermixed in one trace, all vs the sorted-map oracle
/// (invariants 3, 5; DT7).
#[test]
fn prop_cassandra_overlay_view() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::QuickCheck;

    async fn run(trace: OverlayTrace) -> Result<bool> {
        let fx = fixture().await?;
        let dir = tempfile::tempdir()?;
        let client = FjallClient::open(&FjallConfiguration {
            cache_dir: dir.path().to_path_buf(),
        })?;
        // Box the future: the assembly + trace exceed clippy's large-future
        // threshold on the stack.
        Box::pin(run_overlay_trace(assembly(&fx, &client)?, trace)).await
    }

    init_test_logging();
    QuickCheck::new().tests(get_test_count()).quickcheck(
        (|trace| finish(TEST_RUNTIME.block_on(run(trace))))
            as fn(OverlayTrace) -> quickcheck::TestResult,
    );
}

/// Scan correctness directly over `CassandraStore::scan_cells` — the live
/// `ORDER BY ASC/DESC`, clustering-range bounds, and `LIMIT`/in-code `end` the
/// overlay merge delegates to.
#[test]
fn prop_cassandra_bottom_scan() {
    use crate::test_util::TEST_RUNTIME;
    use quickcheck::QuickCheck;

    async fn run(trace: ScanTrace) -> Result<bool> {
        let fx = fixture().await?;
        run_bottom_scan_trace(fx.bottom_store(ScriptedOracle::default()), trace).await
    }

    init_test_logging();
    QuickCheck::new().tests(get_test_count()).quickcheck(
        (|trace| finish(TEST_RUNTIME.block_on(run(trace))))
            as fn(ScanTrace) -> quickcheck::TestResult,
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
