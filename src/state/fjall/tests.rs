//! Fjall Value cache + layered combinator tests.
//!
//! Broker-free tests run the property suite against
//! `LayeredValueStore<FjallValueStore, MemoryDurableValueStore>` and a
//! handful of directed unit tests for the cache-specific patch rules
//! the property runners do not exercise.
//!
//! Cassandra-backed tests run the property suite against
//! `LayeredValueStore<FjallValueStore, CassandraValueStore>` and are
//! gated on `INTEGRATION_TESTS` like the other Cassandra tests.

use super::{AssignmentEpoch, FjallClient, FjallDirtyValueStore, FjallValueStore};
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cassandra::{CassandraValueStore, ValueQueries};
use crate::state::layered::LayeredValueStore;
use crate::state::memory::{MemoryDirtyValueStore, MemoryDurableValueStore};
use crate::state::oracle::CommitOracle;
use crate::state::pending::PendingIndexStore;
use crate::state::production::ProductionValueDurable;
use crate::state::recovering::RecoveringValueStore;
use crate::state::session::DurableValueBundle;
use crate::state::tests::dirty_value_suite::{self, DirtyTrace};
use crate::state::tests::value_suite::{
    self, ParityTrace, TEST_TTL, bytes, collection_id, collection_ref, event, finish_trace,
};
use crate::state::value::{DurableWalStore, PendingOpSource, ValueKind, ValueOp, ValueStore};
use crate::state::{CollectionId, CommitDecision, EventRef, EventScopeId, Read};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use parking_lot::Mutex;
use quickcheck::{QuickCheck, TestResult};
use std::convert::Infallible;
use std::env;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use thiserror::Error;
use tracing::{Instrument, Span};

// ---- shared helpers ---------------------------------------------------------

/// Owns the temp dir and keyspace backing a test store, keeping both alive
/// for the lifetime of the partition handle the store holds.
struct FjallFixture {
    _dir: TempDir,
    keyspace: Keyspace,
}

impl FjallFixture {
    fn open() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let keyspace = Config::new(dir.path()).open()?;
        Ok(Self {
            _dir: dir,
            keyspace,
        })
    }

    fn partition(&self, name: &str) -> Result<PartitionHandle> {
        Ok(self
            .keyspace
            .open_partition(name, PartitionCreateOptions::default())?)
    }
}

fn make_cache() -> Result<(FjallFixture, FjallValueStore)> {
    let fixture = FjallFixture::open()?;
    let cache = FjallValueStore::new(fixture.partition("value_cache")?);
    Ok((fixture, cache))
}

fn make_dirty(scope: EventScopeId) -> Result<(FjallFixture, FjallDirtyValueStore)> {
    let fixture = FjallFixture::open()?;
    let dirty = FjallDirtyValueStore::new(fixture.partition("value_dirty_overlay")?, scope);
    Ok((fixture, dirty))
}

// ---- direct FjallValueStore unit tests --------------------------------------

#[tokio::test]
async fn cache_get_returns_unknown_on_missing_key() -> Result<()> {
    let (_fixture, cache) = make_cache()?;
    let id = collection_id("missing")?;
    assert_eq!(cache.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn cache_set_then_get_returns_present() -> Result<()> {
    let (_fixture, cache) = make_cache()?;
    let id = collection_id("present")?;
    let payload = bytes(9);
    cache.set(&id, payload.clone()).await?;
    assert_eq!(cache.get(&id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn cache_clear_then_get_returns_absent() -> Result<()> {
    let (_fixture, cache) = make_cache()?;
    let id = collection_id("cleared")?;
    cache.clear(&id).await?;
    assert_eq!(cache.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn cache_set_then_clear_then_get_returns_absent() -> Result<()> {
    let (_fixture, cache) = make_cache()?;
    let id = collection_id("toggled")?;
    cache.set(&id, bytes(1)).await?;
    cache.clear(&id).await?;
    assert_eq!(cache.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn cache_present_with_empty_bytes_round_trips() -> Result<()> {
    let (_fixture, cache) = make_cache()?;
    let id = collection_id("empty-cell")?;
    let payload = Bytes::new();
    cache.set(&id, payload.clone()).await?;
    assert_eq!(cache.get(&id).await?, Read::Present(payload));
    Ok(())
}

// ---- directed LayeredValueStore combinator test -----------------------------
//
// The full-op-set parity property (`prop_layered_fjall_*_full_parity`, below)
// subsumes every healthy-cache patch rule. This one directed test needs the
// fault-injection seam no healthy-backing parity run can reproduce: Layered
// invariant 6 — a cache-write failure after a backing-side success must
// invalidate and still return durable success.

#[tokio::test]
async fn cache_failure_after_backing_success_is_invalidated() -> Result<()> {
    let backing = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();

    // Build a fault-injection cache that errors on every `set` and `clear`.
    let cache = FaultyCache::new();
    let layered = LayeredValueStore::new(cache.clone(), backing.clone());

    // Direct call to set: the backing succeeds, the cache fails — the
    // outer call must still report success.
    layered
        .set(&collection_id, bytes(5))
        .await
        .map_err(into_eyre)?;

    // The cache had set called (and failed), then clear was attempted as
    // a best-effort invalidation. We do not assert the cache reads
    // anything specific — we assert the operation succeeded despite the
    // cache failure and that the backing applied the write.
    assert_eq!(backing.get(&collection_id).await?, Read::Present(bytes(5)));
    assert!(
        cache.set_was_attempted(),
        "cache.set should have been attempted"
    );
    assert!(
        cache.clear_was_attempted(),
        "cache.clear should have been attempted for invalidation"
    );
    Ok(())
}

#[tokio::test]
async fn recover_before_seal_then_rollback_resyncs_cache() -> Result<()> {
    // Regression for a cache-coherence hole the full-op parity property
    // surfaced on `Layered<Fjall, Recovering<…>>`: `recover-before-seal` lives
    // inside `Recovering` (below the cache), so it folds a prior crashed WAL
    // into `applied` without the cache observing it. If the new event is then
    // rolled back — which leaves `applied` at the recovered value — a stale
    // cache survives and the next read returns the pre-recovery value.
    let (_fixture, cache) = make_cache()?;
    let inner = MemoryDurableValueStore::for_tests();
    let backing = RecoveringValueStore::with_default_ttl(inner, AlwaysCommittedOracle, TEST_TTL);
    let layered = LayeredValueStore::new(cache, backing);
    let collection = collection_ref()?;
    let id = collection.id().clone();

    // Applied = A, cache populated.
    layered.set(&id, bytes(1)).await.map_err(into_eyre)?;
    assert_eq!(
        layered.get(&id).await.map_err(into_eyre)?,
        Read::Present(bytes(1))
    );

    // Seal event 1 over A, then seal event 2: the second seal recovers event 1
    // (AlwaysCommitted → apply), folding `[Set 2]` into applied = B. The cache
    // never sees this — it still holds A.
    layered
        .seal(
            &collection,
            event(1),
            vec![ValueOp::Set { payload: bytes(2) }],
        )
        .await
        .map_err(into_eyre)?;
    layered
        .seal(
            &collection,
            event(2),
            vec![ValueOp::Set { payload: bytes(3) }],
        )
        .await
        .map_err(into_eyre)?;

    // Roll back event 2: applied stays at the recovered B; rollback leaves the
    // cache untouched (Layered invariant 4).
    layered
        .rollback_sealed(&collection, event(2))
        .await
        .map_err(into_eyre)?;

    // The visible value must be the recovered B, not the stale cached A.
    assert_eq!(
        layered.get(&id).await.map_err(into_eyre)?,
        Read::Present(bytes(2))
    );
    Ok(())
}

// ---- property runners: LayeredValueStore<FjallValueStore, Memory> -----------
//
// These use `TEST_RUNTIME.block_on` (not `futures::executor::block_on`)
// because every fjall call goes through `tokio::task::spawn_blocking`,
// which needs a Tokio runtime in scope.

/// Builds a fresh fjall cache, runs `run` against it, and maps the outcome to a
/// [`TestResult`] — folding the `make_cache` setup, the `"cache setup failed"`
/// early-return, and the [`finish_trace`] mapping shared by every broker-free
/// runner. The fixture stays alive until `run` returns.
fn run_layered_property<E: fmt::Debug>(
    falsified: &str,
    input_dbg: &str,
    run: impl FnOnce(FjallValueStore) -> Result<bool, E>,
) -> TestResult {
    let (_fixture, cache) = match make_cache() {
        Ok(c) => c,
        Err(e) => {
            return TestResult::error(format!(
                "cache setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };
    finish_trace(run(cache), falsified, input_dbg)
}

fn layered_memory_full_parity(trace: ParityTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_layered_property("layered/backing divergence", &input_dbg, |cache| {
        TEST_RUNTIME.block_on(value_suite::run_layered_parity(
            cache,
            MemoryDurableValueStore::for_tests(),
            MemoryDurableValueStore::for_tests(),
            trace,
            false,
        ))
    })
}

fn layered_recovering_memory_full_parity(trace: ParityTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_layered_property("layered/backing divergence", &input_dbg, |cache| {
        let backing_layered = RecoveringValueStore::with_default_ttl(
            MemoryDurableValueStore::for_tests(),
            AlwaysCommittedOracle,
            TEST_TTL,
        );
        let backing_bare = RecoveringValueStore::with_default_ttl(
            MemoryDurableValueStore::for_tests(),
            AlwaysCommittedOracle,
            TEST_TTL,
        );
        TEST_RUNTIME.block_on(value_suite::run_layered_parity(
            cache,
            backing_layered,
            backing_bare,
            trace,
            true,
        ))
    })
}

#[test]
fn prop_layered_fjall_memory_full_parity() {
    QuickCheck::new().quickcheck(layered_memory_full_parity as fn(ParityTrace) -> TestResult);
}

#[test]
fn prop_layered_fjall_recovering_memory_full_parity() {
    QuickCheck::new()
        .quickcheck(layered_recovering_memory_full_parity as fn(ParityTrace) -> TestResult);
}

// ---- property runners: LayeredValueStore<FjallValueStore, Cassandra> --------

fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: keyspace.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    }
}

fn get_test_count() -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25)
}

async fn setup_cassandra_value_store() -> Result<CassandraValueStore> {
    let config = test_cassandra_config("prosody_test");
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(ValueQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraValueStore::new(cassandra, queries, TEST_TTL))
}

/// Builds a fresh fjall cache plus a live `CassandraValueStore` backing, hands
/// both to `run` (along with the current span for instrumenting the trace), and
/// maps the outcome to a [`TestResult`]. Folds the tempdir,
/// cache-open, and cassandra-setup early-returns shared by every Cassandra
/// runner; the `TempDir` is kept alive until `run` returns and dropped
/// afterwards. `falsified` is the diagnostic shown when the property is
/// violated.
fn run_cassandra_property<E: fmt::Debug>(
    falsified: &str,
    input_dbg: &str,
    run: impl FnOnce(Span, FjallValueStore, CassandraValueStore) -> Result<bool, E>,
) -> TestResult {
    let span = Span::current();
    let fixture = match FjallFixture::open() {
        Ok(f) => f,
        Err(e) => {
            return TestResult::error(format!(
                "cache setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };
    let cache = match fixture.partition("value_cache") {
        Ok(handle) => FjallValueStore::new(handle),
        Err(e) => {
            return TestResult::error(format!(
                "cache open failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };
    let backing = match TEST_RUNTIME
        .block_on(async { setup_cassandra_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!(
                "cassandra setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };
    let result = run(span, cache, backing);
    finish_trace(result, falsified, input_dbg)
}

fn cassandra_full_parity(trace: ParityTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_cassandra_property(
        "layered/backing divergence",
        &input_dbg,
        |span, cache, backing| {
            TEST_RUNTIME.block_on(
                async {
                    let backing_bare = setup_cassandra_value_store().await?;
                    value_suite::run_layered_parity(cache, backing, backing_bare, trace, false)
                        .await
                }
                .instrument(span),
            )
        },
    )
}

fn recovering_cassandra_full_parity(trace: ParityTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_cassandra_property(
        "layered/backing divergence",
        &input_dbg,
        |span, cache, backing| {
            TEST_RUNTIME.block_on(
                async {
                    let backing_layered = RecoveringValueStore::with_default_ttl(
                        backing,
                        AlwaysCommittedOracle,
                        TEST_TTL,
                    );
                    let backing_bare = RecoveringValueStore::with_default_ttl(
                        setup_cassandra_value_store().await?,
                        AlwaysCommittedOracle,
                        TEST_TTL,
                    );
                    value_suite::run_layered_parity(
                        cache,
                        backing_layered,
                        backing_bare,
                        trace,
                        true,
                    )
                    .await
                }
                .instrument(span),
            )
        },
    )
}

#[test]
fn prop_layered_fjall_cassandra_full_parity() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(cassandra_full_parity as fn(ParityTrace) -> TestResult);
}

#[test]
fn prop_layered_fjall_recovering_cassandra_full_parity() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(recovering_cassandra_full_parity as fn(ParityTrace) -> TestResult);
}

// ---- fault-injection cache --------------------------------------------------

/// Cache that errors on writes but tracks which methods were called so
/// the "invalidate after backing success" path can be asserted.
#[derive(Clone, Debug)]
struct FaultyCache {
    inner: Arc<FaultyInner>,
}

#[derive(Debug, Default)]
struct FaultyInner {
    set_calls: Mutex<u32>,
    clear_calls: Mutex<u32>,
}

impl FaultyCache {
    fn new() -> Self {
        Self {
            inner: Arc::new(FaultyInner::default()),
        }
    }

    fn set_was_attempted(&self) -> bool {
        *self.inner.set_calls.lock() > 0
    }

    fn clear_was_attempted(&self) -> bool {
        *self.inner.clear_calls.lock() > 0
    }
}

impl ValueStore for FaultyCache {
    type Error = FaultyError;

    async fn get<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        Ok(Read::Unknown)
    }

    async fn set<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _payload: Bytes,
    ) -> Result<(), Self::Error> {
        *self.inner.set_calls.lock() += 1;
        Err(FaultyError::Injected)
    }

    async fn clear<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        *self.inner.clear_calls.lock() += 1;
        Err(FaultyError::Injected)
    }
}

#[derive(Debug, Error)]
enum FaultyError {
    #[error("injected cache failure")]
    Injected,
}

impl ClassifyError for FaultyError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

// ---- FjallDirtyValueStore directed + property tests -------------------------

#[tokio::test]
async fn fjall_dirty_set_then_get_returns_present() -> Result<()> {
    let (_fixture, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("present")?;
    let payload = bytes(7);
    dirty.set(&id, payload.clone()).await?;
    assert_eq!(dirty.get(&id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_clear_then_get_returns_absent() -> Result<()> {
    let (_fixture, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("absent")?;
    dirty.clear(&id).await?;
    assert_eq!(dirty.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_untouched_collection_returns_unknown() -> Result<()> {
    let (_fixture, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("untouched")?;
    assert_eq!(dirty.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_two_sets_compact_to_one_op() -> Result<()> {
    let (_fixture, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("multi")?;
    dirty.set(&id, bytes(1)).await?;
    dirty.set(&id, bytes(2)).await?;
    // Last-writer-wins: the second set obviates the first, so the overlay
    // holds exactly one compacted op and the visible value is the latest.
    let pending = dirty
        .pending_ops(&id)?
        .ok_or_else(|| eyre::eyre!("expected Some(pending)"))?;
    assert_eq!(pending.count.get(), 1);
    assert_eq!(dirty.get(&id).await?, Read::Present(bytes(2)));
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_clear_pending_ops_removes_overlay() -> Result<()> {
    let (_fixture, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("drained")?;
    dirty.set(&id, bytes(1)).await?;
    dirty.set(&id, bytes(2)).await?;
    dirty.clear_pending_ops(&id)?;
    assert!(dirty.pending_ops(&id)?.is_none());
    assert_eq!(dirty.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_scope_isolation_two_scopes_dont_interfere() -> Result<()> {
    let fixture = FjallFixture::open()?;
    let overlay = fixture.partition("value_dirty_overlay")?;

    let scope_a = EventScopeId::fresh();
    let scope_b = EventScopeId::fresh();
    let dirty_a = FjallDirtyValueStore::new(overlay.clone(), scope_a);
    let dirty_b = FjallDirtyValueStore::new(overlay, scope_b);

    let id = collection_id("shared")?;
    dirty_a.set(&id, bytes(9)).await?;
    assert_eq!(dirty_a.get(&id).await?, Read::Present(bytes(9)));
    assert_eq!(dirty_b.get(&id).await?, Read::Unknown);
    assert!(dirty_b.pending_ops(&id)?.is_none());
    Ok(())
}

/// Builds a fresh `FjallDirtyValueStore` in its own scope, runs `run` against
/// it, and maps the outcome to a [`TestResult`] — folding the `make_dirty`
/// setup and the `"dirty setup failed"` early-return shared by the dirty
/// runners. The fixture stays alive until `run` returns.
fn run_dirty_property<E: fmt::Debug>(
    falsified: &str,
    input_dbg: &str,
    run: impl FnOnce(FjallDirtyValueStore) -> Result<bool, E>,
) -> TestResult {
    let (_fixture, dirty) = match make_dirty(EventScopeId::fresh()) {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!(
                "dirty setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };
    finish_trace(run(dirty), falsified, input_dbg)
}

fn fjall_dirty_property(trace: DirtyTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_dirty_property("model mismatch", &input_dbg, |dirty| {
        TEST_RUNTIME.block_on(dirty_value_suite::run_dirty_trace(dirty, trace))
    })
}

#[test]
fn prop_fjall_dirty_satisfies_invariants() {
    QuickCheck::new().quickcheck(fjall_dirty_property as fn(DirtyTrace) -> TestResult);
}

fn fjall_dirty_matches_memory_property(trace: DirtyTrace) -> TestResult {
    let input_dbg = format!("{trace:#?}");
    run_dirty_property("model mismatch", &input_dbg, |fjall_dirty| {
        TEST_RUNTIME.block_on(dirty_value_suite::run_dirty_parity(
            fjall_dirty,
            MemoryDirtyValueStore::new(),
            trace,
        ))
    })
}

#[test]
fn prop_fjall_dirty_matches_memory_dirty() {
    QuickCheck::new()
        .quickcheck(fjall_dirty_matches_memory_property as fn(DirtyTrace) -> TestResult);
}

// ---- FjallClient + FjallWorkspace tests -------------------------------------

use crate::Partition;
use crate::Topic;
use crate::state::fjall::FjallConfiguration;
use crate::timers::datetime::CompactDateTime;

fn make_client(dir: &TempDir) -> Result<Arc<FjallClient>> {
    let config = FjallConfiguration {
        cache_dir: dir.path().to_path_buf(),
    };
    Ok(FjallClient::open(&config)?)
}

#[tokio::test]
async fn fjall_workspace_drop_deletes_both_partitions() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = make_client(&dir)?;
    let topic: Topic = "test-topic".into();
    let partition: Partition = 7;
    let epoch = AssignmentEpoch::now()?;

    let before = client.keyspace().partition_count();
    let ws = client.workspace(topic, partition, epoch)?;
    let mid = client.keyspace().partition_count();
    assert_eq!(mid, before + 2, "workspace should add two partitions");
    drop(ws);
    // Drop the workspace; fjall delete_partition is synchronous.
    let after = client.keyspace().partition_count();
    assert_eq!(
        after, before,
        "workspace Drop should delete both partitions"
    );
    Ok(())
}

#[tokio::test]
async fn fjall_client_open_sweeps_orphaned_partitions() -> Result<()> {
    let dir = tempfile::tempdir()?;

    // Simulate a crash: open a bare keyspace, create several `value_*`
    // partitions, then drop the keyspace without going through
    // `FjallWorkspace::Drop`. The on-disk partitions persist; the next
    // process startup must reap them.
    {
        let keyspace = Config::new(dir.path()).open()?;
        for name in [
            "value_cache_aaaaaa",
            "value_dirty_ops_aaaaaa",
            "value_dirty_overlay_aaaaaa",
            "value_dirty_meta_aaaaaa",
            "untouched_partition",
        ] {
            let _handle = keyspace.open_partition(name, PartitionCreateOptions::default())?;
        }
        // `keyspace` goes out of scope here without any `delete_partition`
        // calls — exactly the state a crashed process leaves behind.
    }

    // Process restart: opening a FjallClient must wipe the stale
    // workspaces and leave non-`value_*` partitions alone.
    let client = make_client(&dir)?;
    let surviving: Vec<_> = client.keyspace().list_partitions();
    let value_partitions: Vec<_> = surviving
        .iter()
        .filter(|name| name.starts_with("value_"))
        .collect();
    assert!(
        value_partitions.is_empty(),
        "FjallClient::open must sweep stale value_* partitions; surviving: {value_partitions:?}"
    );
    assert!(
        surviving
            .iter()
            .any(|name| name.as_ref() == "untouched_partition"),
        "non-`value_*` partitions must not be swept"
    );
    Ok(())
}

#[tokio::test]
async fn fjall_workspace_distinct_epochs_dont_collide() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = make_client(&dir)?;
    let topic: Topic = "tt".into();
    let partition: Partition = 0;
    let epoch_a = AssignmentEpoch::new(CompactDateTime::from(100_u32));
    let epoch_b = AssignmentEpoch::new(CompactDateTime::from(200_u32));

    let _ws_a = client.workspace(topic, partition, epoch_a)?;
    let _ws_b = client.workspace(topic, partition, epoch_b)?;
    // Each workspace minted two distinct partitions: 4 total.
    let value_count = client
        .keyspace()
        .list_partitions()
        .into_iter()
        .filter(|name| name.starts_with("value_"))
        .count();
    assert_eq!(value_count, 4);
    Ok(())
}

// ---- error conversion helpers -----------------------------------------------

fn into_eyre<E>(e: E) -> eyre::Report
where
    E: Error + Send + Sync + 'static,
{
    eyre::eyre!(e)
}

// ---- always-committed oracle for the recovering combinator runners ----------

#[derive(Clone, Copy, Debug)]
struct AlwaysCommittedOracle;

impl CommitOracle for AlwaysCommittedOracle {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: uuid::Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::Committed)
    }
}

// ---- compile-guard: production bundle satisfies the manager bound -------

/// Pure type-level assertion that the canonical production durable bundle
/// `Layered<FjallValueStore, Recovering<CassandraValueStore, O>>` satisfies
/// the exact bound the keyed-state manager imposes on its durable `D`
/// (the `PartitionStateManager for StateManager` impl in
/// `state/manager/mod.rs`). This guards the `PendingIndexStore`
/// pass-throughs on both [`LayeredValueStore`] and [`RecoveringValueStore`]:
/// without them, `D` fails the bound and this stops compiling. It constructs
/// no values and needs no live Cassandra, so it runs broker-free.
fn assert_satisfies_manager_durable_bound<D>()
where
    D: DurableValueBundle + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>,
{
}

#[test]
fn production_durable_bundle_satisfies_middleware_bound() {
    assert_satisfies_manager_durable_bound::<ProductionValueDurable<AlwaysCommittedOracle>>();
}
