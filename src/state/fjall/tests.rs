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
use crate::Key;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cassandra::{CassandraValueStore, ValueQueries};
use crate::state::dirty_value_test_suite::{self, DirtyTrace};
use crate::state::layered::LayeredValueStore;
use crate::state::memory::{MemoryDirtyValueStore, MemoryDurableValueStore};
use crate::state::oracle::CommitOracle;
use crate::state::recovering::RecoveringValueStore;
use crate::state::value::{
    PendingOpSource, StoredPayload, TransactionValueStore, ValueKind, ValueStore,
};
use crate::state::value_test_suite::{self, DirectTrace, TEST_TTL, Trace, collection_ref, inline};
use crate::state::{
    CollectionId, CommitDecision, CommitMode, EventRef, EventScopeId, Read, StateKey, StateName,
    StateType, StoreOutcome,
};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use fjall::{Config, PartitionCreateOptions};
use parking_lot::Mutex;
use quickcheck::{QuickCheck, TestResult};
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use thiserror::Error;
use tracing::Instrument;
use uuid::Uuid;

// ---- shared helpers ---------------------------------------------------------

fn make_cache() -> Result<(TempDir, FjallValueStore)> {
    let dir = tempfile::tempdir()?;
    let store = FjallValueStore::for_test(&dir)?;
    Ok((dir, store))
}

fn make_dirty(scope: EventScopeId) -> Result<(TempDir, FjallDirtyValueStore)> {
    let dir = tempfile::tempdir()?;
    let keyspace = Arc::new(Config::new(dir.path()).open()?);
    let ops = keyspace.open_partition("value_dirty_ops", PartitionCreateOptions::default())?;
    let overlay =
        keyspace.open_partition("value_dirty_overlay", PartitionCreateOptions::default())?;
    let meta = keyspace.open_partition("value_dirty_meta", PartitionCreateOptions::default())?;
    Ok((
        dir,
        FjallDirtyValueStore::new(keyspace, ops, overlay, meta, scope),
    ))
}

fn key(value: &str) -> Key {
    Arc::from(value)
}

fn collection_id(name: &str) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), key("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

fn event(id: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(id),
    }
}

// ---- direct FjallValueStore unit tests --------------------------------------

#[tokio::test]
async fn cache_get_returns_unknown_on_missing_key() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let id = collection_id("missing")?;
    assert_eq!(cache.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn cache_set_then_get_returns_present() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let id = collection_id("present")?;
    let payload = inline(9);
    cache.set(&id, payload.clone()).await?;
    assert_eq!(cache.get(&id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn cache_clear_then_get_returns_absent() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let id = collection_id("cleared")?;
    cache.clear(&id).await?;
    assert_eq!(cache.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn cache_set_then_clear_then_get_returns_absent() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let id = collection_id("toggled")?;
    cache.set(&id, inline(1)).await?;
    cache.clear(&id).await?;
    assert_eq!(cache.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn cache_present_with_inline_empty_bytes_round_trips() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let id = collection_id("empty-inline")?;
    let payload = StoredPayload::Inline(Bytes::new());
    cache.set(&id, payload.clone()).await?;
    assert_eq!(cache.get(&id).await?, Read::Present(payload));
    Ok(())
}

// ---- directed LayeredValueStore combinator tests ----------------------------
//
// All combinator tests use the memory durable store as the backing so they
// run without a broker. The cache is observed directly via `cache_get` to
// distinguish cache state from the layered store's read-through behavior.

#[tokio::test]
async fn cache_populated_on_miss() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let backing = MemoryDurableValueStore::for_tests();
    let id = collection_id("populated")?;
    let payload = inline(3);
    backing.set(&id, payload.clone()).await?;

    let layered = LayeredValueStore::new(cache.clone(), backing);
    assert_eq!(cache.get(&id).await?, Read::Unknown);
    assert_eq!(
        layered.get(&id).await.map_err(into_eyre)?,
        Read::Present(payload.clone())
    );
    assert_eq!(cache.get(&id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn cache_patched_after_apply_sealed() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let backing = MemoryDurableValueStore::for_tests();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();
    let layered = LayeredValueStore::new(cache.clone(), backing);
    let mut tx = TransactionValueStore::new(layered, dirty, collection, event(1), CommitMode::Wal);

    let payload = inline(11);
    tx.set(&collection_id, payload.clone())
        .await
        .map_err(into_eyre)?;
    tx.seal().await.map_err(into_eyre)?;
    let outcome = tx.apply_sealed().await.map_err(into_eyre)?;
    assert_eq!(
        outcome,
        StoreOutcome::Applied,
        "apply_sealed should report Applied"
    );

    assert_eq!(cache.get(&collection_id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn cache_patched_after_direct_apply() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let backing = MemoryDurableValueStore::for_tests();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();
    let layered = LayeredValueStore::new(cache.clone(), backing);
    let mut tx =
        TransactionValueStore::new(layered, dirty, collection, event(2), CommitMode::Direct);

    let payload = inline(22);
    tx.set(&collection_id, payload.clone())
        .await
        .map_err(into_eyre)?;
    let outcome = tx.direct_apply().await.map_err(into_eyre)?;
    assert_eq!(outcome, StoreOutcome::Applied);

    assert_eq!(cache.get(&collection_id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn cache_untouched_after_seal() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let backing = MemoryDurableValueStore::for_tests();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();
    let layered = LayeredValueStore::new(cache.clone(), backing);
    let mut tx = TransactionValueStore::new(layered, dirty, collection, event(3), CommitMode::Wal);

    let payload = inline(33);
    tx.set(&collection_id, payload).await.map_err(into_eyre)?;
    tx.seal().await.map_err(into_eyre)?;

    // Cache mirrors only applied state; seal does not change applied.
    assert_eq!(cache.get(&collection_id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn cache_untouched_after_rollback_sealed() -> Result<()> {
    let (_dir, cache) = make_cache()?;
    let backing = MemoryDurableValueStore::for_tests();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();
    let layered = LayeredValueStore::new(cache.clone(), backing);
    let mut tx = TransactionValueStore::new(layered, dirty, collection, event(4), CommitMode::Wal);

    let payload = inline(44);
    tx.set(&collection_id, payload).await.map_err(into_eyre)?;
    tx.seal().await.map_err(into_eyre)?;
    let outcome = tx.rollback_sealed().await.map_err(into_eyre)?;
    assert_eq!(outcome, StoreOutcome::Applied);

    assert_eq!(cache.get(&collection_id).await?, Read::Unknown);
    Ok(())
}

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
        .set(&collection_id, inline(5))
        .await
        .map_err(into_eyre)?;

    // The cache had set called (and failed), then clear was attempted as
    // a best-effort invalidation. We do not assert the cache reads
    // anything specific — we assert the operation succeeded despite the
    // cache failure and that the backing applied the write.
    assert_eq!(backing.get(&collection_id).await?, Read::Present(inline(5)));
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

// ---- property runners: LayeredValueStore<FjallValueStore, Memory> -----------
//
// These use `TEST_RUNTIME.block_on` (not `futures::executor::block_on`)
// because every fjall call goes through `tokio::task::spawn_blocking`,
// which needs a Tokio runtime in scope.

fn prop_layered_memory_wal(trace: Trace) -> bool {
    let Ok((_dir, cache)) = make_cache() else {
        return false;
    };
    let backing = MemoryDurableValueStore::for_tests();
    let layered = LayeredValueStore::new(cache, backing);
    TEST_RUNTIME
        .block_on(value_test_suite::run_trace(
            layered,
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
}

fn prop_layered_memory_idempotence(trace: Trace) -> bool {
    let Ok((_dir, cache)) = make_cache() else {
        return false;
    };
    let backing = MemoryDurableValueStore::for_tests();
    let layered = LayeredValueStore::new(cache, backing);
    TEST_RUNTIME
        .block_on(value_test_suite::run_idempotence_trace(
            layered,
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
}

fn prop_layered_memory_direct(trace: DirectTrace) -> bool {
    let Ok((_dir, cache)) = make_cache() else {
        return false;
    };
    let backing = MemoryDurableValueStore::for_tests();
    let layered = LayeredValueStore::new(cache, backing);
    TEST_RUNTIME
        .block_on(value_test_suite::run_direct_trace(
            layered,
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
}

#[test]
fn prop_layered_fjall_memory_trace() {
    QuickCheck::new().quickcheck(prop_layered_memory_wal as fn(Trace) -> bool);
}

#[test]
fn prop_layered_fjall_memory_idempotence_trace() {
    QuickCheck::new().quickcheck(prop_layered_memory_idempotence as fn(Trace) -> bool);
}

#[test]
fn prop_layered_fjall_memory_direct_trace() {
    QuickCheck::new().quickcheck(prop_layered_memory_direct as fn(DirectTrace) -> bool);
}

// ---- property runners: Layered<Fjall, Recovering<Memory, AlwaysCommitted>> --

fn prop_layered_fjall_recovering_memory(trace: Trace) -> bool {
    let Ok((_dir, cache)) = make_cache() else {
        return false;
    };
    let inner = MemoryDurableValueStore::for_tests();
    let recovering = RecoveringValueStore::new(inner, AlwaysCommittedOracle, TEST_TTL);
    let layered = LayeredValueStore::new(cache, recovering);
    TEST_RUNTIME
        .block_on(value_test_suite::run_trace(
            layered,
            MemoryDirtyValueStore::new,
            trace,
        ))
        .unwrap_or(false)
}

#[test]
fn prop_layered_fjall_recovering_memory_trace() {
    QuickCheck::new().quickcheck(prop_layered_fjall_recovering_memory as fn(Trace) -> bool);
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

fn cassandra_wal_property(trace: Trace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!("tempdir failed: {e}\nFailing input:\n{input_dbg}"));
        }
    };
    let cache = match FjallValueStore::for_test(&dir) {
        Ok(c) => c,
        Err(e) => {
            return TestResult::error(format!(
                "cache open failed: {e}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let backing = match runtime
        .block_on(async { setup_cassandra_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!(
                "cassandra setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let layered = LayeredValueStore::new(cache, backing);
    let result = runtime.block_on(
        async { value_test_suite::run_trace(layered, MemoryDirtyValueStore::new, trace).await }
            .instrument(span),
    );
    drop(dir);
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("model mismatch.\nFailing input:\n{input_dbg}")),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

fn cassandra_idempotence_property(trace: Trace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!("tempdir failed: {e}\nFailing input:\n{input_dbg}"));
        }
    };
    let cache = match FjallValueStore::for_test(&dir) {
        Ok(c) => c,
        Err(e) => {
            return TestResult::error(format!(
                "cache open failed: {e}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let backing = match runtime
        .block_on(async { setup_cassandra_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!(
                "cassandra setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let layered = LayeredValueStore::new(cache, backing);
    let result = runtime.block_on(
        async {
            value_test_suite::run_idempotence_trace(layered, MemoryDirtyValueStore::new, trace)
                .await
        }
        .instrument(span),
    );
    drop(dir);
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!(
            "idempotence violated.\nFailing input:\n{input_dbg}"
        )),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

fn cassandra_direct_property(trace: DirectTrace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!("tempdir failed: {e}\nFailing input:\n{input_dbg}"));
        }
    };
    let cache = match FjallValueStore::for_test(&dir) {
        Ok(c) => c,
        Err(e) => {
            return TestResult::error(format!(
                "cache open failed: {e}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let backing = match runtime
        .block_on(async { setup_cassandra_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!(
                "cassandra setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let layered = LayeredValueStore::new(cache, backing);
    let result = runtime.block_on(
        async {
            value_test_suite::run_direct_trace(layered, MemoryDirtyValueStore::new, trace).await
        }
        .instrument(span),
    );
    drop(dir);
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!(
            "partition sealed under direct mode.\nFailing input:\n{input_dbg}"
        )),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

#[test]
fn prop_layered_fjall_cassandra_trace() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(cassandra_wal_property as fn(Trace) -> TestResult);
}

#[test]
fn prop_layered_fjall_cassandra_idempotence_trace() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(cassandra_idempotence_property as fn(Trace) -> TestResult);
}

#[test]
fn prop_layered_fjall_cassandra_direct_trace() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(cassandra_direct_property as fn(DirectTrace) -> TestResult);
}

fn cassandra_recovering_wal_property(trace: Trace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            return TestResult::error(format!("tempdir failed: {e}\nFailing input:\n{input_dbg}"));
        }
    };
    let cache = match FjallValueStore::for_test(&dir) {
        Ok(c) => c,
        Err(e) => {
            return TestResult::error(format!(
                "cache open failed: {e}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let backing = match runtime
        .block_on(async { setup_cassandra_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!(
                "cassandra setup failed: {e:?}\nFailing input:\n{input_dbg}"
            ));
        }
    };

    let recovering = RecoveringValueStore::new(backing, AlwaysCommittedOracle, TEST_TTL);
    let layered = LayeredValueStore::new(cache, recovering);
    let result = runtime.block_on(
        async { value_test_suite::run_trace(layered, MemoryDirtyValueStore::new, trace).await }
            .instrument(span),
    );
    drop(dir);
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("model mismatch.\nFailing input:\n{input_dbg}")),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

#[test]
fn prop_layered_fjall_recovering_cassandra_trace() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(cassandra_recovering_wal_property as fn(Trace) -> TestResult);
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
    ) -> Result<Read<StoredPayload>, Self::Error> {
        Ok(Read::Unknown)
    }

    async fn set<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _payload: StoredPayload,
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
    let (_dir, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("present")?;
    let payload = inline(7);
    dirty.set(&id, payload.clone()).await?;
    assert_eq!(dirty.get(&id).await?, Read::Present(payload));
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_clear_then_get_returns_absent() -> Result<()> {
    let (_dir, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("absent")?;
    dirty.clear(&id).await?;
    assert_eq!(dirty.get(&id).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_untouched_collection_returns_unknown() -> Result<()> {
    let (_dir, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("untouched")?;
    assert_eq!(dirty.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_two_sets_increment_seq() -> Result<()> {
    let (_dir, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("multi")?;
    dirty.set(&id, inline(1)).await?;
    dirty.set(&id, inline(2)).await?;
    let pending = dirty
        .pending_ops(&id)?
        .ok_or_else(|| eyre::eyre!("expected Some(pending)"))?;
    assert_eq!(pending.count.get(), 2);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_clear_pending_ops_removes_overlay_meta_and_ops() -> Result<()> {
    let (_dir, dirty) = make_dirty(EventScopeId::fresh())?;
    let id = collection_id("drained")?;
    dirty.set(&id, inline(1)).await?;
    dirty.set(&id, inline(2)).await?;
    dirty.clear_pending_ops(&id)?;
    assert!(dirty.pending_ops(&id)?.is_none());
    assert_eq!(dirty.get(&id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn fjall_dirty_scope_isolation_two_scopes_dont_interfere() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let keyspace = Arc::new(fjall::Config::new(dir.path()).open()?);
    let ops = keyspace.open_partition("value_dirty_ops", PartitionCreateOptions::default())?;
    let overlay =
        keyspace.open_partition("value_dirty_overlay", PartitionCreateOptions::default())?;
    let meta = keyspace.open_partition("value_dirty_meta", PartitionCreateOptions::default())?;

    let scope_a = EventScopeId::fresh();
    let scope_b = EventScopeId::fresh();
    let dirty_a = FjallDirtyValueStore::new(
        Arc::clone(&keyspace),
        ops.clone(),
        overlay.clone(),
        meta.clone(),
        scope_a,
    );
    let dirty_b = FjallDirtyValueStore::new(keyspace, ops, overlay, meta, scope_b);

    let id = collection_id("shared")?;
    dirty_a.set(&id, inline(9)).await?;
    assert_eq!(dirty_a.get(&id).await?, Read::Present(inline(9)));
    assert_eq!(dirty_b.get(&id).await?, Read::Unknown);
    assert!(dirty_b.pending_ops(&id)?.is_none());
    Ok(())
}

fn fjall_dirty_property(trace: DirtyTrace) -> bool {
    let scope = EventScopeId::fresh();
    let Ok((_dir, dirty)) = make_dirty(scope) else {
        return false;
    };
    TEST_RUNTIME
        .block_on(dirty_value_test_suite::run_dirty_trace(dirty, trace))
        .unwrap_or(false)
}

#[test]
fn prop_fjall_dirty_satisfies_invariants() {
    QuickCheck::new().quickcheck(fjall_dirty_property as fn(DirtyTrace) -> bool);
}

fn fjall_dirty_matches_memory_property(trace: DirtyTrace) -> bool {
    let scope = EventScopeId::fresh();
    let Ok((_dir, fjall_dirty)) = make_dirty(scope) else {
        return false;
    };
    let memory_dirty = MemoryDirtyValueStore::new();
    TEST_RUNTIME
        .block_on(dirty_value_test_suite::run_dirty_parity(
            fjall_dirty,
            memory_dirty,
            trace,
        ))
        .unwrap_or(false)
}

#[test]
fn prop_fjall_dirty_matches_memory_dirty() {
    QuickCheck::new().quickcheck(fjall_dirty_matches_memory_property as fn(DirtyTrace) -> bool);
}

// ---- FjallClient + FjallWorkspace tests -------------------------------------

use crate::Partition;
use crate::Topic;
use crate::state::fjall::FjallConfiguration;
use crate::timers::datetime::CompactDateTime;

fn make_client(dir: &TempDir) -> Result<Arc<FjallClient>> {
    let config = FjallConfiguration::builder()
        .cache_dir(dir.path().to_path_buf())
        .build()?;
    Ok(FjallClient::open(&config)?)
}

#[tokio::test]
async fn fjall_workspace_drop_deletes_all_four_partitions() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = make_client(&dir)?;
    let topic: Topic = "test-topic".into();
    let partition: Partition = 7;
    let epoch = AssignmentEpoch::now()?;

    let before = client.keyspace().partition_count();
    let ws = client.workspace(topic, partition, epoch)?;
    let mid = client.keyspace().partition_count();
    assert_eq!(mid, before + 4, "workspace should add four partitions");
    drop(ws);
    // Drop the workspace; fjall delete_partition is synchronous.
    let after = client.keyspace().partition_count();
    assert_eq!(
        after, before,
        "workspace Drop should delete all four partitions"
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
        let keyspace = fjall::Config::new(dir.path()).open()?;
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
    // Each workspace minted four distinct partitions: 8 total.
    let value_count = client
        .keyspace()
        .list_partitions()
        .into_iter()
        .filter(|name| name.starts_with("value_"))
        .count();
    assert_eq!(value_count, 8);
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
    type Error = AlwaysCommittedOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::Committed)
    }
}

#[derive(Debug, Error)]
enum AlwaysCommittedOracleError {}

impl ClassifyError for AlwaysCommittedOracleError {
    fn classify_error(&self) -> ErrorCategory {
        match *self {}
    }
}
