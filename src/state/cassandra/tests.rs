//! Cassandra Value store integration tests.
//!
//! These tests run against a local Cassandra node and use the shared
//! `prosody_test` keyspace. Each property test iteration mints a fresh
//! `segment_id` via `value_test_suite::collection_ref()` so rows from
//! different iterations and different test functions never collide.

use super::{CassandraValueStore, CassandraValueStoreError, ValueQueries};
use crate::cassandra::{CassandraConfiguration, CassandraStore, TABLE_KEYED_STATE_VALUE};
use crate::state::cassandra::decode::CorruptReason;
use crate::state::memory::MemoryDirtyValueStore;
use crate::state::pending::PendingIndexStore;
use crate::state::value::{DurableWalStore, ValueOp};
use crate::state::value_test_suite::{self, DirectTrace, Trace, collection_ref, inline};
use crate::state::{CollectionId, DurableState, EventRef, StateType, ValueKind};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::eyre::{self, Result};
use quickcheck::{QuickCheck, TestResult};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

fn wal_property(trace: Trace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");

    let store = match runtime.block_on(async { setup_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!("setup failed: {e:?}\nFailing input:\n{input_dbg}"));
        }
    };

    match runtime.block_on(
        async { value_test_suite::run_trace(store, MemoryDirtyValueStore::new, trace).await }
            .instrument(span),
    ) {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("model mismatch.\nFailing input:\n{input_dbg}")),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

fn idempotence_property(trace: Trace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");

    let store = match runtime.block_on(async { setup_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!("setup failed: {e:?}\nFailing input:\n{input_dbg}"));
        }
    };

    match runtime.block_on(
        async {
            value_test_suite::run_idempotence_trace(store, MemoryDirtyValueStore::new, trace).await
        }
        .instrument(span),
    ) {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!(
            "idempotence violated.\nFailing input:\n{input_dbg}"
        )),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

fn direct_property(trace: DirectTrace) -> TestResult {
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");

    let store = match runtime.block_on(async { setup_value_store().await }.instrument(span.clone()))
    {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!("setup failed: {e:?}\nFailing input:\n{input_dbg}"));
        }
    };

    match runtime.block_on(
        async {
            value_test_suite::run_direct_trace(store, MemoryDirtyValueStore::new, trace).await
        }
        .instrument(span),
    ) {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!(
            "partition was sealed under direct mode.\nFailing input:\n{input_dbg}"
        )),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

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

async fn setup_value_store() -> Result<CassandraValueStore> {
    let config = test_cassandra_config("prosody_test");
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(ValueQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraValueStore::new(cassandra, queries))
}

#[test]
fn prop_value_trace_against_cassandra() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(wal_property as fn(Trace) -> TestResult);
}

#[test]
fn prop_durable_resolution_is_idempotent_against_cassandra() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(idempotence_property as fn(Trace) -> TestResult);
}

#[test]
fn prop_direct_mode_never_creates_wal_against_cassandra() {
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(direct_property as fn(DirectTrace) -> TestResult);
}

#[tokio::test]
async fn event_mismatch_returns_typed_error() -> Result<()> {
    init_test_logging();
    let store = setup_value_store().await?;
    let collection = collection_ref()?;

    let event_a = EventRef::Message {
        dedup_id: Uuid::from_u128(0xAAAA),
    };
    let event_b = EventRef::Message {
        dedup_id: Uuid::from_u128(0xBBBB),
    };

    store
        .seal(
            &collection,
            event_a,
            vec![ValueOp::Set { payload: inline(1) }],
        )
        .await?;

    let error = store
        .apply_sealed(&collection, event_b)
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected EventMismatch"))?;
    match error {
        CassandraValueStoreError::EventMismatch { expected, actual } => {
            assert_eq!(expected, event_b);
            assert_eq!(actual, event_a);
        }
        other => return Err(eyre::eyre!("expected EventMismatch, got {other:?}")),
    }
    Ok(())
}

#[tokio::test]
async fn stale_pending_row_after_partial_seal() -> Result<()> {
    init_test_logging();
    let store = setup_value_store().await?;
    let collection = collection_ref()?;

    // Mimic a crash between insert_pending and write_wal: the pending row
    // is present but the value partition has no WAL columns.
    PendingIndexStore::insert_pending::<ValueKind>(&store, collection.id()).await?;

    let state = store.read_partition(collection.id()).await?;
    match state {
        DurableState::Idle { applied: None } => Ok(()),
        other => Err(eyre::eyre!(
            "value partition should be Idle empty, got {other:?}"
        )),
    }
}

#[tokio::test]
async fn corrupt_partition_returns_corrupt_wal() -> Result<()> {
    init_test_logging();
    let store = setup_value_store().await?;
    let collection = collection_ref()?;
    let id: &CollectionId<ValueKind> = collection.id();

    // Write a partial WAL shape directly: wal_event populated, the other
    // two WAL columns left NULL. The decoder must reject this.
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(0xC0C0),
    };
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = match id.state_type() {
        StateType::Application => 0_i8,
    };
    let name = id.name().as_str();
    let cql = format!(
        "UPDATE {keyspace}.{table} SET wal_event = ? WHERE segment_id = ? AND key = ? AND \
         state_type = ? AND name = ?",
        keyspace = "prosody_test",
        table = TABLE_KEYED_STATE_VALUE,
    );
    let prepared = store.store.session().prepare(cql).await?;
    store
        .store
        .session()
        .execute_unpaged(&prepared, (event, segment_id, key, state_type, name))
        .await?;

    let result = store.read_partition(id).await;
    match result {
        Err(CassandraValueStoreError::CorruptWal {
            reason: CorruptReason::PartialWalColumns { mask },
        }) => {
            assert!(mask.event, "wal_event should be present");
            assert!(!mask.ops, "wal_ops should be absent");
            assert!(!mask.format, "wal_format should be absent");
            Ok(())
        }
        other => Err(eyre::eyre!("expected CorruptWal, got {other:?}")),
    }
}
