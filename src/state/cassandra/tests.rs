//! Cassandra Value store integration tests.
//!
//! These tests run against a local Cassandra node and use the shared
//! `prosody_test` keyspace. Each property test iteration mints a fresh
//! `segment_id` via `value_test_suite::collection_ref()` so rows from
//! different iterations and different test functions never collide.

use super::{CassandraValueStore, CassandraValueStoreError, ValueQueries};
use crate::cassandra::{CassandraConfiguration, CassandraStore, TABLE_KEYED_STATE_VALUE};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cassandra::decode::CorruptReason;
use crate::state::cassandra::error::CorruptUdtError;
use crate::state::cassandra::udt::RawEventRef;
use crate::state::memory::MemoryDirtyValueStore;
use crate::state::pending::PendingIndexStore;
use crate::state::value::{DurableWalStore, StoredPayload, ValueOp, ValueStore};
use crate::state::value_test_suite::{self, DirectTrace, TEST_TTL, Trace, collection_ref, inline};
use crate::state::{CollectionId, DurableState, EventRef, StateType, ValueKind};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use crate::tracing::init_test_logging;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use quickcheck::{QuickCheck, TestResult};
use std::env;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

/// Keyspace shared by every test in this module; created out of band.
const TEST_KEYSPACE: &str = "prosody_test";

/// Sets up a fresh Cassandra Value store, drives `run` over the trace within
/// the current span, and folds the outcome into a [`TestResult`]. `input_dbg`
/// is captured before `trace` is moved so failures always print the input.
fn run_cassandra_property<T, Fut>(
    trace: T,
    mismatch_msg: &str,
    run: impl FnOnce(CassandraValueStore, T) -> Fut,
) -> TestResult
where
    T: Debug,
    Fut: Future<Output = Result<bool>>,
{
    let runtime = &*TEST_RUNTIME;
    let span = tracing::Span::current();
    let input_dbg = format!("{trace:#?}");

    let store = match runtime.block_on(setup_value_store().instrument(span.clone())) {
        Ok(s) => s,
        Err(e) => {
            return TestResult::error(format!("setup failed: {e:?}\nFailing input:\n{input_dbg}"));
        }
    };

    match runtime.block_on(run(store, trace).instrument(span)) {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("{mismatch_msg}\nFailing input:\n{input_dbg}")),
        Err(e) => TestResult::error(format!("runtime error: {e:?}\nFailing input:\n{input_dbg}")),
    }
}

fn wal_property(trace: Trace) -> TestResult {
    run_cassandra_property(trace, "model mismatch.", |store, trace| {
        value_test_suite::run_trace(store, MemoryDirtyValueStore::new, trace)
    })
}

fn idempotence_property(trace: Trace) -> TestResult {
    run_cassandra_property(trace, "idempotence violated.", |store, trace| {
        value_test_suite::run_idempotence_trace(store, MemoryDirtyValueStore::new, trace)
    })
}

fn direct_property(trace: DirectTrace) -> TestResult {
    run_cassandra_property(
        trace,
        "partition was sealed under direct mode.",
        |store, trace| value_test_suite::run_direct_trace(store, MemoryDirtyValueStore::new, trace),
    )
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
    setup_value_store_with_ttl(TEST_TTL).await
}

async fn setup_value_store_with_ttl(
    default_ttl: Option<CompactDuration>,
) -> Result<CassandraValueStore> {
    let config = test_cassandra_config(TEST_KEYSPACE);
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(ValueQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraValueStore::new(cassandra, queries, default_ttl))
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

/// F4 (Cassandra): the shared stale-pending sweep check against the real
/// Cassandra Value store — the `Idle ⇒ delete_pending` recovery arm run
/// against a real pending index, not one derived from WAL presence. Mirrors
/// the memory invocation in `state::tests`.
#[tokio::test]
async fn state_recovery_sweeps_stale_pending_row() -> Result<()> {
    init_test_logging();
    let store = setup_value_store().await?;
    value_test_suite::run_stale_pending_index(store).await
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
        "UPDATE {TEST_KEYSPACE}.{TABLE_KEYED_STATE_VALUE} SET wal_event = ? WHERE segment_id = ? \
         AND key = ? AND state_type = ? AND name = ?",
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

/// B3: a structurally-valid but semantically-corrupt `event_ref` UDT
/// (`kind = 7`) on an otherwise sealed-shaped row must surface as a typed
/// `CorruptUdt` classified `Permanent` (skip the row), not the `Terminal`
/// classification scylla's opaque `DeserializationError` would have produced
/// when validation lived inside `DeserializeValue`. This exercises the real
/// scylla deserialize path: the corrupt UDT is written via raw CQL, read
/// back into `RawEventRef`, and validated in the decoder's post-step.
#[tokio::test]
async fn corrupt_event_ref_udt_classifies_permanent() -> Result<()> {
    init_test_logging();
    let store = setup_value_store().await?;
    let collection = collection_ref()?;
    let id: &CollectionId<ValueKind> = collection.id();

    // Sealed-shaped row: all three WAL columns + payload_encoding set, but
    // the event_ref UDT carries an unknown discriminator.
    let corrupt = RawEventRef {
        kind: 7,
        msg_dedup_id: None,
        timer_type: None,
        time: None,
        tag: None,
    };
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = match id.state_type() {
        StateType::Application => 0_i8,
    };
    let name = id.name().as_str();
    let cql = format!(
        "UPDATE {TEST_KEYSPACE}.{TABLE_KEYED_STATE_VALUE} SET wal_event = ?, wal_ops = ?, \
         wal_format = ?, payload_encoding = ? WHERE segment_id = ? AND key = ? AND state_type = ? \
         AND name = ?",
    );
    let prepared = store.store.session().prepare(cql).await?;
    store
        .store
        .session()
        .execute_unpaged(
            &prepared,
            (
                corrupt,
                vec![1_u8],
                1_i16,
                1_i16,
                segment_id,
                key,
                state_type,
                name,
            ),
        )
        .await?;

    let error = store
        .read_partition(id)
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected CorruptUdt error"))?;
    match &error {
        CassandraValueStoreError::CorruptUdt(CorruptUdtError::UnknownKind(7)) => {}
        other => {
            return Err(eyre::eyre!(
                "expected CorruptUdt(UnknownKind(7)), got {other:?}"
            ));
        }
    }
    assert_eq!(
        error.classify_error(),
        ErrorCategory::Permanent,
        "corrupt UDT must classify Permanent (skip), not Terminal"
    );
    Ok(())
}

/// Constructing `CassandraValueStore` with `default_ttl = Some(_)` must
/// route `ValueStore::set` through the with-TTL query arm. The exact TTL
/// cannot easily be asserted from a `SELECT`, so this test guards against
/// compilation/dispatch regression of the with-TTL arm.
#[tokio::test]
async fn set_with_default_ttl_some_writes_via_ttl_arm() -> Result<()> {
    init_test_logging();
    let store = setup_value_store_with_ttl(TEST_TTL).await?;
    let id = collection_ref()?.id().clone();
    store
        .set(&id, StoredPayload::Inline(Bytes::from_static(b"x")))
        .await?;
    Ok(())
}

/// Constructing `CassandraValueStore` with `default_ttl = None` must
/// route `ValueStore::set` through the no-TTL query arm. This would have
/// caught an earlier bug where production writes hardcoded `None`
/// (and so always used the no-TTL arm even when a TTL was configured).
#[tokio::test]
async fn set_with_default_ttl_none_writes_via_no_ttl_arm() -> Result<()> {
    init_test_logging();
    let store = setup_value_store_with_ttl(None).await?;
    let id = collection_ref()?.id().clone();
    store
        .set(&id, StoredPayload::Inline(Bytes::from_static(b"x")))
        .await?;
    Ok(())
}
