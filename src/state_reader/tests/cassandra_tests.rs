//! Live-Cassandra instantiations of the backend-generic reader runner.
//!
//! The [`CassandraReaderBackend`] seeds committed state through the **real**
//! owner [`KeyedStateSession`](crate::state::session::KeyedStateSession) over a
//! `CassandraStore<FixedOracle>` and reads it back through the production
//! oracle-free carriers ([`CassandraCellResources`]). The same
//! `run_reader_{value,map,deque}_trace` runner the memory suite uses
//! ([`reader_suite`](super::reader_suite)) runs here over live CQL, closing the
//! Cassandra-coverage gap (Value + Map + Deque + scan + two-group probe).
//!
//! Isolation (per TESTING.md's Cassandra row rule): the shared `prosody_test`
//! keyspace, a fresh subsystem/group/key token minted **inside** each
//! evaluation, `partition_count = 1` (so `partition_for_key` trivially agrees
//! with the owner's write partition), and fixed per-kind descriptor names whose
//! `structural_identity` differs by kind. A fresh group id yields a fresh
//! `UUIDv5` segment, so cell rows are disjoint; a fresh subsystem yields a
//! fresh publication partition; a fresh key isolates the reader cache.

use super::reader_suite::{
    ReaderCase, ValueOp, run_reader_deque_trace, run_reader_map_trace, run_reader_value_trace,
};
use super::support::{ReaderBackend, owner_commit_cell, source_state_key, state_name};
use crate::Key;
use crate::Topic;
use crate::cassandra::CassandraStore as CassandraConn;
use crate::codec::JsonCodec;
use crate::loader::MemoryLoader;
use crate::state::StateName;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
    CassandraStore as CassandraCellStore, CellQueries, IdentityQueries, PublicationQueries,
};
use crate::state::descriptor::{DescriptorIdentity, deque_state, map_state, value_state};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::fjall::test_db;
use crate::state::order_codec::I64KeyCodec;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::tests::collection_suite::{DequeOp, MapOp, Trace};
use crate::state::tests::support::FixedOracle;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::stores::ReaderStores;
use crate::state_reader::{PartitionCount, StateReader};
use crate::subsystem::SubsystemName;
use crate::test_util::{
    TEST_KEYSPACE, TEST_RUNTIME, integration_test_count, test_cassandra_config,
};
use crate::tracing::init_test_logging;
use color_eyre::eyre::{Result, ensure, eyre};
use internment::Intern;
use quickcheck::{QuickCheck, TestResult};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// The live-Cassandra [`ReaderBackend`]. Holds ONE
/// `CassandraStore<FixedOracle>` (shared session + prepared queries + one
/// `MarkerMemo`/`MarkerPresence` lifecycle), cloned into a fresh owner session
/// per event; the reader reads through [`CassandraCellResources`] over the same
/// session and queries.
struct CassandraReaderBackend {
    store: CassandraCellStore<FixedOracle>,
    cells: CassandraCellResources,
    publications: CassandraPublicationStore,
    identities: CassandraDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
}

impl ReaderBackend for CassandraReaderBackend {
    type OwnerCell = CassandraCellStore<FixedOracle>;

    fn registry(&self) -> Arc<CollectionDefRegistry> {
        self.registry.clone()
    }

    fn owner_cell(&self) -> Self::OwnerCell {
        self.store.clone()
    }

    async fn publish(
        &self,
        subsystem: &SubsystemName,
        name: &StateName,
        group: &str,
        topic: Topic,
        count: PartitionCount,
        identity: &DurableDescriptorIdentity,
    ) -> Result<()> {
        self.publications
            .upsert(
                subsystem,
                name,
                &StatePublication {
                    group_id: Arc::from(group),
                    topic,
                    partition_count: count,
                },
            )
            .await
            .map_err(|e| eyre!("upsert: {e}"))?;
        self.identities
            .register_identity(group, identity)
            .await
            .map_err(|e| eyre!("register identity: {e}"))?;
        Ok(())
    }

    fn deps(&self) -> SharedDeps<JsonCodec> {
        SharedDeps::from_parts(
            ReaderStores::Cassandra {
                cells: self.cells.clone(),
                publications: self.publications.clone(),
                identities: self.identities.clone(),
            },
            // Value/Map/Deque never consult the loader; the memory arm avoids a
            // live Kafka consumer config while keeping the REAL Cassandra
            // cell/publication/identity stores under test.
            ReaderLoader::Memory(MemoryLoader::new()),
            ReaderCache::with_budget(1 << 20),
        )
    }
}

/// The fixed per-kind names — distinct so each kind's `structural_identity`
/// differs and no first-kind name freezes the identity for the others.
const VALUE_NAME: &str = "reader-value";
const MAP_NAME: &str = "reader-map";
const DEQUE_NAME: &str = "reader-deque";

/// The reader's fixed topic (fixed avoids topic-intern growth; isolation is by
/// group/subsystem/key).
fn reader_topic() -> Topic {
    Intern::<str>::from("reader-topic")
}

/// Builds the heavy environment: a session, prepared queries, a process
/// presence latch, and a registry carrying the three per-kind defs, plus the
/// shared owner cell store and the reader's carriers.
async fn cassandra_backend() -> Result<CassandraReaderBackend> {
    let conn = CassandraConn::new(&test_cassandra_config()).await?;
    let cell_queries = Arc::new(CellQueries::new(conn.session(), TEST_KEYSPACE).await?);
    let identity_queries = Arc::new(IdentityQueries::new(conn.session(), TEST_KEYSPACE).await?);
    let publication_queries =
        Arc::new(PublicationQueries::new(conn.session(), TEST_KEYSPACE).await?);

    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &value_state::<JsonCodec>(VALUE_NAME),
        CollectionDef::new(None),
    )?;
    registry.register(
        &map_state::<I64KeyCodec, JsonCodec>(MAP_NAME),
        CollectionDef::new(None),
    )?;
    registry.register(
        &deque_state::<JsonCodec>(DEQUE_NAME),
        CollectionDef::new(None),
    )?;
    let registry = Arc::new(registry);

    let presence = test_db::presence("state_reader_cassandra_presence")?;
    let store = CassandraCellStore::new(
        conn.clone(),
        cell_queries.clone(),
        FixedOracle::committed(),
        registry.clone(),
        presence,
    );
    let cells = CassandraCellResources::new(conn.clone(), cell_queries);
    let publications = CassandraPublicationStore::new(conn.clone(), publication_queries);
    let identities = CassandraDescriptorIdentityStore::new(conn, identity_queries);
    Ok(CassandraReaderBackend {
        store,
        cells,
        publications,
        identities,
        registry,
    })
}

/// Converts a runner `Result<bool>` into a `TestResult` (a store/setup error is
/// a broken environment, never a shrinkable property failure). Mirrors
/// `cell/tests.rs::finish`.
fn finish(result: Result<bool>) -> TestResult {
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::failed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// A fresh single-group namespace `(subsystem, group, key)` per evaluation, so
/// shrink evaluations reuse nothing.
fn namespace() -> Result<(SubsystemName, String, Key)> {
    let token = Uuid::new_v4().simple().to_string();
    Ok((
        SubsystemName::try_new(format!("reader-{token}"))?,
        format!("{token}-00"),
        Key::from(format!("{token}-key")),
    ))
}

/// The committed==oracle Value property over live Cassandra.
#[test]
fn prop_cassandra_reader_value() {
    fn property(trace: Trace<ValueOp>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let backend = cassandra_backend().await?;
            let (sub, group, key) = namespace()?;
            let case = ReaderCase {
                sub: &sub,
                group: &group,
                topic: reader_topic(),
                key: &key,
                count: PartitionCount::MIN,
            };
            run_reader_value_trace(&backend, value_state::<JsonCodec>(VALUE_NAME), &case, trace)
                .await
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(Trace<ValueOp>) -> TestResult);
}

/// The committed==oracle Map property over live Cassandra (point + `get_many` +
/// ordered scan).
#[test]
fn prop_cassandra_reader_map() {
    fn property(trace: Trace<MapOp>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let backend = cassandra_backend().await?;
            let (sub, group, key) = namespace()?;
            let case = ReaderCase {
                sub: &sub,
                group: &group,
                topic: reader_topic(),
                key: &key,
                count: PartitionCount::MIN,
            };
            Box::pin(run_reader_map_trace(
                &backend,
                map_state::<I64KeyCodec, JsonCodec>(MAP_NAME),
                &case,
                trace,
            ))
            .await
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(Trace<MapOp>) -> TestResult);
}

/// The committed==oracle Deque property over live Cassandra (len +
/// front-relative get + ordered scan).
#[test]
fn prop_cassandra_reader_deque() {
    fn property(trace: Trace<DequeOp>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let backend = cassandra_backend().await?;
            let (sub, group, key) = namespace()?;
            let case = ReaderCase {
                sub: &sub,
                group: &group,
                topic: reader_topic(),
                key: &key,
                count: PartitionCount::MIN,
            };
            Box::pin(run_reader_deque_trace(
                &backend,
                deque_state::<JsonCodec>(DEQUE_NAME),
                &case,
                trace,
            ))
            .await
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(Trace<DequeOp>) -> TestResult);
}

/// Probe-and-pin over TWO admitted live-Cassandra sources: the lowest-ordered
/// `SourceId` group answers. Both groups commit divergent Values under one
/// fresh subsystem; `-00 < -01` lexicographically, so the reader must observe
/// `-00`'s value.
///
/// FALSIFICATION: reverse `ValidatedPublications::new`'s sort
/// (`b.id.cmp(&a.id)`) → the higher group pins → the assert reds.
#[test]
fn reader_two_group_lowest_wins() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let backend = cassandra_backend().await?;
        let descriptor = value_state::<JsonCodec>(VALUE_NAME);
        let name = state_name(VALUE_NAME)?;
        let topic = reader_topic();
        let count = PartitionCount::MIN;
        let token = Uuid::new_v4().simple().to_string();
        let sub = SubsystemName::try_new(format!("reader-{token}"))?;
        let key = Key::from(format!("{token}-key"));
        let lowest_group = format!("{token}-00");
        let decoy_group = format!("{token}-01");
        let identity = DurableDescriptorIdentity::from_identity(
            descriptor.state_type(),
            name.as_str(),
            &descriptor.structural_identity(),
        );

        for (group, value) in [(&lowest_group, "lowest"), (&decoy_group, "decoy")] {
            let state_key = source_state_key(topic, group, &key, count)?;
            let committed = Value::from(value);
            owner_commit_cell(
                backend.owner_cell(),
                &backend.registry(),
                &state_key,
                descriptor,
                1,
                move |handle| async move {
                    handle.set(committed).await.map_err(|e| eyre!("set: {e}"))
                },
            )
            .await?;
            backend
                .publish(&sub, &name, group, topic, count, &identity)
                .await?;
        }

        let deps = backend.deps();
        let reader = StateReader::new(&deps, sub, descriptor)?;
        ensure!(
            reader.get(key).await? == Some(Value::from("lowest")),
            "the lowest-SourceId group must win the probe"
        );
        Ok(())
    })
}
