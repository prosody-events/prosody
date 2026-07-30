//! Live-Cassandra instantiations of the backend-generic reader runner.
//!
//! The [`CassandraReaderBackend`] seeds committed state through the **real**
//! owner [`KeyedStateSession`](crate::state::session::KeyedStateSession) over a
//! `CassandraStore<FixedOracle>` and reads it back through the production
//! oracle-free carriers ([`CassandraCellResources`]). The same
//! `run_reader_{value,map,deque}_trace` runner the memory suite uses
//! ([`reader_suite`](super::reader_suite)) runs here over live CQL, adding
//! Cassandra coverage for Value, Map, Deque, scans, and the two-group probe.
//!
//! Isolation follows TESTING.md's Cassandra row rule. Each evaluation runs in
//! the shared `prosody_test` keyspace and generates a fresh subsystem, group,
//! and key token before it starts. `partition_count` is always `1`, so
//! `partition_for_key` trivially agrees with the owner's write partition. Each
//! kind uses a fixed descriptor name, and the three names differ so their
//! `structural_identity` values differ too. A fresh group id yields a fresh
//! `UUIDv5` segment, so cell rows stay disjoint. A fresh subsystem yields a
//! fresh publication partition, and a fresh key isolates the reader cache.

use super::reader_suite::{
    ReaderCase, ValueOp, run_reader_deque_trace, run_reader_map_trace, run_reader_value_trace,
};
use super::support::{
    ReaderBackend, collect_stream, owner_commit_cell, source_state_key, state_name,
};
use crate::Key;
use crate::Topic;
use crate::cassandra::CassandraStore as CassandraConn;
use crate::codec::JsonCodec;
use crate::loader::MemoryLoader;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
    CassandraStore as CassandraCellStore, CellQueries, IdentityQueries, PublicationQueries,
};
use crate::state::cell_key::Direction;
use crate::state::descriptor::deque::DEQUE_POINT_ITERATION_MAX;
use crate::state::descriptor::{DescriptorIdentity, deque_state, map_state, value_state};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::fjall::test_db;
use crate::state::order_codec::I64KeyCodec;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::tests::collection_suite::{DequeOp, MapOp, Trace};
use crate::state::tests::support::FixedOracle;
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderComponents;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::deps::StateReaderDependencies;
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

/// The live-Cassandra [`ReaderBackend`]. It holds one
/// `CassandraStore<FixedOracle>`, which bundles a shared session, prepared
/// queries, and one `MarkerMemo`/`MarkerPresence` lifecycle. That store is
/// cloned into a fresh owner session for each event. The reader reads through
/// [`CassandraCellResources`] over the same session and the same queries.
struct CassandraReaderBackend {
    store: CassandraCellStore<FixedOracle>,
    cells: CassandraCellResources,
    publications: CassandraPublicationStore,
    identities: CassandraDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
}

impl ReaderBackend for CassandraReaderBackend {
    type DepsBackend = ReaderComponents<
        JsonCodec,
        CassandraCellResources,
        CassandraPublicationStore,
        CassandraDescriptorIdentityStore,
        MemoryLoader<Value>,
    >;
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
                StateType::Application,
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

    fn deps(
        &self,
    ) -> StateReaderDependencies<
        JsonCodec,
        ReaderComponents<
            JsonCodec,
            CassandraCellResources,
            CassandraPublicationStore,
            CassandraDescriptorIdentityStore,
            MemoryLoader<Value>,
        >,
    > {
        StateReaderDependencies::from_parts(
            ReaderComponents::new(
                self.cells.clone(),
                self.publications.clone(),
                self.identities.clone(),
                MemoryLoader::new(),
            ),
            ReaderCache::with_budget(1 << 20),
        )
    }
}

/// Names for the three registered kinds. Each name is distinct, so each
/// kind's `structural_identity` differs instead of collapsing onto whichever
/// kind registered first.
const VALUE_NAME: &str = "reader-value";
const MAP_NAME: &str = "reader-map";
const DEQUE_NAME: &str = "reader-deque";

/// The reader's fixed topic. Keeping it fixed avoids growing the topic intern
/// table. Isolation between evaluations comes from the group, subsystem, and
/// key instead.
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

/// Instantiates a live-Cassandra `prop_cassandra_reader_<kind>` test. Each
/// evaluation builds a fresh [`cassandra_backend`] and namespace, then runs
/// `$runner` over an arbitrary `Trace<$op>` for `$descriptor_ctor($name)`,
/// scaled by `INTEGRATION_TESTS`. The three instantiations below are
/// identical except for the descriptor constructor, collection name, trace
/// op, and runner.
macro_rules! cassandra_reader_prop {
    ($test_name:ident, $op:ty, $descriptor_ctor:expr, $name:expr, $runner:ident) => {
        #[test]
        fn $test_name() {
            fn property(trace: Trace<$op>) -> TestResult {
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
                    Box::pin($runner(&backend, $descriptor_ctor($name), &case, trace)).await
                }))
            }
            init_test_logging();
            QuickCheck::new()
                .tests(integration_test_count(25))
                .quickcheck(property as fn(Trace<$op>) -> TestResult);
        }
    };
}

// The Value property: committed state must equal the oracle, over live
// Cassandra.
cassandra_reader_prop!(
    prop_cassandra_reader_value,
    ValueOp,
    value_state::<JsonCodec>,
    VALUE_NAME,
    run_reader_value_trace
);

// The Map property: committed state must equal the oracle, over live
// Cassandra (point reads, `get_many`, and the ordered stream).
cassandra_reader_prop!(
    prop_cassandra_reader_map,
    MapOp,
    map_state::<I64KeyCodec, JsonCodec>,
    MAP_NAME,
    run_reader_map_trace
);

// The Deque property: committed state must equal the oracle, over live
// Cassandra (len, front-relative get, and the ordered stream).
cassandra_reader_prop!(
    prop_cassandra_reader_deque,
    DequeOp,
    deque_state::<JsonCodec>,
    DEQUE_NAME,
    run_reader_deque_trace
);

/// A probe-and-pin test over two admitted live-Cassandra sources: the
/// lowest-ordered `SourceId` group must answer. Both groups commit divergent
/// values under one fresh subsystem. Because `-00` sorts lexicographically
/// before `-01`, the reader must observe `-00`'s value.
///
/// FALSIFICATION: reverse `ValidatedPublications::new`'s sort to
/// `b.id.cmp(&a.id)`. The higher group then pins, and the assert goes red.
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

/// Exercises the reader's live-Cassandra deque scan path for committed data.
/// A window one element past `DEQUE_POINT_ITERATION_MAX` forces the reader
/// stream off its point-read arm and onto
/// `CassandraCellResources::scan_committed`. The small trace collections used
/// elsewhere never reach that path. The whole window is committed through the
/// real owner in a single event. The reader then streams it forward and
/// backward, and both directions must equal the ordered model.
///
/// FALSIFICATION: drop the first yield in
/// `CassandraCellResources::scan_committed`. The forward stream then loses
/// its front element and the assert goes red.
#[test]
fn reader_deque_scan_committed() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let backend = cassandra_backend().await?;
        let descriptor = deque_state::<JsonCodec>(DEQUE_NAME);
        let name = state_name(DEQUE_NAME)?;
        let topic = reader_topic();
        let count = PartitionCount::MIN;
        let (sub, group, key) = namespace()?;
        let identity = DurableDescriptorIdentity::from_identity(
            descriptor.state_type(),
            name.as_str(),
            &descriptor.structural_identity(),
        );

        let width = DEQUE_POINT_ITERATION_MAX + 1;
        let state_key = source_state_key(topic, &group, &key, count)?;
        owner_commit_cell(
            backend.owner_cell(),
            &backend.registry(),
            &state_key,
            descriptor,
            1,
            move |handle| async move {
                for i in 0..width {
                    handle
                        .push_back(Value::from(i as i64))
                        .await
                        .map_err(|e| eyre!("push: {e}"))?;
                }
                Ok(())
            },
        )
        .await?;
        backend
            .publish(&sub, &name, &group, topic, count, &identity)
            .await?;

        let deps = backend.deps();
        let reader = StateReader::new(&deps, sub, descriptor)?;
        let model: Vec<Value> = (0..width).map(|i| Value::from(i as i64)).collect();
        let forward = Box::pin(collect_stream(
            reader.stream(key.clone(), Direction::Forward).await?,
        ))
        .await?;
        ensure!(
            forward == model,
            "forward scan must equal the ordered model"
        );
        let backward = Box::pin(collect_stream(
            reader.stream(key, Direction::Backward).await?,
        ))
        .await?;
        let mut expect_backward = model;
        expect_backward.reverse();
        ensure!(
            backward == expect_backward,
            "backward scan must equal the reversed model"
        );
        Ok(())
    })
}
