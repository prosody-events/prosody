//! Live-Cassandra harness shared by the directory's tests and the process
//! runtime's.
//!
//! Isolation follows the Cassandra row rule: every test mints fresh node ids
//! and prefixes each generated group id with a fresh token, so rows are
//! disjoint in the shared `prosody_test` keyspace and no test creates a
//! keyspace of its own.

use crate::cassandra::{CassandraStore, TABLE_NODES_BY_GROUP};
use crate::router::directory::{
    Endpoint, GROUP_SHARDS, GroupMembership, NetworkId, NodeDirectory, NodeRegistration,
    RegistrationTtl,
};
use crate::router::{Host, NodeId};
use crate::test_util::{TEST_KEYSPACE, test_cassandra_config};
use color_eyre::Result;
use fixedstr::Flexstr;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::time::Duration;
use tokio::sync::OnceCell;
use uuid::Uuid;

/// Characters a generated host, hostname or label is built from.
const LABEL_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789.:-";

/// Longest generated label. It spans both sides of `Flexstr<64>`'s inline
/// limit, so the heap-spilling representation round-trips too.
const MAX_LABEL: usize = 70;

/// One store per test process. `CassandraStore::new` runs the migrator, so a
/// store per property iteration would spend the run on schema checks.
static STORE: OnceCell<CassandraStore> = OnceCell::const_new();

/// A registration whose every field is generated, including the absent forms
/// of the three optional ones.
#[derive(Clone, Debug)]
pub(crate) struct ArbRegistration(pub(crate) NodeRegistration);

impl Arbitrary for ArbRegistration {
    fn arbitrary(g: &mut Gen) -> Self {
        let token = token();
        let group = bool::arbitrary(g).then(|| GroupMembership {
            cluster: Flexstr::make(&format!("{token}-cluster")),
            group: Flexstr::make(&format!("{token}-{}", label(g))),
        });
        Self(NodeRegistration {
            node: node_id(g),
            direct: endpoint(g),
            advertised: bool::arbitrary(g).then(|| endpoint(g)),
            network: bool::arbitrary(g).then(|| NetworkId::make(&label(g))),
            group,
            hostname: Host::make(&label(g)),
        })
    }
}

/// The shared store, built once for the whole test process.
pub(crate) async fn store() -> Result<&'static CassandraStore> {
    STORE
        .get_or_try_init(|| async { CassandraStore::new(&test_cassandra_config()).await })
        .await
        .map_err(Into::into)
}

/// A directory over the shared store, publishing `lease`.
pub(crate) async fn directory(lease: Duration) -> Result<NodeDirectory> {
    let ttl = RegistrationTtl::try_from(lease)?;
    Ok(NodeDirectory::new(store().await?.clone(), ttl).await?)
}

/// A token unique to one evaluation, so generated group ids collide neither
/// across iterations nor across runs.
pub(crate) fn token() -> String {
    Uuid::new_v4().simple().to_string()
}

/// A membership under a fresh token.
pub(crate) fn membership() -> GroupMembership {
    let token = token();
    GroupMembership {
        cluster: Flexstr::make(&format!("{token}-cluster")),
        group: Flexstr::make(&format!("{token}-group")),
    }
}

/// A fixed registration for `node`, with every optional field present.
pub(crate) fn registration(node: NodeId, group: GroupMembership) -> NodeRegistration {
    NodeRegistration {
        node,
        direct: Endpoint {
            host: Host::make("10.1.2.3"),
            port: 7777,
        },
        advertised: Some(Endpoint {
            host: Host::make("gateway.example"),
            port: 443,
        }),
        network: Some(NetworkId::make("east")),
        group: Some(group),
        hostname: Host::make("worker-7"),
    }
}

/// The membership index shards that hold `node`, found by scanning every
/// shard.
///
/// Scanning rather than recomputing the production derivation is deliberate:
/// an expectation built on `shard_for` would move with a wrong derivation and
/// could never observe one.
pub(crate) async fn member_shards(membership: &GroupMembership, node: NodeId) -> Result<Vec<i32>> {
    let session = store().await?.session();
    let cql = format!(
        "SELECT node_id FROM {TEST_KEYSPACE}.{TABLE_NODES_BY_GROUP} WHERE kafka_cluster_id = ? \
         AND group_id = ? AND shard = ? AND node_id = ?"
    );
    let mut found = Vec::new();
    for shard in 0_i32..GROUP_SHARDS as i32 {
        let row = session
            .query_unpaged(
                cql.as_str(),
                (
                    membership.cluster.as_str(),
                    membership.group.as_str(),
                    shard,
                    Uuid::from(node),
                ),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Uuid,)>()?;
        if row.is_some() {
            found.push(shard);
        }
    }
    Ok(found)
}

/// Converts a property body's `Result<()>` into a `TestResult`: a store or
/// setup failure is a broken environment, never a shrinkable property failure.
pub(crate) fn finish(result: Result<()>) -> TestResult {
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}

/// A node id from sixteen generated bytes, so the generator covers ids no UUID
/// version would mint.
fn node_id(g: &mut Gen) -> NodeId {
    let mut bytes = [0_u8; 16];
    for byte in &mut bytes {
        *byte = u8::arbitrary(g);
    }
    NodeId::from_bytes(bytes)
}

/// A generated endpoint. Ports span the whole range, both ends included.
fn endpoint(g: &mut Gen) -> Endpoint {
    Endpoint {
        host: Host::make(&label(g)),
        port: u16::arbitrary(g),
    }
}

/// A generated label of one to [`MAX_LABEL`] characters.
fn label(g: &mut Gen) -> String {
    let length = 1 + usize::arbitrary(g) % MAX_LABEL;
    (0..length)
        .map(|_| char::from(*g.choose(LABEL_ALPHABET).unwrap_or(&b'a')))
        .collect()
}
