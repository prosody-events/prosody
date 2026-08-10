//! Test data and backend constructors shared by the directory and runtime.
//!
//! Isolation follows the Cassandra row rule: a directory row is keyed by node
//! id alone and every test mints fresh ids, so rows are disjoint in the shared
//! `prosody_test` keyspace and no test creates a keyspace of its own.

use super::suite::SUITE_CAPACITY;
use crate::cassandra::CassandraStore;
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::directory::{
    Endpoint, NetworkId, NodeDirectory, NodeRegistration, RegistrationTtl,
};
use crate::router::{Host, MAX_LABEL_BYTES, NodeId};
use crate::test_util::test_cassandra_config;
use color_eyre::Result;
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen, TestResult};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use uuid::Uuid;

/// Characters a generated host, hostname or label is built from.
const LABEL_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789.:-";

/// Longest generated label. Every label the directory resolves is inside
/// [`MAX_LABEL_BYTES`], so a generated registration stays resolvable.
/// [`run_label_bound_case`](super::suite::run_label_bound_case) owns the other
/// side.
const MAX_LABEL: usize = MAX_LABEL_BYTES;

/// One store per test process. `CassandraStore::new` runs the migrator, so a
/// store per property iteration would spend the run on schema checks.
static STORE: OnceCell<CassandraStore> = OnceCell::const_new();

/// A bounded in-process directory for tests that do not need Cassandra.
#[derive(Clone)]
pub(crate) struct TestDirectory {
    registrations: Arc<Mutex<Vec<NodeRegistration>>>,
    capacity: usize,
    ttl: RegistrationTtl,
}

impl TestDirectory {
    pub(crate) fn new(capacity: NonZeroUsize, ttl: RegistrationTtl) -> Self {
        Self {
            registrations: Arc::new(Mutex::new(Vec::with_capacity(capacity.get()))),
            capacity: capacity.get(),
            ttl,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.registrations.lock().len()
    }
}

impl NodeDirectory for TestDirectory {
    type Error = Infallible;

    fn ttl(&self) -> RegistrationTtl {
        self.ttl
    }

    async fn register(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        let mut registrations = self.registrations.lock();
        if let Some(stored) = registrations
            .iter_mut()
            .find(|stored| stored.node == registration.node)
        {
            stored.clone_from(registration);
        } else {
            if registrations.len() == self.capacity {
                registrations.remove(0);
            }
            registrations.push(registration.clone());
        }
        Ok(())
    }

    async fn read(&self, node: NodeId) -> Result<Option<NodeRegistration>, Self::Error> {
        Ok(self
            .registrations
            .lock()
            .iter()
            .find(|registration| registration.node == node)
            .cloned())
    }

    async fn deregister(&self, registration: &NodeRegistration) -> Result<(), Self::Error> {
        self.registrations
            .lock()
            .retain(|stored| stored.node != registration.node);
        Ok(())
    }
}

/// A registration whose every field is generated.
#[derive(Clone, Debug)]
pub(crate) struct ArbRegistration(pub(crate) NodeRegistration);

impl Arbitrary for ArbRegistration {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(NodeRegistration {
            node: node_id(g),
            direct: endpoint(g),
            advertised: bool::arbitrary(g).then(|| endpoint(g)),
            network: bool::arbitrary(g).then(|| NetworkId::make(&label(g))),
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
pub(crate) async fn cassandra_directory(lease: Duration) -> Result<CassandraNodeDirectory> {
    let ttl = RegistrationTtl::try_from(lease)?;
    Ok(CassandraNodeDirectory::new(store().await?.clone(), ttl).await?)
}

/// An in-process directory holding [`SUITE_CAPACITY`] registrations under
/// `lease`.
pub(crate) fn test_directory(lease: Duration) -> Result<TestDirectory> {
    test_directory_holding(SUITE_CAPACITY, lease)
}

/// An in-process directory holding `capacity` registrations under `lease`, for
/// a suite whose pool is larger than [`SUITE_CAPACITY`].
pub(crate) fn test_directory_holding(
    capacity: NonZeroUsize,
    lease: Duration,
) -> Result<TestDirectory> {
    Ok(TestDirectory::new(
        capacity,
        RegistrationTtl::try_from(lease)?,
    ))
}

/// A token unique to one evaluation.
pub(crate) fn token() -> String {
    Uuid::new_v4().simple().to_string()
}

/// A fixed registration for `node`, with every optional field present.
pub(crate) fn registration(node: NodeId) -> NodeRegistration {
    NodeRegistration {
        node,
        direct: Endpoint::from_static("http://10.1.2.3:7777"),
        advertised: Some(Endpoint::from_static("http://gateway.example:443")),
        network: Some(NetworkId::make("east")),
        hostname: Host::make("worker-7"),
    }
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
pub(crate) fn node_id(g: &mut Gen) -> NodeId {
    let mut bytes = [0_u8; 16];
    for byte in &mut bytes {
        *byte = u8::arbitrary(g);
    }
    NodeId::from_bytes(bytes)
}

/// A generated endpoint. Ports span the whole range, both ends included.
pub(crate) fn endpoint(g: &mut Gen) -> Endpoint {
    let connect = format!("http://{}:{}", label(g), 1 + u16::arbitrary(g) % u16::MAX);
    match Endpoint::from_shared(connect) {
        Ok(endpoint) => endpoint,
        Err(_) => Endpoint::from_static("http://fallback.example:1"),
    }
}

/// A generated label of one to [`MAX_LABEL`] characters.
pub(crate) fn label(g: &mut Gen) -> String {
    let length = 1 + usize::arbitrary(g) % MAX_LABEL;
    (0..length)
        .map(|_| char::from(*g.choose(LABEL_ALPHABET).unwrap_or(&b'a')))
        .collect()
}
