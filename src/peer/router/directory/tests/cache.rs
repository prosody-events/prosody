use super::support::{cassandra_directory, finish, registration, test_directory_holding};
use crate::peer::router::PeerId;
use crate::peer::router::directory::cache::AddressResolver;
use crate::peer::router::directory::{
    DirectAddress, PeerDirectory, PeerRegistration, RegistrationTtl,
};
use crate::test_util::{TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use futures::future::join_all;
use quickcheck::{QuickCheck, TestResult};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::task::yield_now;
use tonic::codegen::http::Uri;

/// How many registrations the cache under test admits.
const CAPACITY: usize = 8;

/// How many peers the shared pool holds. More than [`CAPACITY`], so a request
/// stream can push the cache past its bound.
const POOL: usize = 12;

/// What the test directory holds: the whole pool, so its eviction cannot
/// produce a cache answer.
const POOL_CAPACITY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(POOL - 1);

/// The head of every request stream. Nine distinct ids exceed [`CAPACITY`], so
/// the occupancy bound is exercised on every iteration.
const PREFIX: [usize; 10] = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8];

/// How many callers ask for one cold peer at once.
const CONCURRENT: usize = 16;

const POOL_LEASE: Duration = Duration::from_hours(1);

/// The Cassandra pool, registered once for the whole test process under a lease
/// long enough to outlive the run.
static POOL_PEERS: OnceCell<Vec<(PeerId, Uri)>> = OnceCell::const_new();

/// A directory that counts reads and yields each one for concurrency tests.
#[derive(Clone)]
struct CountingDirectory<D> {
    inner: D,
    reads: Arc<AtomicUsize>,
}

impl<D: PeerDirectory> PeerDirectory for CountingDirectory<D> {
    type Error = D::Error;

    fn ttl(&self) -> RegistrationTtl {
        self.inner.ttl()
    }

    async fn register(&self, registration: &PeerRegistration) -> Result<(), Self::Error> {
        self.inner.register(registration).await
    }

    async fn read(&self, peer: PeerId) -> Result<Option<PeerRegistration>, Self::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        yield_now().await;
        self.inner.read(peer).await
    }

    async fn deregister(&self, registration: &PeerRegistration) -> Result<(), Self::Error> {
        self.inner.deregister(registration).await
    }
}

/// The bounds that make the address cache safe to key by a peer id an outsider
/// chooses, proved together over one generated request stream.
///
/// **Occupancy.** However long the stream and however many distinct peers it
/// names, the cache never holds more than its capacity, and no request issues
/// more than one directory read.
///
/// **Single flight.** Many callers asking for one cold peer at once issue
/// exactly one read; every caller after the first parks on the placeholder.
/// This is the bound that matters on the response path, and it is the one the
/// cache guarantees — a cached entry itself is best-effort, because
/// `quick_cache` may evict an entry it admitted into a full cache at once.
///
/// **Absence.** A peer the directory does not hold is not cached. A later
/// registration for that id can therefore become visible.
///
/// The cache reads through a [`PeerDirectory`] and nothing more, so this is the
/// default loop and it needs no cluster.
#[test]
fn prop_address_cache_bounded_single_flight() {
    fn property(generated: Vec<usize>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = test_directory_holding(POOL_CAPACITY, POOL_LEASE)?;
            let pool = register_pool(&directory).await?;
            run_address_cache_cases(&directory, &pool, generated).await
        }))
    }
    init_test_logging();
    QuickCheck::new().quickcheck(property as fn(Vec<usize>) -> TestResult);
}

/// The same bounds over the Cassandra directory, so a read that crosses the
/// wire is held to them too.
#[test]
fn prop_address_cache_bounded_single_flight_over_cassandra() {
    fn property(generated: Vec<usize>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = cassandra_directory(POOL_LEASE).await?;
            let pool = POOL_PEERS
                .get_or_try_init(|| register_pool(&directory))
                .await?;
            run_address_cache_cases(&directory, pool, generated).await
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(10))
        .quickcheck(property as fn(Vec<usize>) -> TestResult);
}

/// Runs every address-cache case against one directory and one pool.
async fn run_address_cache_cases<D: PeerDirectory>(
    directory: &D,
    pool: &[(PeerId, Uri)],
    generated: Vec<usize>,
) -> Result<()> {
    occupancy_holds(directory, pool, generated).await?;
    one_read_per_cold_burst(directory, pool).await?;
    absence_is_not_cached(directory).await
}

/// Drives the generated stream and checks the two bounds that hold at every
/// position: occupancy, and one read per request at most. Each served value is
/// checked against the URI its peer registered, so a mixed-up entry is caught.
async fn occupancy_holds<D: PeerDirectory>(
    directory: &D,
    pool: &[(PeerId, Uri)],
    generated: Vec<usize>,
) -> Result<()> {
    let (resolver, reads) = resolver(directory.clone());
    let requests = PREFIX
        .iter()
        .copied()
        .chain(generated.into_iter().map(|index| index % POOL));
    for (position, index) in requests.enumerate() {
        let (peer, uri) = &pool[index];
        let before = reads.load(Ordering::Relaxed);
        let registration = resolver
            .resolve(*peer)
            .await?
            .ok_or_else(|| eyre!("request {position}: a registered peer must resolve"))?;
        let issued = reads.load(Ordering::Relaxed) - before;
        assert!(
            resolver.len() <= CAPACITY,
            "request {position}: the cache holds {} entries, over its capacity of {CAPACITY}",
            resolver.len()
        );
        assert!(
            issued <= 1,
            "request {position}: a miss must read through once, not {issued} times"
        );
        assert_eq!(
            registration.direct.endpoint().uri(),
            uri,
            "request {position}: the cache served another peer's registration"
        );
    }
    Ok(())
}

/// A burst of callers for one cold peer issues one read: the winner takes the
/// placeholder and every other caller parks on it.
async fn one_read_per_cold_burst<D: PeerDirectory>(
    directory: &D,
    pool: &[(PeerId, Uri)],
) -> Result<()> {
    let (resolver, reads) = resolver(directory.clone());
    let (peer, uri) = &pool[1];
    let burst = join_all((0..CONCURRENT).map(|_| resolver.resolve(*peer))).await;
    for served in burst {
        let served = served?.ok_or_else(|| eyre!("a registered peer must resolve"))?;
        assert_eq!(
            served.direct.endpoint().uri(),
            uri,
            "every caller in the burst must be served the peer it asked for"
        );
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "{CONCURRENT} callers for one cold peer must issue one read"
    );
    Ok(())
}

/// A registration that appears after a miss becomes visible on the next read.
async fn absence_is_not_cached<D: PeerDirectory>(directory: &D) -> Result<()> {
    let (resolver, reads) = resolver(directory.clone());
    let peer = PeerId::new();
    assert!(
        resolver.resolve(peer).await?.is_none(),
        "a peer the directory does not hold must resolve as absent"
    );
    directory.register(&registration(peer)).await?;
    assert!(
        resolver.resolve(peer).await?.is_some(),
        "a registration written after a miss must become visible"
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        2,
        "a miss must not enter the cache"
    );
    Ok(())
}

/// Adds read observation to a directory and builds its resolver.
fn resolver<D: PeerDirectory>(
    directory: D,
) -> (AddressResolver<CountingDirectory<D>>, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let directory = CountingDirectory {
        inner: directory,
        reads: Arc::clone(&reads),
    };
    (AddressResolver::new(CAPACITY, directory), reads)
}

/// Registers [`POOL`] peers with distinct socket addresses.
async fn register_pool<D: PeerDirectory>(directory: &D) -> Result<Vec<(PeerId, Uri)>> {
    let mut peers = Vec::with_capacity(POOL);
    for index in 0..POOL {
        let peer = PeerId::new();
        let direct = DirectAddress::new(SocketAddr::from((
            [127, 0, 0, 1],
            10_000 + u16::try_from(index)?,
        )))?;
        let uri = direct.endpoint().uri().clone();
        let mut written = registration(peer);
        written.direct = direct;
        directory.register(&written).await?;
        peers.push((peer, uri));
    }
    Ok(peers)
}
