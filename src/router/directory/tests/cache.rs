use super::support::{cassandra_directory, finish, registration, test_directory_holding, token};
use crate::router::directory::cache::AddressCache;
use crate::router::directory::{Endpoint, NodeDirectory, NodeRegistration, RegistrationTtl};
use crate::router::{Host, NodeId};
use crate::test_util::{TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use futures::future::join_all;
use quanta::Clock;
use quickcheck::{QuickCheck, TestResult};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::task::yield_now;

/// How many registrations the cache under test admits.
const CAPACITY: usize = 8;

/// How many nodes the shared pool holds. More than [`CAPACITY`], so a request
/// stream can push the cache past its bound.
const POOL: usize = 12;

/// What the test directory holds: the whole pool, so no answer
/// here can be given by an eviction inside the directory rather than by the
/// cache.
///
/// `match`, not `NonZeroUsize::new(..).unwrap_or(..)`: `Option::unwrap_or` is
/// not const, and the tests forbid `unwrap`.
const POOL_CAPACITY: NonZeroUsize = match NonZeroUsize::new(POOL) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

/// The head of every request stream. Nine distinct ids exceed [`CAPACITY`], so
/// the occupancy bound is exercised on every iteration.
const PREFIX: [usize; 10] = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8];

/// How many callers ask for one cold node at once.
const CONCURRENT: usize = 16;

/// The lease the cached entries age on.
const POOL_LEASE: Duration = Duration::from_hours(1);

/// The Cassandra pool, registered once for the whole test process under a lease
/// long enough to outlive the run.
static POOL_NODES: OnceCell<Vec<(NodeId, u16)>> = OnceCell::const_new();

/// The three bounds that make the address cache safe to key by a node id an
/// outsider chooses, proved together over one generated request stream.
///
/// **Occupancy.** However long the stream and however many distinct nodes it
/// names, the cache never holds more than its capacity, and no request issues
/// more than one directory read.
///
/// **Single flight.** Many callers asking for one cold node at once issue
/// exactly one read; every caller after the first parks on the placeholder.
/// This is the bound that matters on the response path, and it is the one the
/// cache guarantees — a cached entry itself is best-effort, because
/// `quick_cache` may evict an entry it admitted into a full cache at once.
///
/// **Age.** A fresh entry is served with no read. Past the lease the same
/// entry is read again rather than served.
///
/// **Absence.** A node the directory does not hold is cached as absent, so a
/// burst for an unknown id issues one read and not one per request.
///
/// The cache reads through a [`NodeDirectory`] and nothing more, so this is the
/// default loop and it needs no cluster.
#[test]
fn prop_address_cache_bounded_single_flight() {
    fn property(generated: Vec<usize>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = test_directory_holding(POOL_CAPACITY, POOL_LEASE)?;
            let pool = register_pool(&directory).await?;
            run_address_cache_cases(&directory, &pool, directory.ttl(), generated).await
        }))
    }
    init_test_logging();
    QuickCheck::new().quickcheck(property as fn(Vec<usize>) -> TestResult);
}

/// The same four bounds over the Cassandra directory, so a read that crosses
/// the wire is held to them too.
#[test]
fn prop_address_cache_bounded_single_flight_over_cassandra() {
    fn property(generated: Vec<usize>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = cassandra_directory(POOL_LEASE).await?;
            let pool = POOL_NODES
                .get_or_try_init(|| register_pool(&directory))
                .await?;
            run_address_cache_cases(&directory, pool, directory.ttl(), generated).await
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(10))
        .quickcheck(property as fn(Vec<usize>) -> TestResult);
}

/// Runs every address-cache case against one directory and one pool.
async fn run_address_cache_cases<D: NodeDirectory>(
    directory: &D,
    pool: &[(NodeId, u16)],
    ttl: RegistrationTtl,
    generated: Vec<usize>,
) -> Result<()> {
    occupancy_holds(directory, pool, ttl, generated).await?;
    one_read_per_cold_burst(directory, pool, ttl).await?;
    a_fresh_entry_is_served_until_the_lease_ends(directory, pool, ttl).await?;
    absence_is_cached(directory, ttl).await
}

/// Drives the generated stream and checks the two bounds that hold at every
/// position: occupancy, and one read per request at most. Each served value is
/// checked against the port its node registered, so a mixed-up entry is caught.
async fn occupancy_holds<D: NodeDirectory>(
    directory: &D,
    pool: &[(NodeId, u16)],
    ttl: RegistrationTtl,
    generated: Vec<usize>,
) -> Result<()> {
    let (clock, _mock) = Clock::mock();
    let cache = AddressCache::with_clock(CAPACITY, ttl, clock);
    let reads = AtomicUsize::new(0);
    let requests = PREFIX
        .iter()
        .copied()
        .chain(generated.into_iter().map(|index| index % POOL));
    for (position, index) in requests.enumerate() {
        let (node, port) = pool[index];
        let before = reads.load(Ordering::Relaxed);
        let resolved = resolve(&cache, directory, &reads, node)
            .await?
            .ok_or_else(|| eyre!("request {position}: a registered node must resolve"))?;
        let issued = reads.load(Ordering::Relaxed) - before;
        assert!(
            cache.len() <= CAPACITY,
            "request {position}: the cache holds {} entries, over its capacity of {CAPACITY}",
            cache.len()
        );
        assert!(
            issued <= 1,
            "request {position}: a miss must read through once, not {issued} times"
        );
        assert_eq!(
            resolved.direct.port, port,
            "request {position}: the cache served another node's registration"
        );
    }
    Ok(())
}

/// A burst of callers for one cold node issues one read: the winner takes the
/// placeholder and every other caller parks on it.
async fn one_read_per_cold_burst<D: NodeDirectory>(
    directory: &D,
    pool: &[(NodeId, u16)],
    ttl: RegistrationTtl,
) -> Result<()> {
    let (clock, _mock) = Clock::mock();
    let cache = AddressCache::with_clock(CAPACITY, ttl, clock);
    let reads = AtomicUsize::new(0);
    let (node, port) = pool[1];
    let burst = join_all((0..CONCURRENT).map(|_| resolve(&cache, directory, &reads, node))).await;
    for served in burst {
        let served = served?.ok_or_else(|| eyre!("a registered node must resolve"))?;
        assert_eq!(
            served.direct.port, port,
            "every caller in the burst must be served the node it asked for"
        );
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "{CONCURRENT} callers for one cold node must issue one read"
    );
    Ok(())
}

/// A fresh entry is served without a read; past the lease the same entry is
/// read again. The cache holds one entry here, so admission cannot be undone
/// by an eviction and the hit is deterministic.
async fn a_fresh_entry_is_served_until_the_lease_ends<D: NodeDirectory>(
    directory: &D,
    pool: &[(NodeId, u16)],
    ttl: RegistrationTtl,
) -> Result<()> {
    let (clock, mock) = Clock::mock();
    let cache = AddressCache::with_clock(CAPACITY, ttl, clock);
    let reads = AtomicUsize::new(0);
    let (node, _) = pool[0];

    drop(resolve(&cache, directory, &reads, node).await?);
    let after_fill = reads.load(Ordering::Relaxed);
    drop(resolve(&cache, directory, &reads, node).await?);
    assert_eq!(
        reads.load(Ordering::Relaxed),
        after_fill,
        "a fresh entry must be served without a read"
    );

    mock.increment(POOL_LEASE + Duration::from_secs(1));
    drop(resolve(&cache, directory, &reads, node).await?);
    assert!(
        reads.load(Ordering::Relaxed) > after_fill,
        "an entry older than the lease must be read again, not served"
    );
    assert!(
        cache.len() <= CAPACITY,
        "the refill pushed the cache over its capacity of {CAPACITY}"
    );
    Ok(())
}

/// A node the directory does not hold is cached as absent, so repeated
/// requests for an unknown id issue one read and not one per request.
async fn absence_is_cached<D: NodeDirectory>(directory: &D, ttl: RegistrationTtl) -> Result<()> {
    let (clock, _mock) = Clock::mock();
    let cache = AddressCache::with_clock(CAPACITY, ttl, clock);
    let reads = AtomicUsize::new(0);
    let unknown = NodeId::new();
    for attempt in 1_u8..=3 {
        assert!(
            resolve(&cache, directory, &reads, unknown).await?.is_none(),
            "attempt {attempt}: a node the directory does not hold must resolve as absent"
        );
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "repeated requests for an absent node must issue one read"
    );
    Ok(())
}

/// Resolves `node`, counting the directory reads the cache actually issues.
///
/// The fill suspends once before it reads. That is what makes
/// [`one_read_per_cold_burst`] a detector: the test directory answers without
/// ever suspending, so a fill that never yields would run to completion before
/// the second caller of a burst is polled, and every later caller would find a
/// fresh entry however the cache filled it. With the yield the first caller
/// parks holding the placeholder, so a cache that lost single flight issues one
/// read per caller and the count reds.
async fn resolve<D: NodeDirectory>(
    cache: &AddressCache,
    directory: &D,
    reads: &AtomicUsize,
    node: NodeId,
) -> Result<Option<Arc<NodeRegistration>>> {
    Ok(cache
        .resolve(node, || async move {
            reads.fetch_add(1, Ordering::Relaxed);
            yield_now().await;
            directory.read(node).await
        })
        .await?)
}

/// Registers [`POOL`] nodes, each with a distinct direct port so a mixed-up
/// cache entry serves an observably wrong value.
async fn register_pool<D: NodeDirectory>(directory: &D) -> Result<Vec<(NodeId, u16)>> {
    let mut nodes = Vec::with_capacity(POOL);
    for index in 0..POOL {
        let node = NodeId::new();
        // The port is the value the assertions join on, so it must differ per
        // node.
        let port = 20_000 + index as u16;
        let mut written = registration(node);
        written.direct = Endpoint {
            host: Host::make(&format!("pool-{}", token())),
            port,
        };
        directory.register(&written).await?;
        nodes.push((node, port));
    }
    Ok(nodes)
}
