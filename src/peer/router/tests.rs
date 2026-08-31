use super::{
    EndpointKind, LocalTarget, NetworkRoute, NetworkRouter, PeerId, RelayHop, choose_route,
};
use crate::peer::metrics::PeerMetrics;
use crate::peer::requester::registry::PendingRegistry;
use crate::peer::requester::registry::tests::TestRegistration;
use crate::peer::response::frame::FrameHeader;
use crate::peer::response::frame::encode::{Staged, stage_success};
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{
    DropReason, PeerMetricSource, ResponseRoute, RouteDelivery, RouteOutcome, Then,
};
use crate::peer::router::Host;
use crate::peer::router::directory::cache::AddressResolver;
use crate::peer::router::directory::tests::support::TestDirectory;
use crate::peer::router::directory::tests::support::{registration, test_directory};
use crate::peer::router::directory::{
    DirectAddress, Endpoint, NetworkId, PeerDirectory, PeerRegistration,
};
use crate::peer::router::loopback::LoopbackSender;
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::future::ready;
use std::net::SocketAddr;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::{Uuid, Version};

#[derive(Clone)]
struct CountedNetwork(Arc<AtomicUsize>, PeerMetrics);

impl ResponseRoute for CountedNetwork {
    fn deliver(
        &self,
        _frame: Staged,
        _deadline: RequestDeadline,
        _context: &opentelemetry::Context,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        ready(Ok(RouteOutcome::Delivered(RouteDelivery::Remote(
            EndpointKind::Direct,
        ))))
    }
}

impl PeerMetricSource for CountedNetwork {
    fn peer_metrics(&self) -> &PeerMetrics {
        &self.1
    }
}

/// The lease the router's own read runs under.
const LEASE: Duration = Duration::from_secs(30);

/// How many registrations the router's cache holds in this suite.
const CACHE_CAPACITY: usize = 8;

/// Ids are minted fresh, never derived from anything a restart could repeat:
/// two mints of the same process already differ, and each is a random UUID.
#[test]
fn every_minted_peer_id_is_a_fresh_random_uuid() {
    let first = PeerId::new();
    let second = PeerId::new();
    assert_ne!(first, second, "two mints must not collide");
    for id in [first, second] {
        assert_eq!(
            Uuid::from_bytes(id.into_bytes()).get_version(),
            Some(Version::Random),
            "{id} must be a random UUID"
        );
    }
}

/// A router hands out only what a peer published, and nothing at all for a peer
/// the directory does not hold.
///
/// This is where the addressing rule is enforced rather than described: a
/// response reaches an address because a process wrote that address about
/// itself, and there is no other source for one.
#[test]
fn a_router_addresses_only_what_the_directory_published() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let directory = test_directory(LEASE)?;
        let published = registration(PeerId::new());
        directory.register(&published).await?;

        let router = test_router(directory);

        let route = router
            .route(published.peer)
            .await?
            .ok_or_else(|| eyre!("a published peer must resolve"))?;
        let (_, address) = route.endpoint();
        assert_eq!(address.uri(), published.direct.endpoint().uri());
        assert!(
            router
                .direct(published.peer)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.endpoint().uri()),
            "the lookup a forward uses must hand out the endpoint the peer published"
        );
        assert!(
            router.route(PeerId::new()).await?.is_none(),
            "a peer the directory does not hold must reach no address"
        );
        assert!(
            router.direct(PeerId::new()).await?.is_none(),
            "a peer the directory does not hold must reach no endpoint to forward to"
        );
        Ok(())
    })
}

/// A router answers from its own cache, and every clone of it shares the one
/// cache and transport the process owns.
///
/// The entry is removed after the first resolution, so a router that read the
/// directory again would answer nothing. The lease is far longer than this test
/// runs, so the cached entry cannot age out first.
#[test]
fn a_router_reads_through_its_cache_and_shares_it_with_every_clone() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let directory = test_directory(LEASE)?;
        let published = registration(PeerId::new());
        directory.register(&published).await?;

        let router = test_router(directory.clone());

        assert!(
            router
                .direct(published.peer)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.endpoint().uri()),
            "a published peer must resolve"
        );
        directory.deregister(&published).await?;
        assert!(
            directory.read(published.peer).await?.is_none(),
            "the entry must be gone before the cached answer is asserted"
        );
        assert!(
            router
                .direct(published.peer)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.endpoint().uri()),
            "a router must answer from its cache once the entry is gone"
        );

        let clone = router.clone();
        assert!(
            ptr::eq(router.sender(), clone.sender()),
            "a clone must share the one transport the process owns"
        );
        Ok(())
    })
}

/// How the two labels stand to each other.
///
/// The rules read only whether both labels are present and whether they match,
/// so a case is one of these five shapes rather than a pair of free strings.
#[derive(Clone, Copy, Debug)]
enum Labels {
    /// Both processes carry the same label.
    Agree,
    /// Both carry a label, and the two differ.
    Differ,
    /// Only the dialer carries one.
    DialerOnly,
    /// Only the target carries one.
    TargetOnly,
    /// Neither carries one.
    Neither,
}

/// What the target published beside its direct endpoint.
#[derive(Clone, Copy, Debug)]
enum Published {
    /// A direct endpoint alone.
    DirectOnly,
    /// A direct endpoint and an entry point that reaches it.
    WithAdvertised,
}

/// One case: the label pair, and what the target published.
#[derive(Clone, Copy, Debug)]
struct Declared {
    labels: Labels,
    published: Published,
}

impl Arbitrary for Declared {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            labels: *g
                .choose(&[
                    Labels::Agree,
                    Labels::Differ,
                    Labels::DialerOnly,
                    Labels::TargetOnly,
                    Labels::Neither,
                ])
                .unwrap_or(&Labels::Neither),
            published: *g
                .choose(&[Published::DirectOnly, Published::WithAdvertised])
                .unwrap_or(&Published::DirectOnly),
        }
    }
}

impl Labels {
    /// The dialer's own label.
    fn here(self) -> Option<NetworkId> {
        match self {
            Self::Agree | Self::Differ | Self::DialerOnly => Some(NetworkId::make("here")),
            Self::TargetOnly | Self::Neither => None,
        }
    }

    /// The label the target published.
    fn there(self) -> Option<NetworkId> {
        match self {
            Self::Agree => Some(NetworkId::make("here")),
            Self::Differ | Self::TargetOnly => Some(NetworkId::make("elsewhere")),
            Self::DialerOnly | Self::Neither => None,
        }
    }
}

/// The declared rules decide one route.
///
/// The expected route is written out here as data. Every row is a decision an
/// operator can read off the labels alone: neighbours use the direct address, a
/// peer known to be elsewhere is reached only through its entry point or not at
/// all, and an unknown label uses the direct address.
#[quickcheck]
fn prop_a_route_follows_the_declared_labels(declared: Declared) -> TestResult {
    let advertised = matches!(declared.published, Published::WithAdvertised);
    let direct = Endpoint::from_static("http://10.0.0.9:7000");
    let entry = Endpoint::from_static("http://10.0.0.9:7001");
    let published = PeerRegistration {
        peer: PeerId::new(),
        direct: match DirectAddress::new(SocketAddr::from(([10, 0, 0, 9], 7000))) {
            Ok(address) => address,
            Err(error) => return TestResult::error(error.to_string()),
        },
        advertised: advertised.then(|| entry.clone()),
        network: declared.labels.there(),
        hostname: Host::make("declared"),
    };

    // The table, as an operator reads it off the two labels.
    let expected = match (declared.labels, advertised) {
        (Labels::Differ, true) => Some((EndpointKind::Advertised, entry.uri())),
        (Labels::Differ, false) => None,
        _ => Some((EndpointKind::Direct, direct.uri())),
    };

    let Some(route) = choose_route(declared.labels.here().as_ref(), &published) else {
        return if expected.is_none() {
            TestResult::passed()
        } else {
            TestResult::error(format!(
                "{declared:?} must reach {expected:?}, but reached nothing"
            ))
        };
    };
    let Some(expected) = expected else {
        return TestResult::error(format!(
            "{declared:?} must reach nothing, but reached {route:?}"
        ));
    };
    let (kind, endpoint) = route.endpoint();
    assert_eq!((kind, endpoint.uri()), expected);
    TestResult::passed()
}

#[test]
fn a_local_target_never_reaches_the_network_route() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let peer = PeerId::new();
        let subsystem = SubsystemName::try_new("local")?;
        let registry = PendingRegistry::new();
        let request = TestRegistration::new(
            &registry,
            slice::from_ref(&subsystem),
            Duration::from_secs(1),
        )?;
        let network_calls = Arc::new(AtomicUsize::new(0));
        let route = Then(
            LocalTarget::new(peer, registry),
            CountedNetwork(Arc::clone(&network_calls), PeerMetrics::default()),
        );
        let frame = stage_success::<CountingCodec>(
            &FrameHeader {
                target: peer,
                request: request.id(),
                subsystem,
                relay: None,
            },
            &Vec::new(),
        )?;
        let delivered = route
            .deliver(
                frame,
                RequestDeadline::from_unix_micros(4_102_444_800_000_000),
                &opentelemetry::Context::new(),
            )
            .await;

        assert!(matches!(
            delivered,
            Ok(RouteOutcome::Delivered(RouteDelivery::Local))
        ));
        assert_eq!(network_calls.load(Ordering::Relaxed), 0);
        Ok(())
    })
}

fn test_router(directory: TestDirectory) -> NetworkRoute<LoopbackSender, TestDirectory> {
    let (transport, _recorded) = LoopbackSender::new();
    NetworkRoute::new(
        AddressResolver::new(CACHE_CAPACITY, directory),
        Arc::new(transport),
        None,
        PeerMetrics::default(),
    )
}
