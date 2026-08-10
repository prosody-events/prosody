use super::{
    LocalTarget, NetworkRoute, NetworkRouter, NodeId, Preference, RelayHop, SendFailure,
    choose_route,
};
use crate::requester::registry::PendingRegistry;
use crate::requester::registry::tests::TestRegistration;
use crate::response::ResponseStatus;
use crate::response::frame::FrameHeader;
use crate::response::frame::encode::{Staged, stage};
use crate::response::frame::tests::CountingCodec;
use crate::response::headers::RequestDeadline;
use crate::response::sender::{DropReason, ResponseRoute, RouteDelivery, RouteOutcome, Then};
use crate::router::Host;
use crate::router::directory::cache::AddressResolver;
use crate::router::directory::tests::support::TestDirectory;
use crate::router::directory::tests::support::{registration, test_directory};
use crate::router::directory::{Endpoint, NetworkId, NodeDirectory, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::LoopbackSender;
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tonic::Code;
use tonic::codegen::http::Uri;
use uuid::{Uuid, Version};

#[derive(Clone)]
struct CountedNetwork(Arc<AtomicUsize>);

impl ResponseRoute for CountedNetwork {
    async fn deliver(
        &self,
        _frame: Staged,
        _deadline: RequestDeadline,
        _context: &opentelemetry::Context,
    ) -> Result<RouteOutcome, DropReason> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(RouteOutcome::Delivered(RouteDelivery::Remote {
            preference: Preference::Direct,
            from: None,
        }))
    }
}

/// The lease the router's own read runs under.
const LEASE: Duration = Duration::from_secs(30);

/// How many registrations the router's cache holds in this suite.
const CACHE_CAPACITY: usize = 8;

/// Ids are minted fresh, never derived from anything a restart could repeat:
/// two mints of the same process already differ, and each is a random UUID.
#[test]
fn every_minted_node_id_is_a_fresh_random_uuid() {
    let first = NodeId::new();
    let second = NodeId::new();
    assert_ne!(first, second, "two mints must not collide");
    for id in [first, second] {
        assert_eq!(
            Uuid::from_bytes(id.into_bytes()).get_version(),
            Some(Version::Random),
            "{id} must be a random UUID"
        );
    }
}

/// What each failure means to the two questions the send path asks it.
///
/// The answers are written out here as data rather than read back from the
/// classifiers, so a classifier that changed its mind about a failure fails
/// this test. The two columns are different decisions and neither can be read
/// off the other:
///
/// - **Ambiguous.** Another attempt on this endpoint could still get an answer.
/// - **Wrong endpoint.** Nothing proved this endpoint serves the node, so the
///   node's other endpoint is worth trying instead.
///
/// The second column carries a third claim the send path depends on: a failure
/// that is not a wrong endpoint is always a status, so the walk may record the
/// endpoint that gave it without asking anything else.
#[test]
fn every_failure_answers_the_two_questions_the_send_path_asks() {
    // failure, wrong endpoint
    let table = [
        (SendFailure::Unreachable, true),
        (SendFailure::Expired, true),
        (answer(Code::Unavailable), true),
        (answer(Code::Unimplemented), true),
        (answer(Code::Cancelled), true),
        (answer(Code::DeadlineExceeded), false),
        (answer(Code::FailedPrecondition), false),
        (answer(Code::ResourceExhausted), false),
        (answer(Code::NotFound), false),
        (answer(Code::Ok), false),
    ];
    for (failure, wrong_endpoint) in table {
        assert_eq!(
            failure.is_wrong_endpoint(),
            wrong_endpoint,
            "{failure} must{} send the response to the node's other endpoint",
            if wrong_endpoint { "" } else { " not" }
        );
        assert!(
            wrong_endpoint || matches!(failure, SendFailure::Status(_)),
            "{failure} is not a wrong endpoint, so it must be a status the node itself answered"
        );
    }
}

/// A router hands out only what a node published, and nothing at all for a node
/// the directory does not hold.
///
/// This is where the addressing rule is enforced rather than described: a
/// response reaches an address because a process wrote that address about
/// itself, and there is no other source for one.
#[test]
fn a_router_addresses_only_what_the_directory_published() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let directory = test_directory(LEASE)?;
        let published = registration(NodeId::new());
        directory.register(&published).await?;

        let router = test_router(directory)?;

        let route = router
            .route(published.node)
            .await?
            .ok_or_else(|| eyre!("a published node must resolve"))?;
        let walked: Vec<&_> = route
            .candidates(None)
            .into_iter()
            .flatten()
            .map(|(_, endpoint)| endpoint)
            .collect();
        assert!(
            !walked.is_empty(),
            "a published node must reach at least one endpoint"
        );
        for address in walked {
            let uri = address.uri();
            assert!(
                uri == published.direct.uri()
                    || published
                        .advertised
                        .as_ref()
                        .is_some_and(|endpoint| uri == endpoint.uri()),
                "a router handed out {address:?}, which the node never published"
            );
        }
        assert!(
            router
                .direct(published.node)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.uri()),
            "the lookup a forward uses must hand out the endpoint the node published"
        );
        assert!(
            router.route(NodeId::new()).await?.is_none(),
            "a node the directory does not hold must reach no address"
        );
        assert!(
            router.direct(NodeId::new()).await?.is_none(),
            "a node the directory does not hold must reach no endpoint to forward to"
        );
        Ok(())
    })
}

/// A router answers from its own cache, and every clone of it shares the one
/// cache, fleet and transport the process owns.
///
/// The entry is removed after the first resolution, so a router that read the
/// directory again would answer nothing. The lease is far longer than this test
/// runs, so the cached entry cannot age out first.
#[test]
fn a_router_reads_through_its_cache_and_shares_it_with_every_clone() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let directory = test_directory(LEASE)?;
        let published = registration(NodeId::new());
        directory.register(&published).await?;

        let router = test_router(directory.clone())?;

        assert!(
            router
                .direct(published.node)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.uri()),
            "a published node must resolve"
        );
        directory.deregister(&published).await?;
        assert!(
            directory.read(published.node).await?.is_none(),
            "the entry must be gone before the cached answer is asserted"
        );
        assert!(
            router
                .direct(published.node)
                .await?
                .is_some_and(|endpoint| endpoint.uri() == published.direct.uri()),
            "a router must answer from its cache once the entry is gone"
        );

        let clone = router.clone();
        assert!(
            Arc::ptr_eq(&router.fleet, &clone.fleet),
            "a clone must share the one fleet the process owns"
        );
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

    /// Whether both processes carry a label.
    const fn both(self) -> bool {
        matches!(self, Self::Agree | Self::Differ)
    }
}

/// The declared rules decide a route, and a remembered endpoint decides only
/// the order the route is walked in.
///
/// The expected route is written out here as data. Every row is a decision an
/// operator can read off the labels alone: neighbours use the direct address, a
/// node known to be elsewhere is reached only through its entry point or not at
/// all, and an unknown label prefers the entry point where one exists.
#[quickcheck]
fn prop_a_route_follows_the_declared_labels(declared: Declared) -> TestResult {
    let advertised = matches!(declared.published, Published::WithAdvertised);
    let direct = Endpoint::from_static("http://10.0.0.9:7000");
    let entry = Endpoint::from_static("http://10.0.0.9:7001");
    let published = NodeRegistration {
        node: NodeId::new(),
        direct: direct.clone(),
        advertised: advertised.then(|| entry.clone()),
        network: declared.labels.there(),
        hostname: Host::make("declared"),
    };

    // The table, as an operator reads it off the two labels.
    let expected: Vec<(Preference, Uri)> = match (declared.labels, advertised) {
        (Labels::Agree, true) => vec![
            (Preference::Direct, direct.uri().clone()),
            (Preference::Advertised, entry.uri().clone()),
        ],
        (Labels::Agree, false) => vec![(Preference::Direct, direct.uri().clone())],
        (labels, true) if labels.both() => {
            vec![(Preference::Advertised, entry.uri().clone())]
        }
        (labels, false) if labels.both() => Vec::new(),
        (_, true) => vec![(Preference::Advertised, entry.uri().clone())],
        (_, false) => vec![(Preference::Direct, direct.uri().clone())],
    };

    let Some(route) = choose_route(declared.labels.here().as_ref(), Arc::new(published)) else {
        return if expected.is_empty() {
            TestResult::passed()
        } else {
            TestResult::error(format!(
                "{declared:?} must reach {expected:?}, but reached nothing"
            ))
        };
    };
    if expected.is_empty() {
        return TestResult::error(format!(
            "{declared:?} must reach nothing, but reached {route:?}"
        ));
    }
    let walked: Vec<(Preference, Uri)> = route
        .candidates(None)
        .into_iter()
        .flatten()
        .map(|(preference, endpoint)| (preference, endpoint.uri().clone()))
        .collect();
    assert_eq!(
        walked, expected,
        "{declared:?} must reach exactly the endpoints the rules name"
    );

    // A remembered endpoint the route offers is walked first; one it does not
    // offer changes nothing.
    for remembered in [Preference::Direct, Preference::Advertised] {
        let ordered: Vec<Preference> = route
            .candidates(Some(remembered))
            .into_iter()
            .flatten()
            .map(|(preference, _)| preference)
            .collect();
        let mut names: Vec<Preference> = walked.iter().map(|(preference, _)| *preference).collect();
        names.sort_by_key(|preference| *preference != remembered);
        assert_eq!(
            ordered, names,
            "a remembered {remembered:?} must lead a route that offers it, and change nothing \
             otherwise"
        );
    }
    TestResult::passed()
}

/// The failure a destination that answered `code` produces.
fn answer(code: Code) -> SendFailure {
    SendFailure::Status(code)
}

#[test]
fn a_local_target_never_reaches_the_network_route() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let node = NodeId::new();
        let subsystem = SubsystemName::try_new("local")?;
        let registry = PendingRegistry::new();
        let request = TestRegistration::new(
            &registry,
            slice::from_ref(&subsystem),
            Duration::from_secs(1),
        )?;
        let network_calls = Arc::new(AtomicUsize::new(0));
        let route = Then(
            LocalTarget::new(node, registry),
            CountedNetwork(Arc::clone(&network_calls)),
        );
        let frame = stage::<CountingCodec>(
            &FrameHeader {
                target: node,
                request: request.id(),
                subsystem,
                status: ResponseStatus::Success,
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

fn test_router(directory: TestDirectory) -> Result<NetworkRoute<LoopbackSender, TestDirectory>> {
    let (transport, _recorded) = LoopbackSender::new();
    Ok(NetworkRoute::new(
        AddressResolver::new(CACHE_CAPACITY, directory),
        Arc::new(DestinationFleet::new(FleetConfiguration::default())?),
        Arc::new(transport),
        None,
    ))
}
