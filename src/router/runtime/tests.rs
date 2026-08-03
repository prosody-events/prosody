use super::{
    MAX_ADDRESS_CACHE_CAPACITY, PeerRuntime, PeerRuntimeError, RouterConfiguration,
    discover_registration, refresh_delay, routed_host,
};
use crate::router::directory::tests::support::{directory, member_shards, membership, store};
use crate::router::directory::{Endpoint, RegistrationTtl};
use crate::router::grpc::{BoundListener, TransportConfiguration};
use crate::router::{Host, NodeId};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::{Instant, interval};
use validator::Validate;

/// The Cassandra contact point the routed-address probe aims at.
const CONTACT: &str = "localhost:9042";

/// A peer listener on a port the operating system chooses.
///
/// Registration reads the bound listener rather than a port number, so a test
/// binds a real one and the published port is always a port that exists.
async fn listener() -> Result<BoundListener> {
    Ok(BoundListener::bind(&TransportConfiguration::default()).await?)
}

/// The same contact point written as an address. A probe against it exercises
/// no name resolution, so it always aims at one address family.
const NUMERIC_CONTACT: &str = "127.0.0.1:9042";

/// The lease the runtime tests read under. It equals the default a runtime
/// starts with, and its refresh delay is at least a fifth of it, so no refresh
/// runs while a test observes the first write.
const LEASE: Duration = Duration::from_secs(30);

/// A process that enables no peer feature still takes its place in the
/// directory: it registers before `start` returns, and a clean shutdown
/// removes both rows. Shutting down again changes nothing.
#[test]
fn runtime_registers_on_start_and_deregisters_on_shutdown() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        // Every peer-related field is unset: registration is unconditional.
        let config = RouterConfiguration::default();
        let directory = directory(LEASE).await?;
        let membership = membership();
        let bound = listener().await?;
        let runtime = PeerRuntime::start(
            store().await?.clone(),
            &bound,
            CONTACT,
            &config,
            Some(membership.clone()),
        )
        .await?;
        let node = runtime.node();

        let registered = directory
            .read(node)
            .await?
            .ok_or_else(|| eyre!("a started runtime must already resolve"))?;
        assert_eq!(
            registered.node, node,
            "the published row must belong to this process"
        );
        assert_eq!(
            registered.direct.port,
            bound.address().port(),
            "the runtime must publish the port the listener bound"
        );
        assert_eq!(
            registered.group.as_ref(),
            Some(&membership),
            "the runtime must publish the group it was started with"
        );
        assert!(
            registered.advertised.is_none() && registered.network.is_none(),
            "an unconfigured process publishes no entry point and no network"
        );
        assert_eq!(
            member_shards(&membership, node).await?.len(),
            1,
            "a started runtime must occupy exactly one index shard"
        );
        assert_eq!(
            runtime.addresses().resolve(node).await?.as_deref(),
            Some(&registered),
            "the runtime must resolve its own node through its cache"
        );

        for attempt in 1_u8..=2 {
            runtime.shutdown().await?;
            assert!(
                directory.read(node).await?.is_none(),
                "attempt {attempt}: shutdown must remove the node row"
            );
            assert!(
                member_shards(&membership, node).await?.is_empty(),
                "attempt {attempt}: shutdown must remove the index entry"
            );
        }
        Ok(())
    })
}

/// A removed node stops being served: the cached address ages out on the same
/// lease the row carried, so resolution reports the node unreachable instead of
/// handing back an address to dial. This is the path a dialer takes, cache and
/// all.
#[test]
fn a_resolved_address_stops_being_served_once_its_row_is_gone() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration {
            registration_ttl: RegistrationTtl::try_from(RegistrationTtl::MIN)?,
            ..RouterConfiguration::default()
        };
        let runtime = PeerRuntime::start(
            store().await?.clone(),
            &listener().await?,
            CONTACT,
            &config,
            None,
        )
        .await?;
        let node = runtime.node();
        assert!(
            runtime.addresses().resolve(node).await?.is_some(),
            "a started runtime must resolve its own node"
        );
        // The shutdown removes the row and stops the refresher, so only the
        // cached entry can still answer.
        runtime.shutdown().await?;

        // A cache entry ages out on the process clock and emits no event, so a
        // bounded poll is the only observation available. The deadline is a
        // hang guard; the assertion is the absence below it.
        let deadline = Instant::now() + Duration::from_mins(1);
        let mut ticker = interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let resolved = runtime.addresses().resolve(node).await?;
            if resolved.is_none() {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "a removed node stayed resolvable: {resolved:?}"
            );
        }
        Ok(())
    })
}

/// `start` refuses a configuration its own rules reject. A bound that nothing
/// enforces at startup is not a bound.
#[test]
fn start_refuses_an_invalid_configuration() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration {
            address_cache_capacity: 0,
            ..RouterConfiguration::default()
        };
        let outcome = PeerRuntime::start(
            store().await?.clone(),
            &listener().await?,
            CONTACT,
            &config,
            None,
        )
        .await;
        assert!(
            matches!(outcome, Err(PeerRuntimeError::Configuration(_))),
            "a cache capacity of zero must stop the runtime from starting"
        );
        Ok(())
    })
}

/// A configured entry point never reaches `direct`. `direct` is what a
/// neighbour on the same network dials, so it stays the discovered address on
/// the port the listener bound, however the entry point is configured.
#[test]
fn a_configured_entry_point_never_reaches_the_direct_endpoint() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration::builder()
            .advertised_host("gateway.example")
            .advertised_port(443_u16)
            .network("east")
            .build()?;
        let bound = listener().await?;
        let registration = discover_registration(NodeId::new(), &bound, CONTACT, &config, None)?;
        assert_eq!(
            registration.direct.port,
            bound.address().port(),
            "the direct endpoint must publish the port the listener bound"
        );
        assert_ne!(
            registration.direct.host,
            Host::make("gateway.example"),
            "the direct endpoint must not publish the configured entry point"
        );
        assert_eq!(
            registration.advertised,
            Some(Endpoint {
                host: Host::make("gateway.example"),
                port: 443,
            }),
            "the entry point must publish exactly what the operator configured"
        );
        Ok(())
    })
}

/// Three refresh delays fit inside the lease with a quarter of it unspent,
/// which is the margin two lost refreshes need to heal. The delay also stays
/// above a fifth of the lease, which caps what the margin costs at five
/// refreshes per lease.
#[quickcheck]
fn prop_two_lost_refreshes_still_heal_inside_the_lease(seconds: u64) -> TestResult {
    let span = RegistrationTtl::MAX.as_secs() - RegistrationTtl::MIN.as_secs();
    let lease = Duration::from_secs(RegistrationTtl::MIN.as_secs() + seconds % (span + 1));
    let Ok(ttl) = RegistrationTtl::try_from(lease) else {
        return TestResult::error(format!("{lease:?} must be an acceptable lease"));
    };
    let delay = refresh_delay(ttl);
    assert!(
        delay * 3 + lease / 4 <= lease,
        "a {lease:?} lease produced a refresh delay of {delay:?}, so a third attempt lands too \
         late to heal two lost refreshes"
    );
    assert!(
        delay >= lease / 5,
        "a {lease:?} lease produced a refresh delay of {delay:?}, which renews more often than \
         the lease is worth"
    );
    TestResult::passed()
}

/// The routed probe answers on this platform, which is what discovery relies on
/// for the direct endpoint.
///
/// It cannot tell a local address from a peer one against a loopback target —
/// both are `127.0.0.1` — so swapping the two is out of this test's reach.
#[test]
fn routed_host_answers_for_the_cassandra_contact_point() -> Result<()> {
    init_test_logging();
    let host = routed_host(CONTACT).ok_or_else(|| eyre!("the routed probe found no address"))?;
    host.as_str()
        .parse::<IpAddr>()
        .map_err(|error| eyre!("the routed probe returned {host}, not an address: {error}"))?;
    Ok(())
}

/// The direct host is the routed address, and this machine's name where the
/// probe finds none. Both sources are checked against a registration, because
/// the order between them decides the address peers dial.
///
/// The target is written as an address so that two probes cannot land on
/// different address families of one name. A contact point with no port in it
/// resolves to nothing, so the second source is reached without a network of
/// any kind.
#[test]
fn the_direct_host_is_the_routed_address_then_this_machine() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration::default();
        let routed = routed_host(NUMERIC_CONTACT)
            .ok_or_else(|| eyre!("the routed probe found no address"))?;
        let bound = listener().await?;
        let registration =
            discover_registration(NodeId::new(), &bound, NUMERIC_CONTACT, &config, None)?;
        ensure!(
            routed != registration.hostname,
            "this machine answers {routed} to both sources, so the two cannot be told apart"
        );
        assert_eq!(
            registration.direct.host, routed,
            "the direct endpoint must publish the routed address while the probe answers"
        );

        let unrouted = discover_registration(NodeId::new(), &bound, "no-port-here", &config, None)?;
        assert_eq!(
            unrouted.direct.host, unrouted.hostname,
            "the direct endpoint must fall back to this machine's name"
        );
        Ok(())
    })
}

/// The configuration refuses the degenerate values its fields can express: a
/// blank or oversized label, port zero, a published port with no host beside
/// it, and a cache capacity outside the range one process can hold.
#[test]
fn configuration_refuses_degenerate_values() -> Result<()> {
    let default = RouterConfiguration::default();
    assert!(
        default.validate().is_ok(),
        "the default configuration must validate"
    );

    let built = RouterConfiguration::builder()
        .advertised_host("gateway.example")
        .advertised_port(443_u16)
        .network("east")
        .build()?;
    assert!(
        built.validate().is_ok(),
        "a configured entry point must validate: {built:?}"
    );
    assert_eq!(
        built.registration_ttl, default.registration_ttl,
        "an unset field must keep its default"
    );

    let cases = [
        (
            "blank host",
            RouterConfiguration {
                advertised_host: Some(String::new()),
                ..RouterConfiguration::default()
            },
        ),
        (
            "oversized network",
            RouterConfiguration {
                network: Some("n".repeat(64)),
                ..RouterConfiguration::default()
            },
        ),
        (
            "port zero",
            RouterConfiguration {
                advertised_host: Some("gateway.example".to_owned()),
                advertised_port: Some(0),
                ..RouterConfiguration::default()
            },
        ),
        (
            "published port with no host",
            RouterConfiguration {
                advertised_port: Some(443),
                ..RouterConfiguration::default()
            },
        ),
        (
            "no cache capacity",
            RouterConfiguration {
                address_cache_capacity: 0,
                ..RouterConfiguration::default()
            },
        ),
        (
            "cache capacity past the maximum",
            RouterConfiguration {
                address_cache_capacity: MAX_ADDRESS_CACHE_CAPACITY + 1,
                ..RouterConfiguration::default()
            },
        ),
    ];
    for (name, config) in cases {
        assert!(
            config.validate().is_err(),
            "{name} must not validate: {config:?}"
        );
    }
    Ok(())
}
