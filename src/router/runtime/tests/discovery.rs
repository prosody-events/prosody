//! What a process discovers about itself, and what it publishes from it.

use super::super::{
    PeerInputs, PeerRuntime, RouterConfiguration, discover_registration, routed_host,
};
use super::{CONTACT, NUMERIC_CONTACT, listener};
use crate::requester::config::RequesterConfiguration;
use crate::router::directory::tests::support::store;
use crate::router::directory::{Endpoint, RegistrationTtl};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::TestHealth;
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

/// No configured value ever reaches the direct endpoint.
///
/// `direct` is what a neighbour on the same network dials, so it stays the
/// discovered address on the port the listener bound, however the entry point
/// is configured. The bound port is never zero either: port zero is a request
/// the operating system answers, and the answer is the only port registration
/// can publish. What the operator configured reaches `advertised` alone.
#[quickcheck]
fn prop_the_direct_endpoint_publishes_only_what_it_discovered(label: u8, port: u16) -> TestResult {
    init_test_logging();
    let host = format!("gateway-{label}.example");
    // Port zero is refused by the configuration, so it is not a case this
    // property covers; `configuration_refuses_degenerate_values` owns it.
    let advertised_port = port.max(1);
    let outcome: Result<()> = TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration::builder()
            .advertised_host(host.clone())
            .advertised_port(advertised_port)
            .build()?;
        config.validate()?;
        let bound = listener().await?;
        let registration = discover_registration(NodeId::new(), &bound, CONTACT, &config, None)?;
        ensure!(
            bound.address().port() != 0,
            "the bound port must not be zero"
        );
        ensure!(
            registration.direct.port == bound.address().port(),
            "the direct endpoint did not publish the bound port"
        );
        ensure!(
            registration.direct.host != Host::make(&host),
            "the direct endpoint published the configured entry point"
        );
        ensure!(
            registration.advertised
                == Some(Endpoint {
                    host: Host::make(&host),
                    port: advertised_port,
                }),
            "the entry point did not publish the configured endpoint"
        );
        Ok(())
    });
    match outcome {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
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
            "the two discovery sources must differ for this test"
        );
        assert_eq!(registration.direct.host, routed);

        let unrouted = discover_registration(NodeId::new(), &bound, "no-port-here", &config, None)?;
        assert_eq!(unrouted.direct.host, unrouted.hostname);
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
        let router = RouterConfiguration {
            registration_ttl: RegistrationTtl::try_from(RegistrationTtl::MIN)?,
            ..RouterConfiguration::default()
        };
        let requester = RequesterConfiguration::default();
        let runtime = PeerRuntime::start(PeerInputs {
            store: store().await?.clone(),
            listener: listener().await?,
            health: TestHealth::new(true, true),
            contact: CONTACT,
            group: None,
            router: &router,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
        .await?;
        let node = runtime.node();
        let addresses = runtime.addresses().clone();
        assert!(
            addresses.resolve(node).await?.is_some(),
            "a started runtime must resolve its own node"
        );
        // The shutdown removes the row and stops the refresher, so only the
        // cached entry can still answer.
        runtime.shutdown(|| async {}).await?;

        // A cache entry ages out on the process clock and emits no event, so a
        // bounded poll is the only observation available. The deadline is a
        // hang guard; the assertion is the absence below it.
        let deadline = Instant::now() + Duration::from_mins(1);
        let mut ticker = interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let resolved = addresses.resolve(node).await?;
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
