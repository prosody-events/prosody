//! What a process discovers about itself, and what it publishes from it.

use super::super::RouterConfiguration;
use super::super::tests::{CONTACT, listener};
use super::{
    DiscoveredHost, DiscoveryError, discover_host, join_discovery,
    registration as discover_registration, routed_host,
};
use crate::router::directory::Endpoint;
use crate::router::{Host, NodeId};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::future::pending;
use std::net::IpAddr;
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
        let registration = discover_registration(
            NodeId::new(),
            &bound,
            discover_host(Some(CONTACT))?,
            &config,
        );
        ensure!(
            bound.address().port() != 0,
            "the bound port must not be zero"
        );
        ensure!(
            registration.direct.port == bound.address().port(),
            "the direct endpoint published port {}, not the {} the listener bound",
            registration.direct.port,
            bound.address().port()
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
fn routed_host_answers_for_the_configured_probe_address() -> Result<()> {
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
/// The probe target is a `SocketAddr`, so two probes cannot land on different
/// address families of one name. An absent probe skips the lookup altogether,
/// so the second source is reached without a network of any kind.
#[test]
fn the_direct_host_is_the_routed_address_then_this_machine() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let config = RouterConfiguration::default();
        let routed =
            routed_host(CONTACT).ok_or_else(|| eyre!("the routed probe found no address"))?;
        let bound = listener().await?;
        let registration = discover_registration(
            NodeId::new(),
            &bound,
            discover_host(Some(CONTACT))?,
            &config,
        );
        ensure!(
            routed != registration.hostname,
            "the two discovery sources must differ for this test"
        );
        assert_eq!(
            registration.direct.host, routed,
            "the direct endpoint must publish the routed address while the probe answers"
        );

        let unrouted = discover_registration(NodeId::new(), &bound, discover_host(None)?, &config);
        assert_eq!(unrouted.direct.host, unrouted.hostname);
        Ok(())
    })
}

/// A discovery task that does not join is reported, not swallowed.
///
/// An aborted task is that failure in deterministic form: a task that can never
/// complete answers with a cancelled join error, without a timer and without a
/// panic. The process then has no host for its direct endpoint, so the outcome
/// must name the task rather than read as a discovery that found nothing.
#[test]
fn a_discovery_task_that_does_not_join_is_reported() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let task = tokio::spawn(pending::<Result<DiscoveredHost, DiscoveryError>>());
        task.abort();
        ensure!(
            matches!(join_discovery(task).await, Err(DiscoveryError::Task(_))),
            "a discovery task that did not join was not reported as a task failure"
        );
        Ok(())
    })
}
