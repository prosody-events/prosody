use super::{PeerRuntime, PeerRuntimeError, RouterConfiguration, refresh_delay, routed_host};
use crate::router::directory::RegistrationTtl;
use crate::router::directory::tests::support::{directory, member_shards, membership};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::net::IpAddr;
use std::time::Duration;
use validator::Validate;

/// The Cassandra contact point the routed-address probe aims at.
const CONTACT: &str = "localhost:9042";

/// The lease the runtime tests register under. Its refresh delay is at least a
/// third of it, so no refresh runs while a test observes the first write.
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
        let runtime = PeerRuntime::start(
            directory.clone(),
            7777,
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
            registered.direct.port, 7777,
            "the runtime must publish the listener's port"
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
            runtime.resolve(node).await?,
            Some(registered),
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
        let directory = directory(LEASE).await?;
        let outcome = PeerRuntime::start(directory, 7777, CONTACT, &config, None).await;
        assert!(
            matches!(outcome, Err(PeerRuntimeError::Configuration(_))),
            "a cache capacity of zero must stop the runtime from starting"
        );
        Ok(())
    })
}

/// A refresh always lands inside the lease with room to spare: between a third
/// and a half of it, so two consecutive refreshes can be lost before a row
/// expires.
#[quickcheck]
fn prop_refresh_delay_stays_inside_the_lease(seconds: u64) -> TestResult {
    let span = RegistrationTtl::MAX.as_secs() - RegistrationTtl::MIN.as_secs();
    let lease = Duration::from_secs(RegistrationTtl::MIN.as_secs() + seconds % (span + 1));
    let Ok(ttl) = RegistrationTtl::try_from(lease) else {
        return TestResult::error(format!("{lease:?} must be an acceptable lease"));
    };
    let delay = refresh_delay(ttl);
    assert!(
        delay >= lease / 3 && delay <= lease / 2,
        "a {lease:?} lease produced a refresh delay of {delay:?}"
    );
    TestResult::passed()
}

/// The routed probe answers on this platform, which is what the discovery
/// order relies on when no host is configured.
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

/// The configuration refuses the degenerate values its fields can express: a
/// blank or oversized label, port zero, and a lease outside the range a
/// registration accepts.
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
                network: Some("n".repeat(65)),
                ..RouterConfiguration::default()
            },
        ),
        (
            "port zero",
            RouterConfiguration {
                advertised_port: Some(0),
                ..RouterConfiguration::default()
            },
        ),
        (
            "lease below the minimum",
            RouterConfiguration {
                registration_ttl: Duration::from_secs(1),
                ..RouterConfiguration::default()
            },
        ),
        (
            "lease above the maximum",
            RouterConfiguration {
                registration_ttl: Duration::from_hours(2),
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
    ];
    for (name, config) in cases {
        assert!(
            config.validate().is_err(),
            "{name} must not validate: {config:?}"
        );
    }
    Ok(())
}
