//! What the router configuration accepts, and the refresh pace it sets.

use super::super::{
    PeerInputs, PeerRuntimeError, PreparedPeerRuntime, RouterConfiguration, refresh_delay,
    refresh_registration,
};
use super::listener;
use crate::heartbeat::HeartbeatRegistry;
use crate::router::PeerId;
use crate::router::directory::tests::support::{registration, test_directory};
use crate::router::directory::{PeerDirectory, RegistrationTtl};
use crate::router::fleet::config::FleetConfiguration;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::advance;
use validator::Validate;

/// The lease the refused runtimes below publish under. Neither reaches a write,
/// so the value only has to be one a directory accepts.
const REFUSED_LEASE: Duration = RegistrationTtl::MIN;

/// `start` refuses a configuration its own rules reject. A bound that nothing
/// enforces at startup is not a bound.
#[test]
fn start_refuses_an_invalid_configuration() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let router = RouterConfiguration {
            network: Some(String::new()),
            ..RouterConfiguration::default()
        };
        // `start` refuses before it publishes anything, so an in-process
        // directory is enough and this case needs no cluster.
        let outcome = PreparedPeerRuntime::start(PeerInputs {
            directory: test_directory(REFUSED_LEASE)?,
            listener: listener().await?,
            heartbeats: HeartbeatRegistry::test(),
            router: &router,
            fleet: FleetConfiguration::default(),
        })
        .await;
        assert!(
            matches!(outcome, Err(PeerRuntimeError::Configuration(_))),
            "a blank network must stop the runtime from starting"
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

/// Heartbeat checks do not postpone the next directory refresh.
#[tokio::test(start_paused = true)]
async fn heartbeat_checks_preserve_the_refresh_deadline() -> Result<()> {
    let directory = test_directory(Duration::from_mins(1))?;
    let registered = registration(PeerId::new());
    let (stop, stopped) = watch::channel(false);
    let refresh = tokio::spawn(refresh_registration(
        directory.clone(),
        registered.clone(),
        HeartbeatRegistry::test().register("directory refresh"),
        directory.ttl(),
        stopped,
    ));

    yield_now().await;
    advance(Duration::from_secs(10)).await;
    yield_now().await;
    advance(Duration::from_secs(10)).await;
    yield_now().await;
    assert!(
        directory.read(registered.peer).await?.is_some(),
        "heartbeat checks postponed the registration refresh"
    );

    stop.send_replace(true);
    refresh.await?;
    Ok(())
}

/// The configuration refuses each degenerate network label it can express.
///
/// The label rule counts bytes, not characters, because bytes are what keeps a
/// label inline. A label of 32 multi-byte characters is therefore refused,
/// while one of 63 ASCII bytes — the last that stays inline — is accepted.
#[test]
fn configuration_refuses_degenerate_values() {
    let default = RouterConfiguration::default();
    assert!(default.validate().is_ok(), "the default must validate");

    let longest = RouterConfiguration {
        network: Some("n".repeat(63)),
        ..RouterConfiguration::default()
    };
    assert!(
        longest.validate().is_ok(),
        "a label of exactly the inline capacity must validate"
    );

    let cases = [
        RouterConfiguration {
            network: Some(String::new()),
            ..RouterConfiguration::default()
        },
        RouterConfiguration {
            network: Some("n".repeat(64)),
            ..RouterConfiguration::default()
        },
        RouterConfiguration {
            network: Some("é".repeat(32)),
            ..RouterConfiguration::default()
        },
    ];
    for config in cases {
        assert!(
            config.validate().is_err(),
            "a degenerate configuration must not validate: {config:?}"
        );
    }
}

/// Peer caches accept every positive capacity and reject zero.
#[test]
fn peer_cache_capacity_is_positive() {
    for capacity in [1, 100_000, usize::MAX] {
        let config = FleetConfiguration {
            peer_capacity: capacity,
        };
        assert!(
            config.validate().is_ok(),
            "capacity {capacity} must validate"
        );
    }
    let config = FleetConfiguration { peer_capacity: 0 };
    assert!(config.validate().is_err(), "zero must be refused");
}
