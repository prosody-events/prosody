//! What the router configuration accepts, and the refresh pace it sets.

use super::super::{PeerInputs, PeerRuntime, PeerRuntimeError, RouterConfiguration, refresh_delay};
use super::{CONTACT, listener};
use crate::requester::config::RequesterConfiguration;
use crate::router::directory::RegistrationTtl;
use crate::router::directory::tests::support::memory_directory;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::TestHealth;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::time::Duration;
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
            address_cache_capacity: 0,
            ..RouterConfiguration::default()
        };
        let requester = RequesterConfiguration::default();
        // `start` refuses before it publishes anything, so an in-process
        // directory is enough and this case needs no cluster.
        let outcome = PeerRuntime::start(PeerInputs {
            directory: memory_directory(REFUSED_LEASE)?,
            listener: listener().await?,
            health: TestHealth::new(true, true),
            contact: CONTACT,
            group: None,
            router: &router,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
        .await;
        assert!(
            matches!(outcome, Err(PeerRuntimeError::Configuration(_))),
            "a cache capacity of zero must stop the runtime from starting"
        );
        Ok(())
    })
}

/// `start` refuses a response ceiling no frame its own listener accepts could
/// carry.
///
/// Each configuration is valid on its own and neither can see the other, so
/// this is the one place the product of the two is checkable. Without the
/// refusal every response at the admitted size would be dropped at the
/// listener, and the caller would read that only as its own timeout. The
/// accepting side needs no case here: every other runtime suite starts under
/// the default requester, whose ceiling equals the default frame cap.
#[test]
fn start_refuses_a_response_ceiling_above_the_frame_cap() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let bound = listener().await?;
        let cap = bound.frame_cap().bytes();
        let router = RouterConfiguration::default();
        let requester = RequesterConfiguration {
            max_response_bytes: cap + 1,
            ..RequesterConfiguration::default()
        };
        requester.validate()?;
        let outcome = PeerRuntime::start(PeerInputs {
            directory: memory_directory(REFUSED_LEASE)?,
            listener: bound,
            health: TestHealth::new(true, true),
            contact: CONTACT,
            group: None,
            router: &router,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
        .await;
        assert!(
            matches!(outcome, Err(PeerRuntimeError::ResponseCeiling { .. })),
            "a response ceiling above the frame cap must stop the runtime from starting"
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

/// The configuration refuses the degenerate values its fields can express: a
/// blank or oversized label, port zero, a published port with no host beside
/// it, and a cache capacity outside the range one process can hold.
///
/// The label rule counts bytes, not characters, because bytes are what keeps a
/// label inline. A label of 32 multi-byte characters is therefore refused,
/// while one of 63 ASCII bytes — the last that stays inline — is accepted.
#[test]
fn configuration_refuses_degenerate_values() -> Result<()> {
    let default = RouterConfiguration::default();
    assert!(default.validate().is_ok(), "the default must validate");

    let longest = RouterConfiguration {
        advertised_host: Some("n".repeat(63)),
        ..RouterConfiguration::default()
    };
    assert!(
        longest.validate().is_ok(),
        "a label of exactly the inline capacity must validate"
    );

    let built = RouterConfiguration::builder()
        .advertised_host("gateway.example")
        .advertised_port(443_u16)
        .network("east")
        .build()?;
    assert!(built.validate().is_ok(), "the entry point must validate");
    assert_eq!(
        built.address_cache_capacity, default.address_cache_capacity,
        "a field the builder was not given must keep its default"
    );

    let cases = [
        RouterConfiguration {
            advertised_host: Some(String::new()),
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
        RouterConfiguration {
            advertised_host: Some("gateway.example".to_owned()),
            advertised_port: Some(0),
            ..RouterConfiguration::default()
        },
        RouterConfiguration {
            advertised_port: Some(443),
            ..RouterConfiguration::default()
        },
        RouterConfiguration {
            address_cache_capacity: 0,
            ..RouterConfiguration::default()
        },
        RouterConfiguration {
            address_cache_capacity: super::super::config::MAX_ADDRESS_CACHE_CAPACITY + 1,
            ..RouterConfiguration::default()
        },
    ];
    for config in cases {
        assert!(
            config.validate().is_err(),
            "a degenerate configuration must not validate: {config:?}"
        );
    }
    Ok(())
}
