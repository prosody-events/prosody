use super::{Host, select_host};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::cell::Cell;

/// Stands in for the hostname lookup's failure, which `whoami` gives this crate
/// no way to construct.
#[derive(Debug, Eq, PartialEq)]
struct LookupFailed;

/// Every candidate is distinct by construction, so no generated collision can
/// mask a wrong pick.
fn candidates(seed: u8) -> (Host, Host, Host) {
    (
        Host::make(&format!("cfg-{seed}")),
        Host::make(&format!("rt-{seed}")),
        Host::make(&format!("hn-{seed}")),
    )
}

/// Discovery order, laziness, and failure together: the first source that has a
/// host wins, no later source is even consulted, and the hostname lookup's
/// failure reaches the caller exactly when that lookup was the source that had
/// to answer.
#[quickcheck]
fn discovery_prefers_configured_then_routed_then_hostname(
    has_configured: bool,
    has_routed: bool,
    hostname_fails: bool,
    seed: u8,
) -> TestResult {
    let (configured, routed, hostname) = candidates(seed);
    let routed_probed = Cell::new(false);
    let hostname_probed = Cell::new(false);

    let selected = select_host(
        has_configured.then(|| configured.clone()),
        || {
            routed_probed.set(true);
            has_routed.then(|| routed.clone())
        },
        || {
            hostname_probed.set(true);
            if hostname_fails {
                Err(LookupFailed)
            } else {
                Ok(hostname.clone())
            }
        },
    );

    let looked_up = if hostname_fails {
        Err(LookupFailed)
    } else {
        Ok(hostname)
    };
    let (expected, expect_routed_probe, expect_hostname_probe) = match (has_configured, has_routed)
    {
        (true, _) => (Ok(configured), false, false),
        (false, true) => (Ok(routed), true, false),
        (false, false) => (looked_up, true, true),
    };
    assert_eq!(
        selected, expected,
        "configured={has_configured} routed={has_routed} lookup_fails={hostname_fails}: wrong \
         outcome"
    );
    assert_eq!(
        routed_probed.get(),
        expect_routed_probe,
        "configured={has_configured} routed={has_routed}: routed probe consulted out of order"
    );
    assert_eq!(
        hostname_probed.get(),
        expect_hostname_probe,
        "configured={has_configured} routed={has_routed}: hostname lookup consulted out of order"
    );
    TestResult::passed()
}
