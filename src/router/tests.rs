use super::{Host, select_host};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::cell::Cell;

/// Every candidate is distinct by construction, so no generated collision can
/// mask a wrong pick.
fn candidates(seed: u8) -> (Host, Host, Host) {
    (
        Host::make(&format!("cfg-{seed}")),
        Host::make(&format!("rt-{seed}")),
        Host::make(&format!("hn-{seed}")),
    )
}

/// Discovery order and laziness together: the first source that has a host
/// wins, and no later source is even consulted.
#[quickcheck]
fn discovery_prefers_configured_then_routed_then_hostname(
    has_configured: bool,
    has_routed: bool,
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
            Ok(hostname.clone())
        },
    );
    let Ok(selected) = selected else {
        return TestResult::error("the hostname fallback cannot fail in this test");
    };

    let (expected, expect_routed_probe, expect_hostname_probe) = match (has_configured, has_routed)
    {
        (true, _) => (configured, false, false),
        (false, true) => (routed, true, false),
        (false, false) => (hostname, true, true),
    };
    assert_eq!(
        selected, expected,
        "configured={has_configured} routed={has_routed}: wrong host selected"
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
