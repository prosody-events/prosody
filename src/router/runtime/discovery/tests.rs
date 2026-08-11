//! What a process discovers about itself, and what it publishes from it.

use super::super::RouterConfiguration;
use super::super::tests::listener;
use super::{
    DiscoveredHost, DiscoveryError, discover_host, join_discovery,
    registration as discover_registration,
};
use crate::router::PeerId;
use crate::router::directory::Endpoint;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::future::pending;
use validator::Validate;

/// No configured value ever reaches the direct endpoint.
///
/// `direct` is what a neighbour on the same network dials, so it stays the
/// discovered address on the port the listener bound, however the entry point
/// is configured. The bound port is never zero either: port zero is a request
/// the operating system answers, and the answer is the only port registration
/// can publish. What the operator configured reaches `advertised` alone.
#[quickcheck]
fn prop_the_direct_endpoint_publishes_only_what_it_discovered(label: u8) -> TestResult {
    init_test_logging();
    let connect = format!("http://gateway-{label}.example");
    let outcome: Result<()> = TEST_RUNTIME.block_on(async {
        let advertised = Endpoint::from_shared(connect)?;
        let config = RouterConfiguration::builder()
            .advertised(advertised.clone())
            .build()?;
        config.validate()?;
        let bound = listener().await?;
        let registration = discover_registration(PeerId::new(), &bound, discover_host()?, &config)?;
        ensure!(
            registration.direct.uri() != advertised.uri(),
            "the direct endpoint published the configured entry point"
        );
        ensure!(
            registration
                .advertised
                .as_ref()
                .is_some_and(|endpoint| endpoint.uri() == advertised.uri()),
            "the entry point did not publish the configured endpoint"
        );
        Ok(())
    });
    match outcome {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
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
