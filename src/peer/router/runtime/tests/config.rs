//! What the router configuration accepts, and the refresh pace it sets.

use super::super::{
    PeerInputs, PeerRuntimeError, PreparedPeerRuntime, RouterConfiguration, refresh_delay,
    refresh_registration,
};
use super::listener;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::heartbeat::HeartbeatRegistry;
use crate::peer::router::PeerId;
use crate::peer::router::cache_config::PeerCacheConfiguration;
use crate::peer::router::directory::tests::support::{registration, test_directory};
use crate::peer::router::directory::{PeerDirectory, PeerRegistration, RegistrationTtl};
use crate::test_util::{TEST_RUNTIME, capture_events};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::future::{Future, Ready, ready};
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::advance;
use tracing::Level;
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
            cache: PeerCacheConfiguration::default(),
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
    let span = MAX_CASSANDRA_TTL_SECS as u64 - RegistrationTtl::MIN.as_secs();
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

/// A directory whose writes fail while `failing` holds, as during a store
/// outage.
#[derive(Clone, Default)]
struct OutageDirectory {
    failing: Arc<AtomicBool>,
}

impl OutageDirectory {
    fn outcome<T>(&self, value: T) -> Ready<Result<T, IoError>> {
        ready(if self.failing.load(Ordering::Relaxed) {
            Err(IoError::other("store unavailable"))
        } else {
            Ok(value)
        })
    }
}

impl PeerDirectory for OutageDirectory {
    type Error = IoError;

    fn ttl(&self) -> RegistrationTtl {
        RegistrationTtl::DEFAULT
    }

    fn register(
        &self,
        _registration: &PeerRegistration,
    ) -> impl Future<Output = Result<(), IoError>> {
        self.outcome(())
    }

    fn read(
        &self,
        _peer: PeerId,
    ) -> impl Future<Output = Result<Option<PeerRegistration>, IoError>> {
        self.outcome(None)
    }

    fn deregister(
        &self,
        _registration: &PeerRegistration,
    ) -> impl Future<Output = Result<(), IoError>> {
        self.outcome(())
    }
}

/// Advances the paused clock by `ticks` steps of `tick` and lets the refresh
/// task run after each step.
async fn run_for(ticks: u32, tick: Duration) {
    for _ in 0..ticks {
        advance(tick).await;
        yield_now().await;
    }
}

/// A failed refresh inside the lease heals on a later refresh, so it does not
/// alert. Once the lease lapses, other peers drop responses, so it alerts once.
/// A later success reports the recovery and re-arms the alert for the next
/// lapse.
#[tokio::test(start_paused = true)]
async fn refresh_failure_alerts_once_for_each_lapse() -> Result<()> {
    const LAPSED: &str = "peer registration of this process expired";
    const RESTORED: &str = "peer registration of this process is active again";
    let lease = Duration::from_mins(1);
    let (events, _guard) = capture_events(Level::INFO);
    let directory = OutageDirectory::default();
    directory.failing.store(true, Ordering::Relaxed);
    let (stop, stopped) = watch::channel(false);
    let refresh = tokio::spawn(refresh_registration(
        directory.clone(),
        registration(PeerId::new()),
        HeartbeatRegistry::test().register("directory refresh"),
        RegistrationTtl::try_from(lease)?,
        stopped,
    ));

    // Each step is shorter than the smallest refresh delay, so every refresh
    // runs before the clock passes the next one.
    let tick = lease / 10;
    run_for(9, tick).await;
    assert_eq!(
        events.count(LAPSED),
        0,
        "a refresh inside the lease alerted"
    );

    // Several more refreshes fail after the lapse. They repeat no alert.
    run_for(12, tick).await;
    assert_eq!(events.count(LAPSED), 1, "a lapse must alert exactly once");

    directory.failing.store(false, Ordering::Relaxed);
    run_for(12, tick).await;
    assert_eq!(
        events.count(RESTORED),
        1,
        "a recovery must log exactly once"
    );

    directory.failing.store(true, Ordering::Relaxed);
    run_for(21, tick).await;
    assert_eq!(events.count(LAPSED), 2, "a second lapse must alert again");

    stop.send_replace(true);
    refresh.await?;
    Ok(())
}

/// A network label must not be empty. Its length has no protocol limit.
#[test]
fn configuration_refuses_degenerate_values() {
    let default = RouterConfiguration::default();
    assert!(default.validate().is_ok(), "the default must validate");

    let long = RouterConfiguration {
        network: Some("é".repeat(128)),
        ..RouterConfiguration::default()
    };
    assert!(
        long.validate().is_ok(),
        "a long nonempty label must validate"
    );

    let empty = RouterConfiguration {
        network: Some(String::new()),
        ..RouterConfiguration::default()
    };
    assert!(
        empty.validate().is_err(),
        "an empty label must not validate"
    );
}

/// Peer caches accept every positive capacity and reject zero.
#[test]
fn peer_cache_capacity_is_positive() {
    for capacity in [1, 100_000, usize::MAX] {
        let config = PeerCacheConfiguration {
            peer_capacity: capacity,
        };
        assert!(
            config.validate().is_ok(),
            "capacity {capacity} must validate"
        );
    }
    let config = PeerCacheConfiguration { peer_capacity: 0 };
    assert!(config.validate().is_err(), "zero must be refused");
}
