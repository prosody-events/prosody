//! Shared support for the integration suite.
//!
//! This module owns the process-wide test settings — the pre-migrated keyspace,
//! the shared runtime, the property-test iteration count — and the Cassandra
//! configuration every test connects with. The Kafka fixtures, shared handlers,
//! and channel helpers live in the child modules and are re-exported here, so
//! callers reach everything as `common::<item>`.

#![allow(
    dead_code,
    reason = "Shared test utilities: each tests/*.rs binary compiles this module separately, so a \
              helper used by only some binaries is dead in the rest"
)]

// Reached as `common::handler::…`, `common::kafka::…`, `common::receive::…`.
// The children are not re-exported here: each binary uses a different subset,
// so a named or glob re-export warns as unused in the binaries that skip it.
pub(crate) mod handler;
pub(crate) mod kafka;
pub(crate) mod receive;

use prosody::cassandra::config::CassandraConfiguration;
use prosody::high_level::config::TriggerStoreConfiguration;
use prosody::peer::PeerConfiguration;
use std::env;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::LazyLock;
use std::time::Duration as StdDuration;
use tokio::runtime::{Builder, Runtime};

/// The shared, pre-migrated keyspace every integration test runs against.
///
/// Tests never create per-test keyspaces — minting one per test leaks schema
/// (orphaned keyspaces bloat the cluster and eventually time out migration
/// tests). Isolation comes from per-test topics and consumer groups instead.
pub(crate) const TEST_KEYSPACE: &str = "prosody_test";

/// Shared multi-threaded runtime for all integration tests.
///
/// # Rationale for `expect`
///
/// `LazyLock` requires a non-fallible closure. Runtime creation failure is
/// unrecoverable in test infrastructure - tests cannot run without a runtime.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra"
)]
pub(crate) static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Number of times to repeat a property test against a live backend.
///
/// Read from `INTEGRATION_TESTS` (default 25, matching TESTING.md). CI cranks
/// this up; dev loops stay fast.
#[must_use]
pub(crate) fn integration_test_count() -> u64 {
    integration_test_count_or(25)
}

/// [`integration_test_count`] with a caller-chosen default for when
/// `INTEGRATION_TESTS` is unset.
///
/// For properties whose per-iteration cost is intrinsically heavy (multiple
/// seconds of live-broker protocol per iteration), a lower local default
/// keeps dev loops fast; the environment variable still overrides it.
#[must_use]
pub(crate) fn integration_test_count_or(default: u64) -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// The trigger store every integration test schedules timers through.
#[must_use]
pub(crate) fn create_cassandra_trigger_store_config() -> TriggerStoreConfiguration {
    TriggerStoreConfiguration::Cassandra(test_cassandra_config())
}

/// The Cassandra configuration shared by every integration test: the local
/// node and the pre-migrated [`TEST_KEYSPACE`]. Both
/// [`create_cassandra_trigger_store_config`] and the tests that open a
/// Cassandra store directly build from this one value.
#[must_use]
pub(crate) fn test_cassandra_config() -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: StdDuration::from_mins(10),
    }
}

/// The peer configuration for parallel integration tests.
pub(crate) fn test_peer_config() -> color_eyre::Result<PeerConfiguration> {
    Ok(PeerConfiguration::builder()
        .bind_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .build()?)
}
