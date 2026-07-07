use crate::cassandra::CassandraConfiguration;
use quickcheck::{Arbitrary, Gen};
use serde_json::{Map, Value};
use std::env;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

/// The shared, pre-migrated keyspace every Cassandra-backed test runs against.
///
/// Tests never create per-test keyspaces — minting one per test leaks schema
/// (orphaned keyspaces bloat the cluster and eventually time out migration
/// tests). Isolation comes from fresh per-test identifiers (segment ids,
/// group ids, topics) instead.
pub const TEST_KEYSPACE: &str = "prosody_test";

/// Shared multi-threaded runtime for all unit tests in the crate.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
)]
pub static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Depth-bounded `serde_json::Value` generator shared by the state-codec
/// and descriptor round-trip properties.
///
/// Floats are deliberately excluded: JSON has no NaN and float identity
/// is not the invariant under test — structural round-tripping is.
#[derive(Clone, Debug)]
pub struct ArbJson(pub Value);

impl Arbitrary for ArbJson {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(arbitrary_json(g, 3))
    }
}

/// Property-test iteration count for live-backend suites: `INTEGRATION_TESTS`
/// if set, else `default`. CI cranks it up; dev loops stay fast.
pub(crate) fn integration_test_count(default: u64) -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Configuration for the local test cluster (`localhost:9042`) over the
/// shared, pre-migrated [`TEST_KEYSPACE`].
pub(crate) fn test_cassandra_config() -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    }
}

fn arbitrary_json(g: &mut Gen, depth: u8) -> Value {
    let variants = if depth == 0 { 4 } else { 6 };
    match u8::arbitrary(g) % variants {
        0 => Value::Null,
        1 => Value::Bool(bool::arbitrary(g)),
        2 => Value::from(i64::arbitrary(g)),
        3 => Value::String(String::arbitrary(g)),
        4 => Value::Array(
            (0..u8::arbitrary(g) % 4)
                .map(|_| arbitrary_json(g, depth - 1))
                .collect(),
        ),
        _ => Value::Object(
            (0..u8::arbitrary(g) % 4)
                .map(|_| (String::arbitrary(g), arbitrary_json(g, depth - 1)))
                .collect::<Map<_, _>>(),
        ),
    }
}
