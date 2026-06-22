//! Live-cluster instantiation of the backend-generic descriptor-identity
//! suite.
//!
//! These run the same [`identity_suite`](crate::state::tests::identity_suite)
//! runners as the memory suite against [`CassandraDescriptorIdentityStore`]'s
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore)
//! impl over the real `keyed_state_identity` table — so the production
//! point-read, the `INSERT … IF NOT EXISTS` LWT, and the conflict-row
//! name-matched decoding all run under the same invariants the model checks.
//! Each iteration uses a fresh `group_id`, so the shared keyspace never
//! collides across runs.

use super::identity::{CassandraDescriptorIdentityStore, IdentityQueries};
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::state::tests::identity_suite::{
    IdentityTrace, run_concurrent_conflicting, run_concurrent_identical, run_identity_trace,
};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const TEST_KEYSPACE: &str = "prosody_test";

/// Property-test iteration count for live-backend runs (default 25), from
/// `INTEGRATION_TESTS`.
fn get_test_count() -> u64 {
    use std::env;
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25)
}

async fn setup() -> Result<CassandraDescriptorIdentityStore> {
    let config = CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    };
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(IdentityQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraDescriptorIdentityStore::new(cassandra, queries))
}

/// A fresh group per iteration so the shared keyspace never collides.
fn group() -> String {
    Uuid::new_v4().to_string()
}

/// The backend-generic store contract (immutability, namespacing, idempotence)
/// over Cassandra — the same runner the memory suite drives.
#[test]
fn prop_cassandra_identity_trace() {
    fn prop(trace: IdentityTrace) -> TestResult {
        let store = match TEST_RUNTIME.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        match TEST_RUNTIME.block_on(run_identity_trace(&store, &group(), trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(IdentityTrace) -> TestResult);
}

/// N concurrent registrations of one identity converge on exactly one
/// `Applied` against the live LWT.
#[test]
fn prop_cassandra_concurrent_identical_registration() {
    fn prop(key_seed: u8, ident_seed: u8, n: u8) -> TestResult {
        let store = match TEST_RUNTIME.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        let n = 1 + usize::from(n % 8);
        match TEST_RUNTIME.block_on(run_concurrent_identical(
            &store,
            &group(),
            key_seed,
            ident_seed,
            n,
        )) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("concurrent identical registration did not converge"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(u8, u8, u8) -> TestResult);
}

/// Two concurrent registrations of differing identities: one wins the LWT, the
/// loser decodes the winner's echoed row.
#[test]
fn prop_cassandra_concurrent_conflicting_registration() {
    fn prop(key_seed: u8) -> TestResult {
        let store = match TEST_RUNTIME.block_on(setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        match TEST_RUNTIME.block_on(run_concurrent_conflicting(&store, &group(), key_seed)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("conflicting registration did not converge on a winner"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    init_test_logging();
    QuickCheck::new()
        .tests(get_test_count())
        .quickcheck(prop as fn(u8) -> TestResult);
}
