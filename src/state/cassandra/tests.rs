//! Live-cluster instantiation of the backend-generic descriptor-identity and
//! publication suites.
//!
//! These run the same [`identity_suite`](crate::state::tests::identity_suite)
//! and [`publication_suite`](crate::state::tests::publication_suite) runners
//! as the memory suite, but against the real Cassandra stores.
//! [`CassandraDescriptorIdentityStore`] implements
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore)
//! over the `keyed_state_identity` table. [`CassandraPublicationStore`]
//! implements [`PublicationStore`](crate::state::publication::PublicationStore)
//! over `keyed_state_publication`. Both run the production point-read, the
//! `INSERT … IF NOT EXISTS` LWT, the conflict-row name-matched decoding, and
//! the routing upsert/remove/read under the same invariants the models check.
//! Each iteration uses a fresh `group_id`, so the shared keyspace never
//! collides across runs.

use super::identity::{CassandraDescriptorIdentityStore, IdentityQueries};
use super::publication::{CassandraPublicationStore, PublicationQueries};
use crate::cassandra::CassandraStore;
use crate::state::tests::identity_suite::{
    IdentityTrace, run_concurrent_conflicting, run_concurrent_identical, run_identity_trace,
};
use crate::state::tests::publication_suite::{
    PublicationTrace, cleanup_publication_trace, run_publication_trace,
};
use crate::test_util::{TEST_RUNTIME, integration_test_count, test_cassandra_config};
use crate::tracing::init_test_logging;
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
use std::sync::Arc;
use uuid::Uuid;

async fn setup() -> Result<CassandraDescriptorIdentityStore> {
    let config = test_cassandra_config();
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(IdentityQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraDescriptorIdentityStore::new(cassandra, queries))
}

/// A fresh group per iteration so the shared keyspace never collides.
fn group() -> String {
    Uuid::new_v4().to_string()
}

async fn publication_setup() -> Result<CassandraPublicationStore> {
    let config = test_cassandra_config();
    let cassandra = CassandraStore::new(&config).await?;
    let queries = Arc::new(PublicationQueries::new(cassandra.session(), &config.keyspace).await?);
    Ok(CassandraPublicationStore::new(cassandra, queries))
}

/// The backend-generic publication contract over Cassandra — the same runner
/// the memory suite drives. A fresh subsystem token per iteration keeps the
/// shared keyspace collision-free. `CassandraStore::new` runs the
/// `keyed_state_publication` migration on connect, so this test also proves
/// that migration applies cleanly to `prosody_test`.
#[test]
fn prop_cassandra_publication_trace() {
    fn prop(trace: PublicationTrace) -> TestResult {
        let store = match TEST_RUNTIME.block_on(publication_setup()) {
            Ok(store) => store,
            Err(error) => return TestResult::error(format!("store setup failed: {error:?}")),
        };
        let token = Uuid::new_v4().to_string();
        let outcome = TEST_RUNTIME.block_on(run_publication_trace(&store, &token, trace));
        let cleanup = TEST_RUNTIME.block_on(cleanup_publication_trace(&store, &token));
        match (outcome, cleanup) {
            (Ok(true), Ok(())) => TestResult::passed(),
            (Ok(false), Ok(())) => TestResult::failed(),
            (Err(error), _) | (_, Err(error)) => TestResult::error(format!("{error:?}")),
        }
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(PublicationTrace) -> TestResult);
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
        .tests(integration_test_count(25))
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
        .tests(integration_test_count(25))
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
        .tests(integration_test_count(25))
        .quickcheck(prop as fn(u8) -> TestResult);
}
