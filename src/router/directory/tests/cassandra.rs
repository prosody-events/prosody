use super::suite::{
    DirectoryTrace, STABLE_LEASE, expected_answers, first_divergence, run_directory_trace,
    run_idempotent_deregister_case, run_label_bound_case,
};
use super::support::{cassandra_directory, finish, registration, store};
use crate::cassandra::TABLE_NODE_DIRECTORY;
use crate::router::NodeId;
use crate::router::directory::{NodeDirectory, RegistrationTtl};
use crate::test_util::{TEST_KEYSPACE, TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{QuickCheck, TestResult};
use scylla::statement::Consistency;
use std::time::Duration;
use tokio::time::{Instant, interval};
use uuid::Uuid;

/// The Cassandra directory answers each trace as the map oracle answers it.
#[test]
fn prop_cassandra_directory_matches_the_model() {
    fn property(trace: DirectoryTrace) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let cassandra = cassandra_directory(STABLE_LEASE).await?;
            let cassandra_answers = run_directory_trace(&cassandra, &trace).await?;
            let expected = expected_answers(&trace);

            if let Some(divergence) = first_divergence(&trace, &cassandra_answers, &expected) {
                return Err(eyre!("Cassandra and model: {divergence}"));
            }
            Ok(())
        }))
    }

    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(10))
        .quickcheck(property as fn(DirectoryTrace) -> TestResult);
}

/// Every label obeys the byte bound in the Cassandra directory.
#[test]
fn cassandra_directory_enforces_the_label_bound() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME
        .block_on(async { run_label_bound_case(&cassandra_directory(STABLE_LEASE).await?).await })
}

/// Repeated deletion stays harmless in the Cassandra directory.
#[test]
fn cassandra_directory_deregisters_idempotently() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        run_idempotent_deregister_case(&cassandra_directory(STABLE_LEASE).await?).await
    })
}

/// A registration lives on a lease and nothing else. Every cell a node writes
/// carries a TTL inside the lease, and past the lease with no refresh the node
/// row is gone — so resolution finds nothing and the node reads as unreachable
/// rather than as a stale address to dial.
#[test]
fn registration_cells_carry_a_ttl_and_expire() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(RegistrationTtl::MIN).await?;
        let session = store().await?.session();
        let node = NodeId::new();
        let written = registration(node);
        directory.register(&written).await?;

        // Only non-NULL regular columns are asked. `TTL()` of a NULL column is
        // NULL. `TTL()` cannot read a primary-key column.
        let leases = session
            .query_unpaged(
                format!(
                    "SELECT TTL(direct_host), TTL(hostname), TTL(network) FROM \
                     {TEST_KEYSPACE}.{TABLE_NODE_DIRECTORY} WHERE node_id = ?"
                ),
                (Uuid::from(node),),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<i32>, Option<i32>, Option<i32>)>()?
            .ok_or_else(|| eyre!("a registration just written must have a row"))?;
        let lease_seconds = directory.ttl().seconds();
        for lease in [leases.0, leases.1, leases.2] {
            let lease = lease.ok_or_else(|| eyre!("a registered cell carries no lease"))?;
            ensure!(
                (1_i32..=lease_seconds).contains(&lease),
                "a cell's lease must be inside the registration lease, not {lease}s"
            );
        }

        // Server expiry emits no event, so a bounded poll is the available
        // observation. The deadline is a hang guard. The absence is the test.
        let deadline = Instant::now() + Duration::from_mins(1);
        let mut ticker = interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let resolved = directory.read(node).await?;
            if resolved.is_none() {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "the registration outlived its lease: node {resolved:?}"
            );
        }
        Ok(())
    })
}

/// An unusable Cassandra row reads as absent.
#[test]
fn unusable_row_reads_as_absent() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(STABLE_LEASE).await?;
        let store = store().await?;
        let query = format!(
            "INSERT INTO {TEST_KEYSPACE}.{TABLE_NODE_DIRECTORY} (node_id, direct_host, \
             direct_port, hostname) VALUES (?, ?, ?, ?) USING TTL 300"
        );
        for (host, port, reason) in [
            (None, None, "has no direct endpoint"),
            (Some("localhost"), Some(0_i32), "uses port zero"),
            (Some(""), Some(1_i32), "has an empty host"),
        ] {
            let node = NodeId::new();
            store
                .session()
                .query_unpaged(
                    query.as_str(),
                    (Uuid::from(node), host, port, "invalid-row"),
                )
                .await?;
            assert!(
                directory.read(node).await?.is_none(),
                "a row that {reason} must not resolve"
            );
        }
        Ok(())
    })
}

/// Every directory statement runs at `LOCAL_ONE`, so no read or write on the
/// response path pays an inter-datacentre round trip.
#[test]
fn directory_statements_run_at_local_one() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = cassandra_directory(STABLE_LEASE).await?;
        for consistency in directory.statement_consistencies() {
            assert_eq!(
                consistency,
                Some(Consistency::LocalOne),
                "every directory statement must run at LOCAL_ONE"
            );
        }
        Ok(())
    })
}

/// A lease exists only inside the range the type publishes, and it carries
/// exactly what the caller asked for. Outside that range there is no
/// [`RegistrationTtl`] at all, so no configuration and no write can hold one.
///
/// The values are fixed rather than generated: both bounds and the default are
/// three points out of 3600, and a generator reaches them too rarely to fail on
/// a bound that goes missing.
#[test]
fn a_lease_exists_only_inside_its_range() -> Result<()> {
    let second = Duration::from_secs(1);
    for asked in [
        RegistrationTtl::MIN,
        RegistrationTtl::DEFAULT.duration(),
        RegistrationTtl::MAX,
    ] {
        let ttl = RegistrationTtl::try_from(asked)?;
        assert_eq!(
            ttl.duration(),
            asked,
            "a lease must publish the requested duration"
        );
    }
    for refused in [
        Duration::ZERO,
        RegistrationTtl::MIN.saturating_sub(second),
        RegistrationTtl::MAX + second,
    ] {
        assert!(
            RegistrationTtl::try_from(refused).is_err(),
            "{refused:?} is outside the accepted range but converted"
        );
    }
    Ok(())
}
