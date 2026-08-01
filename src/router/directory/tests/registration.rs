use super::support::{
    ArbRegistration, directory, finish, member_shards, membership, registration, store, token,
};
use crate::cassandra::TABLE_NODE_DIRECTORY;
use crate::router::NodeId;
use crate::router::directory::{GROUP_SHARDS, RegistrationTtl, shard_for};
use crate::test_util::{TEST_KEYSPACE, TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{QuickCheck, TestResult};
use quickcheck_macros::quickcheck;
use scylla::statement::Consistency;
use std::time::Duration;
use tokio::time::{Instant, interval};
use uuid::Uuid;

/// A lease long enough that nothing under test expires while it runs.
const STABLE_LEASE: Duration = Duration::from_mins(10);

/// What a node publishes is what another process reads back: every field of a
/// registration survives the round trip, and a registration that names a group
/// lands in exactly one shard of that group's index while one that names none
/// lands in no shard at all.
#[test]
fn prop_registration_round_trip() {
    fn property(ArbRegistration(written): ArbRegistration) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = directory(STABLE_LEASE).await?;
            directory.register(&written).await?;
            let read = directory
                .read(written.node)
                .await?
                .ok_or_else(|| eyre!("a registration just written must resolve"))?;
            assert_eq!(
                read, written,
                "the registration did not survive the round trip"
            );
            let shards = match &written.group {
                Some(membership) => member_shards(membership, written.node).await?,
                None => Vec::new(),
            };
            let expected = usize::from(written.group.is_some());
            ensure!(
                shards.len() == expected,
                "a registration naming {:?} must occupy {expected} index shard(s), not {shards:?}",
                written.group
            );
            Ok(true)
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(ArbRegistration) -> TestResult);
}

/// A registration lives on a lease and nothing else. Every cell a node writes
/// carries a TTL inside the lease, and past the lease with no refresh both the
/// node row and its index entry are gone — so resolution finds nothing and the
/// node reads as unreachable rather than as a stale address to dial.
#[test]
fn registration_cells_carry_a_ttl_and_expire() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = directory(RegistrationTtl::MIN).await?;
        let session = store().await?.session();
        let membership = membership();
        let node = NodeId::new();
        let written = registration(node, membership.clone());
        directory.register(&written).await?;

        // Only non-NULL regular columns are asked: `TTL()` of a NULL column is
        // NULL whatever the statement's TTL, and `TTL()` cannot be asked of a
        // primary-key column at all.
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

        // Server-side expiry emits no event, so a bounded poll is the only
        // observation available. The deadline is a hang guard; the assertion
        // is the absence below it.
        let deadline = Instant::now() + Duration::from_mins(1);
        let mut ticker = interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let resolved = directory.read(node).await?;
            let shards = member_shards(&membership, node).await?;
            if resolved.is_none() && shards.is_empty() {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "the registration outlived its lease: node {resolved:?}, shards {shards:?}"
            );
        }
        Ok(())
    })
}

/// A row that lost its direct endpoint reads as absent, so a caller reports
/// the node unreachable instead of dialing a partial address.
#[test]
fn half_written_row_reads_as_absent() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = directory(STABLE_LEASE).await?;
        let node = NodeId::new();
        store()
            .await?
            .session()
            .query_unpaged(
                // The seeded row carries a lease of its own, so a row no
                // production path would write does not outlive the test.
                format!(
                    "INSERT INTO {TEST_KEYSPACE}.{TABLE_NODE_DIRECTORY} (node_id, hostname) \
                     VALUES (?, ?) USING TTL 300"
                ),
                (Uuid::from(node), format!("orphan-{}", token())),
            )
            .await?;
        assert!(
            directory.read(node).await?.is_none(),
            "a row with no direct endpoint must not resolve"
        );
        Ok(())
    })
}

/// A shutdown delete removes both rows, and repeating it changes nothing: a
/// delete of an absent row is a no-op.
#[test]
fn deregister_removes_both_rows_and_repeats_harmlessly() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let directory = directory(STABLE_LEASE).await?;
        let membership = membership();
        let node = NodeId::new();
        let written = registration(node, membership.clone());
        directory.register(&written).await?;
        assert!(
            directory.read(node).await?.is_some(),
            "the registration must resolve before it is removed"
        );

        for attempt in 1_u8..=2 {
            directory.deregister(&written).await?;
            assert!(
                directory.read(node).await?.is_none(),
                "attempt {attempt}: the node row must be gone"
            );
            assert!(
                member_shards(&membership, node).await?.is_empty(),
                "attempt {attempt}: the index entry must be gone"
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
        let directory = directory(STABLE_LEASE).await?;
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

/// A node's index shard is always one of the partitions the index has, so no
/// membership row lands where a listing never reads.
#[quickcheck]
fn prop_shard_is_in_range(high: u64, low: u64) -> TestResult {
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&high.to_be_bytes());
    id[8..].copy_from_slice(&low.to_be_bytes());
    let shard = shard_for(NodeId::from_bytes(id));
    assert!(
        (0_i32..GROUP_SHARDS as i32).contains(&shard),
        "shard {shard} is outside the {GROUP_SHARDS} index partitions"
    );
    TestResult::passed()
}
