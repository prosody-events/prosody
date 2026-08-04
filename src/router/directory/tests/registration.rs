use super::support::{ArbRegistration, directory, finish, membership, registration, store, token};
use crate::cassandra::TABLE_NODE_DIRECTORY;
use crate::router::directory::{
    Endpoint, GroupMembership, NetworkId, NodeRegistration, RegistrationTtl,
};
use crate::router::{Host, MAX_LABEL_BYTES, NodeId};
use crate::test_util::{TEST_KEYSPACE, TEST_RUNTIME, integration_test_count};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use fixedstr::Flexstr;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use scylla::statement::Consistency;
use std::time::Duration;
use tokio::time::{Instant, interval};
use uuid::Uuid;

/// A lease long enough that nothing under test expires while it runs.
const STABLE_LEASE: Duration = Duration::from_mins(10);

/// Every label a registration carries. One case pushes one of them over the
/// bound; the rest stay at it.
const LABELS: &[Label] = &[
    Label::DirectHost,
    Label::AdvertisedHost,
    Label::Network,
    Label::Cluster,
    Label::Group,
    Label::Hostname,
];

/// One label of a registration, named so a case can push exactly one of them
/// over the bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Label {
    DirectHost,
    AdvertisedHost,
    Network,
    Cluster,
    Group,
    Hostname,
}

impl Arbitrary for Label {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(LABELS).unwrap_or(&Self::DirectHost)
    }
}

/// What a node publishes is what another process reads back: every field of a
/// registration survives the round trip.
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
            Ok(())
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(ArbRegistration) -> TestResult);
}

/// A label is bounded at both ends of a row: one at the bound resolves and
/// stays off the heap, and one byte more makes the whole row unresolvable.
///
/// The bound is what makes the address cache bounded in bytes as well as in
/// entries. The cache charges one unit per entry however many bytes that entry
/// holds, so a label that reached the heap would let one entry grow to whatever
/// the process that published it chose. Shortening the label instead would
/// resolve a different host, which is why the row goes rather than the label.
#[test]
fn prop_a_label_over_the_bound_makes_a_row_unresolvable() {
    fn property(over: Label) -> TestResult {
        finish(TEST_RUNTIME.block_on(async {
            let directory = directory(STABLE_LEASE).await?;
            let bounded = labelled(NodeId::new(), None);
            directory.register(&bounded).await?;
            let read = directory
                .read(bounded.node)
                .await?
                .ok_or_else(|| eyre!("a registration at the bound must resolve"))?;
            ensure!(
                read == bounded,
                "a registration at the bound did not survive the round trip"
            );
            let inline = read.direct.host.is_fixed()
                && read.hostname.is_fixed()
                && read.network.as_ref().is_some_and(Flexstr::is_fixed)
                && read
                    .advertised
                    .as_ref()
                    .is_some_and(|entry| entry.host.is_fixed());
            ensure!(
                inline,
                "a resolved registration must hold no label on the heap"
            );

            let oversized = labelled(NodeId::new(), Some(over));
            directory.register(&oversized).await?;
            ensure!(
                directory.read(oversized.node).await?.is_none(),
                "a row whose {over:?} is one byte over the bound must not resolve"
            );
            Ok(())
        }))
    }
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(property as fn(Label) -> TestResult);
}

/// A registration lives on a lease and nothing else. Every cell a node writes
/// carries a TTL inside the lease, and past the lease with no refresh the node
/// row is gone — so resolution finds nothing and the node reads as unreachable
/// rather than as a stale address to dial.
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

/// A shutdown delete removes the node row, and repeating it changes nothing: a
/// delete of an absent row is a no-op.
#[test]
fn deregister_removes_the_row_and_repeats_harmlessly() -> Result<()> {
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
            "a lease must publish the duration it was asked for"
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

/// A registration for `node` whose every label is exactly [`MAX_LABEL_BYTES`]
/// long, except `over`, which is one byte longer.
fn labelled(node: NodeId, over: Option<Label>) -> NodeRegistration {
    let text = |label: Label| "n".repeat(MAX_LABEL_BYTES + usize::from(over == Some(label)));
    NodeRegistration {
        node,
        direct: Endpoint {
            host: Host::make(&text(Label::DirectHost)),
            port: 7777,
        },
        advertised: Some(Endpoint {
            host: Host::make(&text(Label::AdvertisedHost)),
            port: 443,
        }),
        network: Some(NetworkId::make(&text(Label::Network))),
        group: Some(GroupMembership {
            cluster: Flexstr::make(&text(Label::Cluster)),
            group: Flexstr::make(&text(Label::Group)),
        }),
        hostname: Host::make(&text(Label::Hostname)),
    }
}
