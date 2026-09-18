//! The catalog reports exactly what the production stores hold, on either
//! backend.

use super::support::{cassandra_fixture, finish, run_memory};
use super::trace::{CatalogTrace, expected_snapshot, fresh_group, segment};
use crate::cassandra::TABLE_SEGMENTS;
use crate::maintenance::Catalog;
use crate::test_util::{
    TEST_KEYSPACE, TEST_RUNTIME, integration_test_count, shared_cassandra_store,
};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentVersion;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::{QuickCheck, TestResult};

/// The memory catalog reports exactly the segments, deferred keys, and timer
/// segment rows the production memory stores hold.
///
/// The model is the oracle, so a catalog and a store that are wrong together
/// still fail.
#[test]
fn prop_memory_catalog_matches_the_trace() {
    fn property(trace: CatalogTrace) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let group = fresh_group();

            let observed = run_memory(&trace, &group).await?;
            ensure!(
                observed == expected_snapshot(&trace, &group),
                "memory catalog disagreed with the model\nobserved: {observed:#?}\nexpected: {:#?}",
                expected_snapshot(&trace, &group)
            );
            Ok(())
        }))
    }

    QuickCheck::new().quickcheck(property as fn(CatalogTrace) -> TestResult);
}

/// The Cassandra catalog and the memory catalog answer one trace identically,
/// and both match the model.
///
/// A fresh group id per iteration keeps the rows disjoint in the shared
/// keyspace, which every Cassandra test writes to and nothing cleans.
#[test]
fn prop_catalog_parity() {
    fn property(trace: CatalogTrace) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let group = fresh_group();

            let memory = run_memory(&trace, &group).await?;
            let cassandra = cassandra_fixture().await?.run(&trace, &group).await?;

            let expected = expected_snapshot(&trace, &group);
            ensure!(
                memory == expected,
                "memory catalog disagreed with the model\nobserved: {memory:#?}\nexpected: \
                 {expected:#?}"
            );
            ensure!(
                cassandra == expected,
                "Cassandra catalog disagreed with the model\nobserved: {cassandra:#?}\nexpected: \
                 {expected:#?}"
            );
            Ok(())
        }))
    }

    QuickCheck::new()
        .tests(integration_test_count(10))
        .quickcheck(property as fn(CatalogTrace) -> TestResult);
}

/// A timer segment row written before the version column existed reads as V1.
///
/// The row is seeded with a raw insert, because every production write path
/// binds a version, so no trace can reach this decode.
#[test]
fn legacy_timer_segment_row_reads_as_v1() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let group = fresh_group();
        let id = segment(&group, 0).timer_id();
        let slab_size = CompactDuration::new(600);

        shared_cassandra_store()
            .await?
            .session()
            .query_unpaged(
                format!(
                    "INSERT INTO {TEST_KEYSPACE}.{TABLE_SEGMENTS} (id, name, slab_size) VALUES \
                     (?, ?, ?)"
                ),
                (id.as_uuid(), group.as_str(), slab_size),
            )
            .await?;

        let row = cassandra_fixture()
            .await?
            .catalog()
            .timer_segment(id)
            .await?
            .ok_or_else(|| eyre!("the seeded timer segment row was not read back"))?;
        ensure!(
            row.version == SegmentVersion::V1,
            "a missing version column must decode as V1, got {:?}",
            row.version
        );
        Ok(())
    })
}
