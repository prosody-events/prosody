//! The catalog reports exactly what the production stores hold, on either
//! backend.

use super::support::{cassandra_fixture, collect_keys, finish, run_memory};
use super::trace::{CatalogTrace, SLAB_SIZE, expected_snapshot, fresh_group, segment};
use crate::cassandra::TABLE_SEGMENTS;
use crate::maintenance::Catalog;
use crate::maintenance::catalog::cassandra::CATALOG_PAGE_SIZE;
use crate::test_util::{
    TEST_KEYSPACE, TEST_RUNTIME, integration_test_count, shared_cassandra_store,
};
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

        shared_cassandra_store()
            .await?
            .session()
            .query_unpaged(
                format!(
                    "INSERT INTO {TEST_KEYSPACE}.{TABLE_SEGMENTS} (id, name, slab_size) VALUES \
                     (?, ?, ?)"
                ),
                (id.as_uuid(), group.as_str(), SLAB_SIZE),
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

/// Both key scans report every key of a segment that holds more than one page
/// of them.
///
/// A trace cannot reach this: its key pool is far smaller than one page, so
/// every trace scan reads a single page.
#[test]
fn key_scans_cross_a_page_boundary() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let group = fresh_group();
        let fixture = cassandra_fixture().await?;
        let catalog = fixture.catalog();

        // The seed below spans a page boundary only while both scans fetch
        // pages of this size. Prove that first, or the rest of the test reads
        // one page and proves nothing.
        ensure!(
            catalog.key_scan_page_sizes() == [CATALOG_PAGE_SIZE; 2],
            "both key scans must fetch pages of {CATALOG_PAGE_SIZE} rows, got {:?}",
            catalog.key_scan_page_sizes()
        );

        let expected = fixture
            .seed_keys(&group, usize::try_from(CATALOG_PAGE_SIZE)? + 1)
            .await?;
        let id = segment(&group, 0).defer_id();
        ensure!(
            collect_keys(catalog.message_keys(id)).await? == expected,
            "the message key scan must report every key across the page boundary"
        );
        ensure!(
            collect_keys(catalog.timer_keys(id)).await? == expected,
            "the timer key scan must report every key across the page boundary"
        );
        Ok(())
    })
}
