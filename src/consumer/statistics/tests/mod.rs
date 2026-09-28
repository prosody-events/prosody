//! Tests for the statistics gauges: the borrowed assignment iterator, series
//! retirement between reports, and retirement on drop.

mod capture;
mod support;

use capture::{
    FETCH_BYTES, FETCH_MESSAGES, METADATA_AGE, gauge_value, metrics_with, partition_gauge,
    test_meter,
};
use color_eyre::Result;
use color_eyre::eyre::ensure;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use support::{
    Assignment, Entry, GROUP, Report, TOPIC, Topology, assigned_depths, identity,
    statistics_of_partitions, statistics_with,
};

/// The assignment iterator yields each real desired id paired with its own
/// fetch-queue depth, checked against an independent filter over the same
/// generated map.
#[quickcheck]
fn assigned_partitions_match_an_independent_oracle(topology: Topology) -> TestResult {
    let statistics = statistics_of_partitions(
        topology
            .entries
            .into_iter()
            .map(|entry| (entry.id, entry.statistics()))
            .collect(),
    );
    let mut expected: Vec<(&str, i32, i64)> = statistics
        .topics
        .values()
        .flat_map(|topic| &topic.partitions)
        .filter(|&(&id, partition)| id != -1_i32 && partition.desired)
        .map(|(&id, partition)| (TOPIC, id, partition.fetchq_cnt))
        .collect();
    expected.sort_unstable();

    let yielded = assigned_depths(&statistics);
    if yielded == expected {
        TestResult::passed()
    } else {
        TestResult::error(format!(
            "the iterator yielded {yielded:?}, expected {expected:?}"
        ))
    }
}

/// Observing two statistics reports exports exactly the assignment the second
/// one holds. Each series is checked against an oracle over the two generated
/// reports: a partition the second report holds carries its own counters, a
/// partition only the first report held reads zero, and metadata age is the
/// oldest among topics holding an assignment.
#[quickcheck]
fn observing_statistics_exports_the_second_assignment(first: Report, second: Report) -> TestResult {
    let (provider, exporter) = test_meter();
    let metrics = metrics_with(GROUP, &provider);
    let previous = first.assigned();
    let held = second.assigned();
    let expected_age = second.metadata_age();

    metrics.observe(first.into_statistics());
    metrics.observe(second.into_statistics());
    if let Err(error) = provider.force_flush() {
        return TestResult::error(format!("flushing the test meter failed: {error:#}"));
    }

    match exported_series_match(&exporter, &previous, &held, expected_age) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// The oracle for [`observing_statistics_exports_the_second_assignment`].
fn exported_series_match(
    exporter: &InMemoryMetricExporter,
    previous: &Assignment,
    held: &Assignment,
    expected_age: u64,
) -> Result<()> {
    for (&(topic, id), &(messages, bytes)) in held {
        for (name, expected) in [(FETCH_MESSAGES, messages), (FETCH_BYTES, bytes)] {
            let observed = partition_gauge(exporter, name, topic, id)?;
            ensure!(
                observed == Some(expected),
                "{name} for held {topic}:{id} was {observed:?}, expected {expected}"
            );
        }
    }

    for &(topic, id) in previous.keys() {
        if held.contains_key(&(topic, id)) {
            continue;
        }
        for name in [FETCH_MESSAGES, FETCH_BYTES] {
            let observed = partition_gauge(exporter, name, topic, id)?;
            ensure!(
                observed == Some(0),
                "{name} for retired {topic}:{id} was {observed:?}, expected 0"
            );
        }
    }

    let age = gauge_value(exporter, METADATA_AGE, &identity())?;
    ensure!(
        age == Some(expected_age),
        "metadata age was {age:?}, expected {expected_age}"
    );
    Ok(())
}

/// Dropping the gauges retires the last assignment's series, so a stopped
/// consumer stops reporting fetch-queue depth.
#[test]
fn dropping_the_gauges_zeroes_the_last_assignment() -> Result<()> {
    let (provider, exporter) = test_meter();
    let metrics = metrics_with(GROUP, &provider);

    metrics.observe(statistics_with(&[(
        TOPIC,
        100,
        &[Entry::assigned(0, 5, 50)],
    )]));
    drop(metrics);
    provider.force_flush()?;

    for name in [FETCH_MESSAGES, FETCH_BYTES] {
        let value = partition_gauge(&exporter, name, TOPIC, 0)?;
        ensure!(value == Some(0), "{name} was {value:?} after the drop");
    }
    let age = gauge_value(&exporter, METADATA_AGE, &identity())?;
    ensure!(age == Some(0), "metadata age was {age:?} after the drop");
    Ok(())
}
