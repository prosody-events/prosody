//! Tests for the Kafka consumer observation: the shared partition-count rule,
//! the borrowed assignment iterator, the gauges, and the real startup install.

mod support;

use super::{KafkaSnapshot, PartitionCountObservationError, metrics::KafkaMetrics};
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::observer::KafkaObserver;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state_reader::PartitionCountError;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use opentelemetry::metrics::MeterProvider;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::statistics::Partition as StatsPartition;
use rdkafka::statistics::Topic as StatsTopic;
use rdkafka::{ClientConfig, Statistics};
use std::collections::HashMap;
use std::time::Duration;
use support::{
    Entry, FETCH_BYTES, FETCH_MESSAGES, GROUP, METADATA_AGE, TOPIC, Topology, assigned_ids,
    contiguous, gauge_value, guard_of, initialize_with, partition_gauge, statistics_with,
    test_meter,
};

/// The statistics view reports a count exactly when an independent calculation
/// over the generated ids does: drop `-1`, reject unknown entries, sort, and
/// compare with `0..len`. One generated map feeds oracle and subject, so a
/// duplicate id cannot desync them.
///
/// The assignment iterator is checked on the same value: it must yield exactly
/// the real desired ids.
#[quickcheck]
fn statistics_count_matches_an_independent_oracle(topology: Topology) -> TestResult {
    let partitions: HashMap<i32, StatsPartition> = topology
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.statistics()))
        .collect();

    let mut real: Vec<i32> = partitions
        .keys()
        .copied()
        .filter(|&id| id != -1_i32)
        .collect();
    real.sort_unstable();
    let any_unknown = partitions
        .iter()
        .any(|(&id, partition)| id != -1_i32 && partition.unknown);
    let expected: Option<usize> = match i32::try_from(real.len()) {
        Ok(len) if !real.is_empty() && !any_unknown && real.iter().copied().eq(0_i32..len) => {
            Some(real.len())
        }
        _ => None,
    };
    let mut expected_assigned: Vec<i32> = partitions
        .iter()
        .filter(|&(&id, partition)| id != -1_i32 && partition.desired)
        .map(|(&id, _)| id)
        .collect();
    expected_assigned.sort_unstable();

    let guard = guard_of(Statistics {
        topics: [(
            TOPIC.to_owned(),
            StatsTopic {
                topic: TOPIC.to_owned(),
                partitions,
                ..StatsTopic::default()
            },
        )]
        .into_iter()
        .collect(),
        ..Statistics::default()
    });

    let observed = guard.snapshot.partition_count(TOPIC);
    let agrees = match (expected, &observed) {
        (Some(expected), Ok(observed)) => usize::try_from(i32::from(*observed)) == Ok(expected),
        (None, Err(_)) => true,
        _ => false,
    };
    if !agrees {
        return TestResult::error(format!(
            "expected count {expected:?}, observed {:?}",
            observed.map(i32::from)
        ));
    }

    let yielded: Vec<(&str, i32)> = guard
        .assigned_partitions()
        .map(|(topic, id, _)| (topic, id))
        .collect();
    if yielded.iter().any(|&(topic, _)| topic != TOPIC) {
        return TestResult::error("the iterator yielded a topic outside the tree");
    }
    let mut ids: Vec<i32> = yielded.iter().map(|&(_, id)| id).collect();
    ids.sort_unstable();
    TestResult::from_bool(ids == expected_assigned)
}

/// Both observation generations report the same count for equivalent valid
/// topology, and both report an absent topic the same way.
#[test]
fn metadata_and_statistics_agree_on_valid_topology() -> Result<()> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    cluster.create_topic("parity-three", 3, 1)?;
    cluster.create_topic("parity-one", 1, 1)?;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()?;
    let from_metadata =
        KafkaSnapshot::InitialMetadata(consumer.fetch_metadata(None, Duration::from_secs(10))?);

    for (topic, count) in [("parity-three", 3_i32), ("parity-one", 1_i32)] {
        let observed = from_metadata.partition_count(topic)?;
        ensure!(
            i32::from(observed) == count,
            "startup metadata reported {} partitions for {topic}, expected {count}",
            i32::from(observed)
        );
        let equivalent = statistics_with(&[(topic, 0, &contiguous(count))]);
        let from_statistics = KafkaSnapshot::ConsumerStatistics(Box::new(equivalent));
        ensure!(
            from_statistics.partition_count(topic)? == observed,
            "the two generations disagreed on {topic}"
        );
    }

    ensure!(
        matches!(
            from_metadata.partition_count("parity-absent"),
            Err(PartitionCountObservationError::TopicUnknown(_))
        ),
        "startup metadata must report an absent topic as unknown"
    );
    let empty = KafkaSnapshot::ConsumerStatistics(Box::default());
    ensure!(
        matches!(
            empty.partition_count("parity-absent"),
            Err(PartitionCountObservationError::TopicUnknown(_))
        ),
        "statistics must report an absent topic as unknown"
    );
    Ok(())
}

/// The assignment iterator yields borrowed entries for real desired partitions
/// only. The internal entry is marked desired and the undesired entry has a
/// real id, so each filter conjunct is on its own the reason an entry is
/// excluded.
#[test]
fn assigned_iterator_excludes_internal_and_undesired() -> Result<()> {
    let guard = guard_of(statistics_with(&[
        (
            TOPIC,
            0,
            &[
                Entry::assigned(0, 17, 170),
                Entry::revoked(1),
                Entry::internal(),
            ],
        ),
        ("idle", 0, &[Entry::revoked(0)]),
    ]));

    let yielded: Vec<(&str, i32, i64)> = guard
        .assigned_partitions()
        .map(|(topic, id, partition)| (topic, id, partition.fetchq_cnt))
        .collect();
    ensure!(
        yielded == vec![(TOPIC, 0_i32, 17_i64)],
        "expected only the assigned partition with its own queue depth, got {yielded:?}"
    );
    Ok(())
}

/// Metadata age reports the oldest metadata among topics this instance actually
/// holds, ignoring topics the client knows but was not assigned.
#[test]
fn metadata_age_covers_only_assigned_topics() -> Result<()> {
    let (provider, exporter) = test_meter();
    let metrics = KafkaMetrics::new(&provider.meter("observer-tests"), GROUP);

    metrics.record(
        None,
        &statistics_with(&[
            ("assigned", 100, &[Entry::assigned(0, 0, 0)]),
            ("idle", 999, &[Entry::revoked(0)]),
        ]),
    );
    provider.force_flush()?;

    let age = gauge_value(&exporter, METADATA_AGE, &[])?;
    ensure!(
        age == Some(100),
        "expected the assigned topic's age of 100, got {age:?}"
    );
    Ok(())
}

/// A partition the next statistics report no longer holds has both of its
/// per-partition series zeroed, while the surviving partition keeps reporting.
#[test]
fn removed_assignment_zeroes_retired_series() -> Result<()> {
    let (provider, exporter) = test_meter();
    let metrics = KafkaMetrics::new(&provider.meter("observer-tests"), GROUP);

    let first = statistics_with(&[(
        TOPIC,
        0,
        &[Entry::assigned(0, 5, 50), Entry::assigned(1, 7, 70)],
    )]);
    metrics.record(None, &first);
    let second = statistics_with(&[(TOPIC, 0, &[Entry::assigned(0, 9, 90), Entry::revoked(1)])]);
    metrics.record(Some(&guard_of(first)), &second);
    provider.force_flush()?;

    for (name, kept) in [(FETCH_MESSAGES, 9_u64), (FETCH_BYTES, 90_u64)] {
        let held = partition_gauge(&exporter, name, TOPIC, 0)?;
        ensure!(
            held == Some(kept),
            "{name} for the retained partition was {held:?}, expected {kept}"
        );
        let gone = partition_gauge(&exporter, name, TOPIC, 1)?;
        ensure!(
            gone == Some(0),
            "{name} for the revoked partition was {gone:?}, expected 0"
        );
    }
    Ok(())
}

/// Shutdown zeroes the last assignment's series, so a stopped consumer stops
/// reporting fetch-queue depth.
#[test]
fn shutdown_zeroes_the_last_assignment() -> Result<()> {
    let (provider, exporter) = test_meter();
    let metrics = KafkaMetrics::new(&provider.meter("observer-tests"), GROUP);

    let last = statistics_with(&[(TOPIC, 100, &[Entry::assigned(0, 5, 50)])]);
    metrics.record(None, &last);
    metrics.zero_assigned(&guard_of(last));
    provider.force_flush()?;

    for name in [FETCH_MESSAGES, FETCH_BYTES] {
        let value = partition_gauge(&exporter, name, TOPIC, 0)?;
        ensure!(value == Some(0), "{name} was {value:?} after shutdown");
    }
    let age = gauge_value(&exporter, METADATA_AGE, &[])?;
    ensure!(age == Some(0), "metadata age was {age:?} after shutdown");
    Ok(())
}

/// Real consumer startup installs metadata into the observer the caller passed
/// in, before the poll loop can dispatch anything. A construction path that
/// minted its own observer would leave this handle empty.
#[tokio::test(flavor = "multi_thread")]
async fn startup_installs_metadata_into_the_callers_observer() -> Result<()> {
    let config = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id("observer-startup-group")
        .subscribed_topics(vec![TOPIC.to_owned()])
        .mock(true)
        .build()?;
    let observer = KafkaObserver::new(&config.group_id);
    let consumer = initialize_with(&config, observer.clone()).await??;

    // Collect the verdict first: a live consumer must be shut down on every
    // path or rdkafka's threads hang the test binary.
    let outcome = observer.snapshot().map_or_else(
        || Err(eyre!("startup installed no observation")),
        |guard| {
            ensure!(
                matches!(*guard.snapshot, KafkaSnapshot::InitialMetadata(_)),
                "the first observation must be startup metadata"
            );
            Ok(())
        },
    );
    consumer.shutdown().await;
    outcome
}

/// A consumer refuses to start when its mandatory startup metadata fetch fails,
/// and installs nothing.
#[tokio::test(flavor = "multi_thread")]
async fn startup_fails_when_metadata_is_unreachable() -> Result<()> {
    let config = ConsumerConfiguration::builder()
        // Port 1 is unassigned, so the connection is refused rather than hung.
        .bootstrap_servers(vec!["127.0.0.1:1".to_owned()])
        .group_id("observer-unreachable-group")
        .subscribed_topics(vec![TOPIC.to_owned()])
        .build()?;
    let observer =
        KafkaObserver::with_startup_timeout(&config.group_id, Duration::from_millis(250));

    match initialize_with(&config, observer.clone()).await? {
        Ok(consumer) => {
            consumer.shutdown().await;
            bail!("construction succeeded without a startup observation");
        }
        Err(error) => ensure!(
            matches!(error, ConsumerError::Kafka(_)),
            "expected a Kafka error, got {error:#}"
        ),
    }
    ensure!(
        observer.snapshot().is_none(),
        "a failed startup fetch must install no observation"
    );
    Ok(())
}

/// Statistics replace the whole observation, and a guard taken beforehand keeps
/// answering from its own generation.
#[test]
fn statistics_replace_the_whole_observation() -> Result<()> {
    let observer = KafkaObserver::new("observer-replace-group");
    ensure!(
        matches!(
            observer.partition_count(TOPIC),
            Err(PartitionCountObservationError::NoSnapshot)
        ),
        "a fresh observer must report that it holds no observation"
    );

    observer.observe_statistics(statistics_with(&[(TOPIC, 0, &contiguous(3))]));
    let first = observer
        .snapshot()
        .ok_or_else(|| eyre!("observing statistics installed no observation"))?;
    ensure!(
        i32::from(observer.partition_count(TOPIC)?) == 3_i32,
        "expected the observed three-partition topology"
    );

    observer.observe_statistics(statistics_with(&[(TOPIC, 0, &contiguous(1))]));
    ensure!(
        i32::from(observer.partition_count(TOPIC)?) == 1_i32,
        "the second report must replace the first"
    );
    ensure!(
        assigned_ids(&first) == vec![0_i32, 1_i32, 2_i32],
        "the guard taken before the replacement must keep its own generation"
    );
    Ok(())
}

/// A missing or incomplete observation is worth retrying; a structurally
/// invalid count is not. The count arm is unreachable through the validator — a
/// contiguous nonempty id set is always positive — so it is classified on a
/// directly built value.
#[test]
fn observation_errors_classify_for_retry() -> Result<()> {
    for error in [
        PartitionCountObservationError::NoSnapshot,
        PartitionCountObservationError::TopicUnknown(TOPIC.to_owned()),
        PartitionCountObservationError::TopicIncomplete(TOPIC.to_owned()),
    ] {
        ensure!(
            error.classify_error() == ErrorCategory::Transient,
            "{error} must be transient"
        );
    }
    let invalid = PartitionCountObservationError::Count(PartitionCountError::NonPositive(0));
    ensure!(
        invalid.classify_error() == ErrorCategory::Permanent,
        "an invalid count must be permanent"
    );
    Ok(())
}
