//! Tests for keyed-state wiring: publication setup and the routing set read
//! from broker metadata.

use super::*;
use crate::JsonCodec;
use crate::state::descriptor::{StateDescriptor, value_state};
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::collections::BTreeSet;

/// The topics the broker metadata tests create, with their partition counts.
/// Sixty-five partitions exceed the inline capacity of the id buffer.
const BROKER_TOPICS: [(&str, i32); 3] = [
    ("routes-three", 3_i32),
    ("routes-one", 1_i32),
    ("routes-wide", 65_i32),
];

/// A generated partition id list: `0..size` rotated, then independent
/// mutations that drop an id, leave a gap, repeat an id, or add librdkafka's
/// internal `-1`.
///
/// No `shrink` override: the list is short, so its `Debug` output is already
/// the whole reproducer.
#[derive(Clone, Debug)]
struct PartitionIds(Vec<i32>);

impl Arbitrary for PartitionIds {
    fn arbitrary(g: &mut Gen) -> Self {
        let size = *g
            .choose(&[0_i32, 1_i32, 2_i32, 3_i32, 16_i32, 64_i32, 65_i32, 200_i32])
            .unwrap_or(&3_i32);
        let mut ids: Vec<i32> = (0_i32..size).collect();
        if one_in_four(g) && !ids.is_empty() {
            let index = usize::arbitrary(g) % ids.len();
            ids.remove(index);
        }
        if one_in_four(g) {
            ids.push(size + 2_i32);
        }
        if one_in_four(g) && !ids.is_empty() {
            let index = usize::arbitrary(g) % ids.len();
            ids.push(ids[index]);
        }
        if one_in_four(g) {
            ids.push(-1_i32);
        }
        if !ids.is_empty() {
            let shift = usize::arbitrary(g) % ids.len();
            ids.rotate_left(shift);
        }
        Self(ids)
    }
}

fn one_in_four(g: &mut Gen) -> bool {
    *g.choose(&[true, false, false, false]).unwrap_or(&false)
}

/// A count exists exactly when the ids are a permutation of `0..len` for a
/// nonempty list. The oracle decides that with a set, not with a sort.
#[quickcheck]
fn contiguous_count_matches_an_independent_oracle(ids: PartitionIds) -> TestResult {
    let PartitionIds(ids) = ids;
    let distinct: BTreeSet<i32> = ids.iter().copied().collect();
    let in_range = ids
        .iter()
        .all(|&id| usize::try_from(id).is_ok_and(|id| id < ids.len()));
    let expected =
        (!ids.is_empty() && distinct.len() == ids.len() && in_range).then_some(ids.len());

    let observed = contiguous_count(ids.iter().copied(), "generated");
    let agrees = match (expected, &observed) {
        (Some(expected), Ok(count)) => usize::try_from(i32::from(*count)) == Ok(expected),
        (None, Err(RoutingError::Invalid(_))) => true,
        _ => false,
    };
    if agrees {
        TestResult::passed()
    } else {
        TestResult::error(format!(
            "ids {ids:?}: expected {expected:?}, observed {:?}",
            observed.map(i32::from)
        ))
    }
}

/// Broker metadata gives a count for every topic in the cluster, including
/// topics this client never consumed. It rejects a topic the broker does not
/// know, and a pattern subscription.
#[test]
fn broker_metadata_counts_every_topic() -> Result<()> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    for (topic, count) in BROKER_TOPICS {
        cluster.create_topic(topic, count, 1)?;
    }
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .create()?;
    let metadata = client.fetch_metadata(None, METADATA_TIMEOUT)?;

    for (topic, count) in BROKER_TOPICS {
        let observed = i32::from(partition_count(&metadata, topic)?);
        ensure!(
            observed == count,
            "{topic} reported {observed} partitions, expected {count}"
        );
    }
    ensure!(
        matches!(
            partition_count(&metadata, "routes-absent"),
            Err(RoutingError::Unknown(_))
        ),
        "an absent topic must be unknown"
    );
    ensure!(
        matches!(
            partition_count(&metadata, "^routes-.*"),
            Err(RoutingError::Pattern(_))
        ),
        "a pattern must be rejected, not looked up"
    );
    Ok(())
}

/// The routing fetch succeeds when the broker knows every topic, and fails
/// construction when one topic is missing.
#[tokio::test]
async fn fetch_routes_requires_every_topic() -> Result<()> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    for (topic, count) in BROKER_TOPICS {
        cluster.create_topic(topic, count, 1)?;
    }
    let bootstrap = cluster.bootstrap_servers();
    let group: ConsumerGroup = Arc::from("routes");
    let topics = |names: &[&str]| {
        PublicationTopics::new(names.iter().map(|&name| Topic::from(name)).collect())
    };

    let Some(present) = topics(&["routes-three", "routes-one"]) else {
        bail!("the present topic set must not be empty");
    };
    fetch_routes(bootstrap.clone(), group.clone(), present).await?;

    let Some(missing) = topics(&["routes-three", "routes-absent"]) else {
        bail!("the missing topic set must not be empty");
    };
    let failure = fetch_routes(bootstrap, group, missing).await;
    ensure!(
        matches!(
            failure,
            Err(ConsumerError::KeyedState(KeyedStateInitError::Routing(
                RoutingError::Unknown(_)
            )))
        ),
        "a missing topic must fail the fetch, got {:?}",
        failure.map(|_| ())
    );
    Ok(())
}

/// Published routing from memory is valid only when the Kafka topology is
/// also mocked. A live consumer has no real partition count for the in-memory
/// publication store.
#[tokio::test]
async fn published_memory_state_requires_mock_mode() -> Result<()> {
    let mut state = KeyedStateConfiguration::builder()
        .subsystem(Some(SubsystemName::try_new("orders")?))
        .build()?;
    let _ = state.register(value_state::<JsonCodec>("cart").published(true));
    let consumer = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["unused:9092".to_owned()])
        .group_id("orders")
        .subscribed_topics(&["orders".to_owned()])
        .mock(false)
        .build()?;
    let inputs = KeyedStateInputs::new(state, &consumer, "v1", Duration::from_secs(30))?;

    let result = inputs.memory_publication_setup(MemoryPublicationStore::new());

    assert!(matches!(
        result,
        Err(ConsumerError::KeyedState(
            KeyedStateInitError::PublishedMemoryStorage
        ))
    ));
    Ok(())
}
