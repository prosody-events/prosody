//! Tests for keyed-state wiring: route setup and the partition counts read
//! from broker metadata.

use super::*;
use crate::JsonCodec;
use crate::state::descriptor::{StateDescriptor, value_state};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;

/// The topics the broker metadata tests create, with their partition counts.
const BROKER_TOPICS: [(&str, i32); 2] = [("routes-three", 3_i32), ("routes-one", 1_i32)];

/// Broker metadata gives a count for every topic in the cluster, including
/// topics this client never consumed. It rejects a topic the broker does not
/// know.
#[test]
fn broker_metadata_counts_every_topic() -> Result<()> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    for (topic, count) in BROKER_TOPICS {
        cluster.create_topic(topic, count, 1)?;
    }
    let client: BaseConsumer = base_config(cluster.bootstrap_servers()).create()?;
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
            Err(RoutingError::Broker(_, UnknownTopicOrPartition))
        ),
        "an absent topic must be unknown"
    );
    Ok(())
}

/// The count fetch returns the count of each topic that the broker knows,
/// and fails construction when one topic is missing.
#[tokio::test]
async fn fetch_counts_requires_every_topic() -> Result<()> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    for (topic, count) in BROKER_TOPICS {
        cluster.create_topic(topic, count, 1)?;
    }
    let bootstrap = cluster.bootstrap_servers();
    let topics = |names: &[&str]| names.iter().map(|&name| Topic::from(name)).collect();

    let counts = fetch_counts(bootstrap.clone(), topics(&["routes-three", "routes-one"])).await?;
    let broker: Vec<_> = BROKER_TOPICS
        .iter()
        .map(|&(topic, count)| Ok((Topic::from(topic), PartitionCount::try_from(count)?)))
        .collect::<Result<_>>()?;
    ensure!(counts == broker, "the fetch returned {counts:?}");

    let failure = fetch_counts(bootstrap, topics(&["routes-three", "routes-absent"])).await;
    ensure!(
        matches!(
            failure,
            Err(ConsumerError::KeyedState(KeyedStateInitError::Routing(
                RoutingError::Broker(_, UnknownTopicOrPartition)
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

    let result = inputs.memory_routes(MemoryPublicationStore::new());

    assert!(matches!(
        result.map(|_| ()),
        Err(ConsumerError::KeyedState(
            KeyedStateInitError::PublishedMemoryStorage
        ))
    ));
    Ok(())
}

/// A consumer with a published collection rejects a pattern subscription at
/// construction. The check covers the mock memory arm.
#[tokio::test]
async fn published_state_rejects_pattern_subscription() -> Result<()> {
    let mut state = KeyedStateConfiguration::builder()
        .subsystem(Some(SubsystemName::try_new("orders")?))
        .build()?;
    let _ = state.register(value_state::<JsonCodec>("cart").published(true));
    let consumer = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["unused:9092".to_owned()])
        .group_id("orders")
        .subscribed_topics(&["^orders-.*".to_owned()])
        .mock(true)
        .build()?;
    let inputs = KeyedStateInputs::new(state, &consumer, "v1", Duration::from_secs(30))?;

    let result = inputs.memory_routes(MemoryPublicationStore::new());

    assert!(matches!(
        result.map(|_| ()),
        Err(ConsumerError::KeyedState(KeyedStateInitError::Routing(
            RoutingError::Pattern(_)
        )))
    ));
    Ok(())
}
