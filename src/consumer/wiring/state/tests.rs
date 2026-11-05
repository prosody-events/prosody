//! Tests for keyed-state wiring: publication setup and the routing set read
//! from broker metadata.

use super::*;
use crate::JsonCodec;
use crate::state::descriptor::{StateDescriptor, value_state};
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure};
use rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;

/// The topics the broker metadata tests create, with their partition counts.
const BROKER_TOPICS: [(&str, i32); 2] = [("routes-three", 3_i32), ("routes-one", 1_i32)];

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
            Err(RoutingError::Broker(_, UnknownTopicOrPartition))
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

    let result = inputs.memory_publication_setup(MemoryPublicationStore::new());

    assert!(matches!(
        result,
        Err(ConsumerError::KeyedState(
            KeyedStateInitError::PublishedMemoryStorage
        ))
    ));
    Ok(())
}
