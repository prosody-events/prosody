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

/// The topics the broker metadata test creates, with their partition counts.
const BROKER_TOPICS: [(&str, i32); 2] = [("routes-three", 3_i32), ("routes-one", 1_i32)];

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

/// A consumer with a published collection needs literal topics, and its
/// memory storage needs mock mode. A mock consumer with literal topics builds
/// its routes.
#[test]
fn published_routes_need_literal_topics_and_mock_memory() -> Result<()> {
    let routes = |mock: bool, topic: &str| -> Result<Result<Routes<_>, ConsumerError>> {
        let mut state = KeyedStateConfiguration::builder()
            .subsystem(Some(SubsystemName::try_new("orders")?))
            .build()?;
        let _ = state.register(value_state::<JsonCodec>("cart").published(true));
        let consumer = ConsumerConfiguration::builder()
            .bootstrap_servers(vec!["unused:9092".to_owned()])
            .group_id("orders")
            .subscribed_topics(&[topic.to_owned()])
            .mock(mock)
            .build()?;
        Ok(
            KeyedStateInputs::new(state, &consumer, "v1", Duration::from_secs(30))
                .and_then(|inputs| inputs.memory_routes(MemoryPublicationStore::new())),
        )
    };

    ensure!(
        matches!(
            routes(false, "orders")?,
            Err(ConsumerError::KeyedState(
                KeyedStateInitError::PublishedMemoryStorage
            ))
        ),
        "a live consumer must not publish from memory"
    );
    ensure!(
        matches!(
            routes(true, "^orders-.*")?,
            Err(ConsumerError::KeyedState(KeyedStateInitError::Routing(
                RoutingError::Pattern(_)
            )))
        ),
        "a pattern subscription must not publish"
    );
    ensure!(
        matches!(routes(true, "orders")?, Ok(Routes::Subsystem(_))),
        "a mock consumer with literal topics must publish"
    );
    Ok(())
}
