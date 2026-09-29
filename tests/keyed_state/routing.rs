//! Publication when the group splits its subscribed topics across members.
//!
//! Two pipeline consumers in one group subscribe to the same two
//! single-partition topics. The cooperative-sticky assignor gives each member
//! one partition. Partition zero of the lexically first topic owns publication.
//! Its owner must publish a routing row for the other topic too, although it
//! never consumes that topic. The owner must also process its own partition.
//!
//! The consumers report librdkafka statistics every 100 ms. So the owner has
//! a statistics report before its assignment, and that report covers only the
//! topics the owner consumes. A routing set that reads partition counts from
//! those reports cannot find the other topic. The owner then retries state
//! acquisition and processes nothing.

use crate::common::handler::FallibleTestHandler;
use crate::common::kafka::{create_topic_with_partitions, wait_for_assignment};
use crate::common::{TEST_KEYSPACE, create_cassandra_trigger_store_config};
use crate::fresh_subsystem;
use crate::publication::publication_store;
use color_eyre::eyre::{Result, bail, ensure, eyre};
use prosody::admin::ProsodyAdminClient;
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::{
    CommonConfiguration, ConsumerConfiguration, ConsumerSetup, KeyedStateConfiguration,
    PipelineMiddlewareConfiguration, ProsodyConsumer,
};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::state::descriptor::{StateDescriptor, ValueDescriptor, value_state};
use prosody::state::publication::PublicationStore;
use prosody::state::{StateName, StateType};
use prosody::subsystem::SubsystemName;
use prosody::telemetry::Telemetry;
use prosody::tracing::init_test_logging;
use prosody::{JsonCodec, Topic};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::timeout;
use tracing::info;
use uuid::Uuid;

/// The broker every client in this test connects to.
const BOOTSTRAP: &str = "localhost:9094";

/// The published collection both members register.
const COLLECTION: &str = "routed";

/// The key of the one message the test produces.
const KEY: &str = "routed-key";

/// A statistics interval short enough that the first report arrives before the
/// group assigns partitions.
const STATISTICS_INTERVAL: Duration = Duration::from_millis(100);

/// Hang-guard for the message to reach the handler. Never the assertion.
const PROCESSED_GUARD: Duration = Duration::from_mins(1);

/// The owner of the leader partition processes its message and publishes one
/// routing row for each subscribed topic, including the topic it never holds.
#[tokio::test]
async fn test_leader_owner_publishes_topic_it_never_holds() -> Result<()> {
    init_test_logging();

    let (first, admin) = create_topic_with_partitions(1).await?;
    let outcome = match create_topic_with_partitions(1).await {
        Ok((second, _)) => {
            let outcome = Box::pin(run_split_group(first, second)).await;
            outcome.and(delete_topic(admin, second).await)
        }
        Err(error) => Err(error),
    };
    outcome.and(delete_topic(admin, first).await)
}

/// Starts both members, runs the scenario, and shuts both members down on
/// every path.
async fn run_split_group(first: Topic, second: Topic) -> Result<()> {
    let (leader, follower) = if first < second {
        (first, second)
    } else {
        (second, first)
    };
    let subsystem = fresh_subsystem()?;
    let group_id = Uuid::new_v4().to_string();
    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(vec![BOOTSTRAP.to_owned()])
        .group_id(group_id.clone())
        .probe_port(None)
        .subscribed_topics(&[leader.to_string(), follower.to_string()])
        .statistics_interval(STATISTICS_INTERVAL)
        .build()?;
    let (messages_tx, mut messages) = channel(10);

    // Both members must join the first generation, so the assignor splits the
    // two partitions between them.
    let (one, two) = match tokio::join!(
        start_member(&consumer_config, &subsystem, messages_tx.clone()),
        start_member(&consumer_config, &subsystem, messages_tx),
    ) {
        (Ok(one), Ok(two)) => (one, two),
        (Ok(started), Err(error)) | (Err(error), Ok(started)) => {
            started.shutdown().await;
            return Err(error);
        }
        (Err(error), Err(_)) => return Err(error),
    };

    let outcome = async {
        tokio::try_join!(wait_for_assignment(&one, 1), wait_for_assignment(&two, 1))?;
        let split = (
            one.assigned_partition_count(),
            two.assigned_partition_count(),
        );
        info!(?split, "group assigned the partitions");
        ensure!(
            split == (1, 1),
            "each member must hold exactly one partition, got {split:?}"
        );

        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(vec![BOOTSTRAP.to_owned()])
                .source_system("routing-test")
                .build()?,
            Telemetry::new().sender(),
        )?;
        let payload = json!({ "id": "evt-routed" });
        producer.send([], leader, KEY, payload.clone()).await?;

        let Ok(received) = timeout(PROCESSED_GUARD, messages.recv()).await else {
            bail!("the owner of {leader}:0 did not process its message within {PROCESSED_GUARD:?}");
        };
        ensure!(
            received == Some((KEY.to_owned(), payload)),
            "the handler must receive the produced message, got {received:?}"
        );

        assert_routing_set(&subsystem, &group_id, leader, follower).await
    }
    .await;

    one.shutdown().await;
    two.shutdown().await;
    outcome
}

/// Builds one pipeline member that registers [`COLLECTION`] published under
/// `subsystem` and forwards every message to `messages_tx`.
async fn start_member(
    consumer_config: &ConsumerConfiguration,
    subsystem: &SubsystemName,
    messages_tx: Sender<(String, Value)>,
) -> Result<ProsodyConsumer<JsonCodec>> {
    let mut keyed_state = KeyedStateConfiguration::builder().build()?;
    keyed_state.subsystem = Some(subsystem.clone());
    let routed: ValueDescriptor = value_state(COLLECTION);
    let _routed = keyed_state.register(routed.published(true));

    let common = CommonConfiguration {
        scheduler: SchedulerConfigurationBuilder::default().build()?,
        timeout: TimeoutConfigurationBuilder::default().build()?,
        dedup: DeduplicationConfigurationBuilder::default().build()?,
        keyed_state,
    };

    Ok(Box::pin(ProsodyConsumer::<JsonCodec>::pipeline_consumer(
        ConsumerSetup {
            consumer: consumer_config,
            trigger_store: &create_cassandra_trigger_store_config(),
            common: &common,
        },
        PipelineMiddlewareConfiguration {
            retry: RetryConfigurationBuilder::default().build()?,
            monopolization: MonopolizationConfigurationBuilder::default().build()?,
            defer: DeferConfigurationBuilder::default().build()?,
        },
        Telemetry::new(),
        FallibleTestHandler { messages_tx },
    ))
    .await?)
}

/// Requires exactly two routing rows for `group_id` under `(subsystem,
/// COLLECTION)`: one per subscribed topic, each with a partition count of one.
async fn assert_routing_set(
    subsystem: &SubsystemName,
    group_id: &str,
    leader: Topic,
    follower: Topic,
) -> Result<()> {
    let name = StateName::try_new(COLLECTION).map_err(|e| eyre!("name: {e}"))?;
    let mut rows: Vec<_> = publication_store()
        .await?
        .read_publications(subsystem, StateType::Application, &name)
        .await?
        .into_iter()
        .filter(|row| row.group_id.as_ref() == group_id)
        .map(|row| (row.topic, i32::from(row.partition_count)))
        .collect();
    rows.sort_unstable();

    ensure!(
        rows == [(leader, 1_i32), (follower, 1_i32)],
        "{TEST_KEYSPACE}.keyed_state_publication must hold one row per topic with partition count \
         1, got {rows:?}"
    );
    Ok(())
}

/// Deletes `topic`. Call it only after every consumer of `topic` stops.
async fn delete_topic(admin: &ProsodyAdminClient, topic: Topic) -> Result<()> {
    admin.delete_topic(&topic).await.map_err(Into::into)
}
