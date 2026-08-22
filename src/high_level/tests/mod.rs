use super::*;
use crate::EventIdentity;
use crate::JsonCodec;
use crate::Key;
use crate::PeerConfiguration;
use crate::cassandra::config::CassandraConfigurationBuilder;
use crate::codec::{BinaryPayload, JsonBinaryCodec, JsonBinaryMessageCodec, UnitCodec};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::{ConsumerConfiguration, DemandType, KeyedStateConfiguration};
use crate::high_level::erased::{
    ErasedConsumerState, ErasedReadCache, ErasedReaderBuildError, new_erased,
};
use crate::high_level::mode::Mode;
use crate::producer::ProducerConfiguration;
use crate::state::descriptor::value_state;
use crate::state::registry::{CollectionDef, RegisterStateError};
use crate::state_reader::ReaderBackend;
use crate::state_reader::tests::support::{
    mock_count, owner_commit, publish_source, registry_of, source_state_key, state_name, topic,
};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use crate::timers::Trigger;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use lifecycle::subsystem;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::future::ready;
use std::slice::from_ref;
use std::time::Duration;
use tokio::time::timeout;

/// Builds a mock-mode pipeline `HighLevelClient<T>`, ready to `register`
/// (Configured) and `subscribe`/`unsubscribe`. Optionally overrides the
/// producer's source system (default: derived from `group_id`).
fn create_test_client<T>(
    group_id: &str,
    source_system: Option<&str>,
) -> Result<MemoryHighLevelClient<T>>
where
    T: ClientHandler,
    T::Payload: EventIdentity + Clone,
{
    let mut producer_builder = ProducerConfiguration::builder();
    producer_builder
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .mock(false);
    if let Some(source) = source_system {
        producer_builder.source_system(source);
    }

    let mut consumer_builder = ConsumerConfiguration::builder();
    consumer_builder
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .mock(false);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        ..ConsumerBuilders::new()?
    };
    Ok(TEST_RUNTIME.block_on(MemoryHighLevelClient::<T>::new(
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
    ))?)
}

fn create_peer_test_client<T>(group_id: &str) -> Result<MemoryHighLevelClient<T>>
where
    T: ClientHandler,
    T::Payload: EventIdentity + Clone,
{
    let mut producer = ProducerConfiguration::builder();
    producer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .source_system("peer-requester");
    let mut consumer = ConsumerConfiguration::builder();
    consumer
        .bootstrap_servers(vec!["unused-in-mock-mode:9092".to_owned()])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .poll_interval(Duration::from_millis(1));
    let builders = ConsumerBuilders {
        consumer,
        keyed_state: KeyedStateConfiguration::builder()
            .subsystem(Some(subsystem("echo")?))
            .build()?,
        ..ConsumerBuilders::new()?
    };
    Ok(TEST_RUNTIME.block_on(MemoryHighLevelClient::<T>::new(
        Mode::Pipeline,
        &mut producer,
        &builders,
    ))?)
}

#[derive(Clone)]
struct EchoHandler;

impl FallibleHandler for EchoHandler {
    type Error = Infallible;
    type Output = Value;
    type Payload = Value;

    fn on_message<C>(
        &self,
        _ctx: C,
        message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Value, Infallible>>
    where
        C: EventContext<Payload = Value>,
    {
        ready(Ok(message.payload().clone()))
    }

    fn on_excise<C>(
        &self,
        _ctx: C,
        _message: ConsumerMessage<()>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Value, Infallible>>
    where
        C: EventContext<Payload = Value>,
    {
        ready(Ok(Value::Null))
    }

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Value, Infallible>>
    where
        C: EventContext<Payload = Value>,
    {
        ready(Ok(Value::Null))
    }

    async fn shutdown(self) {}
}

impl ClientHandler for EchoHandler {
    type Codecs = JsonCodecs;
}

/// A mock client answers through its explicit response subsystem.
#[test]
fn a_mock_client_round_trips_one_peer_request() -> Result<()> {
    init_test_logging();
    let client = create_peer_test_client::<EchoHandler>("peer-round-trip")?;
    TEST_RUNTIME.block_on(async {
        client.subscribe(EchoHandler).await?;
        let outcome: Result<()> = async {
            let state = client.consumer_state().await;
            let ConsumerState::Running { consumer, .. } = &*state else {
                return Err(eyre!("the subscribed client is not running"));
            };
            let assigned = timeout(
                Duration::from_secs(10),
                consumer.wait_for_assigned_partitions(3),
            )
            .await
            .map_err(|_| eyre!("the mock consumer did not receive its partition"))?;
            ensure!(
                assigned == 3,
                "the mock consumer does not own all partitions"
            );
            drop(state);
            let payload = json!({"answer": 42_i32});
            let subsystem = SubsystemName::try_new("echo")?;
            let outcomes = client
                .request(
                    [],
                    Topic::from("test-topic"),
                    "key",
                    payload.clone(),
                    from_ref(&subsystem),
                    Duration::from_secs(1),
                )
                .await?;
            assert_eq!(outcomes.get(&subsystem), Some(&Ok(payload)));
            Ok(())
        }
        .await;
        client.unsubscribe().await?;
        outcome
    })
}

/// Request validation uses the router before the consumer starts.
#[test]
fn a_request_does_not_require_subscription() -> Result<()> {
    let client = create_peer_test_client::<EchoHandler>("peer-before-subscribe")?;
    let subsystems = [];
    let error = TEST_RUNTIME
        .block_on(client.request(
            [],
            Topic::from("test-topic"),
            "key",
            Value::Null,
            &subsystems,
            Duration::from_secs(1),
        ))
        .err()
        .ok_or_else(|| eyre!("the request accepted an empty subsystem list"))?;
    assert!(matches!(error, RequestError::NoSubsystems));
    Ok(())
}

#[test]
fn source_system_uses_the_explicit_value_or_consumer_group() -> Result<()> {
    let group_id = "my-test-group";
    let explicit_source = "my-explicit-source";
    for (configured, expected) in [(None, group_id), (Some(explicit_source), explicit_source)] {
        let client = create_test_client::<NoOpHandler>(group_id, configured)?;
        assert_eq!(client.source_system(), expected);
        assert!(client.producer_config().mock);
    }
    Ok(())
}

/// Minimal no-op handler so the lifecycle tests can drive `subscribe` /
/// `unsubscribe` in mock mode. It never errors, so `Infallible` is its error.
#[derive(Clone)]
struct NoOpHandler;

impl FallibleHandler for NoOpHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = Value;

    fn on_message<C>(
        &self,
        _ctx: C,
        _message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    fn on_timer<C>(
        &self,
        _ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    async fn shutdown(self) {}
}

impl ClientHandler for NoOpHandler {
    type Codecs = Codecs<JsonCodec, UnitCodec>;
}

#[derive(Clone)]
struct BinaryHandler;

impl FallibleHandler for BinaryHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = BinaryPayload;

    fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    fn on_excise<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<()>,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    async fn shutdown(self) {}
}

impl ClientHandler for BinaryHandler {
    type Codecs = Codecs<JsonBinaryMessageCodec, UnitCodec>;
}

/// Erased backend selection reads only mock mode. Invalid consumer-only fields
/// remain deferred, so the producer half constructs and subscription reports
/// the retained configuration error.
mod lifecycle;
