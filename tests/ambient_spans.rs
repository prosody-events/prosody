//! Pins that dispatch runs every user handler inside its event span.
//!
//! The partition loop instruments `on_message` with the message's `receive`
//! span and `on_timer` with the trigger's dispatch span, so `Span::current()`
//! inside a handler is the event span — bare `info_span!`s nest and
//! `EventContext::schedule` captures a real scheduling context without any
//! explicit parenting. These tests drive a real consumer (live Kafka +
//! Cassandra) and assert that identity by span id from inside the handlers.
//!
//! A plain `Registry` is installed as the global subscriber (this test binary
//! never calls `init_test_logging`, whose ERROR filter would disable spans
//! and make the id comparison vacuous); the `is_some` guards fail loudly if
//! spans are ever disabled.

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, eyre};
use prosody::Topic;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::codec::JsonCodec;
use prosody::high_level::CassandraHighLevelClient;
use prosody::prelude::*;
use prosody::timers::duration::CompactDuration;
use serde_json::json;
use std::convert::Infallible;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, timeout};
use tracing::Span;
use tracing::subscriber::set_global_default;
use uuid::Uuid;

mod common;

/// Reports whether the handler's ambient span is exactly the event's span.
#[derive(Clone)]
struct AmbientProbe {
    sender: Sender<(String, bool)>,
    fire_at: CompactDateTime,
}

fn ambient_is(event_span: &Span) -> bool {
    let ambient = Span::current();
    ambient.id().is_some() && ambient.id() == event_span.id()
}

impl FallibleHandler for AmbientProbe {
    type Error = Infallible;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<(), Infallible>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let ok = ambient_is(&message.span());
        let scheduled = context.schedule(self.fire_at, TimerType::Application).await;
        let _ = self.sender.send(("message".to_owned(), ok)).await;
        let _ = self
            .sender
            .send(("schedule".to_owned(), scheduled.is_ok()))
            .await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Infallible>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let ok = ambient_is(&trigger.span());
        let _ = self.sender.send(("timer".to_owned(), ok)).await;
        Ok(())
    }

    async fn shutdown(self) {}
}

impl ClientHandler for AmbientProbe {
    type Codecs = Codecs<JsonCodec, JsonCodec, UnitCodec>;
}

#[tokio::test]
async fn handlers_run_inside_their_event_spans() -> Result<()> {
    set_global_default(tracing_subscriber::registry())?;

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap_servers = vec!["localhost:9094".to_owned()];

    let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers.clone())?)?;
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let mut consumer_config = ConsumerConfiguration::builder();
    consumer_config
        .bootstrap_servers(bootstrap_servers.clone())
        .group_id(Uuid::new_v4().to_string())
        .probe_port(None)
        .subscribed_topics([topic.to_string()]);

    let mut producer_config = ProducerConfiguration::builder();
    producer_config
        .bootstrap_servers(bootstrap_servers)
        .source_system("ambient-spans-test");

    let mut cassandra_config = CassandraConfigurationBuilder::default();
    cassandra_config.nodes(vec!["localhost:9042".to_owned()]);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_config,
        peer: common::test_peer_config()?,
        ..ConsumerBuilders::new()?
    };

    let (sender, mut receiver) = channel(4);
    let client = CassandraHighLevelClient::<AmbientProbe>::new(
        cassandra_config.build()?,
        Mode::Pipeline,
        &mut producer_config,
        &consumer_builders,
    )
    .await?;

    let fire_at = CompactDateTime::now()?.add_duration(CompactDuration::new(2))?;
    client.subscribe(AmbientProbe { sender, fire_at }).await?;
    client.send(topic, "ambient-key", json!({})).await?;

    // Collect all reports before asserting: the consumer must be shut down
    // before a failure propagates, or lingering rdkafka threads hang the test.
    let mut reports = Vec::with_capacity(3);
    let outcome: Result<()> = async {
        while reports.len() < 3 {
            // Hang-guard only; correctness is the per-site assert below.
            let report = timeout(Duration::from_mins(1), receiver.recv())
                .await
                .map_err(|_| eyre!("timed out waiting for reports ({} of 3)", reports.len()))?
                .ok_or_else(|| eyre!("probe channel closed early"))?;
            reports.push(report);
        }
        Ok(())
    }
    .await;

    client.unsubscribe().await?;
    admin.delete_topic(&topic).await?;
    outcome?;

    for (site, ok) in reports {
        assert!(ok, "{site} must succeed inside its event span");
    }
    Ok(())
}
