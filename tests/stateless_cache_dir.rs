//! A stateless consumer (`ProsodyConsumer::new`, no middleware, no keyed-state
//! registrations) over a Cassandra trigger store must not spin up the
//! keyed-state machinery: the `settle` boundary never runs and the registry is
//! provably empty, so the per-consumer Kafka loader and the fjall workspace
//! would be pure overhead. This pins the observable proxy — the fjall cache
//! directory is never created.

use color_eyre::eyre::{Result, ensure};
use prosody::JsonCodec;
use prosody::consumer::middleware::CloneProvider;
use prosody::consumer::{ConsumerConfiguration, KeyedStateConfiguration, ProsodyConsumer};
use prosody::telemetry::Telemetry;
use prosody::tracing::init_test_logging;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

mod common;

use common::create_cassandra_trigger_store_config;
use common::handler::ChannelHandler;

#[tokio::test]
async fn stateless_cassandra_consumer_does_not_create_the_fjall_cache_dir() -> Result<()> {
    init_test_logging();

    // A cache-dir path under a fresh tempdir root — the subdir does not exist
    // yet, so its presence afterwards is exactly "did the consumer create it?".
    let tmp = tempfile::tempdir()?;
    let cache_dir = tmp.path().join("fjall-workspace");
    assert!(!cache_dir.exists(), "precondition: cache dir absent");

    let consumer_config = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .group_id(format!("stateless-cache-dir-{}", Uuid::new_v4()))
        .probe_port(None)
        .subscribed_topics(&[Uuid::new_v4().to_string()])
        .build()?;

    let keyed_state = KeyedStateConfiguration::builder()
        .cache_dir(cache_dir.clone())
        .build()?;

    let (messages_tx, _messages_rx) = channel(1);
    let consumer: ProsodyConsumer<JsonCodec> = ProsodyConsumer::new(
        &consumer_config,
        &create_cassandra_trigger_store_config(),
        keyed_state,
        CloneProvider::new(ChannelHandler::new(messages_tx)),
        Telemetry::new(),
    )
    .await?;

    // Capture the observable before shutting down so rdkafka threads always get
    // torn down even if the assertion below fails (a dropped-but-not-shutdown
    // consumer hangs the test binary).
    let created = cache_dir.exists();
    consumer.shutdown().await;

    ensure!(
        !created,
        "a stateless Cassandra consumer must not create the fjall cache dir"
    );
    Ok(())
}
