//! The consumer layers the benchmark measures, from the bare Kafka client up
//! to the full high-level client.
//!
//! Each layer consumes the same loaded topic with a fresh consumer group. The
//! gap between two adjacent layers is the cost of what the upper one adds.

use crate::progress::{Measurement, Progress};
use crate::settings::Settings;
use crate::workload::Workload;
use color_eyre::eyre::{Result, bail};
use prosody::JsonCodec;
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::middleware::CloneProvider;
use prosody::consumer::middleware::scheduler::SchedulerConfiguration;
use prosody::consumer::{
    ConsumerConfiguration, ConsumerConfigurationBuilder, KeyedStateConfiguration, ProsodyConsumer,
};
use prosody::high_level::config::TriggerStoreConfiguration;
use prosody::high_level::{CassandraHighLevelClient, ConsumerBuilders, Mode};
use prosody::peer::PeerConfiguration;
use prosody::producer::ProducerConfiguration;
use prosody::telemetry::Telemetry;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use strum::{Display, EnumString};
use tokio::task::spawn_blocking;
use uuid::Uuid;

mod handlers;

use handlers::{BenchHandler, DirectHandler};

/// One way to consume the topic.
#[derive(Clone, Copy, Debug, Display, EnumString)]
#[strum(serialize_all = "kebab-case")]
pub(crate) enum Layer {
    /// A bare `rdkafka` poll loop. No decode, no dispatch: the ceiling.
    Rdkafka,
    /// `ProsodyConsumer` with an `EventHandler` and no middleware: poll,
    /// decode, partition and key dispatch, and offset tracking. Its handler
    /// concurrency has the scheduler's limit.
    Direct,
    /// The high-level client in best-effort mode: adds the common middleware
    /// block, deduplication, and the settle boundary.
    BestEffort,
    /// The high-level client in pipeline mode: adds retry, defer, and
    /// monopolization detection.
    Pipeline,
}

impl Layer {
    /// Every layer, from the bottom up.
    pub(crate) const ALL: [Self; 4] = [
        Self::Rdkafka,
        Self::Direct,
        Self::BestEffort,
        Self::Pipeline,
    ];

    /// Consumes the whole workload and measures the drain.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer fails to start or does not finish
    /// before the hang-guard.
    pub(crate) async fn run(self, settings: &Settings, workload: &Workload) -> Result<Measurement> {
        let group = format!("prosody-bench-{self}-{}", Uuid::new_v4());
        match self {
            Self::Rdkafka => rdkafka(settings, workload, group).await,
            Self::Direct => Box::pin(direct(settings, workload, group)).await,
            Self::BestEffort => {
                Box::pin(high_level(settings, workload, group, Mode::BestEffort)).await
            }
            Self::Pipeline => Box::pin(high_level(settings, workload, group, Mode::Pipeline)).await,
        }
    }
}

/// Polls with a bare `BaseConsumer` on a blocking thread until every message
/// arrives.
async fn rdkafka(settings: &Settings, workload: &Workload, group: String) -> Result<Measurement> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", settings.bootstrap.join(","))
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()?;
    consumer.subscribe(&[workload.topic.as_ref()])?;

    let progress = Progress::new(workload.messages);
    let deadline = settings.timeout;
    let started = Instant::now();
    spawn_blocking(move || {
        while !progress.is_done() {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(message)) => {
                    // Read the payload so that the poll cannot skip it.
                    let _ = message.payload_len();
                    progress.record();
                }
                Some(Err(error)) => return Err(error.into()),
                None if started.elapsed() > deadline => bail!("rdkafka layer timed out"),
                None => {}
            }
        }
        Ok(progress.measure(started))
    })
    .await?
}

/// Runs `ProsodyConsumer::new` with a handler that counts and commits.
async fn direct(settings: &Settings, workload: &Workload, group: String) -> Result<Measurement> {
    let config = consumer_config(settings, workload, group).build()?;
    let progress = Arc::new(Progress::new(workload.messages));
    let max_concurrency = SchedulerConfiguration::builder().build()?.max_concurrency;
    let handler = DirectHandler::new(progress.clone(), settings.handler_delay, max_concurrency);

    let started = Instant::now();
    let consumer = ProsodyConsumer::<JsonCodec>::new(
        &config,
        &TriggerStoreConfiguration::InMemory,
        KeyedStateConfiguration::builder().build()?,
        CloneProvider::new(handler),
        Telemetry::new(),
    )
    .await?;

    let result = progress.finished(started, settings.timeout).await;
    consumer.shutdown().await;
    result
}

/// Runs the Cassandra-backed high-level client in `mode`.
async fn high_level(
    settings: &Settings,
    workload: &Workload,
    group: String,
    mode: Mode,
) -> Result<Measurement> {
    let consumer_builders = ConsumerBuilders {
        consumer: consumer_config(settings, workload, group),
        peer: PeerConfiguration::builder()
            .bind_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .build()?,
        ..ConsumerBuilders::new()?
    };

    let mut producer = ProducerConfiguration::builder();
    producer
        .bootstrap_servers(settings.bootstrap.clone())
        .source_system("prosody-bench");

    let mut cassandra = CassandraConfigurationBuilder::default();
    cassandra.nodes(settings.cassandra.clone());

    let progress = Arc::new(Progress::new(workload.messages));
    let handler = BenchHandler::new(progress.clone(), settings.handler_delay);

    let started = Instant::now();
    let client = Box::pin(CassandraHighLevelClient::<BenchHandler>::new(
        cassandra.build()?,
        mode,
        &mut producer,
        &consumer_builders,
    ))
    .await?;
    client.subscribe(handler).await?;

    let result = progress.finished(started, settings.timeout).await;
    client.shutdown().await?;
    result
}

/// The consumer settings both prosody layers share. Every other field keeps
/// its `PROSODY_*` environment value or default.
fn consumer_config(
    settings: &Settings,
    workload: &Workload,
    group: String,
) -> ConsumerConfigurationBuilder {
    let mut builder = ConsumerConfiguration::builder();
    builder
        .bootstrap_servers(settings.bootstrap.clone())
        .group_id(group)
        .subscribed_topics([workload.topic.to_string()])
        .probe_port(None);
    builder
}
