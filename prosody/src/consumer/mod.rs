use std::error::Error;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::HashMap;
use confique::Config;
use crossbeam_utils::CachePadded;
use educe::Educe;
use parking_lot::Mutex;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use serde::Deserialize;
use thiserror::Error;
use tokio::task::{JoinHandle, spawn_blocking};
use tracing::error;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::{Partition, Topic};
use crate::consumer::context::Context;
use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::partition::PartitionManager;
use crate::consumer::poll::poll;

mod context;
mod extractor;
pub mod message;
mod partition;
mod poll;

type WatermarkVersion = CachePadded<AtomicUsize>;
type Managers = Mutex<HashMap<(Topic, Partition), PartitionManager>>;

pub trait Keyed {
    type Key;

    fn key(&self) -> &Self::Key;
}

pub trait MessageHandler {
    type Error: Error;

    fn handle(
        &self,
        context: &mut MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Config, Deserialize, Validate)]
pub struct ConsumerConfiguration {
    #[config(env = "PROSODY_BOOTSTRAP_SERVERS")]
    #[validate(length(min = 1_u64))]
    pub bootstrap_servers: Vec<String>,

    #[config(env = "PROSODY_GROUP_ID")]
    #[validate(length(min = 1_u64))]
    pub group_id: String,

    #[config(env = "PROSODY_SUBSCRIBED_TOPICS")]
    #[validate(length(min = 1_u64))]
    pub subscribed_topics: Vec<String>,

    #[config(env = "PROSODY_MAX_UNCOMMITTED", default = 32)]
    #[validate(range(min = 1_usize))]
    pub max_uncommitted: usize,

    #[config(env = "PROSODY_MAX_ENQUEUED_PER_KEY", default = 8)]
    #[validate(range(min = 1_usize))]
    pub max_enqueued_per_key: usize,

    #[config(env = "PROSODY_PARTITION_SHUTDOWN_TIMEOUT")]
    #[serde(with = "humantime_serde", default = "default_partition_timeout")]
    pub partition_shutdown_timeout: Option<Duration>,

    #[config(env = "PROSODY_POLL_INTERVAL", default = "100ms")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

    #[config(env = "PROSODY_COMMIT_INTERVAL", default = "1s")]
    #[serde(with = "humantime_serde")]
    pub commit_interval: Duration,
}

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct KafkaConsumer {
    #[educe(Debug(ignore))]
    shutdown: Arc<AtomicBool>,

    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl KafkaConsumer {
    pub fn new<T>(config: ConsumerConfiguration, message_handler: T) -> Result<Self, ConsumerError>
    where
        T: MessageHandler + Clone + Send + Sync + 'static,
    {
        config.validate()?;

        let watermark_version: Arc<WatermarkVersion> = Arc::default();
        let managers: Arc<Managers> = Arc::default();
        let shutdown: Arc<AtomicBool> = Arc::default();

        let context = Context::new(
            &config,
            message_handler,
            watermark_version.clone(),
            managers.clone(),
        );

        let consumer: BaseConsumer<_> = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("group.id", config.group_id)
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set_log_level(RDKafkaLogLevel::Error)
            .create_with_context(context)?;

        let topics: Vec<&str> = config
            .subscribed_topics
            .iter()
            .map(String::as_str)
            .collect();

        consumer.subscribe(&topics)?;

        let cloned_shutdown = shutdown.clone();
        let handle = Arc::new(Mutex::new(Some(spawn_blocking(move || {
            poll(
                config.poll_interval,
                config.commit_interval,
                &consumer,
                &watermark_version,
                &managers,
                &cloned_shutdown,
            );
        }))));

        Ok(Self { shutdown, handle })
    }

    pub async fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let Some(handle) = self.handle.lock().take() else {
            return;
        };

        if let Err(error) = handle.await {
            error!("consumer shutdown failed: {error:#}");
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("invalid consumer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    #[error("failed to retrieve hostname: {0:#}")]
    Hostname(#[from] io::Error),

    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),
}

#[allow(clippy::unnecessary_wraps)]
fn default_partition_timeout() -> Option<Duration> {
    Some(Duration::from_secs(5))
}
