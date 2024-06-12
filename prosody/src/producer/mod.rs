use std::io;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

use confique::Config;
use educe::Educe;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Deserialize;
use serde_json::{to_vec, Value};
use thiserror::Error;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::{Validate, ValidationErrors};
use whoami::fallible::hostname;

use crate::producer::injector::RecordInjector;
use crate::propagator::new_propagator;
use crate::Topic;

mod injector;

#[derive(Clone, Config, Deserialize, Validate)]
pub struct ProducerConfiguration {
    #[config(env = "PROSODY_BOOTSTRAP_SERVERS")]
    #[validate(length(min = 1))]
    pub bootstrap_servers: Vec<String>,

    #[config(env = "PROSODY_SEND_TIMEOUT")]
    #[serde(with = "humantime_serde", default = "default_send_timeout")]
    pub send_timeout: Option<Duration>,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct Producer {
    send_timeout: Timeout,

    #[educe(Debug(ignore))]
    producer: FutureProducer,

    #[educe(Debug(ignore))]
    propagator: TextMapCompositePropagator,
}

impl Clone for Producer {
    fn clone(&self) -> Self {
        Self {
            send_timeout: self.send_timeout,
            producer: self.producer.clone(),
            propagator: new_propagator(),
        }
    }
}

impl Producer {
    pub fn new(config: ProducerConfiguration) -> Result<Self, ProducerError> {
        config.validate()?;

        let send_timeout = match config.send_timeout {
            None => Timeout::Never,
            Some(duration) => Timeout::After(duration),
        };

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers.join(","))
            .set("client.id", hostname()?)
            .set("compression.codec", "lz4")
            .set("enable.idempotence", "true")
            .set_log_level(RDKafkaLogLevel::Error)
            .create()?;

        Ok(Self {
            send_timeout,
            producer,
            propagator: new_propagator(),
        })
    }

    pub async fn send(&self, topic: Topic, key: &str, payload: Value) -> Result<(), ProducerError> {
        let serialized = to_vec(&payload)?;
        let mut record = FutureRecord::to(&topic)
            .key(key)
            .payload(&serialized)
            .timestamp(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);

        self.propagator.inject_context(
            &Span::current().context(),
            &mut RecordInjector::new(&mut record),
        );

        self.producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(error, _)| ProducerError::Kafka(error))?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("invalid producer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    #[error("failed to serialize payload: {0:#}")]
    Serialization(#[from] serde_json::Error),

    #[error("failed to set timestamp: {0:#}")]
    SystemTime(#[from] SystemTimeError),

    #[error("failed to get hostname: {0:#}")]
    Hostname(#[from] io::Error),

    #[error("Kafka error: {0:#}")]
    Kafka(#[from] KafkaError),
}

fn default_send_timeout() -> Option<Duration> {
    Some(Duration::from_secs(1))
}
