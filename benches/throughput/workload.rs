//! The benchmark topic: create it, load it with messages of one size, and
//! delete it.

use crate::settings::Settings;
use color_eyre::eyre::Result;
use futures::{StreamExt, TryStreamExt, stream};
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use prosody::telemetry::Telemetry;
use prosody::{JsonCodec, Topic};
use serde_json::{Map, Value, json};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Sends in flight while the topic loads. The producer batches them.
const LOAD_CONCURRENCY: usize = 4_096;

/// The serialized size of one generated field: `"f00000":"<32 hex>",`.
const FIELD_BYTES: usize = 45;

/// The serialized size of the envelope around the fields.
const ENVELOPE_BYTES: usize = 80;

/// A loaded topic that every layer consumes from the start.
///
/// Each layer uses a fresh consumer group, so each one reads the whole topic.
/// Deduplication ids include the group, so no layer skips a message that an
/// earlier layer handled.
pub(crate) struct Workload {
    pub(crate) topic: Topic,
    pub(crate) messages: u32,
    /// The serialized size of each payload in bytes. All payloads of a
    /// workload have the same size.
    pub(crate) payload_bytes: f64,
    admin: &'static ProsodyAdminClient,
}

impl Workload {
    /// Creates a topic and loads it with messages of about `size` bytes.
    ///
    /// Keys cycle through `settings.keys` distinct values. Each payload has a
    /// unique `id`, so deduplication treats every message as new.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic cannot be created or a send fails.
    pub(crate) async fn load(settings: &Settings, size: usize) -> Result<(Self, Duration)> {
        let fields = size.saturating_sub(ENVELOPE_BYTES) / FIELD_BYTES;
        let payload_bytes = payload_bytes(fields)?;
        let topic: Topic = format!("prosody-bench-{}", Uuid::new_v4()).as_str().into();
        let admin =
            ProsodyAdminClient::cached(&AdminConfiguration::new(settings.bootstrap.clone())?)?;
        admin
            .create_topic(
                &TopicConfiguration::builder()
                    .name(topic.to_string())
                    .partition_count(settings.partitions)
                    .replication_factor(1_u16)
                    .build()?,
            )
            .await?;

        let workload = Self {
            topic,
            messages: settings.messages_for(size),
            payload_bytes,
            admin,
        };

        let started = Instant::now();
        match workload.produce(settings, fields).await {
            Ok(()) => Ok((workload, started.elapsed())),
            Err(error) => {
                workload.delete().await;
                Err(error)
            }
        }
    }

    /// Deletes the topic. A failure is logged, not returned: the run's result
    /// matters more than the cleanup.
    pub(crate) async fn delete(self) {
        if let Err(error) = self.admin.delete_topic(&self.topic).await {
            tracing::error!("failed to delete topic {}: {error:#}", self.topic);
        }
    }

    async fn produce(&self, settings: &Settings, fields: usize) -> Result<()> {
        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(settings.bootstrap.clone())
                .source_system("prosody-bench")
                .build()?,
            Telemetry::new().sender(),
        )?;

        let keys = settings.keys.max(1);

        stream::iter(0..self.messages)
            .map(|index| {
                let producer = &producer;
                let payload = payload(fields);
                async move {
                    let key = format!("key-{}", index % keys);
                    producer.send([], self.topic, &key, payload).await?;
                    Ok::<_, color_eyre::Report>(())
                }
            })
            .buffer_unordered(LOAD_CONCURRENCY)
            .try_collect()
            .await
    }
}

/// A JSON object with a unique `id` and `fields` random string fields.
///
/// Random hex values keep LZ4 from compressing the payload to almost nothing,
/// and many short fields make the decode cost close to a real event.
fn payload(fields: usize) -> Value {
    let data: Map<String, Value> = (0..fields)
        .map(|index| {
            (
                format!("f{index:05}"),
                Value::String(Uuid::new_v4().simple().to_string()),
            )
        })
        .collect();

    json!({
        "id": Uuid::new_v4().to_string(),
        "type": "bench",
        "data": data,
    })
}

/// The serialized size of a payload with `fields` fields. The size does not
/// depend on the random values, so one sample gives the size of every payload.
fn payload_bytes(fields: usize) -> Result<f64> {
    let bytes = serde_json::to_vec(&payload(fields))?.len();
    Ok(f64::from(u32::try_from(bytes)?))
}
