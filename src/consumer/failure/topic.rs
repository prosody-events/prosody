use chrono::SecondsFormat;
use derive_builder::Builder;
use tracing::error;
use validator::{Validate, ValidationErrors};

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};
use crate::producer::{ProducerError, ProsodyProducer};
use crate::util::from_env;
use crate::Topic;

#[derive(Builder, Clone, Debug, Validate)]
pub struct FailureTopicConfiguration {
    /// Failure topic.
    ///
    /// Environment variable: `PROSODY_FAILURE_TOPIC`
    /// Default: None (must be specified)
    ///
    /// The topic to send messages which have failed to be processed.
    #[builder(default = "from_env(\"PROSODY_FAILURE_TOPIC\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    failure_topic: String,
}

#[derive(Clone, Debug)]
pub struct FailureTopicStrategy {
    config: FailureTopicConfiguration,
    producer: ProsodyProducer,
    group_id: String,
}

impl FailureTopicStrategy {
    pub fn new(
        config: FailureTopicConfiguration,
        group_id: String,
        producer: ProsodyProducer,
    ) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self {
            config,
            producer,
            group_id,
        })
    }
}

#[derive(Clone, Debug)]
struct FailureTopicHandler<T> {
    topic: Topic,
    producer: ProsodyProducer,
    group_id: String,
    handler: T,
}

impl FailureStrategy for FailureTopicStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        FailureTopicHandler {
            topic: self.config.failure_topic.as_str().into(),
            producer: self.producer.clone(),
            group_id: self.group_id.clone(),
            handler,
        }
    }
}

impl<T> FallibleHandler for FailureTopicHandler<T>
where
    T: FallibleHandler,
{
    type Error = ProducerError;

    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let key = message.key.clone();
        let offset = message.offset;

        let timestamp = message
            .timestamp
            .to_rfc3339_opts(SecondsFormat::AutoSi, true);

        let Err(error) = self.handler.handle(context, message.clone()).await else {
            return Ok(());
        };

        error!(
            %topic, %partition, %key, %offset,
            "failed to process message: {error:#}; sending to {}",
            self.topic
        );

        let headers = [
            ("source-topic", topic.as_ref()),
            ("source-partition", &partition.to_string()),
            ("source-offset", &offset.to_string()),
            ("source-timestamp", &timestamp),
            ("source-group-id", &self.group_id),
        ];

        self.producer
            .send(headers, self.topic, &key, message.payload)
            .await
    }
}

impl<T> MessageHandler for FailureTopicHandler<T>
where
    T: FallibleHandler,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();
        let Err(error) = FallibleHandler::handle(self, context, message).await else {
            uncommitted_offset.commit();
            return;
        };

        error!("failed to send message to failure topic: {error:#}; discarding message");
        uncommitted_offset.commit();
    }

    async fn shutdown(self) {}
}
