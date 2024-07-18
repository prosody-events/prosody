use std::cmp::min;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::{thread_rng, Rng};
use tokio::time::sleep;
use tracing::error;
use validator::{Validate, ValidationErrors};

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, Keyed, MessageHandler};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};

#[derive(Builder, Clone, Debug, Validate)]
pub struct RetryConfiguration {
    /// Exponential backoff base.
    ///
    /// Environment variable: `PROSODY_RETRY_BASE`
    /// Default: 2
    ///
    /// Must be at least 2.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_RETRY_BASE\", 2)?",
        setter(into)
    )]
    #[validate(range(min = 2_u8))]
    base: u8,

    /// Maximum number of retries.
    ///
    /// Environment variable: `PROSODY_MAX_RETRIES`
    /// Default: 3
    ///
    /// When composed with other retry strategies, this represents the maximum
    /// number of retries before falling back to the next retry strategy.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_RETRIES\", 10)?",
        setter(into)
    )]
    max_retries: u32,

    /// Maximum retry delay.
    ///
    /// Environment variable: `PROSODY_RETRY_MAX_DELAY`
    /// Default: 1 minute
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_MAX_DELAY\", \
                   Duration::from_secs(60))?",
        setter(into)
    )]
    max_delay: Duration,
}

#[derive(Clone, Debug)]
pub struct RetryStrategy(RetryConfiguration);

impl RetryStrategy {
    pub fn new(config: RetryConfiguration) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self(config))
    }
}

#[derive(Clone, Debug)]
struct RetryHandler<T> {
    config: RetryConfiguration,
    handler: T,
}

impl<T> RetryHandler<T> {
    fn sleep_time(&self, attempt: u32) -> Duration {
        let exp_backoff = self.config.base.saturating_pow(attempt);
        let jitter = thread_rng().gen_range(0..exp_backoff);
        let jitter = Duration::from_millis(u64::from(jitter));
        min(jitter, self.config.max_delay)
    }
}

impl FailureStrategy for RetryStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        RetryHandler {
            config: self.0.clone(),
            handler,
        }
    }
}

impl<T> FallibleHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let key = message.key.clone();
        let offset = message.offset;
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            match self.handler.handle(context.clone(), message.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if attempt > self.config.max_retries {
                        error!(
                            %topic, %partition, %key, %offset, %attempt,
                            "failed to handle message: {error:#}; maximum attempts reached"
                        );
                        return Err(error);
                    }

                    let sleep_time = self.sleep_time(attempt);

                    error!(
                        %topic, %partition, %key, %offset, %attempt,
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    sleep(sleep_time).await;
                }
            }
        }
    }
}

impl<T> MessageHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            match self.handler.handle(context.clone(), message.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    let sleep_time = self.sleep_time(attempt);

                    error!(
                        %topic, %partition, %key, %offset, %attempt,
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    sleep(sleep_time).await;
                }
            }
        }

        uncommitted_offset.commit();
    }

    async fn shutdown(self) {}
}
