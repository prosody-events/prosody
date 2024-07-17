use std::cmp::min;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::{thread_rng, Rng};
use tokio::time::sleep;
use tracing::error;
use validator::{Validate, ValidationErrors};

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};
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
    #[validate(range(min = 2_u32))]
    base: u32,

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
    pub fn new(config: RetryConfiguration) -> Result<RetryStrategy, ValidationErrors> {
        config.validate()?;
        Ok(Self(config))
    }
}

#[derive(Clone, Debug)]
struct RetryHandler<T> {
    config: RetryConfiguration,
    handler: T,
}

impl<T> FailureStrategy<T> for RetryStrategy
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    fn with_handler(&self, handler: T) -> impl HandlerProvider
    where
        T: FallibleHandler,
    {
        RetryHandler {
            config: self.0.clone(),
            handler,
        }
    }
}

impl<T> MessageHandler for RetryHandler<T>
where
    T: FallibleHandler + Send + Sync,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            match self.handler.handle(context.clone(), message.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    let exp_backoff = self.config.base.saturating_pow(attempt);
                    let jitter = thread_rng().gen_range(0..exp_backoff);
                    let jitter = Duration::from_millis(u64::from(jitter));
                    let sleep_time = min(jitter, self.config.max_delay);

                    error!(
                        %attempt, "failed to handle message: {error:#}; retrying after {}",
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
