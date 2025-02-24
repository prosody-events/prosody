//! Retry strategy for failure handling in message processing.
//!
//! This module provides a `RetryStrategy` that wraps handlers and retries
//! failed message processing attempts with an exponential backoff.

use std::cmp::min;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::{Rng, rng};
use tokio::select;
use tokio::time::sleep;
use tracing::{error, info};
use validator::{Validate, ValidationErrors};

use crate::consumer::failure::{ClassifyError, ErrorCategory, FailureStrategy, FallibleHandler};
use crate::consumer::message::{ConsumerMessage, MessageContext, UncommittedMessage};
use crate::consumer::{EventHandler, HandlerProvider, Keyed};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};

/// Configuration for the retry strategy.
#[derive(Builder, Clone, Debug, Validate)]
pub struct RetryConfiguration {
    /// Base exponential backoff delay.
    ///
    /// Environment variable: `PROSODY_RETRY_BASE`
    /// Default: 20 ms
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_BASE\", \
                   Duration::from_millis(20))?",
        setter(into)
    )]
    base: Duration,

    /// Maximum number of retries.
    ///
    /// Environment variable: `PROSODY_MAX_RETRIES`
    /// Default: 3
    ///
    /// When composed with other retry strategies, this represents the maximum
    /// number of retries before falling back to the next retry strategy.
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_MAX_RETRIES\", 3)?",
        setter(into)
    )]
    max_retries: u32,

    /// Maximum retry delay.
    ///
    /// Environment variable: `PROSODY_RETRY_MAX_DELAY`
    /// Default: 5 minutes
    #[builder(
        default = "from_duration_env_with_fallback(\"PROSODY_RETRY_MAX_DELAY\", \
                   Duration::from_secs(5 * 60))?",
        setter(into)
    )]
    max_delay: Duration,
}

impl RetryConfiguration {
    /// Creates a new `RetryConfigurationBuilder`.
    ///
    /// # Returns
    ///
    /// A `RetryConfigurationBuilder` instance.
    #[must_use]
    pub fn builder() -> RetryConfigurationBuilder {
        RetryConfigurationBuilder::default()
    }
}

/// A strategy that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryStrategy(RetryConfiguration);

impl RetryStrategy {
    /// Creates a new `RetryStrategy` with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the retry strategy.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `RetryStrategy` if the configuration is
    /// valid, or `ValidationErrors` if the configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if any validation defined in the
    /// `RetryConfiguration` struct fails.
    pub fn new(config: RetryConfiguration) -> Result<Self, ValidationErrors> {
        config.validate()?;
        Ok(Self(config))
    }
}

/// A handler wrapped with retry functionality.
#[derive(Clone, Debug)]
struct RetryHandler<T> {
    base_delay_millis: u64,
    max_delay_millis: u64,
    max_retries: u32,
    handler: T,
}

impl<T> RetryHandler<T> {
    /// Calculates the sleep time for a given retry attempt.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The current retry attempt number.
    ///
    /// # Returns
    ///
    /// The duration to sleep before the next retry attempt.
    fn sleep_time(&self, attempt: u32) -> Duration {
        let exp_backoff = min(
            2u64.saturating_pow(attempt)
                .saturating_mul(self.base_delay_millis),
            self.max_delay_millis,
        );

        let jitter = rng().random_range(0..exp_backoff);
        Duration::from_millis(jitter)
    }
}

impl FailureStrategy for RetryStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler,
    {
        RetryHandler {
            base_delay_millis: self.0.base.as_millis() as u64,
            max_delay_millis: self.0.max_delay.as_millis() as u64,
            max_retries: self.0.max_retries,
            handler,
        }
    }
}

impl<T> FallibleHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    type Error = T::Error;

    /// Handles a message with retry functionality.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The consumer message to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of message processing.
    ///
    /// # Errors
    ///
    /// Returns the underlying handler's error if all retry attempts fail.
    async fn on_message(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone())
                .await
            else {
                return Ok(());
            };

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    if attempt > self.max_retries {
                        // Log the final failure and return the error
                        error!(
                            partition,
                            key = key.as_str(),
                            offset,
                            attempt,
                            topic = topic.as_ref(),
                            "failed to handle message: {error:#}; maximum attempts reached"
                        );
                        return Err(error);
                    }

                    let sleep_time = self.sleep_time(attempt);

                    // Log the failure and retry information
                    error!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    select! {
                        () = sleep(sleep_time) => {}
                        () = context.on_shutdown() => {
                            return Err(error);
                        }
                    }
                }
                ErrorCategory::Permanent => {
                    error!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "permanently failed to handle message: {error:#}"
                    );
                    return Err(error);
                }
                ErrorCategory::Terminal => {
                    info!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "terminal condition encountered while handling message: {error:#}; \
                         aborting"
                    );
                    return Err(error);
                }
            }
        }
    }
}

impl<T> EventHandler for RetryHandler<T>
where
    T: FallibleHandler,
{
    /// Handles a message with retry functionality and commits the offset upon
    /// success.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The uncommitted message to be processed.
    async fn on_message(&self, context: MessageContext, message: UncommittedMessage) {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone())
                .await
            else {
                uncommitted_offset.commit();
                break;
            };

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    select! {
                        () = sleep(sleep_time) => {}
                        () = context.on_shutdown() => {
                            uncommitted_offset.abort();
                            break;
                        }
                    }
                }
                ErrorCategory::Permanent => {
                    error!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "permanently failed to handle message: {error:#}; discarding message"
                    );
                    uncommitted_offset.commit();
                    break;
                }
                ErrorCategory::Terminal => {
                    info!(
                        partition,
                        key = key.as_str(),
                        offset,
                        attempt,
                        topic = topic.as_ref(),
                        "terminal condition encountered while handling message: {error:#}; \
                         aborting"
                    );
                    uncommitted_offset.abort();
                    break;
                }
            }
        }
    }

    /// Performs any necessary shutdown operations for the handler.
    async fn shutdown(self) {}
}
