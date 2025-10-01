//! Exponential backoff retry middleware.
//!
//! Automatically retries transient failures using exponential backoff with
//! jitter. Only retries [`ErrorCategory::Transient`] errors - permanent and
//! terminal errors are passed through immediately.
//!
//! # Execution Order
//!
//! **Request Path:**
//! 1. Pass control to inner middleware layers
//!
//! **Response Path:**
//! 1. Receive result from inner layers
//! 2. **If error is transient**: Sleep with exponential backoff and retry
//! 3. **If error is permanent/terminal**: Pass through immediately
//! 4. **If max retries exceeded**: Pass through final error
//!
//! # Retry Logic
//!
//! - **Initial delay**: Starts with configured base delay
//! - **Exponential growth**: Each retry doubles the delay (with jitter)
//! - **Maximum delay**: Capped at configured maximum
//! - **Jitter**: Adds randomness to prevent thundering herd
//! - **Cancellation**: Respects shutdown signals during retry delays
//!
//! # Usage
//!
//! Often used multiple times in a pipeline for different failure points:
//!
//! ```rust,no_run
//! # use prosody::consumer::middleware::*;
//! # use prosody::consumer::middleware::concurrency::*;
//! # use prosody::consumer::middleware::retry::*;
//! # use prosody::consumer::middleware::shutdown::*;
//! # use prosody::consumer::middleware::topic::*;
//! # use prosody::consumer::DemandType;
//! # use prosody::consumer::event_context::EventContext;
//! # use prosody::consumer::message::ConsumerMessage;
//! # use prosody::producer::{ProducerConfiguration, ProsodyProducer};
//! # use prosody::timers::Trigger;
//! # use std::convert::Infallible;
//! # #[derive(Clone)]
//! # struct MyHandler;
//! # impl FallibleHandler for MyHandler {
//! #     type Error = Infallible;
//! #     async fn on_message<C>(&self, _: C, _: ConsumerMessage, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn on_timer<C>(&self, _: C, _: Trigger, _: DemandType) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn shutdown(self) {}
//! # }
//! # let config = ConcurrencyLimitConfigurationBuilder::default().build().unwrap();
//! # let retry_config = RetryConfiguration::builder().build().unwrap();
//! # let topic_config = FailureTopicConfiguration::builder().build().unwrap();
//! # let producer_config = ProducerConfiguration::builder().bootstrap_servers(vec!["kafka:9092".to_string()]).build().unwrap();
//! # let producer = ProsodyProducer::new(&producer_config).unwrap();
//! # let handler = MyHandler;
//!
//! let provider = ConcurrencyLimitMiddleware::new(&config).unwrap()
//!     .layer(ShutdownMiddleware)
//!     .layer(RetryMiddleware::new(retry_config.clone()).unwrap()) // Retry handler failures
//!     .layer(FailureTopicMiddleware::new(topic_config, "consumer-group".to_string(), producer).unwrap())
//!     .layer(RetryMiddleware::new(retry_config).unwrap()) // Retry DLQ writes
//!     .into_provider(handler);
//! ```
//!
//! [`ErrorCategory::Transient`]: crate::consumer::middleware::ErrorCategory::Transient

use std::cmp::min;
use std::time::Duration;

use derive_builder::Builder;
use humantime::format_duration;
use rand::Rng;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, info};
use validator::{Validate, ValidationErrors};

use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, EventHandler, HandlerProvider, Keyed, Uncommitted};
use crate::timers::{Trigger, UncommittedTimer};
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback};
use crate::{Partition, Topic};

/// Configuration for retry middleware.
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
    /// number of retries before falling back to the next middleware.
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

/// Middleware that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryMiddleware(RetryConfiguration);

impl RetryMiddleware {
    /// Creates a new `RetryMiddleware` with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the retry middleware.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `RetryMiddleware` if the configuration is
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

/// A provider that retries failed message processing attempts.
#[derive(Clone, Debug)]
pub struct RetryProvider<T> {
    provider: T,
    config: RetryConfiguration,
}

/// A handler wrapped with retry functionality.
#[derive(Clone, Debug)]
pub struct RetryHandler<T> {
    base_delay_millis: u64,
    max_delay_millis: u64,
    max_retries: u32,
    handler: T,
}

impl<T> RetryProvider<T> {
    /// Creates a retry handler for the given topic and partition.
    fn create_handler<H>(&self, handler: H) -> RetryHandler<H> {
        RetryHandler {
            base_delay_millis: self.config.base.as_millis() as u64,
            max_delay_millis: self.config.max_delay.as_millis() as u64,
            max_retries: self.config.max_retries,
            handler,
        }
    }
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

        let jitter = rand::rng().random_range(0..exp_backoff);
        Duration::from_millis(jitter)
    }
}

impl HandlerMiddleware for RetryMiddleware {
    type Provider<T: FallibleHandlerProvider> = RetryProvider<T>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        RetryProvider {
            provider,
            config: self.0.clone(),
        }
    }
}

impl<T> FallibleHandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
    }
}

impl<T> HandlerProvider for RetryProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = RetryHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        self.create_handler(self.provider.handler_for_partition(topic, partition))
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
    /// * `demand_type` - Whether this is normal processing or failure retry.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of message processing.
    ///
    /// # Errors
    ///
    /// Returns the underlying handler's error if all retry attempts fail.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone(), current_demand_type)
                .await
            else {
                return Ok(());
            };

            if context.should_shutdown() {
                return Err(error);
            }

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    if attempt > self.max_retries {
                        // Log the final failure and return the error
                        error!(
                            partition,
                            key = key.as_ref(),
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
                        key = key.as_ref(),
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
                        key = key.as_ref(),
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
                        key = key.as_ref(),
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

    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Retry logic for a fired timer
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            // Try handling the timer
            let Err(error) = self
                .handler
                .on_timer(context.clone(), timer.clone(), current_demand_type)
                .await
            else {
                return Ok(());
            };
            // If shutdown was requested, stop retrying
            if context.should_shutdown() {
                return Err(error);
            }
            match error.classify_error() {
                ErrorCategory::Transient => {
                    if attempt > self.max_retries {
                        error!("failed to handle timer: {error:#}; maximum attempts reached");
                        return Err(error);
                    }
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        "failed to handle timer: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );
                    select! {
                        () = sleep(sleep_time) => {},
                        () = context.on_shutdown() => return Err(error),
                    }
                }
                ErrorCategory::Permanent => {
                    error!("permanently failed to handle timer: {error:#}");
                    return Err(error);
                }
                ErrorCategory::Terminal => {
                    info!(
                        "terminal condition encountered while handling timer: {error:#}; aborting"
                    );
                    return Err(error);
                }
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");

        // No retry-specific state to clean up (timers are handled by tokio)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
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
    /// * `demand_type` - Whether this is normal processing or failure retry.
    async fn on_message<C>(&self, context: C, message: UncommittedMessage, demand_type: DemandType)
    where
        C: EventContext,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            let Err(error) = self
                .handler
                .on_message(context.clone(), message.clone(), current_demand_type)
                .await
            else {
                uncommitted_offset.commit();
                break;
            };

            if context.should_shutdown() {
                uncommitted_offset.abort();
                break;
            }

            // Handle different error categories
            match error.classify_error() {
                ErrorCategory::Transient => {
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        partition,
                        key = key.as_ref(),
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
                        key = key.as_ref(),
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
                        key = key.as_ref(),
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

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        // Retry logic for an uncommitted timer
        let (trigger, uncommitted) = timer.into_inner();
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            // Use the original demand type for the first attempt, failure demand for
            // retries
            let current_demand_type = if attempt == 1 {
                demand_type
            } else {
                DemandType::Failure
            };
            // Try handling the timer
            let Err(error) = self
                .handler
                .on_timer(context.clone(), trigger.clone(), current_demand_type)
                .await
            else {
                uncommitted.commit().await;
                break;
            };
            // If shutdown was requested, abort and stop retrying
            if context.should_shutdown() {
                uncommitted.abort().await;
                break;
            }
            match error.classify_error() {
                ErrorCategory::Transient => {
                    let sleep_time = self.sleep_time(attempt);
                    error!(
                        "failed to handle timer: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );
                    select! {
                        () = sleep(sleep_time) => {},
                        () = context.on_shutdown() => {
                            uncommitted.abort().await;
                            break;
                        }
                    }
                }
                ErrorCategory::Permanent => {
                    error!("permanently failed to handle timer: {error:#}; discarding timer");
                    uncommitted.commit().await;
                    break;
                }
                ErrorCategory::Terminal => {
                    info!(
                        "terminal condition encountered while handling timer: {error:#}; aborting"
                    );
                    uncommitted.abort().await;
                    break;
                }
            }
        }
    }

    /// Performs any necessary shutdown operations for the handler.
    async fn shutdown(self) {
        debug!("shutting down retry handler");

        // No retry-specific state to clean up (timers are handled by tokio)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}
