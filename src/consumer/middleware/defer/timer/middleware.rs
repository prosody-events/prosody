//! Timer defer middleware for handling transient timer failures.
//!
//! This module provides [`TimerDeferMiddleware`] which wraps handlers with
//! timer deferral capability independently of message deferral.

use super::handler::TimerDeferHandler;
use super::store::TimerDeferStoreProvider;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::{DeferralDecider, FailureTracker};
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::telemetry::Telemetry;
use crate::{ConsumerGroup, Partition, Topic};
use std::marker::PhantomData;
use std::sync::Arc;

/// Middleware that defers transiently-failed timers for timer-based retry.
///
/// This middleware handles timer deferral independently of message deferral.
/// Both can be composed via `.layer()`.
///
/// # Type Parameters
///
/// * `S` - Timer defer store provider
/// * `D` - Deferral decider (default: [`FailureTracker`])
/// * `P` - Handler payload type (payload-agnostic; defaults to `()`)
#[derive(Clone)]
pub struct TimerDeferMiddleware<S, D = FailureTracker, P = ()>
where
    S: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    provider: S,
    decider: D,
    consumer_group: ConsumerGroup,
    telemetry: Telemetry,
    _payload: PhantomData<fn() -> P>,
}

impl<S, D, P> TimerDeferMiddleware<S, D, P>
where
    S: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    /// Creates middleware with configuration and store provider.
    #[must_use]
    pub fn new(
        config: DeferConfiguration,
        provider: S,
        decider: D,
        consumer_config: &ConsumerConfiguration,
        telemetry: &Telemetry,
    ) -> Self {
        Self {
            config,
            provider,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
            telemetry: telemetry.clone(),
            _payload: PhantomData,
        }
    }
}

/// Creates [`TimerDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct TimerDeferProvider<T, S, D = FailureTracker>
where
    S: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    inner_provider: T,
    config: DeferConfiguration,
    store_provider: S,
    decider: D,
    consumer_group: ConsumerGroup,
    telemetry: Telemetry,
}

impl<S, D, P> HandlerMiddleware<P> for TimerDeferMiddleware<S, D, P>
where
    S: TimerDeferStoreProvider,
    D: DeferralDecider,
    P: Send + Sync + 'static,
{
    type Provider<T>
        = TimerDeferProvider<T, S, D>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, inner_provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        TimerDeferProvider {
            inner_provider,
            config: self.config.clone(),
            store_provider: self.provider.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T, S, D> FallibleHandlerProvider for TimerDeferProvider<T, S, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    S: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    type Handler = TimerDeferHandler<T::Handler, S::Store, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let store = self.store_provider.create_store(
            topic,
            partition,
            &self.consumer_group,
            self.config.store_cache_size,
        );

        let inner_handler = self.inner_provider.handler_for_partition(topic, partition);

        let sender = self.telemetry.partition_sender(topic, partition);

        TimerDeferHandler {
            handler: inner_handler,
            store,
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
            sender,
            source: self.consumer_group.clone(),
        }
    }
}
