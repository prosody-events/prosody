//! Timer defer middleware for handling transient timer failures.
//!
//! This module provides [`TimerDeferMiddleware`] which wraps handlers with
//! timer deferral capability independently of message deferral.

use super::handler::TimerDeferHandler;
use super::store::cassandra::CassandraTimerDeferStore;
use super::store::cassandra::queries::Queries as TimerQueries;
use super::store::memory::MemoryTimerDeferStore;
use super::store::{CachedTimerDeferStore, TimerDeferStore};
use crate::cassandra::CassandraStore;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::{DeferralDecider, FailureTracker};
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::segment::CassandraSegmentStore;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::{ConsumerGroup, Key, Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;
use thiserror::Error;

/// Cassandra resources for creating timer defer stores.
#[derive(Clone, Debug)]
pub struct CassandraResources {
    /// Shared Cassandra store.
    pub store: CassandraStore,
    /// Prepared timer queries.
    pub queries: Arc<TimerQueries>,
    /// Cassandra segment store for segment persistence.
    pub segment_store: CassandraSegmentStore,
}

/// Timer store variant - either memory or Cassandra.
#[derive(Clone, Debug)]
pub enum TimerStoreKind {
    /// In-memory store for testing.
    Memory,
    /// Cassandra-backed store for production.
    Cassandra(CassandraResources),
}

/// Enum wrapper for different timer defer store implementations.
#[derive(Clone)]
pub enum TimerStoreWrapper {
    /// In-memory store for testing (no cache needed - already O(1) lookups).
    Memory(MemoryTimerDeferStore),
    /// Cassandra-backed store for production (cached to reduce network calls).
    Cassandra(CachedTimerDeferStore<CassandraTimerDeferStore>),
}

/// Unified error type for timer store operations.
#[derive(Debug, Error)]
pub enum TimerStoreError {
    /// Cassandra store error.
    #[error(transparent)]
    Cassandra(#[from] CassandraDeferStoreError),
    /// Memory store error (never occurs).
    #[error(transparent)]
    Memory(#[from] Infallible),
}

impl ClassifyError for TimerStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cassandra(e) => e.classify_error(),
            Self::Memory(e) => match *e {},
        }
    }
}

impl TimerDeferStore for TimerStoreWrapper {
    type Error = TimerStoreError;

    async fn defer_first_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store.defer_first_timer(trigger).await.map_err(Into::into),
            Self::Cassandra(store) => store.defer_first_timer(trigger).await.map_err(Into::into),
        }
    }

    async fn get_next_deferred_timer(
        &self,
        key: &Key,
    ) -> Result<Option<(Trigger, u32)>, Self::Error> {
        match self {
            Self::Memory(store) => store.get_next_deferred_timer(key).await.map_err(Into::into),
            Self::Cassandra(store) => store.get_next_deferred_timer(key).await.map_err(Into::into),
        }
    }

    fn deferred_times(
        &self,
        key: &Key,
    ) -> impl futures::Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        use futures::StreamExt;
        use futures::stream::unfold;

        let key = key.clone();
        let store = self.clone();

        unfold((store, key, false), |(store, key, done)| async move {
            if done {
                return None;
            }

            match &store {
                TimerStoreWrapper::Memory(s) => {
                    let stream = s.deferred_times(&key);
                    futures::pin_mut!(stream);
                    match stream.next().await {
                        Some(Ok(time)) => Some((Ok(time), (store, key, false))),
                        Some(Err(e)) => Some((Err(e.into()), (store, key, true))),
                        None => None,
                    }
                }
                TimerStoreWrapper::Cassandra(s) => {
                    let stream = s.deferred_times(&key);
                    futures::pin_mut!(stream);
                    match stream.next().await {
                        Some(Ok(time)) => Some((Ok(time), (store, key, false))),
                        Some(Err(e)) => Some((Err(e.into()), (store, key, true))),
                        None => None,
                    }
                }
            }
        })
    }

    async fn append_deferred_timer(&self, trigger: &Trigger) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .append_deferred_timer(trigger)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .append_deferred_timer(trigger)
                .await
                .map_err(Into::into),
        }
    }

    async fn remove_deferred_timer(
        &self,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .remove_deferred_timer(key, time)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .remove_deferred_timer(key, time)
                .await
                .map_err(Into::into),
        }
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .set_retry_count(key, retry_count)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .set_retry_count(key, retry_count)
                .await
                .map_err(Into::into),
        }
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store.delete_key(key).await.map_err(Into::into),
            Self::Cassandra(store) => store.delete_key(key).await.map_err(Into::into),
        }
    }
}

/// Middleware that defers transiently-failed timers for timer-based retry.
///
/// This middleware handles timer deferral independently of message deferral.
/// Both can be composed via `.layer()`.
///
/// # Type Parameters
///
/// * `D` - Deferral decider (default: [`FailureTracker`])
#[derive(Clone)]
pub struct TimerDeferMiddleware<D = FailureTracker>
where
    D: DeferralDecider,
{
    config: DeferConfiguration,
    store_kind: TimerStoreKind,
    decider: D,
    consumer_group: ConsumerGroup,
}

impl<D> TimerDeferMiddleware<D>
where
    D: DeferralDecider,
{
    /// Creates middleware with configuration and store kind.
    #[must_use]
    pub fn new(
        config: DeferConfiguration,
        store_kind: TimerStoreKind,
        decider: D,
        consumer_config: &ConsumerConfiguration,
    ) -> Self {
        Self {
            config,
            store_kind,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
        }
    }
}

/// Creates [`TimerDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct TimerDeferProvider<T, D = FailureTracker>
where
    D: DeferralDecider,
{
    inner_provider: T,
    config: DeferConfiguration,
    store_kind: TimerStoreKind,
    decider: D,
    consumer_group: ConsumerGroup,
}

impl<D> HandlerMiddleware for TimerDeferMiddleware<D>
where
    D: DeferralDecider,
{
    type Provider<T: FallibleHandlerProvider> = TimerDeferProvider<T, D>;

    fn with_provider<T>(&self, inner_provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TimerDeferProvider {
            inner_provider,
            config: self.config.clone(),
            store_kind: self.store_kind.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
        }
    }
}

impl<T, D> FallibleHandlerProvider for TimerDeferProvider<T, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    D: DeferralDecider,
{
    type Handler = TimerDeferHandler<T::Handler, TimerStoreWrapper, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        // Create the appropriate store based on store_kind
        let timer_store = match &self.store_kind {
            TimerStoreKind::Memory => TimerStoreWrapper::Memory(MemoryTimerDeferStore::new()),
            TimerStoreKind::Cassandra(resources) => {
                let store = CassandraTimerDeferStore::new(
                    resources.store.clone(),
                    resources.queries.clone(),
                    resources.segment_store.clone(),
                    topic,
                    partition,
                    self.consumer_group.clone(),
                );
                TimerStoreWrapper::Cassandra(CachedTimerDeferStore::new(
                    store,
                    self.config.cache_size,
                ))
            }
        };

        // Inner handler first
        let inner_handler = self.inner_provider.handler_for_partition(topic, partition);

        // Timer defer wraps inner handler
        TimerDeferHandler {
            handler: inner_handler,
            store: timer_store,
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
        }
    }
}
