//! Typed consumer storage composition.
//!
//! One concrete component family binds trigger, defer, deduplication, loader,
//! and keyed-state providers. Runtime configuration never enters mode wiring.

use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::loader::KafkaLoaderError;
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use std::num::NonZeroUsize;
use std::time::Duration;
use thiserror::Error;

pub(crate) mod components;
pub(crate) use components::{ComponentsOf, ConsumerStorageBackend};

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::otel::SpanRelation;

#[derive(Clone, Copy)]
pub(crate) struct ConsumerStorageInputs {
    pub(crate) dedup_ttl: Duration,
    pub(crate) dedup_cache_capacity: NonZeroUsize,
    pub(crate) timer_spans: SpanRelation,
}

/// Converts a deduplication TTL to Cassandra's `i32` seconds representation,
/// rejecting anything past the `USING TTL` ceiling — an over-ceiling TTL would
/// make every dedup-marker write fail at the coordinator, so we fail fast at
/// store creation instead.
pub(super) fn dedup_ttl_seconds(ttl: Duration) -> Result<i32, StoreCreationError> {
    let seconds: i32 = ttl
        .as_secs()
        .try_into()
        .map_err(|_| StoreCreationError::DeduplicationTtl(ttl.as_secs()))?;
    if i64::from(seconds) > MAX_CASSANDRA_TTL_SECS {
        return Err(StoreCreationError::DeduplicationTtl(ttl.as_secs()));
    }
    Ok(seconds)
}

/// Errors that can occur during consumer storage creation.
#[derive(Debug, Error)]
pub enum StoreCreationError {
    /// Failed to create trigger store.
    #[error("failed to create trigger store: {0:#}")]
    TriggerStore(Box<CassandraTriggerStoreError>),

    /// Failed to initialize the shared Cassandra store: session creation or
    /// statement preparation for any of the stores it backs (message/timer
    /// defer, deduplication, keyed-state cell, identity, and publication).
    #[error("failed to initialize cassandra store: {0:#}")]
    Cassandra(Box<CassandraStoreError>),

    /// Failed to create segment store.
    #[error("failed to create segment store: {0:#}")]
    SegmentStore(Box<CassandraDeferStoreError>),

    /// Failed to create the Kafka message loader.
    #[error("failed to create message loader: {0:#}")]
    Loader(#[from] KafkaLoaderError),

    /// Deduplication TTL exceeds Cassandra's maximum.
    #[error("deduplication TTL {0} seconds exceeds Cassandra maximum of 630,720,000 seconds")]
    DeduplicationTtl(u64),
}

impl From<CassandraTriggerStoreError> for StoreCreationError {
    fn from(e: CassandraTriggerStoreError) -> Self {
        Self::TriggerStore(Box::new(e))
    }
}

impl From<CassandraStoreError> for StoreCreationError {
    fn from(e: CassandraStoreError) -> Self {
        Self::Cassandra(Box::new(e))
    }
}

impl From<CassandraDeferStoreError> for StoreCreationError {
    fn from(e: CassandraDeferStoreError) -> Self {
        Self::SegmentStore(Box::new(e))
    }
}

#[cfg(test)]
mod tests;
