//! Unified storage backend for trigger and defer stores.
//!
//! This module provides a type-safe abstraction that ensures trigger stores
//! and defer stores always use the same underlying storage infrastructure,
//! preventing misconfiguration and ensuring only one Cassandra session is
//! created when using Cassandra backend.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::deduplication::cassandra::CassandraDeduplicationStoreProvider;
use crate::consumer::middleware::deduplication::memory::MemoryDeduplicationStoreProvider;
use crate::consumer::middleware::deduplication::queries::DeduplicationQueries;
use crate::consumer::middleware::defer::error::CassandraDeferStoreError;
use crate::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use crate::consumer::middleware::defer::message::store::{
    CassandraMessageDeferStoreProvider, MemoryMessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::segment::CassandraSegmentStore;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries as TimerQueries;
use crate::consumer::middleware::defer::timer::store::{
    CassandraTimerDeferStoreProvider, MemoryTimerDeferStoreProvider,
};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
    CellQueries, IdentityQueries, PublicationQueries,
};
use crate::state::memory::MemoryPublicationStore;
use crate::timers::store::cassandra::{CassandraTriggerStoreError, CassandraTriggerStoreProvider};
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::debug;

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::otel::SpanRelation;

/// Atomically-created set of trigger and defer store providers.
///
/// This enum ensures that trigger and defer stores always use matching
/// storage types, making mismatched stores unrepresentable in the type system.
/// See [`StorePair::new`] for a construction example.
#[derive(Clone)]
pub enum StorePair {
    /// All stores use in-memory storage.
    Memory {
        /// Trigger store provider (Memory) — creates per-partition stores.
        trigger_provider: InMemoryTriggerStoreProvider,
        /// Message defer store provider (Memory).
        message_provider: MemoryMessageDeferStoreProvider,
        /// Timer defer store provider (Memory).
        timer_provider: MemoryTimerDeferStoreProvider,
        /// Deduplication store provider (Memory) — the mandatory commit oracle.
        dedup_provider: MemoryDeduplicationStoreProvider,
        /// Keyed-state routing-only publication store (Memory).
        publication_store: MemoryPublicationStore,
    },
    /// All stores use Cassandra storage with a shared session.
    Cassandra {
        /// Trigger store provider (Cassandra) — creates per-partition stores
        /// with independent caches sharing the Cassandra session.
        trigger_provider: CassandraTriggerStoreProvider,
        /// Message defer store provider (Cassandra with resources).
        message_provider: CassandraMessageDeferStoreProvider,
        /// Timer defer store provider (Cassandra with resources).
        timer_provider: CassandraTimerDeferStoreProvider,
        /// Deduplication store provider (Cassandra) — the mandatory commit
        /// oracle; `DeduplicationQueries` are always prepared.
        dedup_provider: CassandraDeduplicationStoreProvider,
        /// Keyed-state cell-store resources (session + prepared statements)
        /// sharing the same session.
        cell_store: CassandraCellResources,
        /// Keyed-state descriptor-identity store sharing the same session.
        identity_store: CassandraDescriptorIdentityStore,
        /// Keyed-state routing-only publication store sharing the same session.
        publication_store: CassandraPublicationStore,
    },
}

impl StorePair {
    /// Creates both trigger and defer store providers atomically.
    ///
    /// This is an atomic operation - both stores are created or the operation
    /// fails. The stores are guaranteed to use the same underlying storage.
    /// `dedup_cache_capacity` is a `NonZeroUsize` because deduplication is
    /// always wired (it is the keyed-state commit oracle) and a zero
    /// capacity is meaningless.
    ///
    /// # Errors
    ///
    /// Returns error if store initialization fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use prosody::consumer::storage::StorePair;
    /// # use prosody::otel::SpanRelation;
    /// # use prosody::high_level::config::TriggerStoreConfiguration;
    /// # use std::num::NonZeroUsize;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stores = StorePair::new(
    ///     &TriggerStoreConfiguration::InMemory,
    ///     false,
    ///     Duration::from_secs(7 * 24 * 3600),
    ///     NonZeroUsize::new(8192).ok_or("cache capacity must be nonzero")?,
    ///     SpanRelation::FollowsFrom,
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        config: &TriggerStoreConfiguration,
        mock: bool,
        dedup_ttl: Duration,
        dedup_cache_capacity: NonZeroUsize,
        timer_spans: SpanRelation,
    ) -> Result<Self, StoreCreationError> {
        // Pure config validation first: fail fast before any Cassandra IO
        // (memory mode validates too, deliberately).
        let dedup_ttl_secs = dedup_ttl_seconds(dedup_ttl)?;

        let cass_config = match (mock, config) {
            (true, _) | (false, TriggerStoreConfiguration::InMemory) => {
                return Ok(Self::Memory {
                    trigger_provider: InMemoryTriggerStoreProvider::new(),
                    message_provider: MemoryMessageDeferStoreProvider::new(),
                    timer_provider: MemoryTimerDeferStoreProvider::with_linking(timer_spans),
                    dedup_provider: MemoryDeduplicationStoreProvider::new(),
                    publication_store: MemoryPublicationStore::new(),
                });
            }
            (false, TriggerStoreConfiguration::Cassandra(cass_config)) => cass_config,
        };

        // One shared session for every Cassandra store below.
        let store = CassandraStore::new(cass_config).await?;
        let keyspace = &cass_config.keyspace;

        // Create trigger store provider (prepares queries once, creates
        // per-partition stores with independent caches on demand)
        let trigger_provider =
            CassandraTriggerStoreProvider::with_store(store.clone(), keyspace).await?;

        // Create segment store for defer stores (shared across message and timer)
        let segment_store = CassandraSegmentStore::new(store.clone(), keyspace).await?;

        // Prepare queries for message defer stores
        let message_queries = Arc::new(MessageQueries::new(store.session(), keyspace).await?);

        // Prepare queries for timer defer stores
        let timer_queries = Arc::new(TimerQueries::new(store.session(), keyspace).await?);

        let message_provider = CassandraMessageDeferStoreProvider::new(
            store.clone(),
            message_queries,
            segment_store.clone(),
        );

        let timer_provider = CassandraTimerDeferStoreProvider::new(
            store.clone(),
            timer_queries,
            segment_store,
            timer_spans,
        );

        debug!(ttl_secs = dedup_ttl_secs, "deduplication store TTL");
        let dedup_queries = Arc::new(DeduplicationQueries::new(store.session(), keyspace).await?);
        let dedup_provider = CassandraDeduplicationStoreProvider::new(
            store.clone(),
            dedup_queries,
            dedup_ttl_secs,
            dedup_cache_capacity,
        );

        let cell_queries = Arc::new(CellQueries::new(store.session(), keyspace).await?);
        let cell_store = CassandraCellResources::new(store.clone(), cell_queries);
        let identity_queries = Arc::new(IdentityQueries::new(store.session(), keyspace).await?);
        let identity_store = CassandraDescriptorIdentityStore::new(store.clone(), identity_queries);

        let publication_queries =
            Arc::new(PublicationQueries::new(store.session(), keyspace).await?);
        let publication_store = CassandraPublicationStore::new(store.clone(), publication_queries);

        Ok(Self::Cassandra {
            trigger_provider,
            message_provider,
            timer_provider,
            dedup_provider,
            cell_store,
            identity_store,
            publication_store,
        })
    }
}

/// Converts a deduplication TTL to Cassandra's `i32` seconds representation,
/// rejecting anything past the `USING TTL` ceiling — an over-ceiling TTL would
/// make every dedup-marker write fail at the coordinator, so we fail fast at
/// store creation instead.
fn dedup_ttl_seconds(ttl: Duration) -> Result<i32, StoreCreationError> {
    let seconds: i32 = ttl
        .as_secs()
        .try_into()
        .map_err(|_| StoreCreationError::DeduplicationTtl(ttl.as_secs()))?;
    if i64::from(seconds) > MAX_CASSANDRA_TTL_SECS {
        return Err(StoreCreationError::DeduplicationTtl(ttl.as_secs()));
    }
    Ok(seconds)
}

/// Errors that can occur during store pair creation.
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
