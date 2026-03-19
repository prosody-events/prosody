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
use crate::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use crate::consumer::middleware::defer::message::store::{
    CassandraMessageDeferStoreProvider, MemoryMessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::segment::{
    CassandraSegmentStore, CassandraSegmentStoreError,
};
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries as TimerQueries;
use crate::consumer::middleware::defer::timer::store::{
    CassandraTimerDeferStoreProvider, MemoryTimerDeferStoreProvider,
};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::timers::store::cassandra::{CassandraTriggerStoreError, CassandraTriggerStoreProvider};
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Unified storage backend that ensures trigger and defer stores use the same
/// underlying storage infrastructure.
///
/// This type guarantees that:
/// - Only one Cassandra session is created when using Cassandra backend
/// - Trigger and defer stores always use matching storage types
/// - Mock mode uses in-memory storage regardless of configuration
///
/// # Examples
///
/// ```no_run
/// use prosody::consumer::storage::StorageBackend;
/// use prosody::high_level::config::TriggerStoreConfiguration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = TriggerStoreConfiguration::InMemory;
/// let backend = StorageBackend::new(&config, false).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub enum StorageBackend {
    /// In-memory storage for development and testing.
    InMemory,

    /// Cassandra-based persistent storage.
    ///
    /// Contains a shared `CassandraStore` instance that is cloned (via internal
    /// Arc) when creating trigger and defer stores, ensuring only one Scylla
    /// session exists.
    Cassandra {
        /// Shared Cassandra store instance.
        store: CassandraStore,
        /// Keyspace name for query preparation.
        keyspace: String,
    },
}

impl StorageBackend {
    /// Creates a storage backend from trigger store configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Trigger store configuration
    /// * `mock` - If true, uses in-memory storage regardless of config
    ///
    /// # Errors
    ///
    /// Returns error if Cassandra connection fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use prosody::consumer::storage::StorageBackend;
    /// # use prosody::high_level::config::TriggerStoreConfiguration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = TriggerStoreConfiguration::InMemory;
    ///
    /// // Normal mode
    /// let backend = StorageBackend::new(&config, false).await?;
    ///
    /// // Mock mode - always uses InMemory
    /// let mock_backend = StorageBackend::new(&config, true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        config: &TriggerStoreConfiguration,
        mock: bool,
    ) -> Result<Self, CassandraTriggerStoreError> {
        if mock {
            return Ok(Self::InMemory);
        }

        match config {
            TriggerStoreConfiguration::InMemory => Ok(Self::InMemory),
            TriggerStoreConfiguration::Cassandra(cass_config) => {
                let store = CassandraStore::new(cass_config).await?;
                Ok(Self::Cassandra {
                    store,
                    keyspace: cass_config.keyspace.clone(),
                })
            }
        }
    }
}

/// Atomically-created set of trigger and defer store providers.
///
/// This enum ensures that trigger and defer stores always use matching
/// storage types, making mismatched stores unrepresentable in the type system.
///
/// # Examples
///
/// ```no_run
/// # use prosody::consumer::storage::{StorageBackend, StorePair};
/// # use prosody::high_level::config::TriggerStoreConfiguration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = TriggerStoreConfiguration::InMemory;
/// let stores = StorePair::new(&config, false).await?;
///
/// // Pattern match to get all providers - they're guaranteed to match storage types
/// match stores {
///     StorePair::Memory {
///         trigger_provider,
///         message_provider,
///         timer_provider,
///     } => {
///         // All are in-memory
///     }
///     StorePair::Cassandra {
///         trigger_provider,
///         message_provider,
///         timer_provider,
///     } => {
///         // All are Cassandra
///     }
/// }
/// # Ok(())
/// # }
/// ```
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
        /// Deduplication store provider (Memory).
        dedup_provider: MemoryDeduplicationStoreProvider,
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
        /// Deduplication store provider (Cassandra).
        dedup_provider: CassandraDeduplicationStoreProvider,
    },
}

/// Errors that can occur during store pair creation.
#[derive(Debug, Error)]
pub enum StoreCreationError {
    /// Failed to create trigger store.
    #[error("failed to create trigger store: {0:#}")]
    TriggerStore(Box<CassandraTriggerStoreError>),

    /// Failed to create defer store queries.
    #[error("failed to create defer store queries: {0:#}")]
    DeferStore(Box<CassandraStoreError>),

    /// Failed to create segment store.
    #[error("failed to create segment store: {0:#}")]
    SegmentStore(Box<CassandraSegmentStoreError>),
}

impl From<CassandraTriggerStoreError> for StoreCreationError {
    fn from(e: CassandraTriggerStoreError) -> Self {
        Self::TriggerStore(Box::new(e))
    }
}

impl From<CassandraStoreError> for StoreCreationError {
    fn from(e: CassandraStoreError) -> Self {
        Self::DeferStore(Box::new(e))
    }
}

impl From<CassandraSegmentStoreError> for StoreCreationError {
    fn from(e: CassandraSegmentStoreError) -> Self {
        Self::SegmentStore(Box::new(e))
    }
}

impl StorePair {
    /// Creates both trigger and defer store providers atomically.
    ///
    /// This is an atomic operation - both stores are created or the operation
    /// fails. The stores are guaranteed to use the same underlying storage.
    ///
    /// # Arguments
    ///
    /// * `config` - Trigger store configuration (`InMemory` or `Cassandra`)
    /// * `mock` - If true, uses in-memory storage regardless of config
    ///
    /// # Errors
    ///
    /// Returns error if store initialization fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use prosody::consumer::storage::StorePair;
    /// # use prosody::high_level::config::TriggerStoreConfiguration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stores = StorePair::new(&TriggerStoreConfiguration::InMemory, false).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        config: &TriggerStoreConfiguration,
        mock: bool,
        dedup_ttl: Duration,
    ) -> Result<Self, StoreCreationError> {
        let backend = StorageBackend::new(config, mock).await?;
        match &backend {
            StorageBackend::InMemory => Ok(Self::Memory {
                trigger_provider: InMemoryTriggerStoreProvider::new(),
                message_provider: MemoryMessageDeferStoreProvider::new(),
                timer_provider: MemoryTimerDeferStoreProvider::new(),
                dedup_provider: MemoryDeduplicationStoreProvider::new(),
            }),

            StorageBackend::Cassandra { store, keyspace } => {
                // Create trigger store provider (prepares queries once, creates
                // per-partition stores with independent caches on demand)
                let trigger_provider =
                    CassandraTriggerStoreProvider::with_store(store.clone(), keyspace).await?;

                // Create segment store for defer stores (shared across message and timer)
                let segment_store = CassandraSegmentStore::new(store.clone(), keyspace).await?;

                // Prepare queries for message defer stores
                let message_queries =
                    Arc::new(MessageQueries::new(store.session(), keyspace).await?);

                // Prepare queries for timer defer stores
                let timer_queries = Arc::new(TimerQueries::new(store.session(), keyspace).await?);

                // Prepare queries for deduplication stores
                let dedup_queries = Arc::new(
                    DeduplicationQueries::new(store.session(), keyspace)
                        .await?
                        .with_local_one_consistency(),
                );

                let dedup_ttl_secs: i32 = dedup_ttl
                    .as_secs()
                    .try_into()
                    .unwrap_or(630_720_000_i32)
                    .min(630_720_000_i32);

                let message_provider = CassandraMessageDeferStoreProvider::new(
                    store.clone(),
                    message_queries,
                    segment_store.clone(),
                );

                let timer_provider = CassandraTimerDeferStoreProvider::new(
                    store.clone(),
                    timer_queries,
                    segment_store,
                );

                let dedup_provider = CassandraDeduplicationStoreProvider::new(
                    store.clone(),
                    dedup_queries,
                    dedup_ttl_secs,
                );

                Ok(Self::Cassandra {
                    trigger_provider,
                    message_provider,
                    timer_provider,
                    dedup_provider,
                })
            }
        }
    }
}
