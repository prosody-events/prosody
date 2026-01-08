//! Unified storage backend for trigger and defer stores.
//!
//! This module provides a type-safe abstraction that ensures trigger stores
//! and defer stores always use the same underlying storage infrastructure,
//! preventing misconfiguration and ensuring only one Cassandra session is
//! created when using Cassandra backend.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::message::handler::{
    CassandraResources as MessageCassandraResources, MessageStoreKind,
};
use crate::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use crate::consumer::middleware::defer::segment::{
    CassandraSegmentStore, CassandraSegmentStoreError,
};
use crate::consumer::middleware::defer::timer::middleware::{
    CassandraResources as TimerCassandraResources, TimerStoreKind,
};
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries as TimerQueries;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::timers::duration::CompactDuration;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::{CassandraTriggerStore, CassandraTriggerStoreError};
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::store::memory::memory_store;
use std::sync::Arc;
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

/// Atomically-created set of trigger and defer stores.
///
/// This enum ensures that trigger and defer stores always use matching
/// storage types, making mismatched stores unrepresentable in the type system.
///
/// # Examples
///
/// ```no_run
/// # use prosody::consumer::storage::{StorageBackend, StorePair};
/// # use prosody::high_level::config::TriggerStoreConfiguration;
/// # use prosody::timers::duration::CompactDuration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = TriggerStoreConfiguration::InMemory;
/// let slab_size = CompactDuration::new(3600);
/// let stores = StorePair::new(&config, slab_size, false).await?;
///
/// // Pattern match to get all stores - they're guaranteed to match storage types
/// match stores {
///     StorePair::Memory {
///         trigger,
///         message_store_kind,
///         timer_store_kind,
///     } => {
///         // All are in-memory
///     }
///     StorePair::Cassandra {
///         trigger,
///         message_store_kind,
///         timer_store_kind,
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
        /// In-memory trigger store wrapped in `TableAdapter`.
        trigger: TableAdapter<InMemoryTriggerStore>,
        /// Message defer store kind (Memory).
        message_store_kind: MessageStoreKind,
        /// Timer defer store kind (Memory).
        timer_store_kind: TimerStoreKind,
    },
    /// All stores use Cassandra storage with a shared session.
    Cassandra {
        /// Cassandra trigger store wrapped in `TableAdapter`.
        trigger: TableAdapter<CassandraTriggerStore>,
        /// Message defer store kind (Cassandra with resources).
        message_store_kind: MessageStoreKind,
        /// Timer defer store kind (Cassandra with resources).
        timer_store_kind: TimerStoreKind,
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
    /// Creates both trigger and defer stores atomically.
    ///
    /// This is an atomic operation - both stores are created or the operation
    /// fails. The stores are guaranteed to use the same underlying storage.
    ///
    /// # Arguments
    ///
    /// * `config` - Trigger store configuration (`InMemory` or `Cassandra`)
    /// * `slab_size` - Slab size for trigger store time partitioning
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
    /// # use prosody::timers::duration::CompactDuration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stores = StorePair::new(
    ///     &TriggerStoreConfiguration::InMemory,
    ///     CompactDuration::new(3600),
    ///     false,
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Returns the message and timer store kinds.
    #[must_use]
    pub fn store_kinds(&self) -> (MessageStoreKind, TimerStoreKind) {
        match self {
            Self::Memory {
                message_store_kind,
                timer_store_kind,
                ..
            }
            | Self::Cassandra {
                message_store_kind,
                timer_store_kind,
                ..
            } => (message_store_kind.clone(), timer_store_kind.clone()),
        }
    }

    /// Creates both trigger and defer stores atomically.
    ///
    /// This is an atomic operation - both stores are created or the operation
    /// fails. The stores are guaranteed to use the same underlying storage.
    ///
    /// # Arguments
    ///
    /// * `config` - Trigger store configuration (`InMemory` or `Cassandra`)
    /// * `slab_size` - Slab size for trigger store time partitioning
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
    /// # use prosody::timers::duration::CompactDuration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stores = StorePair::new(
    ///     &TriggerStoreConfiguration::InMemory,
    ///     CompactDuration::new(3600),
    ///     false,
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        config: &TriggerStoreConfiguration,
        slab_size: CompactDuration,
        mock: bool,
    ) -> Result<Self, StoreCreationError> {
        let backend = StorageBackend::new(config, mock).await?;
        match &backend {
            StorageBackend::InMemory => Ok(Self::Memory {
                trigger: memory_store(),
                message_store_kind: MessageStoreKind::Memory,
                timer_store_kind: TimerStoreKind::Memory,
            }),

            StorageBackend::Cassandra { store, keyspace } => {
                // All stores share the same CassandraStore via clone (cheap Arc clone)
                let trigger =
                    CassandraTriggerStore::with_store(store.clone(), keyspace, slab_size).await?;

                // Create segment store for defer stores (shared across message and timer)
                let segment_store = CassandraSegmentStore::new(store.clone(), keyspace).await?;

                // Prepare queries for message defer stores
                let message_queries =
                    Arc::new(MessageQueries::new(store.session(), keyspace).await?);

                // Prepare queries for timer defer stores
                let timer_queries = Arc::new(TimerQueries::new(store.session(), keyspace).await?);

                let message_store_kind = MessageStoreKind::Cassandra(MessageCassandraResources {
                    store: store.clone(),
                    queries: message_queries,
                    segment_store: segment_store.clone(),
                });

                let timer_store_kind = TimerStoreKind::Cassandra(TimerCassandraResources {
                    store: store.clone(),
                    queries: timer_queries,
                    segment_store,
                });

                Ok(Self::Cassandra {
                    trigger: TableAdapter::new(trigger),
                    message_store_kind,
                    timer_store_kind,
                })
            }
        }
    }
}
