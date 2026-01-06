//! Unified storage backend for trigger and defer stores.
//!
//! This module provides a type-safe abstraction that ensures trigger stores
//! and defer stores always use the same underlying storage infrastructure,
//! preventing misconfiguration and ensuring only one Cassandra session is
//! created when using Cassandra backend.

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::middleware::defer::message::store::cassandra::{
    CassandraMessageDeferStoreError, CassandraMessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::message::store::memory::MemoryDeferStoreProvider;
use crate::consumer::middleware::defer::timer::store::cassandra::{
    CassandraTimerDeferStoreError, CassandraTimerDeferStoreProvider,
};
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStoreProvider;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::timers::duration::CompactDuration;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::{CassandraTriggerStore, CassandraTriggerStoreError};
use crate::timers::store::memory::InMemoryTriggerStore;
use crate::timers::store::memory::memory_store;
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

/// Atomically-created pair of trigger and defer store providers.
///
/// This enum ensures that trigger and defer store providers always use matching
/// backends, making mismatched stores unrepresentable in the type system.
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
/// // Pattern match to get all providers - they're guaranteed to match backends
/// match stores {
///     StorePair::Memory {
///         trigger,
///         message_defer,
///         timer_defer,
///     } => {
///         // All are in-memory store providers
///     }
///     StorePair::Cassandra {
///         trigger,
///         message_defer,
///         timer_defer,
///     } => {
///         // All are Cassandra store providers
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub enum StorePair {
    /// Both stores use in-memory storage.
    Memory {
        /// In-memory trigger store wrapped in `TableAdapter`.
        trigger: TableAdapter<InMemoryTriggerStore>,
        /// In-memory message defer store provider.
        message_defer: MemoryDeferStoreProvider,
        /// In-memory timer defer store provider.
        timer_defer: MemoryTimerDeferStoreProvider,
    },
    /// Both stores use Cassandra storage with a shared session.
    Cassandra {
        /// Cassandra trigger store wrapped in `TableAdapter`.
        trigger: TableAdapter<CassandraTriggerStore>,
        /// Cassandra message defer store provider.
        message_defer: CassandraMessageDeferStoreProvider,
        /// Cassandra timer defer store provider.
        timer_defer: CassandraTimerDeferStoreProvider,
    },
}

/// Errors that can occur during store pair creation.
#[derive(Debug, Error)]
pub enum StoreCreationError {
    /// Failed to create trigger store.
    #[error("failed to create trigger store: {0:#}")]
    TriggerStore(Box<CassandraTriggerStoreError>),

    /// Failed to create defer store.
    #[error("failed to create defer store: {0:#}")]
    DeferStore(Box<CassandraStoreError>),

    /// Message defer store initialization error (non-Cassandra).
    #[error("message defer store initialization error: {0:#}")]
    MessageDeferStoreInit(Box<CassandraMessageDeferStoreError>),

    /// Timer defer store initialization error (non-Cassandra).
    #[error("timer defer store initialization error: {0:#}")]
    TimerDeferStoreInit(Box<CassandraTimerDeferStoreError>),
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

impl From<CassandraMessageDeferStoreError> for StoreCreationError {
    fn from(e: CassandraMessageDeferStoreError) -> Self {
        match e {
            CassandraMessageDeferStoreError::Cassandra(cassandra_err) => {
                Self::DeferStore(Box::new(cassandra_err))
            }
            other @ CassandraMessageDeferStoreError::InvalidRetryCount { .. } => {
                Self::MessageDeferStoreInit(Box::new(other))
            }
        }
    }
}

impl From<CassandraTimerDeferStoreError> for StoreCreationError {
    fn from(e: CassandraTimerDeferStoreError) -> Self {
        match e {
            CassandraTimerDeferStoreError::Cassandra(cassandra_err) => {
                Self::DeferStore(Box::new(cassandra_err))
            }
            other @ CassandraTimerDeferStoreError::InvalidRetryCount { .. } => {
                Self::TimerDeferStoreInit(Box::new(other))
            }
        }
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
    pub async fn new(
        config: &TriggerStoreConfiguration,
        slab_size: CompactDuration,
        mock: bool,
    ) -> Result<Self, StoreCreationError> {
        let backend = StorageBackend::new(config, mock).await?;
        match &backend {
            StorageBackend::InMemory => Ok(Self::Memory {
                trigger: memory_store(),
                message_defer: MemoryDeferStoreProvider::new(),
                timer_defer: MemoryTimerDeferStoreProvider::new(),
            }),

            StorageBackend::Cassandra { store, keyspace } => {
                // All stores share the same CassandraStore via clone (cheap Arc clone)
                let trigger =
                    CassandraTriggerStore::with_store(store.clone(), keyspace, slab_size).await?;

                let message_defer =
                    CassandraMessageDeferStoreProvider::with_store(store.clone(), keyspace).await?;

                let timer_defer =
                    CassandraTimerDeferStoreProvider::with_store(store.clone(), keyspace).await?;

                Ok(Self::Cassandra {
                    trigger: TableAdapter::new(trigger),
                    message_defer,
                    timer_defer,
                })
            }
        }
    }
}
