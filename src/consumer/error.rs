//! Errors raised while building and running a consumer.

use crate::consumer::config::RecoveryTtlMarginError;
use crate::consumer::middleware::defer::DeferInitError;
use crate::consumer::middleware::monopolization::MonopolizationInitError;
use crate::consumer::middleware::scheduler::SchedulerInitError;
use crate::consumer::middleware::timeout::TimeoutInitError;
use crate::consumer::storage::StoreCreationError;
use crate::error::ErrorCategory;
use crate::state::config::KeyedStateConfigurationBuilderError;
use crate::state::registry::RegisterStateError;
use crate::timers::duration::CompactDurationError;
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use rdkafka::error::KafkaError;
use std::io;
use thiserror::Error;
use tokio::task::JoinError;
use validator::ValidationErrors;

/// Errors that can occur during consumer operations.
///
/// This enum covers various error conditions that might occur when
/// creating, configuring, or operating a Kafka consumer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// Indicates invalid consumer configuration.
    #[error("invalid consumer configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// Indicates an invalid event type pattern.
    #[error("invalid allowed events pattern: {0:#}")]
    AllowedEventsPattern(#[from] aho_corasick::BuildError),

    /// Indicates an IO failure.
    #[error("IO error: {0:#}")]
    Io(#[from] io::Error),

    /// Indicates a failure to retrieve the hostname.
    #[error("failed to get hostname: {0:#}")]
    Hostname(#[from] whoami::Error),

    /// Indicates a Kafka operation failure.
    #[error("Kafka operation failed: {0:#}")]
    Kafka(#[from] KafkaError),

    /// A blocking startup step — the initial Kafka metadata fetch — did not
    /// finish.
    #[error("consumer startup task failed: {0:#}")]
    StartupTask(#[from] JoinError),

    /// Indicates a Cassandra trigger store operation failure.
    #[error("Cassandra trigger store operation failed: {0:#}")]
    CassandraTriggerStore(Box<CassandraTriggerStoreError>),

    /// Indicates a scheduler initialization failure.
    #[error("Scheduler initialization failed: {0:#}")]
    Scheduler(#[from] SchedulerInitError),

    /// Indicates a timeout middleware initialization failure.
    #[error("Timeout initialization failed: {0:#}")]
    Timeout(#[from] TimeoutInitError),

    /// Indicates a monopolization middleware initialization failure.
    #[error("Monopolization initialization failed: {0:#}")]
    Monopolization(#[from] MonopolizationInitError),

    /// Indicates a defer middleware initialization failure.
    #[error("Defer initialization failed: {0:#}")]
    Defer(#[from] DeferInitError),

    /// Indicates storage backend creation failure.
    #[error("Failed to create storage backend: {0:#}")]
    StorageBackend(Box<StoreCreationError>),

    /// Indicates an invalid timer slab size.
    #[error("Invalid timer slab size: {0:#}")]
    InvalidSlabSize(#[from] CompactDurationError),

    /// Indicates a keyed-state initialization failure.
    #[error("Keyed-state initialization failed: {0:#}")]
    KeyedState(#[from] KeyedStateInitError),
}

/// Errors raised while wiring the keyed-state layer into a pipeline
/// consumer.
#[derive(Debug, Error)]
pub enum KeyedStateInitError {
    /// The keyed-state configuration could not be built from the environment.
    #[error("invalid keyed-state configuration: {0:#}")]
    Configuration(#[from] KeyedStateConfigurationBuilderError),

    /// A descriptor registration was invalid or conflicted.
    #[error(transparent)]
    Register(#[from] RegisterStateError),

    /// The deduplication TTL is too short to outlive the keyed-state
    /// recovery window.
    #[error(transparent)]
    RecoveryTtlMargin(#[from] RecoveryTtlMarginError),

    /// The local keyed-state cache's disk workspace could not be opened.
    ///
    /// The inner `FjallClientError` is crate-internal, so this variant carries
    /// a rendered message plus the error's classification instead of the source
    /// type. The reader's
    /// [`StateReaderError::Store`](crate::state_reader::StateReaderError::Store)
    /// uses the same boundary.
    #[error("failed to open the keyed-state cache: {message}")]
    Cache {
        /// Rendered cache-open error, full source chain.
        message: String,
        /// The cache error's captured classification.
        category: ErrorCategory,
    },

    /// Startup reconciliation of keyed-state publication routing rows failed,
    /// typically because the publication store was unreachable.
    ///
    /// This variant carries a rendered message plus the error's classification
    /// instead of the source type. A per-collection permanent decode failure
    /// is logged and skipped, so any error surfaced here comes from the store
    /// itself. Such a failure is typically transient, and the deploy retries.
    #[error("keyed-state publication reconciliation failed: {message}")]
    Publication {
        /// Rendered reconciliation error, full source chain.
        message: String,
        /// The reconciliation error's captured classification.
        category: ErrorCategory,
    },

    /// Keyed-state collections were registered on the low-level
    /// [`ProsodyConsumer::new`](crate::consumer::ProsodyConsumer::new)
    /// constructor, which runs no state middleware to stage or recover them.
    /// Build a high-level consumer (e.g.
    /// [`ProsodyConsumer::pipeline_consumer`](crate::consumer::ProsodyConsumer::pipeline_consumer))
    /// to use keyed state.
    #[error(
        "keyed-state collections require a high-level consumer; the low-level `new` constructor \
         runs no state middleware"
    )]
    StateUnsupported,
}

/// Collapses the two hops from a keyed-state configuration failure, so
/// `ConsumerBuilders::new()?` works directly in a function returning
/// [`ConsumerError`].
impl From<KeyedStateConfigurationBuilderError> for ConsumerError {
    fn from(error: KeyedStateConfigurationBuilderError) -> Self {
        Self::KeyedState(KeyedStateInitError::Configuration(error))
    }
}

impl From<CassandraTriggerStoreError> for ConsumerError {
    fn from(e: CassandraTriggerStoreError) -> Self {
        Self::CassandraTriggerStore(Box::new(e))
    }
}

impl From<StoreCreationError> for ConsumerError {
    fn from(e: StoreCreationError) -> Self {
        Self::StorageBackend(Box::new(e))
    }
}
