//! Errors raised while building and running a consumer.

use crate::consumer::config::RecoveryTtlMarginError;
use crate::consumer::middleware::defer::DeferInitError;
use crate::consumer::middleware::monopolization::MonopolizationInitError;
use crate::consumer::middleware::scheduler::SchedulerInitError;
use crate::consumer::middleware::timeout::TimeoutInitError;
use crate::consumer::storage::StoreCreationError;
use crate::error::ErrorCategory;
use crate::peer::router::config::{PeerConfigurationBuilderError, PeerConfigurationError};
use crate::peer::router::runtime::PeerRuntimeError;
use crate::state::config::KeyedStateConfigurationBuilderError;
use crate::state::registry::RegisterStateError;
use crate::state_reader::StateReaderError;
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

    /// The mock Kafka cluster could not create a subscribed topic.
    #[error("mock Kafka topic creation failed: {message}")]
    MockCluster {
        /// The mock cluster's error report.
        message: String,
    },

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

    /// Shared reader and consumer infrastructure could not be constructed.
    #[error("Failed to create shared state infrastructure: {0:#}")]
    StateReader(#[from] StateReaderError),

    /// Indicates an invalid timer slab size.
    #[error("Invalid timer slab size: {0:#}")]
    InvalidSlabSize(#[from] CompactDurationError),

    /// Indicates a keyed-state initialization failure.
    #[error("Keyed-state initialization failed: {0:#}")]
    KeyedState(#[from] KeyedStateInitError),

    /// The peer runtime could not start.
    #[error("peer initialization failed: {0:#}")]
    Peer(#[from] PeerInitError),
}

/// Errors raised while the peer runtime starts.
///
/// Most variants carry a rendered message instead of a `#[source]` field. The
/// source types are crate-private, so a public error cannot name them, and this
/// crate does not box an error as a trait object.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PeerInitError {
    /// The peer configuration is invalid.
    #[error("invalid peer configuration: {message}")]
    Configuration {
        /// The rendered source chain.
        message: String,
    },
    /// The process could not discover its host.
    #[error("peer host discovery failed: {message}")]
    Discovery {
        /// The rendered source chain.
        message: String,
    },
    /// The peer directory could not start or register this process.
    #[error("peer directory failed: {message}")]
    Directory {
        /// The rendered source chain.
        message: String,
    },
    /// The peer cache bound is invalid.
    #[error("peer cache configuration is invalid: {message}")]
    Cache {
        /// The rendered source chain.
        message: String,
    },
    /// The peer listener could not start.
    #[error("peer listener failed: {message}")]
    Listener {
        /// The rendered source chain.
        message: String,
    },
}

impl From<PeerConfigurationError> for PeerInitError {
    fn from(error: PeerConfigurationError) -> Self {
        Self::Configuration {
            message: format!("{error:#}"),
        }
    }
}

impl From<PeerRuntimeError> for PeerInitError {
    fn from(error: PeerRuntimeError) -> Self {
        match error {
            PeerRuntimeError::Configuration(error) => Self::Configuration {
                message: format!("{error:#}"),
            },
            PeerRuntimeError::Discovery(error) => Self::Discovery {
                message: format!("{error:#}"),
            },
            PeerRuntimeError::Cache(error) => Self::Cache {
                message: format!("{error:#}"),
            },
            PeerRuntimeError::Listener(error) => Self::Listener {
                message: format!("{error:#}"),
            },
        }
    }
}

/// Errors raised while a consumer stops.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ShutdownError {
    /// The poll loop task did not end cleanly. It panicked, or something
    /// aborted it, so in-flight message processing may not have finished.
    #[error("the consumer poll loop did not end cleanly: {message}")]
    PollLoop {
        /// The rendered join failure.
        message: String,
    },
    /// The peer directory did not confirm the removal of this peer. The row
    /// may or may not survive, and one that survives expires on its lease.
    #[error("the peer directory did not confirm the removal of this peer: {message}")]
    Directory {
        /// The rendered source chain.
        message: String,
    },
    /// The peer teardown ended without reporting.
    #[error("the peer teardown ended without reporting")]
    Teardown,
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
    /// instead of the source type. Reconciliation propagates every failure so
    /// startup cannot continue with routing rows of unknown freshness.
    #[error("keyed-state publication reconciliation failed: {message}")]
    Publication {
        /// Rendered reconciliation error, full source chain.
        message: String,
        /// The reconciliation error's captured classification.
        category: ErrorCategory,
    },

    /// Published state needs a real topic partition count outside mock mode.
    #[error("published keyed state with in-memory storage requires mock mode")]
    PublishedMemoryStorage,

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

impl From<PeerConfigurationBuilderError> for ConsumerError {
    fn from(error: PeerConfigurationBuilderError) -> Self {
        Self::Peer(PeerInitError::Configuration {
            message: format!("{error:#}"),
        })
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
