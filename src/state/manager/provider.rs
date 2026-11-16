//! The process-wide provider that acquires a partition's state manager.

use super::{IdentityErr, PartitionStateProvider, PublicationErr, StateManager, StateManagerInner};
use crate::error::{ClassifyError, ErrorCategory};
use crate::segment::partition_segment_id;
use crate::state::descriptor_identity::{DescriptorIdentityError, acquire_descriptor_identities};
use crate::state::dirty::DirtyStore;
use crate::state::registry::CollectionDefRegistry;
use crate::state::{StateBackend, StateBackendFactory};
use crate::timers::duration::CompactDuration;
use crate::{Partition, Topic};
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::OnceCell;

/// Process-wide [`PartitionStateProvider`] over a
/// `StateBackendFactory`: acquisition validates descriptor identities and
/// removes the routing rows of unpublished collections before it mints the
/// manager.
#[derive(Clone)]
pub struct StateManagerProvider<F, L> {
    backend: F,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    dedup_ttl: CompactDuration,
    /// Process-level latch for identity validation and for the removal of
    /// unpublished routing rows. It re-runs after an error.
    ready: Arc<OnceCell<()>>,
}

impl<F, L> StateManagerProvider<F, L> {
    /// Creates the provider.
    ///
    /// `consumer_group` supplies state-cell identity through
    /// `segment::partition_segment_id`.
    /// Defer stores use the same derivation, so a partition's defer and state
    /// rows share one id for operational lookup.
    /// It also supplies the descriptor-identity table's `group_id` partition
    /// key. The first acquire validates that identity once per process.
    ///
    /// Timers retain the separate
    /// [`Segment::for_partition`](crate::timers::store::Segment::for_partition)
    /// formula with `NAMESPACE_URL` until migration.
    /// Timer retirement and legacy admission use the partition's trigger store.
    /// They address timers by key, type, and time, never by state segment
    /// id.
    #[must_use]
    pub(crate) fn new(
        backend: F,
        loader: L,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        dedup_ttl: CompactDuration,
    ) -> Self {
        Self {
            backend,
            loader,
            registry,
            consumer_group,
            dedup_ttl,
            ready: Arc::new(OnceCell::new()),
        }
    }
}

impl<F, L, T> PartitionStateProvider<T> for StateManagerProvider<F, L>
where
    F: StateBackendFactory<T>,
    L: Clone + Send + Sync + 'static,
    T: Send,
{
    type AcquireError = StateAcquireError<IdentityErr<F::Backend>, PublicationErr<F::Backend>>;
    type Manager = StateManager<F::Backend, L>;

    async fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> Result<Self::Manager, Self::AcquireError> {
        let segment_id = partition_segment_id(topic, partition, &self.consumer_group);
        let backend = self.backend.for_partition(topic, partition, triggers);
        let dedup = backend.dedup();
        // Invariant: no state op executes under an unvalidated identity —
        // the manager does not exist until the registered descriptors match
        // the group's frozen identity rows. The identity table is group-global,
        // so validation is a once-per-process latch (`get_or_try_init`
        // coalesces concurrent first-acquires and re-runs on a
        // transient `Err`); any partition's identity handle is
        // equivalent. Identity lives on the shared control-plane store,
        // decoupled from any kind's data store.
        let identity = backend.identity();
        let routes = backend.routes();
        self.ready
            .get_or_try_init(|| async {
                acquire_descriptor_identities(&identity, &self.registry, &self.consumer_group)
                    .await
                    .map_err(StateAcquireError::Identity)?;
                // An unvalidated registry must not change routing rows.
                routes
                    .withdraw()
                    .await
                    .map_err(StateAcquireError::Withdrawal)
            })
            .await?;
        let routes = routes
            .for_topic(topic)
            .ok_or(StateAcquireError::Unrouted(topic))?;
        Ok(StateManager {
            inner: Arc::new(StateManagerInner {
                cell: backend.cell(),
                dirty: Arc::new(DirtyStore::new()),
                dedup,
                loader: self.loader.clone(),
                registry: self.registry.clone(),
                segment_id,
                dedup_ttl: self.dedup_ttl,
                checks: backend.checks(),
                routes,
            }),
        })
    }
}

/// Error raised when a [`StateManagerProvider`] cannot acquire a
/// partition's manager.
#[derive(Debug, Error)]
pub enum StateAcquireError<StoreErr, PublicationErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// Durable descriptor-identity validation failed. A mismatch is
    /// Permanent and recurs until the deployed descriptors match the
    /// segment's frozen identity; a store failure retries on the next
    /// acquisition attempt.
    #[error("keyed-state descriptor identity acquisition failed: {0:#}")]
    Identity(DescriptorIdentityError<StoreErr>),

    /// The store could not remove the routing rows of a collection that is
    /// not published. The next acquisition attempt tries again.
    #[error("cannot remove the routing rows of an unpublished collection: {0:#}")]
    Withdrawal(PublicationErr),

    /// Kafka assigned a topic that the consumer does not subscribe to, so the
    /// topic has no routes. A consumer that publishes state subscribes only to
    /// literal topics, so this error marks a broken invariant.
    #[error("topic {0} has no keyed-state routes because the consumer does not subscribe to it")]
    Unrouted(Topic),
}

impl<StoreErr, PublicationErr> ClassifyError for StateAcquireError<StoreErr, PublicationErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Identity(e) => e.classify_error(),
            Self::Withdrawal(e) => e.classify_error(),
            Self::Unrouted(_) => ErrorCategory::Permanent,
        }
    }
}
