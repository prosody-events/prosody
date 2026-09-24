//! The process-wide provider that acquires a partition's state manager.

use super::{IdentityErr, PartitionStateProvider, StateManager, StateManagerInner};
use crate::error::{ClassifyError, ErrorCategory};
use crate::segment::partition_segment_id;
use crate::state::descriptor_identity::{DescriptorIdentityError, acquire_descriptor_identities};
use crate::state::dirty::DirtyStore;
use crate::state::publisher::{AssignmentPublisher, NoPublisher};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{StateBackend, StateBackendFactory};
use crate::timers::duration::CompactDuration;
use crate::{Partition, Topic};
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::OnceCell;

/// Process-wide [`PartitionStateProvider`] over a
/// `StateBackendFactory`: acquisition publishes routing and validates
/// descriptor identities before it mints the manager.
#[derive(Clone)]
pub struct StateManagerProvider<F, L, P = NoPublisher> {
    backend: F,
    loader: L,
    publisher: P,
    registry: Arc<CollectionDefRegistry>,
    consumer_group: Arc<str>,
    dedup_ttl: CompactDuration,
    /// Process-level latch for descriptor-identity validation. The identity
    /// table is group-global, so validating the registry against it is a
    /// once-per-process concern, not per-partition. Shared across provider
    /// clones (an `Arc`), `get_or_try_init` runs the validation once on
    /// success and re-runs on `Err` — preserving retry-until-shutdown while
    /// the invariant *no state op runs under an unvalidated identity* holds.
    validated: Arc<OnceCell<()>>,
}

impl<F, L, P> StateManagerProvider<F, L, P> {
    /// Creates the provider.
    ///
    /// `publisher` uses the assignment that this provider receives. It does not
    /// observe Kafka or track assignments itself.
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
        publisher: P,
        registry: Arc<CollectionDefRegistry>,
        consumer_group: Arc<str>,
        dedup_ttl: CompactDuration,
    ) -> Self {
        Self {
            backend,
            loader,
            publisher,
            registry,
            consumer_group,
            dedup_ttl,
            validated: Arc::new(OnceCell::new()),
        }
    }
}

impl<F, L, P, T> PartitionStateProvider<T> for StateManagerProvider<F, L, P>
where
    F: StateBackendFactory<T>,
    L: Clone + Send + Sync + 'static,
    P: AssignmentPublisher,
    T: Send,
{
    type AcquireError = StateAcquireError<F::Error, IdentityErr<F::Backend>, P::Error>;
    type Manager = StateManager<F::Backend, L>;

    async fn acquire(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> Result<Self::Manager, Self::AcquireError> {
        let segment_id = partition_segment_id(topic, partition, &self.consumer_group);
        let backend = self
            .backend
            .for_partition(topic, partition, triggers)
            .map_err(StateAcquireError::Factory)?;
        let dedup = backend.dedup();
        // Invariant: no state op executes under an unvalidated identity —
        // the manager does not exist until the registered descriptors match
        // the group's frozen identity rows. The identity table is group-global,
        // so validation is a once-per-process latch (`get_or_try_init` coalesces
        // concurrent first-acquires and re-runs on a transient `Err`); any
        // partition's identity handle is equivalent. Identity lives on the
        // shared control-plane store, decoupled from any kind's data store.
        let identity = backend.identity();
        self.validated
            .get_or_try_init(|| {
                acquire_descriptor_identities(&identity, &self.registry, &self.consumer_group)
            })
            .await
            .map_err(StateAcquireError::Identity)?;
        self.publisher
            .publish_if_owner(topic, partition)
            .await
            .map_err(StateAcquireError::Publication)?;
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
            }),
        })
    }
}

/// Error raised when a [`StateManagerProvider`] cannot acquire a
/// partition's manager.
#[derive(Debug, Error)]
pub enum StateAcquireError<FactoryErr, StoreErr, PublicationErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// Routing-set publication failed on its owning assignment.
    #[error("keyed-state publication failed at partition acquisition")]
    Publication(#[source] PublicationErr),

    /// The backend factory failed to mint the partition's backend.
    #[error("keyed-state backend factory failed at partition acquisition")]
    Factory(#[source] FactoryErr),

    /// Durable descriptor-identity validation failed. A mismatch is
    /// Permanent and recurs until the deployed descriptors match the
    /// segment's frozen identity; a store failure retries on the next
    /// acquisition attempt.
    #[error("keyed-state descriptor identity acquisition failed")]
    Identity(#[source] DescriptorIdentityError<StoreErr>),
}

impl<FactoryErr, StoreErr, PublicationErr> ClassifyError
    for StateAcquireError<FactoryErr, StoreErr, PublicationErr>
where
    FactoryErr: ClassifyError + Error + Send + Sync + 'static,
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
    PublicationErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Publication(e) => e.classify_error(),
            Self::Factory(e) => e.classify_error(),
            Self::Identity(e) => e.classify_error(),
        }
    }
}
