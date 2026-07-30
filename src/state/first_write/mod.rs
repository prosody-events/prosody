//! First-write publication of keyed-state routing rows.
//!
//! A published collection is discoverable only after its routing row exists.
//! [`FirstWritePublisher::ensure_one`] establishes that row before any durable
//! state write can commit.

use crate::Topic;
use crate::consumer::observer::{KafkaObserver, PartitionCountObservationError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use quick_cache::sync::Cache;
use std::convert::Infallible;
use std::error::Error;
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, warn};

#[cfg(test)]
mod tests;

const PUBLICATION_MEMO_CAPACITY: usize = 4096;

/// Supplies the current partition count for a published topic.
pub(crate) trait PartitionCountSource: Clone + Send + Sync + 'static {
    type Error: ClassifyError + Error + Send + Sync + 'static;

    fn count_for(&self, topic: &str) -> Result<PartitionCount, Self::Error>;
}

impl PartitionCountSource for KafkaObserver {
    type Error = PartitionCountObservationError;

    fn count_for(&self, topic: &str) -> Result<PartitionCount, Self::Error> {
        KafkaObserver::partition_count(self, topic)
    }
}

/// A partition count used by the in-memory backend.
#[derive(Clone, Copy)]
pub(crate) struct FixedPartitionCount(pub(crate) PartitionCount);

impl PartitionCountSource for FixedPartitionCount {
    type Error = Infallible;

    fn count_for(&self, _topic: &str) -> Result<PartitionCount, Self::Error> {
        Ok(self.0)
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct PublicationMemoKey {
    state_type: StateType,
    name: StateName,
    topic: Topic,
}

/// Process-wide publication resources shared by every partition.
#[derive(Clone)]
pub(crate) struct PublisherTemplate<S: PublicationStore, N: PartitionCountSource> {
    subsystem: SubsystemName,
    group: Arc<str>,
    store: Arc<S>,
    counts: N,
    memo: Arc<Cache<PublicationMemoKey, ()>>,
    registry: Arc<CollectionDefRegistry>,
}

impl<S: PublicationStore, N: PartitionCountSource> PublisherTemplate<S, N> {
    pub(crate) fn new(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<S>,
        counts: N,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self::with_memo_capacity(
            subsystem,
            group,
            store,
            counts,
            registry,
            PUBLICATION_MEMO_CAPACITY,
        )
    }

    fn with_memo_capacity(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<S>,
        counts: N,
        registry: Arc<CollectionDefRegistry>,
        capacity: usize,
    ) -> Self {
        Self {
            subsystem,
            group,
            store,
            counts,
            memo: Arc::new(Cache::new(capacity)),
            registry,
        }
    }

    pub(crate) fn bind(&self, topic: Topic) -> FirstWritePublisher<S, N> {
        FirstWritePublisher {
            template: self.clone(),
            topic,
        }
    }
}

/// A publication template bound to one session topic.
#[derive(Clone)]
pub(crate) struct FirstWritePublisher<S: PublicationStore, N: PartitionCountSource> {
    template: PublisherTemplate<S, N>,
    topic: Topic,
}

/// The publication operation a state session needs.
pub trait FirstWriteBarrier: Clone + Send + Sync + 'static {
    type Error: ClassifyError + Error + Send + Sync + 'static;

    fn publish_if_needed(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<S: PublicationStore, N: PartitionCountSource> FirstWriteBarrier for FirstWritePublisher<S, N> {
    type Error = FirstWriteError<S::Error, N::Error>;

    async fn publish_if_needed(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Result<(), Self::Error> {
        FirstWritePublisher::ensure_one(self, state_type, name).await
    }
}

#[derive(Clone, Copy)]
/// A first-write barrier for backends without publication.
pub struct NoPublisher;

impl FirstWriteBarrier for NoPublisher {
    type Error = Infallible;

    async fn publish_if_needed(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<S: PublicationStore, N: PartitionCountSource> FirstWritePublisher<S, N> {
    /// Ensures the routing row exists before a published collection is written.
    ///
    /// The memo latches only after the idempotent upsert is acknowledged. A
    /// private collection never consults the memo or store. The best-effort
    /// read detects unsupported topic repartitioning but never blocks the
    /// required upsert.
    pub(crate) async fn ensure_one(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Result<(), FirstWriteError<S::Error, N::Error>> {
        let template = &self.template;
        if !template.registry.is_published(state_type, name) {
            return Ok(());
        }
        let key = PublicationMemoKey {
            state_type,
            name: name.clone(),
            topic: self.topic,
        };
        if template.memo.get(&key).is_some() {
            return Ok(());
        }
        let count = template
            .counts
            .count_for(self.topic.as_ref())
            .map_err(FirstWriteError::Count)?;
        match template
            .store
            .read_publications(&template.subsystem, state_type, name)
            .await
        {
            Ok(rows) => {
                if let Some(stored) = rows.iter().find(|row| {
                    row.group_id.as_ref() == template.group.as_ref() && row.topic == self.topic
                }) && stored.partition_count != count
                {
                    error!(
                        collection = %name.as_str(),
                        topic = %self.topic.as_ref(),
                        stored = i32::from(stored.partition_count),
                        current = i32::from(count),
                        "keyed-state publication partition count changed"
                    );
                }
            }
            Err(error) => warn!(
                collection = %name.as_str(),
                topic = %self.topic.as_ref(),
                error = %error,
                "publication read failed; proceeding with the required upsert"
            ),
        }
        template
            .store
            .upsert(
                &template.subsystem,
                state_type,
                name,
                &StatePublication {
                    group_id: template.group.clone(),
                    topic: self.topic,
                    partition_count: count,
                },
            )
            .await
            .map_err(FirstWriteError::Store)?;
        template.memo.insert(key, ());
        Ok(())
    }
}

/// Removes this group's rows for every registered private collection.
pub(crate) async fn reconcile_publications<S: PublicationStore>(
    store: &S,
    registry: &CollectionDefRegistry,
    subsystem: &SubsystemName,
    group: &str,
) -> Result<(), S::Error> {
    for (state_type, name) in registry.collections() {
        if !registry.is_published(state_type, name) {
            store
                .remove_group(subsystem, state_type, name, group)
                .await?;
        }
    }
    Ok(())
}

/// A publication barrier failure.
#[derive(Debug, Error)]
pub(crate) enum FirstWriteError<S, N> {
    #[error(transparent)]
    Store(S),
    #[error(transparent)]
    Count(N),
}

impl<S: ClassifyError, N: ClassifyError> ClassifyError for FirstWriteError<S, N> {
    fn classify_error(&self) -> ErrorCategory {
        let category = match self {
            Self::Store(error) => error.classify_error(),
            Self::Count(error) => error.classify_error(),
        };
        match category {
            ErrorCategory::Permanent => ErrorCategory::Permanent,
            ErrorCategory::Transient | ErrorCategory::Terminal => ErrorCategory::Transient,
        }
    }
}
