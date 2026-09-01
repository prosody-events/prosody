//! Assignment-owned publication of keyed-state routing rows.
//!
//! Partition zero of the first topic in lexical order owns the complete
//! routing set. Kafka assigns that partition to at most one group member.
//! All group members must use the same topic set so they select one leader.

use crate::consumer::observer::{KafkaObserver, PartitionCountObservationError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::STATE_FANOUT_CONCURRENCY;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::{Partition, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use smallvec::SmallVec;
use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
use std::sync::Arc;
use thiserror::Error;

#[cfg(test)]
mod tests;

const PUBLICATION_PARTITION: Partition = 0;

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

/// Publishes routing rows during the owning partition's state acquisition.
pub trait AssignmentPublisher: Clone + Send + Sync + 'static {
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Replaces the routing set if `(topic, partition)` owns publication.
    fn publish_if_owner(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<P> AssignmentPublisher for Option<P>
where
    P: AssignmentPublisher,
{
    type Error = P::Error;

    async fn publish_if_owner(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<(), Self::Error> {
        match self {
            Some(publisher) => publisher.publish_if_owner(topic, partition).await,
            None => Ok(()),
        }
    }
}

/// A publisher for state providers that do not support publication.
#[derive(Clone, Copy, Debug)]
pub struct NoPublisher;

impl AssignmentPublisher for NoPublisher {
    type Error = Infallible;

    fn publish_if_owner(
        &self,
        _topic: Topic,
        _partition: Partition,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }
}

/// A non-empty topic set and its deterministic publication leader.
#[derive(Clone)]
pub(crate) struct PublicationTopics {
    all: Arc<[Topic]>,
    leader: Topic,
}

impl PublicationTopics {
    /// Sorts and deduplicates `topics`, or returns `None` for an empty set.
    pub(crate) fn new(mut topics: Vec<Topic>) -> Option<Self> {
        topics.sort_unstable();
        topics.dedup();
        let leader = topics.first().copied()?;
        Some(Self {
            all: topics.into(),
            leader,
        })
    }
}

/// The sole writer for one consumer group's complete routing set.
#[derive(Clone)]
pub(crate) struct PublicationOwner<S, N> {
    subsystem: SubsystemName,
    group: Arc<str>,
    store: S,
    counts: N,
    registry: Arc<CollectionDefRegistry>,
    topics: PublicationTopics,
}

impl<S, N> PublicationOwner<S, N>
where
    S: PublicationStore,
    N: PartitionCountSource,
{
    /// Creates an owner for the supplied non-empty topic set.
    pub(crate) fn new(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: S,
        counts: N,
        registry: Arc<CollectionDefRegistry>,
        topics: PublicationTopics,
    ) -> Self {
        Self {
            subsystem,
            group,
            store,
            counts,
            registry,
            topics,
        }
    }

    async fn publish(&self) -> Result<(), PublicationError<S::Error, N::Error>> {
        let rows: SmallVec<[StatePublication; 2]> = self
            .topics
            .all
            .iter()
            .copied()
            .map(|topic| {
                self.counts
                    .count_for(topic.as_ref())
                    .map(|partition_count| StatePublication {
                        group_id: self.group.clone(),
                        topic,
                        partition_count,
                    })
                    .map_err(PublicationError::Count)
            })
            .collect::<Result<_, _>>()?;

        stream::iter(self.registry.collections())
            .map(Ok)
            .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |(state_type, name)| {
                let rows = &rows;
                async move {
                    self.store
                        .remove_group(&self.subsystem, state_type, name, &self.group)
                        .await
                        .map_err(PublicationError::Store)?;
                    if self.registry.is_published(state_type, name) {
                        stream::iter(rows)
                            .map(Ok)
                            .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |row| async move {
                                self.store
                                    .upsert(&self.subsystem, state_type, name, row)
                                    .await
                                    .map_err(PublicationError::Store)
                            })
                            .await?;
                    }
                    Ok(())
                }
            })
            .await
    }
}

impl<S, N> AssignmentPublisher for PublicationOwner<S, N>
where
    S: PublicationStore,
    N: PartitionCountSource,
{
    type Error = PublicationError<S::Error, N::Error>;

    async fn publish_if_owner(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<(), Self::Error> {
        if topic != self.topics.leader || partition != PUBLICATION_PARTITION {
            return Ok(());
        }
        self.publish().await
    }
}

/// A routing-set publication failure.
#[derive(Debug, Error)]
pub(crate) enum PublicationError<S, N> {
    #[error(transparent)]
    Store(S),
    #[error(transparent)]
    Count(N),
}

impl<S: ClassifyError, N: ClassifyError> ClassifyError for PublicationError<S, N> {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(error) => error.classify_error(),
            Self::Count(error) => error.classify_error(),
        }
    }
}
