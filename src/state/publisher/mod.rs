//! Assignment-owned publication of keyed-state routing rows.
//!
//! Partition zero of the first topic in lexical order owns the complete
//! routing set. Kafka assigns that partition to at most one group member.
//! All group members must use the same topic set so they select one leader.

use crate::error::ClassifyError;
use crate::state::STATE_FANOUT_CONCURRENCY;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::{ConsumerGroup, Partition, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
use std::sync::Arc;

#[cfg(test)]
mod tests;

const PUBLICATION_PARTITION: Partition = 0;

/// Publishes routing rows during the owning partition's state acquisition.
pub trait AssignmentPublisher: Clone + Send + Sync + 'static {
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Replaces the routing set if `(topic, partition)` owns publication.
    fn publish_if_owner(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'_, Self>;
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
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'_> {
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

    /// Pairs every topic with the partition count `count_for` reports.
    ///
    /// # Errors
    ///
    /// The first error `count_for` returns.
    pub(crate) fn route<E>(
        &self,
        group: &ConsumerGroup,
        mut count_for: impl FnMut(&str) -> Result<PartitionCount, E>,
    ) -> Result<RoutingSet, E> {
        let rows = self
            .all
            .iter()
            .map(|&topic| {
                Ok(StatePublication {
                    group_id: group.clone(),
                    topic,
                    partition_count: count_for(topic.as_ref())?,
                })
            })
            .collect::<Result<_, E>>()?;
        Ok(RoutingSet {
            group: group.clone(),
            rows,
            leader: self.leader,
        })
    }

    /// The empty routing set, for a consumer that publishes no collection.
    pub(crate) fn withdrawal(&self, group: &ConsumerGroup) -> RoutingSet {
        RoutingSet {
            group: group.clone(),
            rows: Arc::new([]),
            leader: self.leader,
        }
    }
}

/// One consumer group's complete routing set: one row for each subscribed
/// topic, with the topic's partition count.
///
/// The rows are empty only when no collection is published. The owner then
/// fetches no counts and only withdraws the group's old rows.
///
/// Partition counts never change, because a Prosody deployment never increases
/// them. So the set is built once, at construction, and every row is final.
///
/// Do not take counts from librdkafka statistics. A statistics report lists
/// only the topics that this client held, and the owner must count every
/// topic.
#[derive(Clone)]
pub(crate) struct RoutingSet {
    group: ConsumerGroup,
    rows: Arc<[StatePublication]>,
    leader: Topic,
}

/// The sole writer for one consumer group's complete routing set.
#[derive(Clone)]
pub(crate) struct PublicationOwner<S> {
    subsystem: SubsystemName,
    store: S,
    registry: Arc<CollectionDefRegistry>,
    routes: RoutingSet,
}

impl<S: PublicationStore> PublicationOwner<S> {
    /// Creates the owner that writes `routes`.
    pub(crate) fn new(
        subsystem: SubsystemName,
        store: S,
        registry: Arc<CollectionDefRegistry>,
        routes: RoutingSet,
    ) -> Self {
        Self {
            subsystem,
            store,
            registry,
            routes,
        }
    }

    async fn publish(&self) -> Result<(), S::Error> {
        let group = &self.routes.group;
        stream::iter(self.registry.collections())
            .map(Ok)
            .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |(state_type, name)| async move {
                self.store
                    .remove_group(&self.subsystem, state_type, name, group)
                    .await?;
                if self.registry.is_published(state_type, name) {
                    stream::iter(self.routes.rows.iter())
                        .map(Ok)
                        .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |row| {
                            self.store.upsert(&self.subsystem, state_type, name, row)
                        })
                        .await?;
                }
                Ok(())
            })
            .await
    }
}

impl<S: PublicationStore> AssignmentPublisher for PublicationOwner<S> {
    type Error = S::Error;

    async fn publish_if_owner(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<(), Self::Error> {
        if topic != self.routes.leader || partition != PUBLICATION_PARTITION {
            return Ok(());
        }
        self.publish().await
    }
}
