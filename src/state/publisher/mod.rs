//! Publication of keyed-state routing rows.
//!
//! A reader in another consumer group finds the state of a collection through
//! its routing rows. [`TopicTable`] owns the rule for when a row is written.

use crate::state::access::StateAccessError;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{STATE_FANOUT_CONCURRENCY, StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::{ConsumerGroup, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

#[cfg(test)]
mod tests;

/// The routing rows of one consumer process. A process builds them once.
#[derive(Clone)]
pub enum Routes<S> {
    /// The consumer has no subsystem, so it owns no routing row.
    Unpublished,
    /// The consumer owns the routing rows of its group under its subsystem.
    Subsystem(Arc<Publisher<S>>),
}

/// The routing rows of one consumer group under one subsystem.
pub struct Publisher<S> {
    store: S,
    subsystem: SubsystemName,
    group: ConsumerGroup,
    /// The registered collections that are not published.
    private: Box<[(StateType, StateName)]>,
    /// The table of each subscribed topic, or `None` when no collection is
    /// published.
    topics: Option<HashMap<Topic, Arc<TopicTable<S>>>>,
}

/// The routes of one topic. The partitions of the topic share them.
#[derive(Clone)]
pub(crate) enum TopicRoutes<S> {
    /// The topic writes no routing row.
    Unpublished,
    /// The topic writes the routing rows of the published collections.
    Published(Arc<TopicTable<S>>),
}

/// The routing row of one topic, and one route for each published collection.
///
/// Invariant: a group advertises a collection under a topic only if a handler
/// for that topic wrote the collection. The row is durable before the first
/// cell write of the collection from the topic. An extra row is allowed. A
/// missing row is never allowed.
///
/// Never remove the rows of a published collection. A process that closed a
/// latch does not write the row again.
pub(crate) struct TopicTable<S> {
    store: S,
    subsystem: SubsystemName,
    row: StatePublication,
    routes: Box<[Route]>,
}

/// The latch of one published collection. It shares one upsert between
/// concurrent first writers. A failed upsert leaves the latch open, so the next
/// write tries again.
struct Route {
    state_type: StateType,
    name: StateName,
    durable: OnceCell<()>,
}

impl<S: PublicationStore> Routes<S> {
    /// Builds the routes of `group` under `subsystem`.
    ///
    /// `counts` holds the partition count of each subscribed topic. The routes
    /// read it only when a collection is published.
    pub(crate) fn new(
        store: S,
        subsystem: SubsystemName,
        registry: &CollectionDefRegistry,
        group: ConsumerGroup,
        counts: impl IntoIterator<Item = (Topic, PartitionCount)>,
    ) -> Self {
        let (published, private): (Vec<_>, Vec<_>) = registry
            .collections()
            .map(|(state_type, name)| (state_type, name.clone()))
            .partition(|(state_type, name)| registry.is_published(*state_type, name));

        let topics = (!published.is_empty()).then(|| {
            counts
                .into_iter()
                .map(|(topic, partition_count)| {
                    let routes = published
                        .iter()
                        .map(|(state_type, name)| Route {
                            state_type: *state_type,
                            name: name.clone(),
                            durable: OnceCell::new(),
                        })
                        .collect();
                    let table = TopicTable {
                        store: store.clone(),
                        subsystem: subsystem.clone(),
                        row: StatePublication {
                            group_id: group.clone(),
                            topic,
                            partition_count,
                        },
                        routes,
                    };
                    (topic, Arc::new(table))
                })
                .collect()
        });

        Self::Subsystem(Arc::new(Publisher {
            store,
            subsystem,
            group,
            private: private.into(),
            topics,
        }))
    }

    /// Removes the rows of the group for each registered collection that is
    /// not published. This step retires a collection that was published
    /// before. Run it once for each process, before the first write.
    ///
    /// # Errors
    ///
    /// The store error of a failed removal.
    pub(crate) async fn withdraw(&self) -> Result<(), S::Error> {
        let Self::Subsystem(publisher) = self else {
            return Ok(());
        };
        let Publisher {
            store,
            subsystem,
            group,
            private,
            ..
        } = publisher.as_ref();
        stream::iter(private.iter())
            .map(Ok)
            .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |(state_type, name)| {
                store.remove_group(subsystem, *state_type, name, group)
            })
            .await
    }

    /// The routes of `topic`, for the sessions of its partitions.
    ///
    /// Returns `None` when a collection is published and the consumer does
    /// not subscribe to `topic`.
    pub(crate) fn for_topic(&self, topic: Topic) -> Option<TopicRoutes<S>> {
        let Self::Subsystem(publisher) = self else {
            return Some(TopicRoutes::Unpublished);
        };
        let Some(topics) = &publisher.topics else {
            return Some(TopicRoutes::Unpublished);
        };
        topics.get(&topic).cloned().map(TopicRoutes::Published)
    }
}

impl<S: PublicationStore> TopicRoutes<S> {
    /// Makes the routing row of the collection durable. Call it before each
    /// durable write of the collection's cells.
    ///
    /// A collection that is not published has no route and writes no row.
    ///
    /// # Errors
    ///
    /// [`StateAccessError::Store`] when the upsert fails.
    pub(crate) async fn ensure(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Result<(), StateAccessError> {
        let Self::Published(table) = self else {
            return Ok(());
        };
        let TopicTable {
            store,
            subsystem,
            row,
            routes,
        } = table.as_ref();
        // A scan suits the few published collections, and it hashes nothing.
        let Some(route) = routes
            .iter()
            .find(|route| route.state_type == state_type && route.name == *name)
        else {
            return Ok(());
        };
        route
            .durable
            .get_or_try_init(|| store.upsert(subsystem, state_type, name, row))
            .await
            .map(|&()| ())
            .map_err(|error| StateAccessError::store(&error))
    }
}
