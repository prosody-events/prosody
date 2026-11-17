//! Publication of keyed-state routing rows.
//!
//! A reader in another consumer group finds the state of a collection through
//! its routing rows. [`Assignment`] owns the rule for when a row is written.

use crate::error::{ClassifyError, ErrorCategory, RetryAlert, log_at};
use crate::state::access::StateAccessError;
use crate::state::identity::CollectionId;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{STATE_FANOUT_CONCURRENCY, StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use crate::{ConsumerGroup, Topic};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

#[cfg(test)]
mod tests;

/// The routing rows of one consumer process. A process builds them once.
///
/// `Routes` and `Publisher` are `pub` because the public trait `StateBackend`
/// returns `Routes`.
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
    /// The partition count of each subscribed topic. `for_topic` reads it only
    /// when a collection is published.
    counts: HashMap<Topic, PartitionCount>,
    /// Holds `true` when `withdraw` is complete. A removal holds the lock.
    withdrawn: Mutex<bool>,
    /// Chooses the log level of a failed removal.
    alert: RetryAlert,
}

/// The routes of one partition assignment.
#[derive(Clone)]
pub(crate) enum TopicRoutes<S> {
    /// The assignment writes no routing row.
    Unpublished,
    /// The assignment writes the routing rows of the published collections.
    Published(Arc<Assignment<S>>),
}

/// The routing rows that one partition assignment writes. It holds the row
/// value and one route for each published collection.
///
/// Invariant: a group advertises a collection under a topic only if a handler
/// for that topic tried to write the collection. In each partition assignment,
/// the row is durable before the first stage or direct write of the collection.
/// Prosody never removes the row of a published collection. If something else
/// removes the row, the next assignment of the topic writes it again.
///
/// The latch of a route shares one upsert between concurrent first writers. A
/// failed upsert leaves the latch open, so the next write tries again.
pub(crate) struct Assignment<S> {
    publisher: Arc<Publisher<S>>,
    row: StatePublication,
    routes: Box<[Route]>,
}

/// One published collection, and the latch that closes when its routing row is
/// durable in this assignment.
struct Route {
    state_type: StateType,
    name: StateName,
    durable: OnceCell<()>,
}

impl<S> Routes<S> {
    /// Builds the routes of `group` under `subsystem`.
    ///
    /// `counts` holds the partition count of each subscribed topic.
    pub(crate) fn new(
        store: S,
        subsystem: SubsystemName,
        group: ConsumerGroup,
        counts: impl IntoIterator<Item = (Topic, PartitionCount)>,
    ) -> Self {
        Self::Subsystem(Arc::new(Publisher {
            store,
            subsystem,
            group,
            counts: counts.into_iter().collect(),
            withdrawn: Mutex::new(false),
            alert: RetryAlert::default(),
        }))
    }

    /// Removes the rows of the group for each collection in `registry` that is
    /// not published. This step retires a collection that was published
    /// before. It runs once for each process.
    ///
    /// An acquisition never waits for the removal of another acquisition. A
    /// failed removal never fails the acquisition. This call logs the error,
    /// and a later acquisition tries again.
    pub(crate) async fn withdraw(&self, registry: &CollectionDefRegistry)
    where
        S: PublicationStore,
    {
        let Self::Subsystem(publisher) = self else {
            return;
        };
        let Ok(mut done) = publisher.withdrawn.try_lock() else {
            return;
        };
        if *done {
            return;
        }

        let removal = stream::iter(
            registry
                .collections()
                .filter(|(state_type, name)| !registry.is_published(*state_type, name)),
        )
        .map(Ok)
        .try_for_each_concurrent(STATE_FANOUT_CONCURRENCY, |(state_type, name)| {
            publisher
                .store
                .remove_group(&publisher.subsystem, state_type, name, &publisher.group)
        })
        .await;
        match removal {
            Ok(()) => *done = true,
            Err(error) => log_at!(
                publisher.alert.level(error.classify_error()),
                error = %error,
                subsystem = %publisher.subsystem,
                group = %publisher.group,
                "Prosody cannot remove the routing rows of collections that are no longer \
                 published. Readers in other consumer groups can still find these collections. \
                 Prosody tries again at the next partition assignment. If this message \
                 repeats, make sure that Cassandra is available."
            ),
        }
    }

    /// The routes of a new partition assignment of `topic`, with one route for
    /// each published collection in `registry`. Each call makes new latches,
    /// so call it once for each assignment.
    ///
    /// Returns `None` when a collection is published and the consumer does not
    /// subscribe to `topic`.
    pub(crate) fn for_topic(
        &self,
        topic: Topic,
        registry: &CollectionDefRegistry,
    ) -> Option<TopicRoutes<S>> {
        let Self::Subsystem(publisher) = self else {
            return Some(TopicRoutes::Unpublished);
        };
        let routes: Box<[Route]> = registry
            .collections()
            .filter(|(state_type, name)| registry.is_published(*state_type, name))
            .map(|(state_type, name)| Route {
                state_type,
                name: name.clone(),
                durable: OnceCell::new(),
            })
            .collect();
        if routes.is_empty() {
            return Some(TopicRoutes::Unpublished);
        }

        let row = StatePublication {
            group_id: publisher.group.clone(),
            topic,
            partition_count: *publisher.counts.get(&topic)?,
        };
        Some(TopicRoutes::Published(Arc::new(Assignment {
            publisher: publisher.clone(),
            row,
            routes,
        })))
    }
}

impl<S: PublicationStore> TopicRoutes<S> {
    /// Makes the routing row of the collection `id` durable. Call it
    /// immediately before each durable write of the collection's cells.
    ///
    /// A collection that is not published has no route and writes no row.
    ///
    /// # Errors
    ///
    /// [`StateAccessError::Store`] when the upsert fails. Its category is never
    /// `Permanent`.
    pub(crate) async fn ensure(&self, id: &CollectionId) -> Result<(), StateAccessError> {
        let Self::Published(assignment) = self else {
            return Ok(());
        };
        let Assignment {
            publisher,
            row,
            routes,
        } = assignment.as_ref();
        let (state_type, name) = (id.state_type(), id.name());
        // A scan suits the few published collections, and it hashes nothing.
        let Some(route) = routes
            .iter()
            .find(|route| route.state_type == state_type && route.name == *name)
        else {
            return Ok(());
        };
        route
            .durable
            .get_or_try_init(|| {
                publisher
                    .store
                    .upsert(&publisher.subsystem, state_type, name, row)
            })
            .await
            .map_err(|error| {
                // A routing failure says nothing about the event's data, so it
                // retries as Transient instead of discarding the event's state.
                let category = match error.classify_error() {
                    ErrorCategory::Terminal => ErrorCategory::Terminal,
                    ErrorCategory::Transient | ErrorCategory::Permanent => ErrorCategory::Transient,
                };
                StateAccessError::store_as(&error, category)
            })?;
        Ok(())
    }
}
