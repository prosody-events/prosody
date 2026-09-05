//! Store message commit markers.

use crate::error::ClassifyError;
use crate::{Partition, Topic};
use std::error::Error;
use std::future::Future;
use uuid::Uuid;

/// The result of a deduplication lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Presence {
    /// No marker exists.
    Absent,
    /// This assignment recorded or read the marker.
    /// Its settle finished, armed a backstop, or kept its source for the next
    /// assignment.
    Settled,
    /// An earlier assignment or owner recorded the marker.
    Inherited,
}

impl Presence {
    /// Return whether the marker exists in either store.
    #[must_use]
    pub fn is_present(self) -> bool {
        !matches!(self, Self::Absent)
    }
}

/// Store and read message commit markers.
pub trait DeduplicationStore: Clone + Send + Sync + 'static {
    /// Store failure. The commit oracle preserves its classification.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Read a marker and identify whether this assignment already observed it.
    fn lookup(&self, id: Uuid) -> impl Future<Output = Result<Presence, Self::Error>> + Send;

    /// Records a deduplication identifier.
    fn insert(&self, id: Uuid) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Factory for creating per-partition [`DeduplicationStore`] instances.
pub trait DeduplicationStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: DeduplicationStore;

    /// Create a store for the given partition and consumer group.
    fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str)
    -> Self::Store;
}
