//! Routing-only keyed-state publication store.
//!
//! The durable table a reader consults to find which consumer groups publish
//! a collection's state, and under which topics. A [`StatePublication`] row
//! carries only routing facts: group, topic, and partition count. It never
//! carries identity. A row that anyone can upsert cannot hold frozen data, so
//! identity is validated separately against `keyed_state_identity`.

use crate::Topic;
use crate::error::ClassifyError;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;

/// One published source of a collection's state: a `(group_id, topic)` pair and
/// the topic's partition count. Routing facts only — no identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatePublication {
    /// The publishing consumer group.
    pub group_id: Arc<str>,
    /// The topic whose messages wrote the state.
    pub topic: Topic,
    /// The topic's Kafka partition count (for the reader's key→partition step).
    pub partition_count: PartitionCount,
}

/// Durable routing table over `((subsystem, state_type, name), group_id,
/// topic)`.
///
/// A reader discovers a collection's sources by reading one
/// `(subsystem, state_type, name)` partition. A publisher advertises a source
/// with an idempotent [`upsert`] and withdraws it with [`remove`]. [`remove`]
/// is the removal path that bounds the memory store's RAM. No LWT, no TTL: a
/// source row is plain routing data. The `state_type` in the address
/// namespaces collections exactly as `keyed_state_identity` does, so a future
/// internal (non-`Application`) namespace can publish without a schema change.
///
/// [`upsert`]: PublicationStore::upsert
/// [`remove`]: PublicationStore::remove
pub trait PublicationStore: Clone + Send + Sync + 'static {
    /// Backend error; classified for the settle-path retry posture.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Idempotently records `row` under `(subsystem, state_type, name)`.
    /// Re-upsert of the same `(group_id, topic)` overwrites the routing facts
    /// in place.
    ///
    /// # Errors
    /// Backend failure (e.g. Cassandra unavailable).
    fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes the `(group_id, topic)` source of `(subsystem, state_type,
    /// name)`. Idempotent — removing an absent row is a no-op.
    ///
    /// # Errors
    /// Backend failure.
    fn remove(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
        topic: Topic,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// All published sources of `(subsystem, state_type, name)` — one
    /// partition read.
    ///
    /// # Errors
    /// Backend failure, or (Cassandra backend) a decoded partition count
    /// outside `[1, i32::MAX]`.
    fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = Result<Vec<StatePublication>, Self::Error>> + Send;
}
