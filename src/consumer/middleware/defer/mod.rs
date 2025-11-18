pub mod config;
pub mod error;
pub mod failure_tracker;
pub mod handler;
pub mod loader;
pub mod store;

#[cfg(test)]
pub mod tests;

pub use config::{DeferConfigError, DeferConfiguration, DeferConfigurationBuilder};
pub use error::DeferInitError;
pub use handler::DeferMiddleware;

use crate::{Key, Partition, Topic};
use uuid::Uuid;

/// State of a key in the defer system.
///
/// Tracks whether a key has deferred messages and the current retry count
/// for backoff calculations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferState {
    /// Key has no deferred messages.
    NotDeferred,
    /// Key has deferred messages with the given retry count.
    Deferred {
        /// Current retry count for backoff calculation.
        retry_count: u32,
    },
}

/// Generates a deterministic `UUIDv5` for a deferred message key.
///
/// The UUID is generated from the consumer group, topic, partition, and message
/// key, ensuring:
/// - Same logical key always maps to the same Cassandra partition
/// - Different consumer groups have isolated defer state
/// - Different partitions have isolated defer state
///
/// # Arguments
///
/// * `consumer_group` - The Kafka consumer group name
/// * `topic` - The Kafka topic
/// * `partition` - The partition number
/// * `key` - The message key
///
/// # Returns
///
/// A deterministic `UUIDv5` computed as:
/// `UUIDv5(NAMESPACE_OID, "{consumer_group}:{topic}/{partition}:{key}")`
#[must_use]
pub fn generate_key_id(
    consumer_group: &str,
    topic: &Topic,
    partition: Partition,
    key: &Key,
) -> Uuid {
    let namespace = Uuid::NAMESPACE_OID;
    let input = format!("{consumer_group}:{topic}/{partition}:{key}");
    Uuid::new_v5(&namespace, input.as_bytes())
}
