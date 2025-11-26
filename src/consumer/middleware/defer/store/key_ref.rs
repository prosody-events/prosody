//! Type-safe reference to defer key identity data.
//!
//! Provides [`DeferKeyRef`] which encapsulates the data needed to identify
//! a deferred message without exposing the internal UUID computation.

use crate::{Key, Partition, Topic};
use uuid::Uuid;

/// Type-safe reference to data identifying a deferred message.
///
/// # Purpose
///
/// Eliminates confusion between `Key` (message key) and the internal `Uuid`
/// (`defer_key`) by encapsulating UUID generation. Callers pass references to
/// identifying data; the store computes the UUID internally via
/// `Uuid::from(&key_ref)`.
///
/// # Benefits
///
/// 1. **Type safety**: Can't accidentally pass `Key` where `DeferKeyRef`
///    expected
/// 2. **Encapsulation**: `as_uuid()` is `pub(crate)`, UUID never leaks outside
///    store
/// 3. **Self-documenting**: API shows exactly what data is needed
/// 4. **Single point**: UUID generation happens in one place
///
/// # Example
///
/// ```ignore
/// // Creating a reference (no allocation)
/// let key_ref = DeferKeyRef::new(
///     &consumer_group,
///     topic,
///     partition,
///     &message_key,
/// );
///
/// // Passing to store (UUID computed internally)
/// store.defer_first_message(&key_ref, offset, expected_retry_time).await?;
///
/// // Querying store
/// let (next_offset, retry_count) = store.get_next_deferred_message(&key_ref).await?;
/// ```
#[derive(Clone, Copy, Debug)]
pub struct DeferKeyRef<'a> {
    /// Consumer group for state isolation.
    pub consumer_group: &'a str,

    /// Kafka topic.
    pub topic: Topic,

    /// Kafka partition.
    pub partition: Partition,

    /// Message key (business key for ordering).
    pub key: &'a Key,
}

impl<'a> DeferKeyRef<'a> {
    /// Creates a new defer key reference.
    ///
    /// Does NOT allocate or compute UUID - that happens lazily when
    /// converted via `Uuid::from(&key_ref)`.
    #[must_use]
    pub fn new(consumer_group: &'a str, topic: Topic, partition: Partition, key: &'a Key) -> Self {
        Self {
            consumer_group,
            topic,
            partition,
            key,
        }
    }
}

/// Computes the UUID for a defer key reference.
///
/// # Implementation
///
/// Computes a deterministic UUID v5:
/// 1. Format string: `"{consumer_group}:{topic}/{partition}:{key}"`
/// 2. UUID v5 from the formatted string using `NAMESPACE_OID`
impl From<&DeferKeyRef<'_>> for Uuid {
    fn from(key: &DeferKeyRef<'_>) -> Self {
        let namespace = Uuid::NAMESPACE_OID;
        let input = format!(
            "{}:{}/{}:{}",
            key.consumer_group, key.topic, key.partition, key.key
        );
        Uuid::new_v5(&namespace, input.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn key_ref_computes_deterministic_uuid() {
        let topic = Topic::from("test-topic");
        let key: Key = Arc::from("test-key");

        let key_ref1 = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key);
        let key_ref2 = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key);

        assert_eq!(Uuid::from(&key_ref1), Uuid::from(&key_ref2));
    }

    #[test]
    fn key_ref_different_groups_produce_different_uuids() {
        let topic = Topic::from("test-topic");
        let key: Key = Arc::from("test-key");

        let key_ref1 = DeferKeyRef::new("group-1", topic, Partition::from(0_i32), &key);
        let key_ref2 = DeferKeyRef::new("group-2", topic, Partition::from(0_i32), &key);

        assert_ne!(Uuid::from(&key_ref1), Uuid::from(&key_ref2));
    }

    #[test]
    fn key_ref_different_partitions_produce_different_uuids() {
        let topic = Topic::from("test-topic");
        let key: Key = Arc::from("test-key");

        let key_ref1 = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key);
        let key_ref2 = DeferKeyRef::new("group", topic, Partition::from(1_i32), &key);

        assert_ne!(Uuid::from(&key_ref1), Uuid::from(&key_ref2));
    }

    #[test]
    fn key_ref_different_topics_produce_different_uuids() {
        let topic1 = Topic::from("topic-1");
        let topic2 = Topic::from("topic-2");
        let key: Key = Arc::from("test-key");

        let key_ref1 = DeferKeyRef::new("group", topic1, Partition::from(0_i32), &key);
        let key_ref2 = DeferKeyRef::new("group", topic2, Partition::from(0_i32), &key);

        assert_ne!(Uuid::from(&key_ref1), Uuid::from(&key_ref2));
    }

    #[test]
    fn key_ref_different_keys_produce_different_uuids() {
        let topic = Topic::from("test-topic");
        let key1: Key = Arc::from("key-1");
        let key2: Key = Arc::from("key-2");

        let key_ref1 = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key1);
        let key_ref2 = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key2);

        assert_ne!(Uuid::from(&key_ref1), Uuid::from(&key_ref2));
    }

    #[test]
    fn key_ref_is_copy() {
        let topic = Topic::from("test-topic");
        let key: Key = Arc::from("test-key");

        let key_ref = DeferKeyRef::new("group", topic, Partition::from(0_i32), &key);
        let copied = key_ref; // Copy

        assert_eq!(Uuid::from(&key_ref), Uuid::from(&copied));
    }
}
