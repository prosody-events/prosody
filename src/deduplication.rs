//! Provides functionality for managing the deduplication of messages
//! based on their unique identifiers (event IDs).
//!
//! This module defines the `IdempotenceCache` structure, which is used to
//! store and check event IDs for deduplication purposes. The cache uses
//! a least-recently-used (LRU) strategy to bound the number of keys. By
//! maintaining a cache of recently processed events, it helps avoid
//! reprocessing duplicate messages.

use crate::{BorrowedEventId, EventId, EventIdentity, Key, Payload};
use educe::Educe;
use lru::LruCache;
use std::num::NonZeroUsize;

/// A cache that stores event IDs for deduplication of messages.
///
/// The `IdempotenceCache` manages message deduplication using an LRU cache
/// strategy. The cache capacity can be specified during creation,
/// and a zero capacity effectively disables deduplication.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct IdempotenceCache {
    /// The underlying LRU cache used for storing event IDs.
    /// Only initialized if a non-zero size is provided.
    #[educe(Debug(ignore))]
    cache: Option<LruCache<Key, EventId>>,
}

impl IdempotenceCache {
    /// Creates a new `IdempotenceCache` with the specified capacity.
    ///
    /// If the size is zero, the cache is not initialized, and deduplication
    /// will always return `false`.
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum number of entries the cache can hold. A zero size
    ///   disables the cache.
    ///
    /// # Returns
    ///
    /// A new instance of `IdempotenceCache` with the given capacity.
    pub fn new(size: usize) -> Self {
        // Initialize the cache only if the size is non-zero.
        Self {
            cache: NonZeroUsize::new(size).map(LruCache::new),
        }
    }

    /// Checks if the provided message is a duplicate based on its event ID.
    ///
    /// Returns `Some(&BorrowedEventId)` if a duplicate is detected; otherwise,
    /// it inserts the event ID into the cache and returns `None`. If the
    /// event ID is `None`, the associated key is removed from the cache.
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the message.
    /// * `payload` - The message payload for which deduplication needs to be
    ///   checked.
    ///
    /// # Returns
    ///
    /// `Some(&BorrowedEventId)` if the message is a duplicate, otherwise
    /// `None`.
    pub fn check_duplicate<'a>(
        &mut self,
        key: &Key,
        payload: &'a Payload,
    ) -> Option<&'a BorrowedEventId> {
        // Short-circuit if the cache is not initialized.
        let Some(cache) = &mut self.cache else {
            return None;
        };

        // Extract the event ID from the payload for deduplication.
        if let Some(event_id) = payload.event_id() {
            // Check if the cache already contains the same event ID for the key.
            if cache
                .get(key)
                .is_some_and(|cached| cached.as_ref() == event_id)
            {
                // Duplicate message detected.
                return Some(event_id);
            }

            // Insert the new event ID into the cache.
            cache.put(key.clone(), event_id.into());
        } else {
            // Remove any entries with the key if the event ID is `None`.
            cache.pop(key);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_cache_is_empty() {
        let cache_size = 10;
        let mut cache = IdempotenceCache::new(cache_size);

        let key: Key = "key1".into();
        let payload: Payload = json!({"id": "event1"});

        // Since the cache is new and empty, the first check should return `None`.
        assert_eq!(cache.check_duplicate(&key, &payload), None);
    }

    #[test]
    fn test_duplicate_detection_same_event_id() {
        let cache_size = 10;
        let mut cache = IdempotenceCache::new(cache_size);

        let key: Key = "key1".into();
        let payload: Payload = json!({"id": "event1"});

        // Initially, the message should not be a duplicate and should return `None`.
        assert_eq!(cache.check_duplicate(&key, &payload), None);

        // Checking the same message should now return `Some`.
        assert_eq!(cache.check_duplicate(&key, &payload), payload.event_id());
    }

    #[test]
    fn test_same_key_different_event_ids() {
        let cache_size = 10;
        let mut cache = IdempotenceCache::new(cache_size);

        let key: Key = "key".into();
        let payload1: Payload = json!({"id": "event_id1"});
        let payload2: Payload = json!({"id": "event_id2"});

        // Insert the first message; should not be a duplicate.
        assert_eq!(cache.check_duplicate(&key, &payload1), None);

        // Second message has the same key but a different event ID; not a duplicate.
        assert_eq!(cache.check_duplicate(&key, &payload2), None);

        // With the event ID changed, checking payload1 again is not a duplicate.
        assert_eq!(cache.check_duplicate(&key, &payload1), None);

        // Payload1 should now be considered a duplicate.
        assert_eq!(cache.check_duplicate(&key, &payload1), payload1.event_id());

        // Payload2 should be inserted again to be identified as a duplicate.
        assert_eq!(cache.check_duplicate(&key, &payload2), None);
    }

    #[test]
    fn test_different_keys_same_event_id() {
        let cache_size = 10;
        let mut cache = IdempotenceCache::new(cache_size);

        let key1: Key = "key1".into();
        let key2: Key = "key2".into();
        let payload: Payload = json!({"id": "event_id"});

        // Insert the first message with `key1`.
        assert_eq!(cache.check_duplicate(&key1, &payload), None);

        // Message with `key2` should not be a duplicate.
        assert_eq!(cache.check_duplicate(&key2, &payload), None);

        // Checking the first message again should be a duplicate.
        assert_eq!(cache.check_duplicate(&key1, &payload), payload.event_id());

        // Checking the second message again should also be a duplicate.
        assert_eq!(cache.check_duplicate(&key2, &payload), payload.event_id());
    }

    #[test]
    fn test_no_cache_when_size_zero() {
        let cache_size = 0;
        let mut cache = IdempotenceCache::new(cache_size);

        let key: Key = "key".into();
        let payload: Payload = json!({"id": "event_id"});

        // With caching disabled, calls should always return `None`.
        assert_eq!(cache.check_duplicate(&key, &payload), None);
        assert_eq!(cache.check_duplicate(&key, &payload), None);
    }

    #[test]
    fn test_removal_on_none_event_id() {
        let cache_size = 10;
        let mut cache = IdempotenceCache::new(cache_size);

        let key: Key = "key".into();
        let payload_with_id: Payload = json!({"id": "event_id"});
        let payload_without_id: Payload = json!({}); // No "id" field.

        // Insert message with an event ID
        assert_eq!(cache.check_duplicate(&key, &payload_with_id), None);
        assert_eq!(
            cache.check_duplicate(&key, &payload_with_id),
            payload_with_id.event_id()
        );

        // Message with no event ID should remove the key from the cache.
        assert_eq!(cache.check_duplicate(&key, &payload_without_id), None);

        // Re-inserting message with event ID should not be a duplicate
        assert_eq!(cache.check_duplicate(&key, &payload_with_id), None);
    }
}
