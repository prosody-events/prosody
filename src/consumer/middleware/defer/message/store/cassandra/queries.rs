//! Prepared CQL statement management for Cassandra defer storage.
//!
//! This module provides the [`Queries`] struct which contains all prepared
//! Cassandra CQL statements needed for defer store operations. Prepared
//! statements are created once during initialization and reused for all
//! database operations, providing better performance and security.

#![allow(dead_code, reason = "fields used in implementation")]

use crate::cassandra::TABLE_DEFERRED_OFFSETS;
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for all prepared Cassandra CQL statements used by the defer store.
    ///
    /// This struct holds the Cassandra session and all prepared statements needed
    /// for defer message storage operations. Prepared statements provide better
    /// performance and security compared to ad-hoc query strings.
    ///
    /// The struct contains prepared statements for:
    /// - Getting the next deferred message for a key
    /// - Appending new deferred messages with TTL
    /// - Updating retry counts
    /// - Removing processed messages
    ///
    /// Segment metadata is managed separately via
    /// [`CassandraSegmentStore`](crate::consumer::middleware::defer::segment::CassandraSegmentStore).
    pub struct Queries {
        /// Gets the next deferred message (oldest offset) and retry count for a key
        get_next_deferred_message: (
            "SELECT offset, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ? LIMIT 1",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Inserts a new deferred message with retry count initialization and TTL
        insert_deferred_message_with_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, offset, retry_count) VALUES (?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Inserts a new deferred message WITHOUT updating retry count (leaves static column unchanged)
        insert_deferred_message_without_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, offset) VALUES (?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Updates the retry count for all messages of a key (static column)
        update_retry_count: (
            "UPDATE $keyspace.{} USING TTL ? SET retry_count = ? WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Removes a specific deferred message
        remove_deferred_message: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset = ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Deletes all data for a key (entire partition including static columns)
        delete_key: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_OFFSETS
        ),
    }
}
