//! Prepared CQL statement management for Cassandra timer defer storage.
//!
//! This module provides the [`Queries`] struct which contains all prepared
//! Cassandra CQL statements needed for timer defer store operations. Prepared
//! statements are created once during initialization and reused for all
//! database operations, providing better performance and security.

#![allow(dead_code, reason = "fields used in implementation")]

use crate::cassandra::{TABLE_DEFERRED_SEGMENTS, TABLE_DEFERRED_TIMERS};
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for all prepared Cassandra CQL statements used by the timer
    /// defer store.
    ///
    /// This struct holds the Cassandra session and all prepared statements needed
    /// for timer defer storage operations. Prepared statements provide better
    /// performance and security compared to ad-hoc query strings.
    ///
    /// The struct contains prepared statements for:
    /// - Inserting and querying segment metadata
    /// - Getting the next deferred timer for a key
    /// - Appending new deferred timers with TTL
    /// - Updating retry counts
    /// - Removing processed timers
    pub struct Queries {
        /// Inserts segment metadata (idempotent)
        insert_segment: (
            "INSERT INTO $keyspace.{} (id, topic, partition, consumer_group) VALUES (?, ?, ?, ?)",
            TABLE_DEFERRED_SEGMENTS
        ),

        /// Gets segment metadata by ID
        get_segment: (
            "SELECT topic, partition, consumer_group FROM $keyspace.{} WHERE id = ?",
            TABLE_DEFERRED_SEGMENTS
        ),

        /// Gets the next deferred timer (oldest `original_time`) and retry count
        /// for a key. Returns: (`original_time`, span, `retry_count`)
        get_next_deferred_timer: (
            "SELECT original_time, span, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ? LIMIT 1",
            TABLE_DEFERRED_TIMERS
        ),

        /// Inserts a new deferred timer with retry count initialization and TTL.
        /// Used for first deferral.
        insert_deferred_timer_with_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, original_time, span, retry_count) VALUES (?, ?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Inserts a new deferred timer WITHOUT updating retry count.
        /// Leaves static column unchanged. Used for queueing additional timers.
        insert_deferred_timer_without_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, original_time, span) VALUES (?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Updates the retry count for all timers of a key (static column).
        update_retry_count: (
            "UPDATE $keyspace.{} USING TTL ? SET retry_count = ? WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Removes a specific deferred timer by key and `original_time`.
        remove_deferred_timer: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Deletes all data for a key (entire partition including static
        /// columns).
        delete_key: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Gets all deferred timer times for a key, sorted ascending by
        /// `original_time` (clustering order). Used by `scheduled()` to
        /// merge with active timers.
        get_deferred_times: (
            "SELECT original_time FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),
    }
}
