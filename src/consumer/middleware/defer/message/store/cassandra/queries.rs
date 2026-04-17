//! Prepared CQL statement management for Cassandra defer storage.

#![allow(dead_code, reason = "fields used in implementation")]

use crate::cassandra::TABLE_DEFERRED_OFFSETS;
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for all prepared Cassandra CQL statements used by the defer store.
    pub struct Queries {
        /// Static-column read (`next_offset`, `retry_count`) — zero clustering scan.
        get_next_static: (
            "SELECT next_offset, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Successor probe: first clustering row with `offset > ?`.
        probe_next: (
            "SELECT offset, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset > ? LIMIT 1",
            TABLE_DEFERRED_OFFSETS
        ),

        /// INSERT with `retry_count = 0` and `next_offset = offset` (first deferral).
        insert_deferred_message_with_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, offset, retry_count, next_offset) VALUES (?, ?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// INSERT without touching `retry_count` or `next_offset` (monotonic append).
        insert_deferred_message_without_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, offset) VALUES (?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Updates the `retry_count` static column.
        update_retry_count: (
            "UPDATE $keyspace.{} USING TTL ? SET retry_count = ? WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// UNLOGGED BATCH — FIFO `complete_retry_success`: DELETE + advance `next_offset` + reset `retry_count = 0`.
        batch_complete_retry: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset = ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_offset = ?, retry_count = 0 WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_OFFSETS
        ),

        /// UNLOGGED BATCH — non-FIFO `complete_retry_success`: DELETE + reset `retry_count = 0`, `next_offset` unchanged.
        batch_complete_retry_no_advance: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset = ?; \
             UPDATE $keyspace.{} USING TTL ? SET retry_count = 0 WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_OFFSETS
        ),

        /// UNLOGGED BATCH — out-of-order append: INSERT + lower `next_offset`.
        batch_append_with_next: (
            "BEGIN UNLOGGED BATCH \
             INSERT INTO $keyspace.{} (segment_id, key, offset) VALUES (?, ?, ?) USING TTL ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_offset = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_OFFSETS
        ),

        /// UNLOGGED BATCH — min-removal repair: DELETE + update `next_offset` to successor.
        batch_remove_and_repair_next: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset = ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_offset = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_OFFSETS
        ),

        /// Removes a specific clustering row (primitive, non-min removal).
        remove_deferred_message: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND offset = ?",
            TABLE_DEFERRED_OFFSETS
        ),

        /// Deletes the entire partition including static columns.
        delete_key: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_OFFSETS
        ),
    }
}
