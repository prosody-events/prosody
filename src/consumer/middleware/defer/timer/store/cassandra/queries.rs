//! Prepared CQL statement management for Cassandra timer defer storage.

#![allow(dead_code, reason = "fields used in implementation")]

use crate::cassandra::TABLE_DEFERRED_TIMERS;
use crate::cassandra_queries;
use crate::timers::datetime::CompactDateTime;
use scylla::{DeserializeValue, SerializeValue};
use std::collections::HashMap;

/// Cassandra UDT for the minimum live timer row cached in the `next_timer`
/// static column. Bundles `time` and `span` atomically so they cannot drift
/// (invariant I4).
///
/// Mirrors the `key_timer_state` UDT pattern from
/// `20260217_add_singleton_slots.cql`.
#[derive(Clone, Debug, DeserializeValue, SerializeValue)]
pub struct DeferredNextTimer {
    /// `original_time` of the minimum live timer row.
    pub time: CompactDateTime,
    /// W3C trace context span map for context reconstruction.
    pub span: HashMap<String, String>,
}

cassandra_queries! {
    /// Container for all prepared Cassandra CQL statements used by the timer defer store.
    pub struct Queries {
        /// Static-column read (`next_timer` UDT, `retry_count`) — zero clustering scan.
        get_next_static: (
            "SELECT next_timer, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Successor probe: first clustering row with `original_time > ?`.
        probe_next: (
            "SELECT original_time, span, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time > ? LIMIT 1",
            TABLE_DEFERRED_TIMERS
        ),

        /// Minimum-row probe for legacy on-read repair: lowest clustering row in the partition.
        probe_min: (
            "SELECT original_time, span, retry_count FROM $keyspace.{} WHERE segment_id = ? AND key = ? LIMIT 1",
            TABLE_DEFERRED_TIMERS
        ),

        /// Legacy on-read repair: synthesize `next_timer` from probed min (LWW, no LWT).
        repair_next_timer: (
            "UPDATE $keyspace.{} USING TTL ? SET next_timer = ? WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// INSERT with `retry_count = 0` and `next_timer` set (first deferral).
        insert_deferred_timer_with_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, original_time, span, retry_count, next_timer) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// INSERT without touching `retry_count` or `next_timer` (monotonic append).
        insert_deferred_timer_without_retry_count: (
            "INSERT INTO $keyspace.{} (segment_id, key, original_time, span) VALUES (?, ?, ?, ?) USING TTL ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Updates the `retry_count` static column.
        update_retry_count: (
            "UPDATE $keyspace.{} USING TTL ? SET retry_count = ? WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// UNLOGGED BATCH — FIFO `complete_retry_success`: DELETE + advance `next_timer` + reset `retry_count = 0`.
        batch_complete_retry: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time = ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_timer = ?, retry_count = 0 WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_TIMERS, TABLE_DEFERRED_TIMERS
        ),

        /// UNLOGGED BATCH — non-FIFO `complete_retry_success`: DELETE + reset `retry_count = 0`, `next_timer` unchanged.
        batch_complete_retry_no_advance: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time = ?; \
             UPDATE $keyspace.{} USING TTL ? SET retry_count = 0 WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_TIMERS, TABLE_DEFERRED_TIMERS
        ),

        /// UNLOGGED BATCH — out-of-order append: INSERT + lower `next_timer`.
        batch_append_with_next: (
            "BEGIN UNLOGGED BATCH \
             INSERT INTO $keyspace.{} (segment_id, key, original_time, span) VALUES (?, ?, ?, ?) USING TTL ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_timer = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_TIMERS, TABLE_DEFERRED_TIMERS
        ),

        /// UNLOGGED BATCH — min-removal repair: DELETE + update `next_timer` to successor.
        batch_remove_and_repair_next: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time = ?; \
             UPDATE $keyspace.{} USING TTL ? SET next_timer = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_DEFERRED_TIMERS, TABLE_DEFERRED_TIMERS
        ),

        /// Removes a specific clustering row (primitive, non-min removal).
        remove_deferred_timer: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND original_time = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Deletes the entire partition including static columns.
        delete_key: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),

        /// Full-partition scan of all live timer times (ascending `original_time` order).
        get_deferred_times: (
            "SELECT original_time FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_DEFERRED_TIMERS
        ),
    }
}
