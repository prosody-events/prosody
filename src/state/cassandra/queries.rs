//! Prepared CQL statements for the Cassandra Value store.
//!
//! Same-partition `UNLOGGED BATCH` carries the WAL apply: it bundles "write
//! folded data" + "clear WAL columns" into one atomic mutation on a single
//! row, so a future contributor cannot split the apply into two sequential
//! calls without noticing they are breaking the atomicity invariant. Other
//! same-row writes (`seal`, `clear_wal`, `write_data_only`) are single
//! `UPDATE`s — Cassandra's row atomicity already covers multi-column
//! writes, and wrapping them in single-statement `BATCH`es would be ceremony.
//!
//! The pending-index writes target a different partition
//! (`keyed_state_pending` has partition key `(segment_id, key)`, separate
//! from `keyed_state_value`'s `(segment_id, key, state_type, name)`).
//! Cross-partition BATCH is unsafe per CLAUDE.md, so they remain sequential
//! prepared-statement calls; the crash residue is design-acceptable per
//! Crash Robustness §WAL Mode.
//!
//! TTL/no-TTL pairs exist because Cassandra cannot bind `NULL` to
//! `USING TTL ?`. `Some(ttl)` picks the TTL variant; `None` picks the
//! no-TTL variant.

use crate::cassandra::{TABLE_KEYED_STATE_PENDING, TABLE_KEYED_STATE_VALUE};
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for the prepared CQL statements used by `CassandraValueStore`.
    pub struct ValueQueries {
        /// Reads the value partition columns (Idle/Sealed/Corrupt shapes).
        read_value_partition: (
            "SELECT data, payload_encoding, wal_event, wal_ops, wal_format \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes the WAL columns with TTL.
        write_wal: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET wal_event = ?, wal_ops = ?, wal_format = ?, payload_encoding = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes the WAL columns without TTL.
        write_wal_no_ttl: (
            "UPDATE $keyspace.{} \
             SET wal_event = ?, wal_ops = ?, wal_format = ?, payload_encoding = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Apply WAL (TTL variant). Same-row `UNLOGGED BATCH` atomically
        /// writes the folded value (data + `payload_encoding`) and clears
        /// the WAL columns. Both statements target the same primary key —
        /// same partition, same row, safe under CLAUDE.md's batching rules.
        batch_apply_wal: (
            "BEGIN UNLOGGED BATCH \
             UPDATE $keyspace.{} USING TTL ? SET data = ?, payload_encoding = ? \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             UPDATE $keyspace.{} \
               SET wal_event = null, wal_ops = null, wal_format = null \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             APPLY BATCH",
            TABLE_KEYED_STATE_VALUE, TABLE_KEYED_STATE_VALUE
        ),

        /// Apply WAL (no-TTL variant). Same atomicity contract as the TTL
        /// variant.
        batch_apply_wal_no_ttl: (
            "BEGIN UNLOGGED BATCH \
             UPDATE $keyspace.{} SET data = ?, payload_encoding = ? \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             UPDATE $keyspace.{} \
               SET wal_event = null, wal_ops = null, wal_format = null \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             APPLY BATCH",
            TABLE_KEYED_STATE_VALUE, TABLE_KEYED_STATE_VALUE
        ),

        /// Clears the WAL columns without refreshing the applied cells.
        ///
        /// Used by `rollback_sealed` when authoritative `data` is present:
        /// the row keeps `data` and `payload_encoding` (which describes
        /// `data`), and the WAL columns become NULL.
        clear_wal: (
            "UPDATE $keyspace.{} \
             SET wal_event = null, wal_ops = null, wal_format = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Clears the WAL columns *and* `payload_encoding`.
        ///
        /// Used by `rollback_sealed` when authoritative `data` is also
        /// NULL: leaving `payload_encoding` set with no `data` would be a
        /// `PayloadEncodingWithoutData` corruption shape per the decoder.
        clear_wal_and_encoding: (
            "UPDATE $keyspace.{} \
             SET wal_event = null, wal_ops = null, wal_format = null, payload_encoding = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes only the applied cells with TTL (direct-apply path).
        write_data_only: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, payload_encoding = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes only the applied cells without TTL (direct-apply path).
        write_data_only_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, payload_encoding = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Inserts the pending-index row (different partition; sequential).
        insert_pending: (
            "INSERT INTO $keyspace.{} \
             (segment_id, key, state_type, kind, name) VALUES (?, ?, ?, ?, ?)",
            TABLE_KEYED_STATE_PENDING
        ),

        /// Deletes the pending-index row (different partition; sequential).
        delete_pending: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND kind = ? AND name = ?",
            TABLE_KEYED_STATE_PENDING
        ),
    }
}
