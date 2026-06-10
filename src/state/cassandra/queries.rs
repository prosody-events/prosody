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

use crate::cassandra::{
    TABLE_KEYED_STATE_DESCRIPTOR, TABLE_KEYED_STATE_PENDING, TABLE_KEYED_STATE_VALUE,
};
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for the prepared CQL statements used by `CassandraValueStore`.
    pub struct ValueQueries {
        /// Reads the value partition columns (Idle/Sealed/Corrupt shapes).
        read_value_partition: (
            "SELECT data, payload_encoding, identity_version, \
             wal_event, wal_ops, wal_format \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes the WAL columns with TTL. Touches only the WAL columns —
        /// never `payload_encoding`, which belongs to the applied triple and
        /// must share that triple's write timestamp and TTL.
        write_wal: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET wal_event = ?, wal_ops = ?, wal_format = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes the WAL columns without TTL. Touches only the WAL columns
        /// (see `write_wal`).
        write_wal_no_ttl: (
            "UPDATE $keyspace.{} \
             SET wal_event = ?, wal_ops = ?, wal_format = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Apply WAL (TTL variant). Same-row `UNLOGGED BATCH` atomically
        /// writes the folded value (data + `payload_encoding`) and clears
        /// the WAL columns. Both statements target the same primary key —
        /// same partition, same row, safe under CLAUDE.md's batching rules.
        batch_apply_wal: (
            "BEGIN UNLOGGED BATCH \
             UPDATE $keyspace.{} USING TTL ? \
               SET data = ?, payload_encoding = ?, identity_version = ? \
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
             UPDATE $keyspace.{} \
               SET data = ?, payload_encoding = ?, identity_version = ? \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             UPDATE $keyspace.{} \
               SET wal_event = null, wal_ops = null, wal_format = null \
               WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?; \
             APPLY BATCH",
            TABLE_KEYED_STATE_VALUE, TABLE_KEYED_STATE_VALUE
        ),

        /// Clears the WAL columns without touching the applied triple.
        ///
        /// Used by `rollback_sealed` in every case. Because `seal` never
        /// writes `payload_encoding`/`identity_version` (the applied triple
        /// is written and cleared only by apply/direct-apply statements),
        /// the row never lands in the `PayloadEncodingWithoutData` shape
        /// after a rollback — clearing the WAL columns alone restores it to
        /// `Idle` whether or not authoritative `data` is present.
        clear_wal: (
            "UPDATE $keyspace.{} \
             SET wal_event = null, wal_ops = null, wal_format = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes only the applied cells with TTL (direct-apply path).
        write_data_only: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, payload_encoding = ?, identity_version = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes only the applied cells without TTL (direct-apply path).
        write_data_only_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, payload_encoding = ?, identity_version = ? \
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

        /// Reads every frozen descriptor-identity row for one segment
        /// (single-partition query).
        read_descriptor_identities: (
            "SELECT name, version, kind, cell_kind, codec_id, schema_label \
             FROM $keyspace.{} WHERE segment_id = ?",
            TABLE_KEYED_STATE_DESCRIPTOR
        ),

        /// Inserts one frozen descriptor-identity row. Single owner per
        /// segment, so a plain INSERT — never an LWT. First-use writes for
        /// multiple names are grouped into a same-partition `UNLOGGED
        /// BATCH` at the call site.
        insert_descriptor_identity: (
            "INSERT INTO $keyspace.{} \
             (segment_id, name, version, kind, cell_kind, codec_id, schema_label) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_KEYED_STATE_DESCRIPTOR
        ),

        /// Streams the pending-index rows for one `(segment, key)`
        /// partition. Used by the keyed-state middleware's `StateRecovery`
        /// sweep — partition key is `(segment_id, key)`, so the scan is
        /// confined to a single partition and avoids the ALLOW FILTERING
        /// anti-pattern.
        scan_pending: (
            "SELECT state_type, kind, name FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ?",
            TABLE_KEYED_STATE_PENDING
        ),
    }
}
