#[cfg(doc)]
use super::{CassandraStore, ScanEdge};
use super::{TABLE_KEYED_STATE_CELL, cassandra_queries};

cassandra_queries! {
    /// Container for the prepared CQL statements used by [`CassandraStore`].
    ///
    /// Every statement binds the leading clustering `kind` as a constant
    /// (`CellKind::Cell` for the cell statements, `CellKind::Marker` for the
    /// marker statements) — a clustering-prefix column cannot be skipped.
    /// Each cell mutation is one `UPDATE`/`INSERT`/`DELETE` of one row; a
    /// multi-cell collection write binds these once per cell into one
    /// same-partition `UNLOGGED BATCH` (via `execute_unlogged_batches`), so all
    /// its cells share one write timestamp and TTL anchor. TTL/no-TTL pairs
    /// exist because Cassandra cannot bind `NULL` to `USING TTL ?`. The scans
    /// are single-section clustering ranges within the `kind=Cell` slice: the
    /// `ORDER BY` direction cannot be bound (forward/backward), and the
    /// **start-side comparator** cannot be bound either, so each direction
    /// carries two start variants — inclusive (`>=`/`<=`) and exclusive
    /// (`>`/`<`, for exclusive anchors). A start edge is therefore either a
    /// bound coordinate (the four incl/excl statements) or
    /// [`Unbounded`](ScanEdge::Unbounded) (the two section-only `_all`
    /// statements, which carry no start comparator). The end bound is enforced
    /// in code (`past_end`), so it needs no statement variant. The `marker_*`
    /// statements maintain and point-read the one fixed-address event-marker row
    /// that bounds recovery. The `gap_*` statements are the section-clear range
    /// deletes (`extend_gap_units`) — writes, never reads. Scan issuance is gated: the
    /// four cell mutators each write exactly one row shape, so the only reader
    /// that walks a whole section (and thus can meet a tombstone field) is an
    /// `_all` scan, reached solely by the map's degraded full-section fallback —
    /// the accepted degraded cost. None use `ALLOW FILTERING`.
    pub struct CellQueries {
        /// Reads one cell's columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads one cell's columns plus each blob's remaining TTL, for the
        /// cache-fill point read. `TTL(column)` is a read function (no schema
        /// change); it returns NULL when the column is NULL or the row has no
        /// TTL. Both blobs are selected so the co-expiry can follow whichever
        /// blob resolution returns (the `decode` module's `blob_ttl`).
        read_cell_ttl: (
            "SELECT data, prev_data, encoding, version, event, TTL(data), TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Batch twin of [`read_cell_ttl`](Self::read_cell_ttl): one section's
        /// cells for a bounded (`1..=CELL_BATCH`) coordinate list, plus each
        /// blob's remaining TTL. `IN` returns matching clustering rows in
        /// coordinate order (not input order) and omits absent coordinates, so
        /// the reader carries the `coordinate` column to re-key each row to its
        /// input position and treats a missing coordinate as an absent row.
        /// One same-partition, single-shard query — never a cross-partition
        /// `IN` (the partition key is fully bound).
        read_cells_batch: (
            "SELECT coordinate, data, prev_data, encoding, version, event, \
             TTL(data), TTL(prev_data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate IN ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads presence for one bounded coordinate list without payloads.
        read_presence_batch: (
            "SELECT coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate IN ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an exclusive `coordinate` anchor.
        scan_forward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an exclusive `coordinate` anchor.
        scan_backward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Section-only forward scan (an [`Unbounded`](ScanEdge::Unbounded)
        /// start edge): no start comparator, walks the whole `kind=Cell` slice
        /// of the section in ascending `coordinate` order.
        scan_forward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Section-only backward scan (an [`Unbounded`](ScanEdge::Unbounded)
        /// start edge): no start comparator, walks the whole `kind=Cell` slice
        /// of the section in descending `coordinate` order.
        scan_backward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only forward scan from an inclusive coordinate.
        scan_presence_forward_incl: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate >= ? ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only forward scan from an exclusive coordinate.
        scan_presence_forward_excl: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only backward scan from an inclusive coordinate.
        scan_presence_backward_incl: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate <= ? ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only backward scan from an exclusive coordinate.
        scan_presence_backward_excl: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ? ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only forward scan of one section.
        scan_presence_forward_all: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Presence-only backward scan of one section.
        scan_presence_backward_all: (
            "SELECT section, coordinate, WRITETIME(data), WRITETIME(prev_data), encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Row-level delete of one `kind=Cell` row: the committed-absent shape
        /// (see the `CellStore` row-absence invariant). One row tombstone that
        /// also includes any future columns — strictly better than nulling every
        /// column. No TTL clause (deletes carry none). Its CQL text matches
        /// `marker_delete`; the two are kept separate because they die
        /// separately and bind a different constant `kind`.
        cell_delete: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Upserts the collection's event-marker row with TTL (co-expiry with
        /// the staged cells): the frozen payload in `data`/`encoding`/`version`
        /// and the staging event in `event`. Deliberately does NOT touch
        /// `prev_data` — a marker row never carries one, and binding an
        /// explicit null would write a needless column tombstone at the fixed
        /// address on every stage.
        marker_write: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Upserts the event-marker row without TTL.
        marker_write_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Point-reads the event-marker row at its fixed address — the cold
        /// recovery seed (cost: one point read at a compaction-merged
        /// position, never a range over a tombstone field).
        marker_read: (
            "SELECT data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Row-level delete of the event-marker row (on settle — the whole
        /// stage resolved). Deleting an absent marker is a harmless no-op.
        marker_delete: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Whole-section gap delete: erases a cleared section with no
        /// survivors as one clustering-range tombstone (`kind` bound
        /// `CellKind::Cell`; a write, never a read — no TTL clause).
        gap_section: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete below the first survivor (`coordinate < ?`).
        gap_below: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete between two adjacent survivors
        /// (`coordinate > ? AND coordinate < ?` — both exclusive, so the
        /// survivors themselves are never inside a gap range).
        gap_between: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? AND coordinate < ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Gap delete above the last survivor (`coordinate > ?`).
        gap_above: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ?",
            TABLE_KEYED_STATE_CELL
        ),
    }
}
