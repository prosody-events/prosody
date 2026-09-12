use super::projection::CassandraProjection;
use super::{CassandraStoreError, TABLE_KEYED_STATE_CELL, cassandra_queries};
use crate::cassandra::macros::{format_sql, prepare_statement};
use crate::state::cell::{Presence, Values};
use crate::state::cell_key::{Direction, EdgeKind};
use educe::Educe;
use futures::try_join;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

const POINT: &str = "SELECT {}, encoding, version, event, TTL(data), TTL(prev_data) FROM \
                     $keyspace.{} WHERE segment_id = ? AND key = ? AND state_type = ? AND name = \
                     ? AND kind = ? AND section = ? AND coordinate = ?";
const BATCH: &str = "SELECT coordinate, {}, encoding, version, event, TTL(data), TTL(prev_data) \
                     FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND state_type = ? AND \
                     name = ? AND kind = ? AND section = ? AND coordinate IN ?";
const SCAN: &str = "SELECT section, coordinate, {}, encoding, version, event FROM $keyspace.{} \
                    WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? AND kind = ? \
                    AND section = ? AND coordinate {} ? ORDER BY coordinate {}";

/// Prepared cell reads, mutations, and collection evidence queries.
#[derive(Debug)]
pub struct CellQueries {
    pub(super) cells: CellStatements,
    pub(super) values: ReadStatements,
    pub(super) presence: ReadStatements,
}

/// Prepared reads for one projection.
#[derive(Educe)]
#[educe(Debug)]
pub struct ReadStatements {
    #[educe(Debug(ignore))]
    pub(super) point: PreparedStatement,
    #[educe(Debug(ignore))]
    pub(super) batch: PreparedStatement,
    #[educe(Debug(ignore))]
    pub(super) scan: ScanStatements,
}

/// Prepared scans for each direction and start edge.
pub(super) struct ScanStatements {
    forward_included: PreparedStatement,
    forward_excluded: PreparedStatement,
    forward_unbounded: PreparedStatement,
    backward_included: PreparedStatement,
    backward_excluded: PreparedStatement,
    backward_unbounded: PreparedStatement,
}

impl CellQueries {
    /// Prepares all cell statements.
    ///
    /// # Errors
    ///
    /// Returns a store error if preparation fails.
    pub async fn new(session: &Session, keyspace: &str) -> Result<Self, CassandraStoreError> {
        Ok(Self {
            cells: CellStatements::new(session, keyspace).await?,
            values: ReadStatements::prepare::<Values>(session, keyspace).await?,
            presence: ReadStatements::prepare::<Presence>(session, keyspace).await?,
        })
    }
}

impl ReadStatements {
    async fn prepare<P: CassandraProjection>(
        session: &Session,
        keyspace: &str,
    ) -> Result<Self, CassandraStoreError> {
        let point = prepare_statement(
            session,
            &format_sql(POINT, keyspace, &[P::SELECT, TABLE_KEYED_STATE_CELL]),
        )
        .await?;
        let batch = prepare_statement(
            session,
            &format_sql(BATCH, keyspace, &[P::SELECT, TABLE_KEYED_STATE_CELL]),
        )
        .await?;
        let prepare_scan = |(comparator, order)| async move {
            prepare_statement(
                session,
                &format_sql(
                    SCAN,
                    keyspace,
                    &[P::SELECT, TABLE_KEYED_STATE_CELL, comparator, order],
                ),
            )
            .await
        };
        let (
            forward_included,
            forward_excluded,
            forward_unbounded,
            backward_included,
            backward_excluded,
            backward_unbounded,
        ) = try_join!(
            prepare_scan(shape(Direction::Forward, EdgeKind::Included)),
            prepare_scan(shape(Direction::Forward, EdgeKind::Excluded)),
            prepare_scan(shape(Direction::Forward, EdgeKind::Unbounded)),
            prepare_scan(shape(Direction::Backward, EdgeKind::Included)),
            prepare_scan(shape(Direction::Backward, EdgeKind::Excluded)),
            prepare_scan(shape(Direction::Backward, EdgeKind::Unbounded))
        )?;
        Ok(Self {
            point,
            batch,
            scan: ScanStatements {
                forward_included,
                forward_excluded,
                forward_unbounded,
                backward_included,
                backward_excluded,
                backward_unbounded,
            },
        })
    }
}

impl ScanStatements {
    /// Selects the prepared scan for this direction and start edge.
    pub(super) fn select(&self, direction: Direction, start: EdgeKind) -> &PreparedStatement {
        match (direction, start) {
            (Direction::Forward, EdgeKind::Included) => &self.forward_included,
            (Direction::Forward, EdgeKind::Excluded) => &self.forward_excluded,
            (Direction::Forward, EdgeKind::Unbounded) => &self.forward_unbounded,
            (Direction::Backward, EdgeKind::Included) => &self.backward_included,
            (Direction::Backward, EdgeKind::Excluded) => &self.backward_excluded,
            (Direction::Backward, EdgeKind::Unbounded) => &self.backward_unbounded,
        }
    }
}

/// Returns the comparator and order. Unbounded starts bind the minimum
/// coordinate.
const fn shape(direction: Direction, start: EdgeKind) -> (&'static str, &'static str) {
    match (direction, start) {
        (Direction::Forward, EdgeKind::Included | EdgeKind::Unbounded) => (">=", "ASC"),
        (Direction::Forward, EdgeKind::Excluded) => (">", "ASC"),
        (Direction::Backward, EdgeKind::Included) => ("<=", "DESC"),
        (Direction::Backward, EdgeKind::Excluded) => ("<", "DESC"),
        (Direction::Backward, EdgeKind::Unbounded) => (">=", "DESC"),
    }
}

cassandra_queries! {
    /// Statements without a projection axis. Each mutation changes one partition.
    pub(super) struct CellStatements {
        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
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

        /// Writes Staged with the collection TTL, frozen payload, and event.
        /// It leaves `prev_data` untouched to avoid a needless tombstone.
        marker_write: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes the committed event and discovery payload with the evidence TTL.
        committed_write: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads both rows of the marker slice.
        marker_state: (
            "SELECT coordinate, data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Deletes Staged after the whole stage resolves. An absent marker makes the
        /// delete a no-op.
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
