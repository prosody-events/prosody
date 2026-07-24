//! Cassandra-backed routing-only publication store.
//!
//! [`CassandraPublicationStore`] implements [`PublicationStore`] over the
//! `keyed_state_publication` table, provisioned by migration
//! `20260722_create_keyed_state_publication.cql`. The table holds only routing
//! facts. Reads are a single-partition clustering `SELECT`. Upsert and remove
//! are plain idempotent `INSERT`/`DELETE`. They set no LWT, no TTL, and no
//! client-set timestamp. Last-write-wins ordering from the session's monotonic
//! timestamp generator is enough for these rows.

use crate::Topic;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore as CassandraSession, TABLE_KEYED_STATE_PUBLICATION};
use crate::cassandra_queries;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::{StateName, StateType};
use crate::state_reader::{PartitionCount, PartitionCountError};
use crate::subsystem::SubsystemName;
use internment::Intern;
use scylla::client::session::Session;
use std::sync::Arc;
use thiserror::Error;

/// Cassandra-backed routing-only publication store.
#[derive(Clone, Debug)]
pub struct CassandraPublicationStore {
    session: CassandraSession,
    queries: Arc<PublicationQueries>,
}

impl CassandraPublicationStore {
    /// Creates a publication store over an existing [`CassandraSession`] and a
    /// prepared [`PublicationQueries`] set.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<PublicationQueries>) -> Self {
        Self { session, queries }
    }

    fn cql(&self) -> &Session {
        self.session.session()
    }
}

impl PublicationStore for CassandraPublicationStore {
    type Error = CassandraPublicationError;

    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), Self::Error> {
        self.cql()
            .execute_unpaged(
                &self.queries.upsert,
                (
                    subsystem.as_str(),
                    state_type,
                    name.as_str(),
                    row.group_id.as_ref(),
                    row.topic.as_ref(),
                    i32::from(row.partition_count),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    async fn remove(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
        topic: Topic,
    ) -> Result<(), Self::Error> {
        self.cql()
            .execute_unpaged(
                &self.queries.remove,
                (
                    subsystem.as_str(),
                    state_type,
                    name.as_str(),
                    group_id,
                    topic.as_ref(),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<Vec<StatePublication>, Self::Error> {
        let rows = self
            .cql()
            .execute_unpaged(
                &self.queries.read_publications,
                (subsystem.as_str(), state_type, name.as_str()),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?;
        let mut out = Vec::with_capacity(rows.rows_num());
        for row in rows
            .rows::<(&str, &str, i32)>()
            .map_err(CassandraStoreError::from)?
        {
            let (group_id, topic, partition_count) = row.map_err(CassandraStoreError::from)?;
            out.push(StatePublication {
                group_id: Arc::from(group_id),
                topic: Intern::<str>::from(topic),
                partition_count: PartitionCount::try_from(partition_count)?,
            });
        }
        Ok(out)
    }
}

cassandra_queries! {
    /// Container for the prepared CQL statements used by
    /// [`CassandraPublicationStore`].
    pub struct PublicationQueries {
        /// Idempotently records one `(group_id, topic)` source of a collection.
        upsert: (
            "INSERT INTO $keyspace.{} \
             (subsystem, state_type, name, group_id, topic, partition_count) \
             VALUES (?, ?, ?, ?, ?, ?)",
            TABLE_KEYED_STATE_PUBLICATION
        ),

        /// Removes one `(group_id, topic)` source.
        /// Idempotent: deleting an absent row is a no-op.
        remove: (
            "DELETE FROM $keyspace.{} \
             WHERE subsystem = ? AND state_type = ? AND name = ? \
             AND group_id = ? AND topic = ?",
            TABLE_KEYED_STATE_PUBLICATION
        ),

        /// Reads every source of one collection — a single-partition clustering
        /// scan, no `ALLOW FILTERING`.
        read_publications: (
            "SELECT group_id, topic, partition_count \
             FROM $keyspace.{} WHERE subsystem = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_PUBLICATION
        ),
    }
}

/// Errors that can occur during Cassandra publication-store operations.
#[derive(Debug, Error)]
pub enum CassandraPublicationError {
    /// Wrapped Cassandra driver error.
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// A decoded `partition_count` was outside `[1, i32::MAX]`. The routing row
    /// is corrupt or hand-edited, so this classifies `Permanent`.
    #[error("invalid partition count: {0}")]
    PartitionCount(#[from] PartitionCountError),
}

impl ClassifyError for CassandraPublicationError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::PartitionCount(e) => e.classify_error(),
        }
    }
}
