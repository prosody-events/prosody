//! Prepared CQL statements for the deduplication table.

use crate::cassandra::TABLE_DEDUPLICATION;
use crate::cassandra_queries;
use scylla::statement::Consistency;

cassandra_queries! {
    /// Prepared statements for deduplication operations.
    pub struct DeduplicationQueries {
        /// Check if a deduplication ID exists.
        check_exists: (
            "SELECT id FROM $keyspace.{} WHERE id = ?",
            TABLE_DEDUPLICATION
        ),
        /// Insert a deduplication ID with TTL.
        insert_with_ttl: (
            "INSERT INTO $keyspace.{} (id) VALUES (?) USING TTL ?",
            TABLE_DEDUPLICATION
        ),
    }
}

impl DeduplicationQueries {
    /// Sets both statements to `LocalOne` consistency for best-effort
    /// reads/writes.
    #[must_use]
    pub fn with_local_one_consistency(mut self) -> Self {
        self.check_exists.set_consistency(Consistency::LocalOne);
        self.insert_with_ttl.set_consistency(Consistency::LocalOne);
        self
    }
}
