//! Prepared CQL statements for the deduplication table.

use crate::cassandra::TABLE_DEDUPLICATION;
use crate::cassandra_queries;

cassandra_queries! {
    /// Prepared statements for deduplication operations.
    ///
    /// Inherits the session default consistency (`LocalQuorum`). The dedup
    /// write is the durability anchor for the post-commit apply hook: once
    /// `insert` returns Ok the framework commits the Kafka offset, so the
    /// write must be observable by any subsequent reader in the local DC.
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
