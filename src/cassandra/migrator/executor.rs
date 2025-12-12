//! Migration execution and tracking.
//!
//! Provides functionality to execute migrations and track their application
//! in the database.

use super::loader::{Migration, parse_cql_statements};
use crate::cassandra::TABLE_SCHEMA_MIGRATIONS;
use humantime::format_duration;
use scylla::client::session::Session;
use std::time::Instant;
use tracing::debug;

/// Migration executor for applying CQL statements and tracking results.
pub struct MigrationExecutor<'a> {
    session: &'a Session,
}

impl<'a> MigrationExecutor<'a> {
    /// Creates a new migration executor.
    pub fn new(session: &'a Session) -> Self {
        Self { session }
    }

    /// Applies a single migration to the database.
    ///
    /// Executes all CQL statements in the migration file and records
    /// the successful application in the migrations tracking table.
    /// Measures execution time for monitoring purposes.
    ///
    /// # Arguments
    ///
    /// * `migration` - The migration to apply
    /// * `keyspace` - The target keyspace for the migration
    ///
    /// # Errors
    ///
    /// Returns an error if statement execution or migration recording fails.
    pub async fn apply_migration(
        &self,
        migration: &Migration,
        keyspace: &str,
    ) -> Result<(), super::MigrationError> {
        let start_time = Instant::now();

        debug!("Starting execution of migration: {}", migration.filename);

        // Parse and execute CQL statements
        let statements = parse_cql_statements(&migration.content);

        debug!("Migration contains {} CQL statement(s)", statements.len());
        for (index, statement_text) in statements.iter().enumerate() {
            if !statement_text.trim().is_empty() {
                debug!("Executing statement {}/{}", index + 1, statements.len());
                self.session
                    .query_unpaged(statement_text.as_str(), &[])
                    .await
                    .map_err(|source| super::MigrationError::StatementExecutionFailed {
                        migration: migration.filename.clone(),
                        statement_index: index + 1,
                        source: Box::new(source),
                    })?;
            }
        }

        let execution_duration = start_time.elapsed();
        let execution_time_ms = execution_duration.as_millis() as i64;

        // Record the migration as applied
        let applied_at = chrono::Utc::now();
        let insert_sql = format!(
            "insert into {keyspace}.{TABLE_SCHEMA_MIGRATIONS} (filename, checksum, applied_at, \
             execution_time_ms) values (?, ?, ?, ?)"
        );
        let insert_stmt =
            self.session.prepare(insert_sql).await.map_err(|e| {
                super::MigrationError::MigrationTrackingPreparationFailed(Box::new(e))
            })?;

        self.session
            .execute_unpaged(
                &insert_stmt,
                (
                    &migration.filename,
                    &migration.checksum,
                    applied_at,
                    execution_time_ms,
                ),
            )
            .await?;

        debug!(
            "Migration {} completed in {}",
            migration.filename,
            format_duration(execution_duration)
        );
        Ok(())
    }
}
