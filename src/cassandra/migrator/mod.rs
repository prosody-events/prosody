//! Embedded database schema migration system for Cassandra.
//!
//! Provides the [`CassandraMigrator`] which handles automatic schema migration
//! for all Cassandra-based storage in Prosody. Migrations are embedded as
//! `.cql` files at compile time and applied automatically during store
//! initialization.
//!
//! ## Features
//!
//! - **Distributed Locking**: Uses Cassandra lightweight transactions (LWT) to
//!   ensure only one process applies migrations at a time across a distributed
//!   system
//! - **Retry Logic**: Implements exponential backoff with full jitter for
//!   transient failures, following the same pattern as other retry mechanisms
//!   in the codebase
//! - **Concurrent Safety**: Multiple application instances can start
//!   simultaneously without migration conflicts or race conditions
//! - **Integrity Validation**: Tracks applied migrations with checksums to
//!   detect modifications and prevent inconsistent schema states
//! - **Automatic Recovery**: Handles network partitions and process crashes
//!   gracefully through lock TTL and retry mechanisms
//!
//! ## Migration Process
//!
//! 1. Creates keyspaces if they don't exist
//! 2. Ensures migration tables (`schema_migrations`, `locks`) exist
//! 3. Performs lightweight check for pending migrations
//! 4. Acquires distributed lock only when migrations are needed
//! 5. Re-checks for pending migrations while holding lock (another process may
//!    have completed them)
//! 6. Applies pending migrations in timestamp order
//! 7. Records successful migrations with execution metadata
//! 8. Releases the migration lock
//!
//! ## Lock Acquisition Strategy
//!
//! The migrator uses exponential backoff with full jitter to handle lock
//! contention:
//! - Base delay: 1 second
//! - Maximum delay: 30 seconds
//! - Maximum retries: 10 attempts
//! - Jitter: Random delay between 0 and calculated backoff (prevents thundering
//!   herd)
//!
//! ## Migration Files
//!
//! Migration files are CQL scripts stored in
//! `src/timers/store/cassandra/migrations/` with timestamp-based naming
//! (`YYYYMMDD_description.cql`). They support template substitution for table
//! names and keyspace references.

#![allow(clippy::same_name_method)]

mod executor;
mod loader;
mod lock;
mod utils;
mod validator;

#[cfg(test)]
mod tests;

use crate::cassandra::{CassandraStoreError, TABLE_SCHEMA_MIGRATIONS};
use crate::error::{ClassifyError, ErrorCategory};
use executor::MigrationExecutor;
use futures::{TryStreamExt, pin_mut};
use humantime::format_duration;
use loader::{Migration, load_embedded_migrations};
use lock::LockManager;
use scylla::client::session::Session;
use scylla::deserialize::TypeCheckError;
use scylla::errors::{
    ExecutionError, IntoRowsResultError, MaybeFirstRowError, MetadataError, PrepareError,
};
use std::collections::HashMap;
use std::str::Utf8Error;
use std::time::Duration;
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use utils::{calculate_backoff, ensure_migration_tables_exist, refresh_metadata};
use validator::{AppliedMigration, get_pending_migrations, validate_applied_migrations};

/// Default timeout for migration lock acquisition.
const MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(10 * 60); // 10 minutes

/// Maximum number of retry attempts for migration.
const MAX_MIGRATION_RETRIES: u32 = 10;

/// Maximum number of retries for table preparation during constructor.
const MAX_TABLE_PREPARATION_RETRIES: u32 = 10;

/// Database migration coordinator for Cassandra schema updates.
///
/// Manages the complete migration lifecycle: loading embedded migrations,
/// tracking applied migrations, validating integrity, and applying pending
/// migrations in the correct order.
pub struct CassandraMigrator<'a> {
    /// Cassandra session for executing migration statements.
    session: &'a Session,
    /// Target keyspace name for migrations.
    keyspace: &'a str,
    /// Migration lock manager.
    lock_manager: LockManager<'a>,
    /// Migration executor.
    executor: MigrationExecutor<'a>,
}

impl<'a> CassandraMigrator<'a> {
    /// Creates a new migration coordinator.
    ///
    /// Initializes the keyspace and tables if needed, and prepares lock
    /// statements for efficient reuse during migration operations.
    ///
    /// # Arguments
    ///
    /// * `session` - Cassandra session for executing migration statements
    /// * `keyspace` - Target keyspace name where migrations will be applied
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Keyspace creation fails
    /// - Table creation fails
    /// - Statement preparation fails
    pub async fn new(session: &'a Session, keyspace: &'a str) -> Result<Self, CassandraStoreError> {
        // Create keyspace if it doesn't exist
        if session.get_cluster_state().get_keyspace(keyspace).is_none() {
            info!("Creating keyspace '{keyspace}'");
            let create_keyspace_cql = format!(
                "create keyspace if not exists {keyspace} WITH replication = {{ 'class' : \
                 'SimpleStrategy', 'replication_factor': 1 }}"
            );
            session.query_unpaged(create_keyspace_cql, &[]).await?;
            refresh_metadata(session).await?;
        }

        ensure_migration_tables_exist(session, keyspace).await?;

        // Tables may not immediately be available after creation, so retry with a limit
        for attempt in 0..MAX_TABLE_PREPARATION_RETRIES {
            if let Ok(lock_manager) = LockManager::new(session, keyspace).await {
                let executor = MigrationExecutor::new(session);
                return Ok(Self {
                    session,
                    keyspace,
                    lock_manager,
                    executor,
                });
            }

            if attempt < MAX_TABLE_PREPARATION_RETRIES - 1 {
                debug!(
                    "Failed to prepare lock statements (attempt {}/{}), refreshing metadata",
                    attempt + 1,
                    MAX_TABLE_PREPARATION_RETRIES
                );
                refresh_metadata(session).await?;
            }
        }

        Err(MigrationError::LockPreparationFailed(MAX_TABLE_PREPARATION_RETRIES).into())
    }

    /// Executes the complete migration process with distributed locking and
    /// retry logic.
    ///
    /// This method implements an intelligent retry loop that:
    /// 1. Checks for pending migrations (lightweight operation)
    /// 2. If migrations are needed, attempts to acquire the distributed lock
    /// 3. If lock acquisition fails, sleeps with backoff and returns to step 1
    /// 4. If lock is acquired, re-checks for pending migrations and applies
    ///    them
    /// 5. Releases the lock and returns
    ///
    /// The key insight is that if lock acquisition fails, another process may
    /// complete the migrations while we're waiting, so we go back to check if
    /// there's still work to do rather than spinning on lock acquisition.
    ///
    /// ## Retry Strategy
    ///
    /// Uses exponential backoff with full jitter for the entire migration
    /// cycle:
    /// - **Base delay**: 1 second
    /// - **Maximum delay**: 30 seconds
    /// - **Maximum retries**: 10 attempts
    /// - **Jitter**: Full jitter (random delay between 0 and calculated
    ///   backoff)
    ///
    /// This approach ensures:
    /// - No unnecessary lock contention when migrations are already complete
    /// - Fair access under high concurrency (prevents thundering herd)
    /// - Efficient early termination when another process completes migrations
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Migration checking fails repeatedly
    /// - Lock acquisition fails after all retries
    /// - Migration validation detects corrupted files
    /// - Any migration statement execution fails
    pub async fn migrate(&self) -> Result<(), CassandraStoreError> {
        let mut attempt = 0;

        // Keep trying until success or max retries
        loop {
            attempt += 1;

            // Check if migrations are needed (lightweight operation)
            if !self.has_pending_migrations().await? {
                return Ok(());
            }

            match self.apply_migrations_with_lock().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt >= MAX_MIGRATION_RETRIES {
                        // Final attempt failed - do one last check before giving up
                        if !self.has_pending_migrations().await? {
                            return Ok(());
                        }

                        tracing::error!(
                            "Migration failed after {MAX_MIGRATION_RETRIES} retries: {e:#}"
                        );
                        return Err(e);
                    }

                    let sleep_duration = calculate_backoff(attempt);
                    warn!(
                        "Migration attempt {}/{} failed: {:#}. Retrying after {}",
                        attempt,
                        MAX_MIGRATION_RETRIES,
                        e,
                        format_duration(sleep_duration)
                    );
                    sleep(sleep_duration).await;
                }
            }
        }
    }

    /// Checks if there are pending migrations without acquiring a lock.
    async fn has_pending_migrations(&self) -> Result<bool, CassandraStoreError> {
        let pending = self.get_pending_migrations(self.keyspace).await?;
        if !pending.is_empty() {
            info!("Found {} pending migration(s) to apply", pending.len());
        }
        Ok(!pending.is_empty())
    }

    /// Acquires the migration lock and applies pending migrations.
    async fn apply_migrations_with_lock(&self) -> Result<(), CassandraStoreError> {
        let lock_guard = self
            .lock_manager
            .acquire("migration", MIGRATION_LOCK_TIMEOUT)
            .await?;

        // Apply migrations while holding the lock
        let result = self.apply_pending_migrations().await;

        // Explicitly release the lock
        lock_guard.release().await?;

        result
    }

    /// Applies pending migrations.
    async fn apply_pending_migrations(&self) -> Result<(), CassandraStoreError> {
        // Re-check for pending migrations (another process might have completed them)
        debug!("Re-checking for pending migrations while holding lock");
        let pending_migrations = self.get_pending_migrations(self.keyspace).await?;

        if pending_migrations.is_empty() {
            info!("All migrations already completed by another process");
            return Ok(());
        }

        info!(
            "Starting migration: {} migration(s) to apply",
            pending_migrations.len()
        );
        for (index, migration) in pending_migrations.iter().enumerate() {
            info!(
                "Applying migration {}/{}: {}",
                index + 1,
                pending_migrations.len(),
                migration.filename
            );
            self.executor
                .apply_migration(migration, self.keyspace)
                .await?;
        }

        info!(
            "Migration completed successfully: {} migration(s) applied",
            pending_migrations.len()
        );
        Ok(())
    }

    /// Gets the list of pending migrations that need to be applied.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace to check for pending migrations
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Failed to load embedded migrations
    /// - Failed to query applied migrations from database
    /// - Migration validation fails
    pub async fn get_pending_migrations(
        &self,
        keyspace: &str,
    ) -> Result<Vec<Migration>, CassandraStoreError> {
        debug!("Checking migration status for keyspace '{keyspace}'");
        let migrations = load_embedded_migrations(keyspace)?;
        let applied_migrations = self.get_applied_migrations(keyspace).await?;
        debug!(
            "Migration status: {} total, {} applied",
            migrations.len(),
            applied_migrations.len()
        );

        // Validate existing migrations
        validate_applied_migrations(&migrations, &applied_migrations)?;

        // Return pending migrations (convert from references to owned values)
        let pending_migrations = get_pending_migrations(&migrations, &applied_migrations);
        Ok(pending_migrations.into_iter().cloned().collect())
    }

    /// Retrieves all previously applied migrations from the database.
    async fn get_applied_migrations(
        &self,
        keyspace: &str,
    ) -> Result<HashMap<String, AppliedMigration>, CassandraStoreError> {
        debug!(
            "Querying applied migrations from {} table",
            TABLE_SCHEMA_MIGRATIONS
        );

        let select_sql =
            format!("select filename, checksum from {keyspace}.{TABLE_SCHEMA_MIGRATIONS}");
        let select_stmt = self
            .session
            .prepare(select_sql)
            .await
            .map_err(|e| MigrationError::AppliedMigrationsPreparationFailed(Box::new(e)))?;

        let stream = self
            .session
            .execute_iter(select_stmt, &[])
            .await?
            .rows_stream::<(String, String)>()?;

        let mut applied = HashMap::new();

        pin_mut!(stream);
        while let Some((filename, checksum)) = cooperative(stream.try_next()).await? {
            applied.insert(filename, AppliedMigration { checksum });
        }

        debug!("Found {} applied migrations", applied.len());
        Ok(applied)
    }
}

/// Errors that can occur during Cassandra schema migration.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// Keyspace metadata not found after refresh.
    #[error("Keyspace not found in cluster metadata")]
    KeyspaceNotFound,

    /// Failed to refresh cluster metadata.
    #[error("Failed to refresh metadata: {0:#}")]
    MetadataRefreshFailed(#[from] MetadataError),

    /// Failed to load embedded migration file.
    #[error("Failed to load migration file: {0}")]
    MigrationFileLoadFailed(String),

    /// Migration file contains invalid UTF-8.
    #[error("Invalid UTF-8 in migration file {file}: {source:#}")]
    InvalidUtf8 {
        /// The filename that contains invalid UTF-8.
        file: String,
        /// The underlying UTF-8 decoding error.
        source: Utf8Error,
    },

    /// Migration filename does not follow the required format.
    #[error("Invalid migration filename format: {0}")]
    InvalidFilenameFormat(String),

    /// Migration filename timestamp is not valid.
    #[error("Invalid timestamp in filename: {0}")]
    InvalidTimestamp(String),

    /// Failed to acquire distributed migration lock.
    #[error("Failed to acquire migration lock - another process may be running migrations")]
    LockAcquisitionFailed,

    /// Failed to execute a statement in a migration.
    #[error("Failed to execute statement {statement_index} in migration {migration}")]
    StatementExecutionFailed {
        /// The name of the migration file that failed.
        migration: String,
        /// The index of the statement within the migration that failed.
        statement_index: usize,
        /// The underlying database execution error.
        #[source]
        source: Box<ExecutionError>,
    },

    /// Migration checksum mismatch - file has been modified after being
    /// applied.
    #[error(
        "Migration {file} has been modified after being applied. Expected checksum: {expected}, \
         found: {actual}"
    )]
    ChecksumMismatch {
        /// The name of the migration file with a checksum mismatch.
        file: String,
        /// The expected checksum from the database.
        expected: String,
        /// The actual checksum computed from the current file.
        actual: String,
    },

    /// Failed to prepare lock-related statements after multiple retries.
    #[error("Failed to prepare lock statements after {0} attempts. Tables may not be available.")]
    LockPreparationFailed(u32),

    /// Database query execution failed.
    #[error(transparent)]
    ExecutionError(Box<ExecutionError>),

    /// Failed to prepare lock-related database statements.
    #[error("Failed to prepare lock statements")]
    LockStatementPreparationFailed(#[source] Box<PrepareError>),

    /// Failed to prepare migration tracking statement.
    #[error("Failed to prepare migration tracking statement")]
    MigrationTrackingPreparationFailed(#[source] Box<PrepareError>),

    /// Failed to prepare query for applied migrations.
    #[error("Failed to prepare applied migrations query")]
    AppliedMigrationsPreparationFailed(#[source] Box<PrepareError>),

    /// Failed to parse lock acquisition result into rows.
    #[error("Failed to parse lock acquisition result into rows")]
    LockResultIntoRowsFailed(Box<IntoRowsResultError>),

    /// Failed to extract first row from lock acquisition result.
    #[error("Failed to extract first row from lock acquisition result")]
    LockResultFirstRowFailed(Box<MaybeFirstRowError>),

    /// Failed to parse applied migrations from query result.
    #[error("Failed to parse applied migrations query result")]
    AppliedMigrationsParsingFailed(Box<TypeCheckError>),
}

impl From<ExecutionError> for MigrationError {
    fn from(error: ExecutionError) -> Self {
        Self::ExecutionError(Box::new(error))
    }
}

impl From<IntoRowsResultError> for MigrationError {
    fn from(error: IntoRowsResultError) -> Self {
        Self::LockResultIntoRowsFailed(Box::new(error))
    }
}

impl From<MaybeFirstRowError> for MigrationError {
    fn from(error: MaybeFirstRowError) -> Self {
        Self::LockResultFirstRowFailed(Box::new(error))
    }
}

impl From<TypeCheckError> for MigrationError {
    fn from(error: TypeCheckError) -> Self {
        Self::AppliedMigrationsParsingFailed(Box::new(error))
    }
}

impl ClassifyError for MigrationError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Metadata refresh failed, lock acquisition failed, statement execution failed,
            // database query execution failed, or lock result row extraction failed. All
            // represent transient network/cluster state that may resolve on retry.
            Self::MetadataRefreshFailed(_)
            | Self::LockAcquisitionFailed
            | Self::StatementExecutionFailed { .. }
            | Self::ExecutionError(_)
            | Self::LockResultFirstRowFailed(_) => ErrorCategory::Transient,

            // Keyspace not found, migration file load failed, invalid UTF-8, invalid filename
            // format, invalid timestamp, checksum mismatch, lock preparation failed, lock
            // statement preparation failed, migration tracking preparation failed, applied
            // migrations preparation failed, lock result parsing failed, or applied migrations
            // parsing failed. All represent configuration, build/packaging, or setup issues
            // that are not recoverable at runtime.
            Self::KeyspaceNotFound
            | Self::MigrationFileLoadFailed(_)
            | Self::InvalidUtf8 { .. }
            | Self::InvalidFilenameFormat(_)
            | Self::InvalidTimestamp(_)
            | Self::ChecksumMismatch { .. }
            | Self::LockPreparationFailed(_)
            | Self::LockStatementPreparationFailed(_)
            | Self::MigrationTrackingPreparationFailed(_)
            | Self::AppliedMigrationsPreparationFailed(_)
            | Self::LockResultIntoRowsFailed(_)
            | Self::AppliedMigrationsParsingFailed(_) => ErrorCategory::Terminal,
        }
    }
}
