//! Embedded database schema migration system for Cassandra.
//!
//! Provides the [`EmbeddedMigrator`] which handles automatic schema migration
//! for the Cassandra-based timer storage. Migrations are embedded as `.cql`
//! files at compile time and applied automatically during store initialization.
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

use crate::timers::store::cassandra::{
    CassandraTriggerStoreError, InnerError, TABLE_SCHEMA_MIGRATIONS,
};
use executor::MigrationExecutor;
use futures::{TryStreamExt, pin_mut};
use humantime::format_duration;
use loader::{Migration, load_embedded_migrations};
use lock::LockManager;
use scylla::client::session::Session;
use std::collections::HashMap;
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
pub struct EmbeddedMigrator<'a> {
    /// Cassandra session for executing migration statements.
    session: &'a Session,
    /// Target keyspace name for migrations.
    keyspace: &'a str,
    /// Migration lock manager.
    lock_manager: LockManager<'a>,
    /// Migration executor.
    executor: MigrationExecutor<'a>,
}

impl<'a> EmbeddedMigrator<'a> {
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
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Keyspace creation fails
    /// - Table creation fails
    /// - Statement preparation fails
    pub async fn new(
        session: &'a Session,
        keyspace: &'a str,
    ) -> Result<Self, CassandraTriggerStoreError> {
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

        Err(InnerError::Migration(format!(
            "Failed to prepare lock statements after {MAX_TABLE_PREPARATION_RETRIES} attempts. \
             Tables may not be available."
        ))
        .into())
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
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Migration checking fails repeatedly
    /// - Lock acquisition fails after all retries
    /// - Migration validation detects corrupted files
    /// - Any migration statement execution fails
    pub async fn migrate(&self) -> Result<(), CassandraTriggerStoreError> {
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
                        return Err(InnerError::Migration(format!(
                            "Migration failed after {MAX_MIGRATION_RETRIES} retries: {e:#}"
                        ))
                        .into());
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
    async fn has_pending_migrations(&self) -> Result<bool, CassandraTriggerStoreError> {
        let pending = self.get_pending_migrations(self.keyspace).await?;
        if !pending.is_empty() {
            info!("Found {} pending migration(s) to apply", pending.len());
        }
        Ok(!pending.is_empty())
    }

    /// Acquires the migration lock and applies pending migrations.
    async fn apply_migrations_with_lock(&self) -> Result<(), CassandraTriggerStoreError> {
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
    async fn apply_pending_migrations(&self) -> Result<(), CassandraTriggerStoreError> {
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
    pub async fn get_pending_migrations(
        &self,
        keyspace: &str,
    ) -> Result<Vec<Migration>, CassandraTriggerStoreError> {
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
    ) -> Result<HashMap<String, AppliedMigration>, CassandraTriggerStoreError> {
        debug!(
            "Querying applied migrations from {} table",
            TABLE_SCHEMA_MIGRATIONS
        );

        let select_sql =
            format!("select filename, checksum from {keyspace}.{TABLE_SCHEMA_MIGRATIONS}");
        let select_stmt = self.session.prepare(select_sql).await?;

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
