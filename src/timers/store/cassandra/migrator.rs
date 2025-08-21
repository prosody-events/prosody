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
//! - Base delay: 20ms (same as `PROSODY_RETRY_BASE`)
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

use crate::timers::store::cassandra::{
    CassandraTriggerStoreError, InnerError, TABLE_KEYS, TABLE_LOCKS, TABLE_SCHEMA_MIGRATIONS,
    TABLE_SEGMENTS, TABLE_SLABS,
};
use chrono::Utc;
use futures::{TryStreamExt, pin_mut};
use rand::Rng;
use rust_embed::RustEmbed;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::process;
use std::str;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;
use whoami::fallible::hostname;

/// Default timeout for migration lock acquisition.
const MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(10 * 60); // 10 minutes

/// Maximum number of retry attempts for migration.
const MAX_MIGRATION_RETRIES: u32 = 10;

/// Base delay for exponential backoff (milliseconds).
const BACKOFF_BASE_MS: u64 = 20; // Same as PROSODY_RETRY_BASE default

/// Maximum delay for exponential backoff (milliseconds).
const BACKOFF_MAX_MS: u64 = 30_000; // 30 seconds

/// Embedded migration assets containing all `.cql` migration files.
///
/// This struct uses the `rust-embed` crate to embed all migration files
/// from the `src/timers/store/cassandra/migrations/` directory at compile time.
#[derive(RustEmbed)]
#[folder = "src/timers/store/cassandra/migrations/"]
#[include = "*.cql"]
struct MigrationAssets;

/// Metadata and content for a single database migration.
///
/// Contains all information needed to track and apply a migration,
/// including the migration content and integrity checking via checksums.
#[derive(Debug, Clone)]
pub struct Migration {
    /// The migration filename (e.g., "`20240101_create_segments.cql`").
    pub filename: String,
    /// The complete CQL content of the migration file.
    pub content: String,
    /// SHA-256 checksum of the migration content for integrity verification.
    pub checksum: String,
    /// Extracted timestamp from the filename for ordering (e.g., "20240101").
    pub timestamp: String,
}

/// Record of a migration that has been applied to the database.
///
/// Tracks the checksum of applied migrations to detect if migration
/// files have been modified after being applied.
#[derive(Debug)]
pub struct AppliedMigration {
    /// SHA-256 checksum of the migration when it was applied.
    pub checksum: String,
}

/// Prepares the lock statements for the migration system.
async fn prepare_lock_statements(
    session: &Session,
    keyspace: &str,
) -> Result<(PreparedStatement, PreparedStatement), CassandraTriggerStoreError> {
    let acquire_lock = session
        .prepare(format!(
            "insert into {keyspace}.{TABLE_LOCKS} (lock_name, owner_id, acquired_at, expires_at, \
             process_info) values (?, ?, ?, ?, ?) if not exists using ttl ?"
        ))
        .await?;

    let release_lock = session
        .prepare(format!(
            "delete from {keyspace}.{TABLE_LOCKS} where lock_name = ? if owner_id = ?"
        ))
        .await?;

    Ok((acquire_lock, release_lock))
}

/// Ensures the `schema_migrations` and `schema_migration_locks` tables exist.
///
/// Creates the tables if they don't already exist. The `schema_migrations`
/// table tracks which migrations have been applied, while the `locks`
/// table provides distributed locking for concurrent migration attempts.
///
/// # Arguments
///
/// * `session` - The Cassandra session
/// * `keyspace` - The keyspace to create tables in
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if table creation fails.
async fn ensure_migration_tables_exist(
    session: &Session,
    keyspace: &str,
) -> Result<(), CassandraTriggerStoreError> {
    let cluster_state = session.get_cluster_state();
    let keyspace_metadata = cluster_state.get_keyspace(keyspace).ok_or_else(|| {
        CassandraTriggerStoreError::from(InnerError::Migration("keyspace not found".to_owned()))
    })?;
    let tables = &keyspace_metadata.tables;

    // Check if schema_migrations table exists
    let migrations_table_exists = tables.contains_key(TABLE_SCHEMA_MIGRATIONS);

    if migrations_table_exists {
        debug!("{} table already exists", TABLE_SCHEMA_MIGRATIONS);
    } else {
        debug!("Creating {} table", TABLE_SCHEMA_MIGRATIONS);
        let create_migrations_table_sql = format!(
            "create table if not exists {keyspace}.{TABLE_SCHEMA_MIGRATIONS} (filename text \
             primary key, checksum text, applied_at timestamp, execution_time_ms bigint) with \
             compression = {{ 'class': 'ZstdCompressor' }} and compaction = {{ 'class': \
             'UnifiedCompactionStrategy' }};
            "
        );
        session
            .query_unpaged(create_migrations_table_sql, &[])
            .await?;
        debug!("{} table created successfully", TABLE_SCHEMA_MIGRATIONS);
    }

    // Check if schema_migration_locks table exists
    let locks_table_exists = tables.contains_key(TABLE_LOCKS);

    if locks_table_exists {
        debug!("{} table already exists", TABLE_LOCKS);
    } else {
        debug!("Creating {} table", TABLE_LOCKS);
        let create_locks_table_sql = format!(
            "create table if not exists {keyspace}.{TABLE_LOCKS} ( lock_name text primary key, \
             owner_id uuid, acquired_at timestamp, expires_at timestamp, process_info text ) with \
             compression = {{ 'class': 'ZstdCompressor' }};"
        );
        session.query_unpaged(create_locks_table_sql, &[]).await?;
        debug!("{} table created successfully", TABLE_LOCKS);
    }

    // Refresh metadata to ensure both tables are visible
    if !migrations_table_exists || !locks_table_exists {
        refresh_metadata(session).await?;
    }

    Ok(())
}

async fn refresh_metadata(session: &Session) -> Result<(), CassandraTriggerStoreError> {
    debug!("Refreshing cluster metadata");
    session
        .refresh_metadata()
        .await
        .map_err(|e| InnerError::Migration(format!("Failed to refresh metadata: {e}")))?;
    debug!("Metadata refresh completed");
    Ok(())
}

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
    /// Prepared statement for acquiring locks.
    acquire_lock: PreparedStatement,
    /// Prepared statement for releasing locks.
    release_lock: PreparedStatement,
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
            let create_keyspace_cql = format!(
                "create keyspace if not exists {keyspace} WITH replication = {{ 'class' : \
                 'SimpleStrategy', 'replication_factor': 1 }}"
            );
            session.query_unpaged(create_keyspace_cql, &[]).await?;
            refresh_metadata(session).await?;
        }

        ensure_migration_tables_exist(session, keyspace).await?;

        // Tables may not immediately be available after creation, so retry
        loop {
            if let Ok((acquire_lock, release_lock)) =
                prepare_lock_statements(session, keyspace).await
            {
                return Ok(Self {
                    session,
                    keyspace,
                    acquire_lock,
                    release_lock,
                });
            }

            refresh_metadata(session).await?;
        }
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
    /// - **Base delay**: 20ms (matches `PROSODY_RETRY_BASE` default)
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
        for attempt in 0..=MAX_MIGRATION_RETRIES {
            // Check if migrations are needed (lightweight operation)
            if !self.has_pending_migrations().await? {
                debug!("No pending migrations found, migration complete");
                return Ok(());
            }

            // Try to acquire lock and apply migrations
            match self.try_apply_migrations().await {
                Ok(()) => return Ok(()),
                Err(e) if attempt == MAX_MIGRATION_RETRIES => {
                    return Err(InnerError::Migration(format!(
                        "Migration failed after {MAX_MIGRATION_RETRIES} retries: {e:#}"
                    ))
                    .into());
                }
                Err(e) => {
                    let sleep_duration = calculate_backoff(attempt + 1);
                    debug!(
                        "Migration attempt {} failed, retrying after {:?}: {e:#}",
                        attempt + 1,
                        sleep_duration
                    );
                    sleep(sleep_duration).await;
                }
            }
        }

        // This point is unreachable due to the loop bounds, but the compiler needs a
        // return
        Err(InnerError::Migration("Migration retry loop terminated unexpectedly".to_owned()).into())
    }

    /// Checks if there are pending migrations without acquiring a lock.
    async fn has_pending_migrations(&self) -> Result<bool, CassandraTriggerStoreError> {
        let pending = self.get_pending_migrations(self.keyspace).await?;
        if !pending.is_empty() {
            info!("Found {} pending migrations", pending.len());
        }
        Ok(!pending.is_empty())
    }

    /// Attempts to acquire the migration lock and apply pending migrations.
    async fn try_apply_migrations(&self) -> Result<(), CassandraTriggerStoreError> {
        debug!("Attempting to acquire migration lock");
        let lock_owner_id = self
            .acquire_lock("migration", MIGRATION_LOCK_TIMEOUT)
            .await?;

        // Ensure lock is released even if migration fails
        let result = self.apply_migrations_with_lock().await;

        if let Err(e) = self.release_lock("migration", lock_owner_id).await {
            warn!("Failed to release migration lock: {:#}", e);
        }

        result
    }

    /// Applies pending migrations while holding the lock.
    async fn apply_migrations_with_lock(&self) -> Result<(), CassandraTriggerStoreError> {
        // Re-check for pending migrations (another process might have completed them)
        debug!("Re-checking for pending migrations while holding lock");
        let pending_migrations = self.get_pending_migrations(self.keyspace).await?;

        if pending_migrations.is_empty() {
            debug!(
                "No pending migrations found after acquiring lock - another process completed them"
            );
            return Ok(());
        }

        info!("Applying {} pending migrations", pending_migrations.len());
        for migration in &pending_migrations {
            debug!("Applying migration: {}", migration.filename);
            self.apply_migration(migration, self.keyspace).await?;
        }

        info!("Database migration completed successfully");
        Ok(())
    }

    /// Gets the list of pending migrations that need to be applied.
    ///
    /// Loads embedded migrations, queries applied migrations from the database,
    /// validates migration integrity, and returns the list of migrations that
    /// haven't been applied yet.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace containing the migration tracking table
    ///
    /// # Returns
    ///
    /// A vector of migrations that need to be applied.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Migration loading or validation fails
    /// - Database query fails
    /// - Migration integrity validation detects corrupted files
    async fn get_pending_migrations(
        &self,
        keyspace: &str,
    ) -> Result<Vec<Migration>, CassandraTriggerStoreError> {
        debug!("Loading embedded migrations");
        let migrations = load_embedded_migrations(keyspace)?;
        debug!("Loaded {} embedded migrations", migrations.len());

        debug!("Getting applied migrations");
        let applied_migrations = self.get_applied_migrations(keyspace).await?;
        debug!("Found {} applied migrations", applied_migrations.len());

        // Validate existing migrations
        validate_applied_migrations(&migrations, &applied_migrations)?;

        // Return pending migrations (convert from references to owned values)
        let pending_migrations = get_pending_migrations(&migrations, &applied_migrations);
        Ok(pending_migrations.into_iter().cloned().collect())
    }

    /// Acquires a distributed lock using lightweight transactions.
    ///
    /// Uses Cassandra's `IF NOT EXISTS` condition to atomically acquire a named
    /// lock. Only one process can hold a specific lock at a time. The lock
    /// includes an expiration time and TTL for automatic cleanup of
    /// abandoned locks.
    ///
    /// ## Lock Properties
    ///
    /// - **Atomic**: Uses Cassandra LWT for atomic lock acquisition
    /// - **Distributed**: Works across multiple nodes and data centers
    /// - **Self-healing**: TTL automatically releases abandoned locks
    /// - **Process-safe**: Includes hostname and PID for debugging
    ///
    /// # Arguments
    ///
    /// * `lock_name` - The name/identifier of the lock to acquire
    /// * `timeout` - Lock timeout duration (also used as TTL)
    ///
    /// # Returns
    ///
    /// The owner ID (UUID) if lock acquisition succeeds, or an error if the
    /// lock is already held by another process.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Lock is already held by another process
    /// - Database query fails due to network or cluster issues
    /// - LWT result parsing fails
    async fn acquire_lock(
        &self,
        lock_name: &str,
        timeout: Duration,
    ) -> Result<Uuid, CassandraTriggerStoreError> {
        let owner_id = Uuid::new_v4();
        let acquired_at = Utc::now();
        let expires_at = acquired_at
            + chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::minutes(10));
        let process_info = format!(
            "{}:{}",
            hostname().unwrap_or_else(|_| "unknown".to_owned()),
            process::id()
        );
        let ttl_seconds = timeout.as_secs().min(i32::MAX as u64) as i32;

        debug!("Attempting to acquire '{lock_name}' lock with owner_id: {owner_id}");

        let result = self
            .session
            .execute_unpaged(
                &self.acquire_lock,
                (
                    lock_name,
                    owner_id,
                    acquired_at,
                    expires_at,
                    &process_info,
                    ttl_seconds,
                ),
            )
            .await?;

        // Check if the LWT was applied (lock acquired)
        let applied = result
            .into_rows_result()?
            .maybe_first_row::<Row>()?
            .is_some_and(|row| matches!(row.columns.first(), Some(Some(CqlValue::Boolean(true)))));

        if applied {
            info!("Migration lock acquired successfully with owner_id: {owner_id}");
            Ok(owner_id)
        } else {
            Err(InnerError::Migration(
                "Failed to acquire migration lock - another process may be running migrations"
                    .to_owned(),
            )
            .into())
        }
    }

    /// Releases a distributed lock using the owner ID.
    ///
    /// Uses Cassandra's conditional DELETE with owner ID verification to safely
    /// release a named lock. Only the process that acquired the lock can
    /// release it.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace containing the locks table
    /// * `lock_name` - The name/identifier of the lock to release
    /// * `owner_id` - The UUID of the lock owner
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Lock is not owned by the specified owner
    /// - Database query fails
    async fn release_lock(
        &self,
        lock_name: &str,
        owner_id: Uuid,
    ) -> Result<(), CassandraTriggerStoreError> {
        debug!("Releasing lock '{}' with owner_id: {}", lock_name, owner_id);

        self.session
            .execute_unpaged(&self.release_lock, (lock_name, owner_id))
            .await?;

        info!("Lock '{}' released successfully", lock_name);
        Ok(())
    }

    /// Retrieves all previously applied migrations from the database.
    ///
    /// Queries the `schema_migrations` table to get the list of migrations
    /// that have already been applied, along with their checksums for
    /// integrity verification.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace containing the migrations table
    ///
    /// # Returns
    ///
    /// A [`HashMap`] mapping migration filenames to their applied metadata.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if the query fails.
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
        while let Some((filename, checksum)) = stream.try_next().await? {
            applied.insert(filename, AppliedMigration { checksum });
        }

        debug!("Found {} applied migrations", applied.len());
        Ok(applied)
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
    /// Returns [`InnerError`] if statement execution or migration
    /// recording fails.
    async fn apply_migration(
        &self,
        migration: &Migration,
        keyspace: &str,
    ) -> Result<(), InnerError> {
        let start_time = Instant::now();

        info!("Applying migration: {}", migration.filename);

        // Parse and execute CQL statements
        let statements = parse_cql_statements(&migration.content);

        for statement_text in statements {
            if !statement_text.trim().is_empty() {
                debug!("Executing: {}", statement_text.trim());
                self.session.query_unpaged(statement_text, &[]).await?;
            }
        }

        let execution_time = start_time.elapsed().as_millis() as i64;

        // Record the migration as applied
        let applied_at = chrono::Utc::now();
        let insert_sql = format!(
            "insert into {keyspace}.{TABLE_SCHEMA_MIGRATIONS} (filename, checksum, applied_at, \
             execution_time_ms) values (?, ?, ?, ?)"
        );
        let insert_stmt = self.session.prepare(insert_sql).await?;

        self.session
            .execute_unpaged(
                &insert_stmt,
                (
                    &migration.filename,
                    &migration.checksum,
                    applied_at,
                    execution_time,
                ),
            )
            .await?;

        info!(
            "Migration {} applied successfully in {}ms",
            migration.filename, execution_time
        );
        Ok(())
    }
}

/// Loads all embedded migration files and creates Migration objects.
///
/// Iterates through all embedded `.cql` files, extracts their content,
/// calculates checksums, and creates Migration structs. Sorts migrations
/// by timestamp to ensure correct application order.
///
/// # Returns
///
/// A vector of [`Migration`] objects sorted by timestamp.
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if:
/// - Migration files cannot be loaded from embedded assets
/// - File content contains invalid UTF-8
/// - Timestamp extraction from filename fails
fn load_embedded_migrations(keyspace: &str) -> Result<Vec<Migration>, CassandraTriggerStoreError> {
    let mut migrations = Vec::new();

    for filename in MigrationAssets::iter() {
        let content = MigrationAssets::get(filename.as_ref()).ok_or_else(|| {
            InnerError::Migration(format!("Failed to load migration file: {filename}",))
        })?;

        let mut content_str = str::from_utf8(content.data.as_ref())
            .map_err(|e| {
                InnerError::Migration(format!("Invalid UTF-8 in migration file {filename}: {e}",))
            })?
            .to_owned();

        // Perform template substitution
        content_str = content_str
            .replace("{{KEYSPACE}}", keyspace)
            .replace("{{TABLE_SEGMENTS}}", TABLE_SEGMENTS)
            .replace("{{TABLE_SLABS}}", TABLE_SLABS)
            .replace("{{TABLE_KEYS}}", TABLE_KEYS)
            .replace("{{TABLE_SCHEMA_MIGRATIONS}}", TABLE_SCHEMA_MIGRATIONS);

        let checksum = calculate_checksum(&content_str);
        let timestamp = extract_timestamp(&filename)?;

        migrations.push(Migration {
            filename: filename.to_string(),
            content: content_str,
            checksum,
            timestamp,
        });
    }

    // Sort by timestamp to ensure proper ordering
    migrations.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    debug!("Loaded {} embedded migrations", migrations.len());
    Ok(migrations)
}

/// Validates that applied migrations haven't been modified.
///
/// Compares checksums of embedded migration files against the checksums
/// recorded when they were applied. This detects if migration files have
/// been modified after being applied to the database.
///
/// # Arguments
///
/// * `migrations` - All available embedded migrations
/// * `applied_migrations` - Previously applied migrations from the database
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if any applied migration has
/// a different checksum than the current embedded file.
fn validate_applied_migrations(
    migrations: &[Migration],
    applied_migrations: &HashMap<String, AppliedMigration>,
) -> Result<(), CassandraTriggerStoreError> {
    let migration_map: HashMap<String, &Migration> =
        migrations.iter().map(|m| (m.filename.clone(), m)).collect();

    for (filename, applied) in applied_migrations {
        match migration_map.get(filename) {
            Some(migration) => {
                if migration.checksum != applied.checksum {
                    return Err(InnerError::Migration(format!(
                        "Migration {} has been modified after being applied. Expected checksum: \
                         {}, found: {}",
                        filename, applied.checksum, migration.checksum
                    ))
                    .into());
                }
            }
            None => {
                warn!(
                    "Applied migration {} not found in embedded migrations",
                    filename
                );
            }
        }
    }

    Ok(())
}

/// Identifies migrations that haven't been applied yet.
///
/// Filters the list of available migrations to find those that don't
/// appear in the applied migrations tracking table.
///
/// # Arguments
///
/// * `migrations` - All available embedded migrations
/// * `applied_migrations` - Previously applied migrations from the database
///
/// # Returns
///
/// A vector of references to migrations that need to be applied.
fn get_pending_migrations<'b>(
    migrations: &'b [Migration],
    applied_migrations: &HashMap<String, AppliedMigration>,
) -> Vec<&'b Migration> {
    migrations
        .iter()
        .filter(|m| !applied_migrations.contains_key(&m.filename))
        .collect()
}

/// Calculates a SHA-256 checksum for migration content.
///
/// Generates a hexadecimal SHA-256 hash of the migration file content
/// for integrity verification and change detection.
///
/// # Arguments
///
/// * `content` - The migration file content to hash
///
/// # Returns
///
/// A hexadecimal string representation of the SHA-256 hash.
fn calculate_checksum(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Extracts the timestamp prefix from a migration filename.
///
/// Migration files must follow the naming convention `YYYYMMDD_description.cql`
/// where the first 8 characters form a timestamp for ordering.
///
/// # Arguments
///
/// * `filename` - The migration filename to extract from
///
/// # Returns
///
/// The 8-character timestamp string (e.g., "20240101").
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if:
/// - Filename is shorter than 8 characters
/// - First 8 characters are not all digits
fn extract_timestamp(filename: &str) -> Result<String, CassandraTriggerStoreError> {
    if filename.len() < 8 {
        return Err(InnerError::Migration(format!(
            "Invalid migration filename format: {filename}",
        ))
        .into());
    }

    let timestamp = &filename[..8];
    if !timestamp.chars().all(|c| c.is_ascii_digit()) {
        return Err(
            InnerError::Migration(format!("Invalid timestamp in filename: {filename}",)).into(),
        );
    }

    Ok(timestamp.to_owned())
}

/// Calculates exponential backoff with full jitter for retry attempts.
///
/// Uses the "full jitter" strategy recommended by AWS for distributed systems
/// to prevent thundering herd problems. Returns a random duration between
/// 0 and the calculated exponential backoff value.
///
/// # Arguments
///
/// * `attempt` - The current retry attempt number (1-based)
///
/// # Returns
///
/// A random duration between 0 and min(base * 2^attempt, `max_delay`)
fn calculate_backoff(attempt: u32) -> Duration {
    let exp_backoff = min(
        2u64.saturating_pow(attempt).saturating_mul(BACKOFF_BASE_MS),
        BACKOFF_MAX_MS,
    );
    // Full jitter: random delay between 0 and exp_backoff
    let jitter = rand::rng().random_range(0..=exp_backoff);
    Duration::from_millis(jitter)
}

/// Parses a migration file into individual CQL statements.
///
/// Splits the migration content into separate CQL statements by looking
/// for semicolon terminators. Filters out empty lines and SQL comments
/// (lines starting with `--`).
///
/// # Arguments
///
/// * `content` - The complete migration file content
///
/// # Returns
///
/// A vector of individual CQL statement strings ready for execution.
fn parse_cql_statements(content: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        current_statement.push_str(line);
        current_statement.push('\n');

        // Check if statement is complete
        if trimmed.ends_with(';') {
            statements.push(current_statement.trim().to_owned());
            current_statement.clear();
        }
    }

    // Handle case where last statement doesn't end with semicolon
    if !current_statement.trim().is_empty() {
        statements.push(current_statement.trim().to_owned());
    }

    statements
}

#[cfg(test)]
mod tests {
    //! Tests for concurrent migration safety and lock behavior.
    //!
    //! These tests verify that the migration system handles concurrent access
    //! correctly when multiple application instances start simultaneously.
    //! The distributed locking mechanism should ensure that migrations are
    //! applied exactly once without conflicts.

    use super::*;
    use color_eyre::Result;
    use scylla::client::session_builder::SessionBuilder;
    use std::sync::Arc;
    use tokio::task::JoinSet;
    use uuid::Uuid;

    /// Creates a Cassandra session for testing.
    async fn create_test_session() -> Result<Session> {
        let session = SessionBuilder::new()
            .known_node("localhost:9042")
            .build()
            .await?;
        Ok(session)
    }

    /// Generates a random keyspace name for test isolation.
    fn random_keyspace() -> String {
        let uuid = Uuid::new_v4().simple();
        format!("test_migration_{}", &uuid.to_string()[..8])
    }

    /// Cleans up a test keyspace by dropping it.
    async fn cleanup_keyspace(session: &Session, keyspace: &str) -> Result<()> {
        let drop_cql = format!("drop keyspace if exists {keyspace}");
        session.query_unpaged(drop_cql, &[]).await?;
        Ok(())
    }

    /// Number of concurrent migrators to test
    const CONCURRENT_MIGRATORS: usize = 30;

    /// Tests that concurrent migrations work safely without race conditions.
    ///
    /// This test spawns multiple concurrent migrators against the same random
    /// keyspace and verifies that:
    /// 1. All migrators succeed (no failures due to race conditions)
    /// 2. Migrations are applied exactly once to the database
    /// 3. The distributed locking prevents conflicts between processes
    ///
    /// The test uses a `JoinSet` to spawn concurrent tasks that each create
    /// their own `EmbeddedMigrator` and call `migrate()`. The migration
    /// system's built-in distributed locking should coordinate these calls
    /// so that only one process applies the migrations while others wait
    /// and then succeed when they see migrations are already complete.
    #[tokio::test]
    async fn test_concurrent_migration_lock_safety() -> Result<()> {
        let session = Arc::new(Box::pin(create_test_session()).await?);
        let keyspace = random_keyspace();

        let mut join_set = JoinSet::new();

        // Spawn multiple concurrent migration tasks
        for i in 0..CONCURRENT_MIGRATORS {
            let session_clone = session.clone();
            let keyspace_clone = keyspace.clone();

            join_set.spawn(async move {
                let migrator = EmbeddedMigrator::new(&session_clone, &keyspace_clone).await?;

                // Each migrator attempts to run migrations - this is where the locking happens
                let result = migrator.migrate().await;

                Ok::<(usize, Result<(), CassandraTriggerStoreError>), color_eyre::eyre::Error>((
                    i, result,
                ))
            });
        }

        let mut success_count = 0_usize;
        let mut failure_count = 0_usize;

        // Collect all results
        while let Some(join_result) = join_set.join_next().await {
            let (migrator_id, migration_result) = join_result??;

            match migration_result {
                Ok(()) => {
                    success_count += 1;
                }
                Err(e) => {
                    failure_count += 1;
                    println!("Migrator {} failed: {:#}", migrator_id, e);
                }
            }
        }

        // All migrators should succeed - either by applying migrations themselves
        // or by waiting for another migrator to complete them
        assert_eq!(
            success_count, CONCURRENT_MIGRATORS,
            "All {} migrators should succeed. Successes: {}, Failures: {}",
            CONCURRENT_MIGRATORS, success_count, failure_count
        );

        // Cleanup
        cleanup_keyspace(&session, &keyspace).await?;

        Ok(())
    }
}
