//! Embedded database schema migration system for Cassandra.
//!
//! Provides the [`EmbeddedMigrator`] which handles automatic schema migration
//! for the Cassandra-based timer storage. Migrations are embedded as `.cql`
//! files at compile time and applied automatically during store initialization.
//!
//! The migration system:
//! - Creates keyspaces if they don't exist
//! - Tracks applied migrations with checksums to detect modifications
//! - Applies pending migrations in timestamp order
//! - Validates migration integrity before executing
//!
//! Migration files are CQL scripts stored in
//! `src/timers/store/cassandra/migrations/` with timestamp-based naming
//! (`YYYYMMDD_description.cql`).

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
        }

        // Ensure migrations and migration locks tables exist
        Self::ensure_migration_tables_exist(session, keyspace).await?;

        // Prepare lock statements once
        let acquire_lock_sql = format!(
            "insert into {keyspace}.{TABLE_LOCKS} (lock_name, owner_id, acquired_at, expires_at, \
             process_info) values (?, ?, ?, ?, ?) if not exists using ttl ?"
        );
        let acquire_lock_stmt = session.prepare(acquire_lock_sql).await?;

        let release_lock_sql =
            format!("delete from {keyspace}.{TABLE_LOCKS} where lock_name = ? if owner_id = ?");
        let release_lock_stmt = session.prepare(release_lock_sql).await?;

        Ok(Self {
            session,
            keyspace,
            acquire_lock: acquire_lock_stmt,
            release_lock: release_lock_stmt,
        })
    }

    /// Executes the complete migration process with distributed locking.
    ///
    /// This method:
    /// 1. Creates the keyspace if it doesn't exist
    /// 2. Ensures the `schema_migrations` and `locks` tables exist
    /// 3. Performs a lightweight check for pending migrations
    /// 4. Only acquires expensive distributed lock if migrations are needed
    /// 5. Re-checks for pending migrations while holding the lock
    /// 6. Applies any pending migrations in timestamp order
    /// 7. Records successful migrations in the tracking table
    /// 8. Releases the migration lock
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Keyspace creation fails
    /// - Migration table creation fails
    /// - Lock acquisition fails or times out
    /// - Migration validation detects corrupted files
    /// - Any migration statement execution fails
    pub async fn migrate(&self) -> Result<(), CassandraTriggerStoreError> {
        // Do a lightweight check first to see if there's any work to do
        let pending_migrations = self.get_pending_migrations(self.keyspace).await?;

        if pending_migrations.is_empty() {
            debug!("No pending migrations found, skipping lock acquisition");
            return Ok(());
        }

        info!(
            "Found {} pending migrations, acquiring migration lock",
            pending_migrations.len()
        );

        // Only acquire lock when there's actual work to do
        let lock_owner_id = self
            .acquire_lock(self.keyspace, "migration", MIGRATION_LOCK_TIMEOUT)
            .await?;

        // Ensure lock is released even if migration fails
        let migration_result = async {
            // Re-check for pending migrations while holding the lock
            // (another process might have applied them while we were waiting for the lock)
            debug!("Re-checking for pending migrations while holding lock");
            let pending_migrations = self.get_pending_migrations(self.keyspace).await?;

            if pending_migrations.is_empty() {
                debug!(
                    "No pending migrations found after acquiring lock - another process completed \
                     them"
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
        .await;

        // Always attempt to release the lock
        if let Err(e) = self
            .release_lock(self.keyspace, "migration", lock_owner_id)
            .await
        {
            warn!("Failed to release lock: {:#}", e);
        }

        migration_result
    }

    /// Ensures the `schema_migrations` and `schema_migration_locks` tables
    /// exist.
    ///
    /// Checks for the existence of both migration tables and creates them if
    /// missing. The `schema_migrations` table tracks applied migrations
    /// with their checksums and execution metadata. The
    /// `schema_migration_locks` table provides distributed locking to
    /// prevent concurrent migration execution.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace where the migration tables should exist
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if table creation or metadata
    /// refresh fails.
    async fn ensure_migration_tables_exist(
        session: &Session,
        keyspace: &str,
    ) -> Result<(), CassandraTriggerStoreError> {
        let cluster_state = session.get_cluster_state();

        // Check if schema_migrations table exists
        let migrations_table_exists = cluster_state
            .get_keyspace(keyspace)
            .and_then(|ks| ks.tables.get(TABLE_SCHEMA_MIGRATIONS))
            .is_some();

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
        let locks_table_exists = cluster_state
            .get_keyspace(keyspace)
            .and_then(|ks| ks.tables.get(TABLE_LOCKS))
            .is_some();

        if locks_table_exists {
            debug!("{} table already exists", TABLE_LOCKS);
        } else {
            debug!("Creating {} table", TABLE_LOCKS);
            let create_locks_table_sql = format!(
                "create table if not exists {keyspace}.{TABLE_LOCKS} ( lock_name text primary \
                 key, owner_id uuid, acquired_at timestamp, expires_at timestamp, process_info \
                 text ) with compression = {{ 'class': 'ZstdCompressor' }} and compaction = {{ \
                 'class': 'UnifiedCompactionStrategy' }};
                "
            );
            session.query_unpaged(create_locks_table_sql, &[]).await?;
            debug!("{} table created successfully", TABLE_LOCKS);
        }

        // Refresh metadata to ensure both tables are visible
        if !migrations_table_exists || !locks_table_exists {
            debug!("Refreshing cluster metadata");
            session
                .refresh_metadata()
                .await
                .map_err(|e| InnerError::Migration(format!("Failed to refresh metadata: {e}")))?;
            debug!("Metadata refresh completed");
        }

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
    /// Uses Cassandra's IF NOT EXISTS condition to atomically acquire a
    /// named lock. Only one process can hold a specific lock at a time. The
    /// lock includes an expiration time and TTL for automatic cleanup of
    /// abandoned locks.
    ///
    /// Implements retry logic with exponential backoff and jitter for transient
    /// failures.
    ///
    /// # Arguments
    ///
    /// * `keyspace` - The keyspace containing the locks table
    /// * `lock_name` - The name/identifier of the lock to acquire
    /// * `timeout` - Lock timeout duration
    ///
    /// # Returns
    ///
    /// The owner ID (UUID) if lock acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Lock is already held by another process after all retries
    /// - Database query fails
    async fn acquire_lock(
        &self,
        _keyspace: &str,
        lock_name: &str,
        timeout: Duration,
    ) -> Result<Uuid, CassandraTriggerStoreError> {
        const BASE_DELAY_MS: u64 = 20; // Same as PROSODY_RETRY_BASE default
        const MAX_DELAY_MS: u64 = 30_000; // 30 seconds

        let owner_id = Uuid::new_v4();
        let process_info = format!(
            "{}:{}",
            hostname().unwrap_or_else(|_| "unknown".to_owned()),
            process::id()
        );
        let ttl_seconds = timeout.as_secs().min(i32::MAX as u64) as i32;

        debug!("Attempting to acquire '{lock_name}' lock with owner_id: {owner_id}");

        let mut attempt = 0;

        loop {
            let acquired_at = Utc::now();
            let expires_at = acquired_at
                + chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::minutes(10));

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
                .maybe_first_row::<(bool,)>()?
                .is_some_and(|(applied,)| applied);

            if applied {
                info!("Migration lock acquired successfully with owner_id: {owner_id}");
                return Ok(owner_id);
            }

            attempt += 1;

            // Calculate sleep time with exponential backoff and full jitter
            let exp_backoff = min(
                2u64.saturating_pow(attempt).saturating_mul(BASE_DELAY_MS),
                MAX_DELAY_MS,
            );
            // Full jitter: sleep for random time between 0 and exp_backoff
            let jitter = rand::rng().random_range(0..=exp_backoff);
            let sleep_time = Duration::from_millis(jitter);

            debug!(
                "Lock acquisition attempt {} failed, retrying after {:?}",
                attempt, sleep_time
            );

            sleep(sleep_time).await;
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
        _keyspace: &str,
        lock_name: &str,
        owner_id: Uuid,
    ) -> Result<(), CassandraTriggerStoreError> {
        debug!("Releasing lock '{}' with owner_id: {}", lock_name, owner_id);

        let result = self
            .session
            .execute_unpaged(&self.release_lock, (lock_name, owner_id))
            .await?;

        // Check if the LWT was applied (lock released)
        let applied: bool = result
            .into_rows_result()?
            .maybe_first_row::<(bool,)>()?
            .is_some_and(|(applied,)| applied);

        if applied {
            info!("Lock '{}' released successfully", lock_name);
            return Ok(());
        }

        warn!(
            "Failed to release lock '{}' - may have been released by another process or expired",
            lock_name
        );
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
