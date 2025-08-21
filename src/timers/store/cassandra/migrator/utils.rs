//! Utility functions for migration operations.
//!
//! Provides helper functions for retry logic and table management.

use crate::timers::store::cassandra::{
    CassandraTriggerStoreError, InnerError, TABLE_LOCKS, TABLE_SCHEMA_MIGRATIONS,
};
use rand::Rng;
use scylla::client::session::Session;
use std::cmp::min;
use std::time::Duration;
use tracing::{debug, info};

/// Base delay for exponential backoff (milliseconds).
const BACKOFF_BASE_MS: u64 = 1000; // 1 second

/// Maximum delay for exponential backoff (milliseconds).
const BACKOFF_MAX_MS: u64 = 30_000; // 30 seconds

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
pub fn calculate_backoff(attempt: u32) -> Duration {
    let exp_backoff = min(
        2u64.saturating_pow(attempt).saturating_mul(BACKOFF_BASE_MS),
        BACKOFF_MAX_MS,
    );
    // Full jitter: random delay between 0 and exp_backoff
    let jitter = rand::rng().random_range(0..=exp_backoff);
    Duration::from_millis(jitter)
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
pub async fn ensure_migration_tables_exist(
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

    if !migrations_table_exists {
        info!("Creating {} table", TABLE_SCHEMA_MIGRATIONS);
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

    if !locks_table_exists {
        info!("Creating {} table", TABLE_LOCKS);
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

/// Refreshes cluster metadata to ensure new tables are visible.
pub async fn refresh_metadata(session: &Session) -> Result<(), CassandraTriggerStoreError> {
    debug!("Refreshing cluster metadata");
    session
        .refresh_metadata()
        .await
        .map_err(|e| InnerError::Migration(format!("Failed to refresh metadata: {e}")))?;
    debug!("Metadata refresh completed");
    Ok(())
}
