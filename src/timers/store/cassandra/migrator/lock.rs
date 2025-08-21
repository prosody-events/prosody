//! Distributed locking for migration coordination.
//!
//! Provides [`MigrationLock`] which uses Cassandra lightweight transactions
//! to ensure only one process applies migrations at a time across a
//! distributed system.

use crate::timers::store::cassandra::{CassandraTriggerStoreError, InnerError, TABLE_LOCKS};
use chrono::Utc;
use humantime::format_duration;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use std::process;
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;
use whoami::fallible::hostname;

/// Distributed lock manager for migration coordination.
pub struct MigrationLock<'a> {
    session: &'a Session,
    acquire_lock: PreparedStatement,
    release_lock: PreparedStatement,
}

impl<'a> MigrationLock<'a> {
    /// Creates a new migration lock manager.
    ///
    /// # Arguments
    ///
    /// * `session` - The Cassandra session
    /// * `keyspace` - The keyspace containing the locks table
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if statement preparation fails.
    pub async fn new(
        session: &'a Session,
        keyspace: &str,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let (acquire_lock, release_lock) = prepare_lock_statements(session, keyspace).await?;

        Ok(Self {
            session,
            acquire_lock,
            release_lock,
        })
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
    pub async fn acquire(
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
        debug!(
            "Setting lock TTL to {} ({}s)",
            format_duration(timeout),
            ttl_seconds
        );

        debug!("Attempting to acquire '{lock_name}' lock");

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
            debug!("Migration lock acquired successfully with owner_id: {owner_id}");
            Ok(owner_id)
        } else {
            debug!("Failed to acquire migration lock - another process holds it");
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
    /// * `lock_name` - The name/identifier of the lock to release
    /// * `owner_id` - The UUID of the lock owner
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Lock is not owned by the specified owner
    /// - Database query fails
    pub async fn release(
        &self,
        lock_name: &str,
        owner_id: Uuid,
    ) -> Result<(), CassandraTriggerStoreError> {
        debug!("Releasing lock '{}' with owner_id: {}", lock_name, owner_id);

        self.session
            .execute_unpaged(&self.release_lock, (lock_name, owner_id))
            .await?;

        debug!("Lock '{}' released successfully", lock_name);
        Ok(())
    }
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
