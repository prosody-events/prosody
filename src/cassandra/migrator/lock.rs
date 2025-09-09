//! Distributed locking for migration coordination.
//!
//! Provides [`LockManager`] which uses Cassandra lightweight transactions
//! to ensure only one process applies migrations at a time across a
//! distributed system.

use crate::cassandra::{CassandraStoreError, TABLE_LOCKS};
use chrono::Utc;
use humantime::format_duration;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use std::process;
use std::time::Duration;
use tracing::{debug, error};
use uuid::Uuid;
use whoami::fallible::hostname;

/// Distributed lock manager for migration coordination.
pub struct LockManager<'a> {
    session: &'a Session,
    acquire_lock: PreparedStatement,
    release_lock: PreparedStatement,
}

/// A distributed lock guard that requires explicit `release()` - warns on drop
/// if not released.
pub struct LockGuard<'a> {
    session: &'a Session,
    release_lock: &'a PreparedStatement,
    lock_name: &'a str,
    owner_id: Uuid,
    released: bool,
}

impl<'a> LockManager<'a> {
    /// Creates a new migration lock manager.
    ///
    /// # Arguments
    ///
    /// * `session` - The Cassandra session
    /// * `keyspace` - The keyspace containing the locks table
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if statement preparation fails.
    pub async fn new(session: &'a Session, keyspace: &str) -> Result<Self, CassandraStoreError> {
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
    /// A lock guard that requires explicit release.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Lock is already held by another process
    /// - Database query fails due to network or cluster issues
    /// - LWT result parsing fails
    pub async fn acquire(
        &'a self,
        lock_name: &'a str,
        timeout: Duration,
    ) -> Result<LockGuard<'a>, CassandraStoreError> {
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
            Ok(LockGuard {
                session: self.session,
                release_lock: &self.release_lock,
                lock_name,
                owner_id,
                released: false,
            })
        } else {
            debug!("Failed to acquire migration lock - another process holds it");
            Err(CassandraStoreError::Migration(
                "Failed to acquire migration lock - another process may be running migrations"
                    .to_owned(),
            ))
        }
    }
}

impl LockGuard<'_> {
    /// Explicitly releases the lock.
    ///
    /// This method consumes the guard and releases the lock. After calling
    /// this, the guard cannot be used again and will not trigger the drop
    /// warning.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if the database query fails.
    pub async fn release(mut self) -> Result<(), CassandraStoreError> {
        if !self.released {
            debug!(
                "Explicitly releasing lock '{}' with owner_id: {}",
                self.lock_name, self.owner_id
            );

            self.session
                .execute_unpaged(self.release_lock, (&self.lock_name, self.owner_id))
                .await?;

            self.released = true;
            debug!("Lock '{}' released successfully", self.lock_name);
        }
        Ok(())
    }
}

impl Drop for LockGuard<'_> {
    /// Warns if the lock is dropped without being explicitly released.
    ///
    /// This is a safety mechanism to help detect potential lock leaks.
    /// In normal operation, locks should be explicitly released using
    /// the `release()` method.
    fn drop(&mut self) {
        if !self.released {
            error!(
                "Lock '{}' with owner_id {} was dropped without being explicitly released! This \
                 may indicate a lock leak.",
                self.lock_name, self.owner_id
            );
        }
    }
}

/// Prepares the lock statements for the migration system.
async fn prepare_lock_statements(
    session: &Session,
    keyspace: &str,
) -> Result<(PreparedStatement, PreparedStatement), CassandraStoreError> {
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
