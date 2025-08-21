//! Tests for concurrent migration safety and lock behavior.
//!
//! These tests verify that the migration system handles concurrent access
//! correctly when multiple application instances start simultaneously.
//! The distributed locking mechanism should ensure that migrations are
//! applied exactly once without conflicts.

use super::EmbeddedMigrator;
use crate::timers::store::cassandra::{CassandraTriggerStoreError, TABLE_LOCKS};
use color_eyre::Result;
use color_eyre::eyre::Error as EyreError;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::iter::Iterator;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

/// Initializes test logging with appropriate levels for migration testing.
fn init_test_logging() -> Result<()> {
    if fmt()
        .compact()
        .with_env_filter(
            EnvFilter::builder()
                .with_env_var("PROSODY_LOG")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy()
                .add_directive("scylla=warn".parse()?),
        )
        .try_init()
        .is_err()
    {
        tracing::info!("logging already initialized");
    }
    Ok(())
}

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
const CONCURRENT_MIGRATORS: usize = 10;

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
    init_test_logging()?;

    let session = Arc::new(Box::pin(create_test_session()).await?);
    let keyspace = random_keyspace();

    let mut join_set = JoinSet::new();

    // Spawn multiple concurrent migration tasks
    for i in 0..CONCURRENT_MIGRATORS {
        let keyspace_clone = keyspace.clone();

        join_set.spawn(async move {
            let session = Box::pin(create_test_session()).await?;
            let migrator = EmbeddedMigrator::new(&session, &keyspace_clone).await?;

            // Each migrator attempts to run migrations - this is where the locking happens
            let result = migrator.migrate().await;

            Ok::<(usize, Result<(), CassandraTriggerStoreError>), EyreError>((i, result))
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
                tracing::error!("Migrator {migrator_id} failed: {e:#}");
            }
        }
    }

    // All migrators should succeed - either by applying migrations themselves
    // or by waiting for another migrator to complete them
    assert_eq!(
        success_count, CONCURRENT_MIGRATORS,
        "All {CONCURRENT_MIGRATORS} migrators should succeed. Successes: {success_count}, \
         Failures: {failure_count}"
    );

    // Verify that all migrations have been applied by checking for zero pending
    // migrations
    let final_migrator = EmbeddedMigrator::new(&session, &keyspace).await?;
    let pending_migrations = final_migrator.get_pending_migrations(&keyspace).await?;
    assert_eq!(
        pending_migrations.len(),
        0,
        "Expected zero pending migrations after all migrators completed, but found {} pending: \
         {:?}",
        pending_migrations.len(),
        pending_migrations
            .iter()
            .map(|m| &m.filename)
            .collect::<Vec<_>>()
    );

    // Verify that all locks have been released by checking the locks table
    let locks_query = format!("SELECT lock_name, owner_id FROM {keyspace}.{TABLE_LOCKS}");
    let remaining_locks = session.query_unpaged(locks_query, &[]).await?;
    let lock_rows = remaining_locks.into_rows_result()?;
    let lock_count = lock_rows
        .rows::<(String, Uuid)>()
        .map(Iterator::count)
        .unwrap_or(0);

    assert_eq!(
        lock_count, 0,
        "Expected zero remaining locks after all migrators completed, but found {lock_count} \
         active locks. This indicates a lock leak in the migration system."
    );

    // Cleanup
    cleanup_keyspace(&session, &keyspace).await?;

    Ok(())
}
