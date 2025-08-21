//! Migration validation and integrity checking.
//!
//! Provides functionality to validate migration integrity and identify
//! pending migrations that need to be applied.

use super::loader::Migration;
use crate::timers::store::cassandra::{CassandraTriggerStoreError, InnerError};
use std::collections::HashMap;
use tracing::warn;

/// Record of a migration that has been applied to the database.
///
/// Tracks the checksum of applied migrations to detect if migration
/// files have been modified after being applied.
#[derive(Debug)]
pub struct AppliedMigration {
    /// SHA-256 checksum of the migration when it was applied.
    pub checksum: String,
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
pub fn validate_applied_migrations(
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
pub fn get_pending_migrations<'b>(
    migrations: &'b [Migration],
    applied_migrations: &HashMap<String, AppliedMigration>,
) -> Vec<&'b Migration> {
    migrations
        .iter()
        .filter(|m| !applied_migrations.contains_key(&m.filename))
        .collect()
}
