//! Migration file loading and parsing.
//!
//! Provides functionality to load embedded migration files, perform template
//! substitution, and parse CQL statements for execution.

use crate::cassandra::{
    TABLE_DEDUPLICATION, TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_SEGMENTS, TABLE_DEFERRED_TIMERS,
    TABLE_KEYS, TABLE_SCHEMA_MIGRATIONS, TABLE_SEGMENTS, TABLE_SLABS, TABLE_TYPED_KEYS,
    TABLE_TYPED_SLABS,
};
use base16ct::HexDisplay;
use rust_embed::RustEmbed;
use sha2::{Digest, Sha256};
use std::str;
use tracing::debug;

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

/// Embedded migration assets containing all `.cql` migration files.
///
/// This struct uses the `rust-embed` crate to embed all migration files
/// from the `src/timers/store/cassandra/migrations/` directory at compile time.
#[derive(RustEmbed)]
#[folder = "src/cassandra/migrations/"]
#[include = "*.cql"]
struct MigrationAssets;

/// Loads all embedded migration files and creates Migration objects.
///
/// Iterates through all embedded `.cql` files, extracts their content,
/// calculates checksums, and creates Migration structs. Sorts migrations
/// by timestamp to ensure correct application order.
///
/// # Arguments
///
/// * `keyspace` - The keyspace name for template substitution
///
/// # Returns
///
/// A vector of [`Migration`] objects sorted by timestamp.
///
/// # Errors
///
/// Returns an error if:
/// - Migration files cannot be loaded from embedded assets
/// - File content contains invalid UTF-8
/// - Timestamp extraction from filename fails
pub fn load_embedded_migrations(keyspace: &str) -> Result<Vec<Migration>, super::MigrationError> {
    let mut migrations = Vec::new();

    for filename in MigrationAssets::iter() {
        let content = MigrationAssets::get(filename.as_ref())
            .ok_or_else(|| super::MigrationError::MigrationFileLoadFailed(filename.to_string()))?;

        let mut content_str = str::from_utf8(content.data.as_ref())
            .map_err(|e| super::MigrationError::InvalidUtf8 {
                file: filename.to_string(),
                source: e,
            })?
            .to_owned();

        // Perform template substitution
        content_str = content_str
            .replace("{{KEYSPACE}}", keyspace)
            .replace("{{TABLE_SEGMENTS}}", TABLE_SEGMENTS)
            .replace("{{TABLE_SLABS}}", TABLE_SLABS)
            .replace("{{TABLE_KEYS}}", TABLE_KEYS)
            .replace("{{TABLE_TYPED_SLABS}}", TABLE_TYPED_SLABS)
            .replace("{{TABLE_TYPED_KEYS}}", TABLE_TYPED_KEYS)
            .replace("{{TABLE_SCHEMA_MIGRATIONS}}", TABLE_SCHEMA_MIGRATIONS)
            .replace("{{TABLE_DEFERRED_SEGMENTS}}", TABLE_DEFERRED_SEGMENTS)
            .replace("{{TABLE_DEFERRED_OFFSETS}}", TABLE_DEFERRED_OFFSETS)
            .replace("{{TABLE_DEFERRED_TIMERS}}", TABLE_DEFERRED_TIMERS)
            .replace("{{TABLE_DEDUPLICATION}}", TABLE_DEDUPLICATION);

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

    debug!("Loaded {} embedded migration file(s)", migrations.len());
    Ok(migrations)
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
pub fn parse_cql_statements(content: &str) -> Vec<String> {
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
    format!("{:x}", HexDisplay(&hasher.finalize()))
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
/// Returns an error if:
/// - Filename is shorter than 8 characters
/// - First 8 characters are not all digits
fn extract_timestamp(filename: &str) -> Result<String, super::MigrationError> {
    if filename.len() < 8 {
        return Err(super::MigrationError::InvalidFilenameFormat(
            filename.to_owned(),
        ));
    }

    let timestamp = &filename[..8];
    if !timestamp.chars().all(|c| c.is_ascii_digit()) {
        return Err(super::MigrationError::InvalidTimestamp(filename.to_owned()));
    }

    Ok(timestamp.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::Result;
    use std::collections::HashMap;

    #[test]
    fn checksums_match_cassandra() -> Result<()> {
        // Checksums recorded from the prosody keyspace in the local Cassandra cluster.
        // Any change here means calculate_checksum produces different output than main,
        // which would break migration idempotency checks against existing deployments.
        let expected: HashMap<&str, &str> = [
            (
                "20250613_create_timers.cql",
                "56b19957531ae94f21f41f9db74e05eef52f19bdc290d3fa94d619dbff4e14e4",
            ),
            (
                "20251023_add_timer_types.cql",
                "eb46357d9e6aae54433e3446d580c643a226062909eeb1ff6e01ef49a9776dad",
            ),
            (
                "20251126_create_deferred_offsets.cql",
                "dea2a6622c7284d7966abb9681bb3cea0e70327d93f02f313cdc906204261d6f",
            ),
            (
                "20251223_create_deferred_timers.cql",
                "7e111666bd66fb41826cfcf0f8daadd5e0bc114b6dacda359cf808030e40b13e",
            ),
            (
                "20260217_add_singleton_slots.cql",
                "c9833d9d611b2e8fe36dc9a651f3a8f3eb9d1567d69224df63f54c24c11623a6",
            ),
            (
                "20260319_create_deduplication.cql",
                "72568cda66e2055d552b5da9310ed518aed82fdbfe2b69db4c460cf7fb37a516",
            ),
        ]
        .into();

        let migrations = load_embedded_migrations("prosody")?;
        for migration in &migrations {
            if let Some(&want) = expected.get(migration.filename.as_str()) {
                assert_eq!(
                    migration.checksum, want,
                    "checksum mismatch for {}",
                    migration.filename
                );
            }
        }
        Ok(())
    }
}
