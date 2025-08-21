//! Migration file loading and parsing.
//!
//! Provides functionality to load embedded migration files, perform template
//! substitution, and parse CQL statements for execution.

use crate::timers::store::cassandra::{
    CassandraTriggerStoreError, InnerError, TABLE_KEYS, TABLE_SCHEMA_MIGRATIONS, TABLE_SEGMENTS,
    TABLE_SLABS,
};
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
#[folder = "src/timers/store/cassandra/migrations/"]
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
/// Returns [`CassandraTriggerStoreError`] if:
/// - Migration files cannot be loaded from embedded assets
/// - File content contains invalid UTF-8
/// - Timestamp extraction from filename fails
pub fn load_embedded_migrations(
    keyspace: &str,
) -> Result<Vec<Migration>, CassandraTriggerStoreError> {
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
