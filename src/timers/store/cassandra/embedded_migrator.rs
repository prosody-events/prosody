#![allow(clippy::same_name_method)]

use crate::timers::store::cassandra::{CassandraTriggerStoreError, InnerError};
use futures::{TryStreamExt, pin_mut};
use rust_embed::RustEmbed;
use scylla::client::session::Session;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{debug, info, warn};

#[derive(RustEmbed)]
#[folder = "src/timers/store/cassandra/migrations/"]
#[include = "*.cql"]
struct MigrationAssets;

#[derive(Debug, Clone)]
pub struct Migration {
    pub filename: String,
    pub content: String,
    pub checksum: String,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct AppliedMigration {
    pub checksum: String,
}

pub struct EmbeddedMigrator<'a> {
    session: &'a Session,
    keyspace: &'a str,
}

impl<'a> EmbeddedMigrator<'a> {
    pub fn new(session: &'a Session, keyspace: &'a str) -> Self {
        Self { session, keyspace }
    }

    pub async fn migrate(&self) -> Result<(), CassandraTriggerStoreError> {
        info!("Starting database migration");

        if self
            .session
            .get_cluster_state()
            .get_keyspace(self.keyspace)
            .is_none()
        {
            // Create keyspace if it doesn't exist
            let create_keyspace_stmt = self
                .session
                .prepare(format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{ 'class' : \
                     'SimpleStrategy', 'replication_factor': 1 }}",
                    self.keyspace
                ))
                .await?;
            self.session
                .execute_unpaged(&create_keyspace_stmt, &[])
                .await?;
        }

        self.session.use_keyspace(self.keyspace, true).await?;

        // Ensure migrations table exists
        self.ensure_migrations_table_exists(self.keyspace).await?;

        // Load and validate migrations
        debug!("Loading embedded migrations");
        let migrations = load_embedded_migrations()?;
        debug!("Loaded {} embedded migrations", migrations.len());

        debug!("Getting applied migrations");
        let applied_migrations = self.get_applied_migrations(self.keyspace).await?;
        debug!("Found {} applied migrations", applied_migrations.len());

        // Validate existing migrations
        validate_applied_migrations(&migrations, &applied_migrations)?;

        // Apply pending migrations
        let pending_migrations = get_pending_migrations(&migrations, &applied_migrations);

        if pending_migrations.is_empty() {
            info!("No pending migrations to apply");
            return Ok(());
        }

        info!("Applying {} pending migrations", pending_migrations.len());
        for migration in pending_migrations {
            debug!("Applying migration: {}", migration.filename);
            self.apply_migration(migration, self.keyspace).await?;
        }

        info!("Database migration completed successfully");
        Ok(())
    }

    async fn ensure_migrations_table_exists(
        &self,
        keyspace: &str,
    ) -> Result<(), CassandraTriggerStoreError> {
        // Check if schema_migrations table exists using cluster metadata
        let cluster_state = self.session.get_cluster_state();
        let table_exists = cluster_state
            .get_keyspace(keyspace)
            .and_then(|ks| ks.tables.get("schema_migrations"))
            .is_some();

        if table_exists {
            debug!("schema_migrations table already exists");
            return Ok(());
        }

        debug!("Creating schema_migrations table");
        let create_table_sql = format!(
            "CREATE TABLE {keyspace}.schema_migrations (filename text PRIMARY KEY, checksum text, \
             applied_at timestamp, execution_time_ms bigint)
            "
        );

        let create_table_stmt = self.session.prepare(create_table_sql).await?;
        self.session
            .execute_unpaged(&create_table_stmt, &[])
            .await?;
        debug!("schema_migrations table created successfully");

        // Refresh metadata to ensure the table is visible
        debug!("Refreshing cluster metadata");
        self.session
            .refresh_metadata()
            .await
            .map_err(|e| InnerError::Migration(format!("Failed to refresh metadata: {e}")))?;
        debug!("Metadata refresh completed");

        Ok(())
    }

    async fn get_applied_migrations(
        &self,
        keyspace: &str,
    ) -> Result<HashMap<String, AppliedMigration>, CassandraTriggerStoreError> {
        debug!("Querying applied migrations from schema_migrations table");

        let select_sql = format!("select filename, checksum from {keyspace}.schema_migrations",);
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

    async fn apply_migration(
        &self,
        migration: &Migration,
        keyspace: &str,
    ) -> Result<(), InnerError> {
        let start_time = std::time::Instant::now();

        info!("Applying migration: {}", migration.filename);

        // Parse and execute CQL statements
        let statements = parse_cql_statements(&migration.content);

        for statement_text in statements {
            if !statement_text.trim().is_empty() {
                debug!("Executing: {}", statement_text.trim());
                let mut prepared = self.session.prepare(statement_text).await?;
                prepared.set_use_cached_result_metadata(true);
                prepared.set_is_idempotent(true);
                self.session.execute_unpaged(&prepared, &[]).await?;
            }
        }

        let execution_time = start_time.elapsed().as_millis() as i64;

        // Record the migration as applied
        let applied_at = chrono::Utc::now();
        let insert_sql = format!(
            "insert into {keyspace}.schema_migrations (filename, checksum, applied_at, \
             execution_time_ms) values (?, ?, ?, ?)",
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

fn load_embedded_migrations() -> Result<Vec<Migration>, CassandraTriggerStoreError> {
    let mut migrations = Vec::new();

    for filename in MigrationAssets::iter() {
        let content = MigrationAssets::get(filename.as_ref()).ok_or_else(|| {
            InnerError::Migration(format!("Failed to load migration file: {filename}",))
        })?;

        let content_str = std::str::from_utf8(content.data.as_ref()).map_err(|e| {
            InnerError::Migration(format!("Invalid UTF-8 in migration file {filename}: {e}",))
        })?;

        let checksum = calculate_checksum(content_str);
        let timestamp = extract_timestamp(&filename)?;

        migrations.push(Migration {
            filename: filename.to_string(),
            content: content_str.to_owned(),
            checksum,
            timestamp,
        });
    }

    // Sort by timestamp to ensure proper ordering
    migrations.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    debug!("Loaded {} embedded migrations", migrations.len());
    Ok(migrations)
}

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

fn get_pending_migrations<'b>(
    migrations: &'b [Migration],
    applied_migrations: &HashMap<String, AppliedMigration>,
) -> Vec<&'b Migration> {
    migrations
        .iter()
        .filter(|m| !applied_migrations.contains_key(&m.filename))
        .collect()
}

fn calculate_checksum(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

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
