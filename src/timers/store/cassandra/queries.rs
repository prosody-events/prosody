//! Prepared CQL statement management for Cassandra timer storage.
//!
//! This module provides the [`Queries`] struct which contains all prepared
//! Cassandra CQL statements needed for timer storage operations. Prepared
//! statements are created once during initialization and reused for all
//! database operations, providing better performance and security.
//!
//! The module handles:
//! - Cassandra session creation and configuration
//! - Schema migration execution via [`EmbeddedMigrator`]
//! - Preparation of all CQL statements for CRUD operations
//! - Load balancing and retry policy configuration
//! - TTL and non-TTL variants of insert statements

use super::migrator::EmbeddedMigrator;
use crate::timers::store::cassandra::{
    CassandraConfiguration, CassandraTriggerStoreError, TABLE_KEYS, TABLE_SEGMENTS, TABLE_SLABS,
};
use educe::Educe;
use scylla::client::Compression;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::DefaultRetryPolicy;
use scylla::statement::Consistency;

use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

/// Container for all prepared Cassandra CQL statements used by the timer store.
///
/// This struct holds the Cassandra session and all prepared statements needed
/// for timer storage operations. Prepared statements provide better performance
/// and security compared to ad-hoc query strings.
///
/// The struct contains prepared statements for:
/// - Segment management (create, read, delete)
/// - Slab operations (insert, delete, range queries)
/// - Trigger operations for both time-based and key-based indices
/// - TTL and non-TTL variants for data lifecycle management
#[derive(Educe)]
#[educe(Debug)]
pub struct Queries {
    #[educe(Debug(ignore))]
    pub session: Session,

    #[educe(Debug(ignore))]
    pub insert_segment: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_segment: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_segment: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_slabs: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_slab_range: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_slab: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_slab_triggers: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_slab_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub clear_slab_triggers: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_key_times: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_key_triggers: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_key_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_key_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub clear_key_triggers: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_no_ttl: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_trigger_no_ttl: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_key_trigger_no_ttl: PreparedStatement,
}

impl Queries {
    /// Creates a new Queries instance with prepared statements for all timer
    /// operations.
    ///
    /// This method establishes a connection to Cassandra, runs schema
    /// migrations, and prepares all CQL statements needed for timer storage
    /// operations.
    ///
    /// # Arguments
    ///
    /// * `configuration` - Cassandra connection settings including nodes,
    ///   credentials, keyspace, and load balancing preferences.
    ///
    /// # Returns
    ///
    /// A [`Queries`] instance with all prepared statements ready for use.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Connection to Cassandra cluster fails
    /// - Schema migration fails
    /// - Keyspace cannot be selected
    /// - Statement preparation fails
    pub async fn new(
        configuration: CassandraConfiguration,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let mut lb_policy = DefaultPolicy::builder()
            .token_aware(true)
            .permit_dc_failover(true);

        if let Some(dc) = configuration.datacenter {
            lb_policy = match configuration.rack {
                None => lb_policy.prefer_datacenter(dc),
                Some(rack) => lb_policy.prefer_datacenter_and_rack(dc, rack),
            }
        }

        let _profile = ExecutionProfile::builder()
            .consistency(Consistency::LocalQuorum)
            .load_balancing_policy(lb_policy.build())
            .retry_policy(Arc::new(DefaultRetryPolicy::new()));

        let mut session = SessionBuilder::new()
            .known_nodes(configuration.nodes)
            .compression(Some(Compression::Lz4));

        if let Some(user) = configuration.user {
            session = session.user(user, configuration.password.unwrap_or_default());
        }

        let session = session.build().await?;
        let migrator = EmbeddedMigrator::new(&session, &configuration.keyspace).await?;
        migrator.migrate().await?;

        let keyspace = &configuration.keyspace;

        let insert_segment = prepare_insert_segment(&session, keyspace).await?;
        let get_segment = prepare_get_segment(&session, keyspace).await?;
        let delete_segment = prepare_delete_segment(&session, keyspace).await?;
        let get_slabs = prepare_get_slabs(&session, keyspace).await?;
        let get_slab_range = prepare_get_slab_range(&session, keyspace).await?;
        let insert_slab = prepare_insert_slab(&session, keyspace).await?;
        let delete_slab = prepare_delete_slab(&session, keyspace).await?;
        let get_slab_triggers = prepare_get_slab_triggers(&session, keyspace).await?;
        let insert_slab_trigger = prepare_insert_slab_trigger(&session, keyspace).await?;
        let delete_slab_trigger = prepare_delete_slab_trigger(&session, keyspace).await?;
        let clear_slab_triggers = prepare_clear_slab_triggers(&session, keyspace).await?;
        let get_key_times = prepare_get_key_times(&session, keyspace).await?;
        let get_key_triggers = prepare_get_key_triggers(&session, keyspace).await?;
        let insert_key_trigger = prepare_insert_key_trigger(&session, keyspace).await?;
        let delete_key_trigger = prepare_delete_key_trigger(&session, keyspace).await?;
        let clear_key_triggers = prepare_clear_key_triggers(&session, keyspace).await?;
        let insert_slab_no_ttl = prepare_insert_slab_no_ttl(&session, keyspace).await?;
        let insert_slab_trigger_no_ttl =
            prepare_insert_slab_trigger_no_ttl(&session, keyspace).await?;
        let insert_key_trigger_no_ttl =
            prepare_insert_key_trigger_no_ttl(&session, keyspace).await?;

        Ok(Self {
            session,
            insert_segment,
            get_segment,
            delete_segment,
            get_slabs,
            get_slab_range,
            insert_slab,
            delete_slab,
            get_slab_triggers,
            insert_slab_trigger,
            delete_slab_trigger,
            clear_slab_triggers,
            get_key_times,
            get_key_triggers,
            insert_key_trigger,
            delete_key_trigger,
            clear_key_triggers,
            insert_slab_no_ttl,
            insert_slab_trigger_no_ttl,
            insert_key_trigger_no_ttl,
        })
    }
}

/// Prepares a CQL statement for inserting a new segment.
///
/// Creates a prepared statement for: `INSERT INTO segments (id, name,
/// slab_size) VALUES (?, ?, ?)`
async fn prepare_insert_segment(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("insert into {keyspace}.{TABLE_SEGMENTS} (id, name, slab_size) values (?, ?, ?)"),
    )
    .await
}

/// Prepares a CQL statement for retrieving a segment by ID.
///
/// Creates a prepared statement for: `SELECT name, slab_size FROM segments
/// WHERE id = ? LIMIT 1`
async fn prepare_get_segment(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("select name, slab_size from {keyspace}.{TABLE_SEGMENTS} where id = ? limit 1"),
    )
    .await
}

/// Prepares a CQL statement for deleting a segment.
///
/// Creates a prepared statement for: `DELETE FROM segments WHERE id = ?`
async fn prepare_delete_segment(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_SEGMENTS} where id = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving all slab IDs for a segment.
///
/// Creates a prepared statement for: `SELECT slab_id FROM segments WHERE id =
/// ?`
async fn prepare_get_slabs(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("select slab_id from {keyspace}.{TABLE_SEGMENTS} where id = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving slab IDs within a range.
///
/// Creates a prepared statement for: `SELECT slab_id FROM segments WHERE id = ?
/// AND slab_id >= ? AND slab_id <= ?`
async fn prepare_get_slab_range(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select slab_id from {keyspace}.{TABLE_SEGMENTS} where id = ? and slab_id >= ? and \
             slab_id <= ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a slab with TTL.
///
/// Creates a prepared statement for: `INSERT INTO segments (id, slab_id) VALUES
/// (?, ?) USING TTL ?`
async fn prepare_insert_slab(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("insert into {keyspace}.{TABLE_SEGMENTS} (id, slab_id) values (?, ?) using ttl ?"),
    )
    .await
}

/// Prepares a CQL statement for deleting a slab.
///
/// Creates a prepared statement for: `DELETE FROM segments WHERE id = ? AND
/// slab_id = ?`
async fn prepare_delete_slab(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_SEGMENTS} where id = ? and slab_id = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving all triggers in a slab.
///
/// Creates a prepared statement for: `SELECT key, time, span FROM slabs WHERE
/// segment_id = ? AND id = ?`
async fn prepare_get_slab_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, span from {keyspace}.{TABLE_SLABS} where segment_id = ? and id = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into a slab with TTL.
///
/// Creates a prepared statement for: `INSERT INTO slabs (segment_id, id, key,
/// time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?`
async fn prepare_insert_slab_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_SLABS} (segment_id, id, key, time, span) values (?, ?, \
             ?, ?, ?) using ttl ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for deleting a specific trigger from a slab.
///
/// Creates a prepared statement for: `DELETE FROM slabs WHERE segment_id = ?
/// AND id = ? AND key = ? AND time = ?`
async fn prepare_delete_slab_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_SLABS} where segment_id = ? and id = ? and key = ? and \
             time = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for clearing all triggers from a slab.
///
/// Creates a prepared statement for: `DELETE FROM slabs WHERE segment_id = ?
/// AND id = ?`
async fn prepare_clear_slab_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_SLABS} where segment_id = ? and id = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving all scheduled times for a key.
///
/// Creates a prepared statement for: `SELECT time FROM keys WHERE segment_id =
/// ? AND key = ?`
async fn prepare_get_key_times(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("select time from {keyspace}.{TABLE_KEYS} where segment_id = ? and key = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving all triggers for a key.
///
/// Creates a prepared statement for: `SELECT key, time, span FROM keys WHERE
/// segment_id = ? AND key = ?`
async fn prepare_get_key_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, span from {keyspace}.{TABLE_KEYS} where segment_id = ? and key = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into the key index with
/// TTL.
///
/// Creates a prepared statement for: `INSERT INTO keys (segment_id, key, time,
/// span) VALUES (?, ?, ?, ?) USING TTL ?`
async fn prepare_insert_key_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_KEYS} (segment_id, key, time, span) values (?, ?, ?, \
             ?) using ttl ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for deleting a specific trigger from the key index.
///
/// Creates a prepared statement for: `DELETE FROM keys WHERE segment_id = ? AND
/// key = ? AND time = ?`
async fn prepare_delete_key_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_KEYS} where segment_id = ? and key = ? and time = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for clearing all triggers for a key.
///
/// Creates a prepared statement for: `DELETE FROM keys WHERE segment_id = ? AND
/// key = ?`
async fn prepare_clear_key_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_KEYS} where segment_id = ? and key = ?"),
    )
    .await
}

/// Prepares a CQL statement for inserting a slab without TTL.
///
/// Creates a prepared statement for: `INSERT INTO segments (id, slab_id) VALUES
/// (?, ?)`
async fn prepare_insert_slab_no_ttl(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("insert into {keyspace}.{TABLE_SEGMENTS} (id, slab_id) values (?, ?)"),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into a slab without TTL.
///
/// Creates a prepared statement for: `INSERT INTO slabs (segment_id, id, key,
/// time, span) VALUES (?, ?, ?, ?, ?)`
async fn prepare_insert_slab_trigger_no_ttl(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_SLABS} (segment_id, id, key, time, span) values (?, ?, \
             ?, ?, ?)"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into the key index without
/// TTL.
///
/// Creates a prepared statement for: `INSERT INTO keys (segment_id, key, time,
/// span) VALUES (?, ?, ?, ?)`
async fn prepare_insert_key_trigger_no_ttl(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_KEYS} (segment_id, key, time, span) values (?, ?, ?, ?)"
        ),
    )
    .await
}

/// Prepares a CQL statement with optimized settings.
///
/// Prepares the given CQL statement and applies performance optimizations:
/// - Enables cached result metadata for better performance
/// - Marks the statement as idempotent for safer retries
///
/// # Arguments
///
/// * `session` - The Cassandra session to use for preparation
/// * `statement` - The CQL statement string to prepare
///
/// # Returns
///
/// A [`PreparedStatement`] ready for execution with optimizations applied.
async fn prepare(
    session: &Session,
    statement: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    let mut statement = session.prepare(statement).await?;
    statement.set_use_cached_result_metadata(true);
    statement.set_is_idempotent(true);

    Ok(statement)
}
