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

use crate::cassandra::{
    TABLE_KEYS, TABLE_SEGMENTS, TABLE_SLABS, TABLE_TYPED_KEYS, TABLE_TYPED_SLABS,
};
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use educe::Educe;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

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
    pub get_slab_triggers_all_types: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_key_triggers_all_types: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_key_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_key_trigger: PreparedStatement,

    #[educe(Debug(ignore))]
    pub clear_key_triggers: PreparedStatement,

    #[educe(Debug(ignore))]
    pub clear_key_triggers_all_types: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_no_ttl: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_trigger_no_ttl: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_key_trigger_no_ttl: PreparedStatement,

    // V1 migration queries
    #[educe(Debug(ignore))]
    pub update_segment_version: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_slabs_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_slab_triggers_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_slab_metadata_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub delete_slab_triggers_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub clear_key_triggers_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_key_trigger_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub get_key_triggers_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_v1: PreparedStatement,

    #[educe(Debug(ignore))]
    pub insert_slab_trigger_v1: PreparedStatement,
}

impl Queries {
    /// Creates a new Queries instance with prepared statements for all timer
    /// operations.
    ///
    /// This method prepares all CQL statements needed for timer storage
    /// operations using an existing Cassandra session.
    ///
    /// # Arguments
    ///
    /// * `session` - Cassandra session to use for statement preparation
    /// * `keyspace` - Keyspace name for statement preparation
    ///
    /// # Returns
    ///
    /// A [`Queries`] instance with all prepared statements ready for use.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Statement preparation fails
    pub async fn new(
        session: &Session,
        keyspace: &str,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let insert_segment = prepare_insert_segment(session, keyspace).await?;
        let get_segment = prepare_get_segment(session, keyspace).await?;
        let delete_segment = prepare_delete_segment(session, keyspace).await?;
        let get_slabs = prepare_get_slabs(session, keyspace).await?;
        let get_slab_range = prepare_get_slab_range(session, keyspace).await?;
        let insert_slab = prepare_insert_slab(session, keyspace).await?;
        let delete_slab = prepare_delete_slab(session, keyspace).await?;
        let get_slab_triggers = prepare_get_slab_triggers(session, keyspace).await?;
        let insert_slab_trigger = prepare_insert_slab_trigger(session, keyspace).await?;
        let delete_slab_trigger = prepare_delete_slab_trigger(session, keyspace).await?;
        let clear_slab_triggers = prepare_clear_slab_triggers(session, keyspace).await?;
        let get_key_times = prepare_get_key_times(session, keyspace).await?;
        let get_key_triggers = prepare_get_key_triggers(session, keyspace).await?;
        let get_slab_triggers_all_types =
            prepare_get_slab_triggers_all_types(session, keyspace).await?;
        let get_key_triggers_all_types =
            prepare_get_key_triggers_all_types(session, keyspace).await?;
        let insert_key_trigger = prepare_insert_key_trigger(session, keyspace).await?;
        let delete_key_trigger = prepare_delete_key_trigger(session, keyspace).await?;
        let clear_key_triggers = prepare_clear_key_triggers(session, keyspace).await?;
        let clear_key_triggers_all_types =
            prepare_clear_key_triggers_all_types(session, keyspace).await?;
        let insert_slab_no_ttl = prepare_insert_slab_no_ttl(session, keyspace).await?;
        let insert_slab_trigger_no_ttl =
            prepare_insert_slab_trigger_no_ttl(session, keyspace).await?;
        let insert_key_trigger_no_ttl =
            prepare_insert_key_trigger_no_ttl(session, keyspace).await?;

        // V1 migration queries
        let update_segment_version = prepare_update_segment_version(session, keyspace).await?;
        let get_slabs_v1 = prepare_get_slabs_v1(session, keyspace).await?;
        let get_slab_triggers_v1 = prepare_get_slab_triggers_v1(session, keyspace).await?;
        let delete_slab_metadata_v1 = prepare_delete_slab_metadata_v1(session, keyspace).await?;
        let delete_slab_triggers_v1 = prepare_delete_slab_triggers_v1(session, keyspace).await?;
        let clear_key_triggers_v1 = prepare_clear_key_triggers_v1(session, keyspace).await?;
        let insert_key_trigger_v1 = prepare_insert_key_trigger_v1(session, keyspace).await?;
        let get_key_triggers_v1 = prepare_get_key_triggers_v1(session, keyspace).await?;
        let insert_slab_v1 = prepare_insert_slab_v1(session, keyspace).await?;
        let insert_slab_trigger_v1 = prepare_insert_slab_trigger_v1(session, keyspace).await?;

        Ok(Self {
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
            get_slab_triggers_all_types,
            get_key_triggers_all_types,
            insert_key_trigger,
            delete_key_trigger,
            clear_key_triggers,
            clear_key_triggers_all_types,
            insert_slab_no_ttl,
            insert_slab_trigger_no_ttl,
            insert_key_trigger_no_ttl,
            update_segment_version,
            get_slabs_v1,
            get_slab_triggers_v1,
            delete_slab_metadata_v1,
            delete_slab_triggers_v1,
            clear_key_triggers_v1,
            insert_key_trigger_v1,
            get_key_triggers_v1,
            insert_slab_v1,
            insert_slab_trigger_v1,
        })
    }
}

/// Prepares a CQL statement for inserting a new segment.
///
/// Creates a prepared statement for: `INSERT INTO segments (id, name,
/// slab_size, version) VALUES (?, ?, ?, ?)`
async fn prepare_insert_segment(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_SEGMENTS} (id, name, slab_size, version) values (?, ?, \
             ?, ?)"
        ),
    )
    .await
}

/// Prepares a CQL statement for retrieving a segment by ID.
///
/// Creates a prepared statement for: `SELECT name, slab_size, version FROM
/// timer_segments WHERE id = ? LIMIT 1`
async fn prepare_get_segment(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select name, slab_size, version from {keyspace}.{TABLE_SEGMENTS} where id = ? limit 1"
        ),
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

/// Prepares a CQL statement for retrieving all triggers of a specific type in a
/// slab.
///
/// Creates a prepared statement for: `SELECT key, time, timer_type, span FROM
/// timer_typed_slabs WHERE segment_id = ? AND slab_size = ? AND id = ? AND
/// timer_type = ?`
async fn prepare_get_slab_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, timer_type, span from {keyspace}.{TABLE_TYPED_SLABS} where \
             segment_id = ? and slab_size = ? and id = ? and timer_type = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into a slab with TTL.
///
/// Creates a prepared statement for: `INSERT INTO timer_typed_slabs
/// (segment_id, slab_size, id, timer_type, key, time, span) VALUES (?, ?, ?, ?,
/// ?, ?, ?) USING TTL ?`
async fn prepare_insert_slab_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_TYPED_SLABS} (segment_id, slab_size, id, timer_type, \
             key, time, span) values (?, ?, ?, ?, ?, ?, ?) using ttl ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for deleting a specific trigger from a slab.
///
/// Creates a prepared statement for: `DELETE FROM timer_typed_slabs WHERE
/// segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ? AND key = ?
/// AND time = ?`
async fn prepare_delete_slab_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_TYPED_SLABS} where segment_id = ? and slab_size = ? \
             and id = ? and timer_type = ? and key = ? and time = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for clearing all triggers from a slab.
///
/// Creates a prepared statement for: `DELETE FROM timer_typed_slabs WHERE
/// segment_id = ? AND slab_size = ? AND id = ?`
async fn prepare_clear_slab_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_TYPED_SLABS} where segment_id = ? and slab_size = ? \
             and id = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for retrieving all scheduled times for a key and
/// timer type.
///
/// Creates a prepared statement for: `SELECT time FROM timer_typed_keys WHERE
/// segment_id = ? AND key = ? AND timer_type = ?`
async fn prepare_get_key_times(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select time from {keyspace}.{TABLE_TYPED_KEYS} where segment_id = ? and key = ? and \
             timer_type = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for retrieving all triggers for a key and timer
/// type.
///
/// Creates a prepared statement for: `SELECT key, time, timer_type, span FROM
/// timer_typed_keys WHERE segment_id = ? AND key = ? AND timer_type = ?`
async fn prepare_get_key_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, timer_type, span from {keyspace}.{TABLE_TYPED_KEYS} where \
             segment_id = ? and key = ? and timer_type = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for retrieving ALL triggers in a slab across all
/// timer types.
///
/// Creates a prepared statement for: `SELECT key, time, timer_type, span FROM
/// timer_typed_slabs WHERE segment_id = ? AND slab_size = ? AND id = ?`
///
/// This queries the entire partition without filtering by `timer_type`,
/// matching Cassandra's efficient partition query capability.
async fn prepare_get_slab_triggers_all_types(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, timer_type, span from {keyspace}.{TABLE_TYPED_SLABS} where \
             segment_id = ? and slab_size = ? and id = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for retrieving ALL triggers for a key across all
/// timer types.
///
/// Creates a prepared statement for: `SELECT key, time, timer_type, span FROM
/// timer_typed_keys WHERE segment_id = ? AND key = ?`
///
/// This queries the entire partition without filtering by `timer_type`,
/// matching Cassandra's efficient partition query capability.
async fn prepare_get_key_triggers_all_types(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "select key, time, timer_type, span from {keyspace}.{TABLE_TYPED_KEYS} where \
             segment_id = ? and key = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into the key index with
/// TTL.
///
/// Creates a prepared statement for: `INSERT INTO timer_typed_keys (segment_id,
/// key, timer_type, time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?`
async fn prepare_insert_key_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_TYPED_KEYS} (segment_id, key, timer_type, time, span) \
             values (?, ?, ?, ?, ?) using ttl ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for deleting a specific trigger from the key index.
///
/// Creates a prepared statement for: `DELETE FROM timer_typed_keys WHERE
/// segment_id = ? AND key = ? AND timer_type = ? AND time = ?`
async fn prepare_delete_key_trigger(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_TYPED_KEYS} where segment_id = ? and key = ? and \
             timer_type = ? and time = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for clearing all triggers for a key and timer type.
///
/// Creates a prepared statement for: `DELETE FROM timer_typed_keys WHERE
/// segment_id = ? AND key = ? AND timer_type = ?`
async fn prepare_clear_key_triggers(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "delete from {keyspace}.{TABLE_TYPED_KEYS} where segment_id = ? and key = ? and \
             timer_type = ?"
        ),
    )
    .await
}

/// Prepares a CQL statement for clearing all triggers for a key across ALL
/// timer types.
///
/// Creates a prepared statement for: `DELETE FROM timer_typed_keys WHERE
/// segment_id = ? AND key = ?`
///
/// This deletes the entire partition `(segment_id, key)` which contains all
/// `(timer_type, time)` clustering keys. More efficient than separately
/// deleting each timer type.
async fn prepare_clear_key_triggers_all_types(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_TYPED_KEYS} where segment_id = ? and key = ?"),
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
/// Creates a prepared statement for: `INSERT INTO timer_typed_slabs
/// (segment_id, slab_size, id, timer_type, key, time, span) VALUES (?, ?, ?, ?,
/// ?, ?, ?)`
async fn prepare_insert_slab_trigger_no_ttl(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_TYPED_SLABS} (segment_id, slab_size, id, timer_type, \
             key, time, span) values (?, ?, ?, ?, ?, ?, ?)"
        ),
    )
    .await
}

/// Prepares a CQL statement for inserting a trigger into the key index without
/// TTL.
///
/// Creates a prepared statement for: `INSERT INTO timer_typed_keys (segment_id,
/// key, timer_type, time, span) VALUES (?, ?, ?, ?, ?)`
async fn prepare_insert_key_trigger_no_ttl(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!(
            "insert into {keyspace}.{TABLE_TYPED_KEYS} (segment_id, key, timer_type, time, span) \
             values (?, ?, ?, ?, ?)"
        ),
    )
    .await
}

/// Prepares a CQL statement for updating segment version.
///
/// Creates a prepared statement for: `UPDATE timer_segments SET version = ?
/// WHERE id = ?`
async fn prepare_update_segment_version(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("update {keyspace}.{TABLE_SEGMENTS} set version = ?, slab_size = ? where id = ?"),
    )
    .await
}

/// Prepares a CQL statement for enumerating active v1 slabs for a segment.
///
/// Creates a prepared statement for: `SELECT slab_id FROM segments WHERE id =
/// ?`
///
/// Note: Cassandra returns NULL for clustering columns when a partition has
/// only static columns set. The caller must filter these out during result
/// processing.
async fn prepare_get_slabs_v1(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("select slab_id from {keyspace}.{TABLE_SEGMENTS} where id = ?"),
    )
    .await
}

/// Prepares a CQL statement for retrieving v1 triggers from a slab.
///
/// Creates a prepared statement for: `SELECT key, time, span FROM timer_slabs
/// WHERE segment_id = ? AND id = ?`
async fn prepare_get_slab_triggers_v1(
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

/// Prepares a CQL statement for deleting v1 slab metadata.
///
/// Creates a prepared statement for: `DELETE FROM segments WHERE id = ? AND
/// slab_id = ?`
async fn prepare_delete_slab_metadata_v1(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_SEGMENTS} where id = ? and slab_id = ?"),
    )
    .await
}

/// Prepares a CQL statement for deleting all v1 triggers for a slab.
///
/// Creates a prepared statement for: `DELETE FROM timer_slabs WHERE segment_id
/// = ? AND id = ?`
async fn prepare_delete_slab_triggers_v1(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_SLABS} where segment_id = ? and id = ?"),
    )
    .await
}

/// Prepares a CQL statement for clearing v1 triggers for a key.
///
/// Creates a prepared statement for: `DELETE FROM timer_keys WHERE segment_id =
/// ? AND key = ?`
async fn prepare_clear_key_triggers_v1(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("delete from {keyspace}.{TABLE_KEYS} where segment_id = ? and key = ?"),
    )
    .await
}

/// Prepares a CQL statement for inserting a v1 trigger into the key index.
///
/// Creates a prepared statement for: `INSERT INTO timer_keys (segment_id, key,
/// time, span) VALUES (?, ?, ?, ?)`
async fn prepare_insert_key_trigger_v1(
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

/// Prepares a CQL statement for getting v1 triggers for a key.
///
/// Creates a prepared statement for: `SELECT key, time, span FROM timer_keys
/// WHERE segment_id = ? AND key = ?`
async fn prepare_get_key_triggers_v1(
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

/// Prepares a CQL statement for inserting a v1 slab ID.
///
/// Creates a prepared statement for: `INSERT INTO segments (id, slab_id) VALUES
/// (?, ?)`
///
/// Note: This is for testing purposes only to enable v1 migration testing.
async fn prepare_insert_slab_v1(
    session: &Session,
    keyspace: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        &format!("insert into {keyspace}.{TABLE_SEGMENTS} (id, slab_id) values (?, ?)"),
    )
    .await
}

/// Prepares a CQL statement for inserting a v1 trigger.
///
/// Creates a prepared statement for: `INSERT INTO timer_slabs (segment_id, id,
/// key, time, span) VALUES (?, ?, ?, ?, ?)`
///
/// Note: This is for testing purposes only to enable v1 migration testing.
async fn prepare_insert_slab_trigger_v1(
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
