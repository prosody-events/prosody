use super::embedded_migrator::EmbeddedMigrator;
use crate::timers::store::cassandra::{CassandraConfiguration, CassandraTriggerStoreError};
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
}

impl Queries {
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
        let migrator = EmbeddedMigrator::new(&session, &configuration.keyspace);
        migrator.migrate().await?;
        session.use_keyspace(configuration.keyspace, true).await?;

        let insert_segment = prepare_insert_segment(&session).await?;
        let get_segment = prepare_get_segment(&session).await?;
        let delete_segment = prepare_delete_segment(&session).await?;
        let get_slabs = prepare_get_slabs(&session).await?;
        let get_slab_range = prepare_get_slab_range(&session).await?;
        let insert_slab = prepare_insert_slab(&session).await?;
        let delete_slab = prepare_delete_slab(&session).await?;
        let get_slab_triggers = prepare_get_slab_triggers(&session).await?;
        let insert_slab_trigger = prepare_insert_slab_trigger(&session).await?;
        let delete_slab_trigger = prepare_delete_slab_trigger(&session).await?;
        let clear_slab_triggers = prepare_clear_slab_triggers(&session).await?;
        let get_key_times = prepare_get_key_times(&session).await?;
        let get_key_triggers = prepare_get_key_triggers(&session).await?;
        let insert_key_trigger = prepare_insert_key_trigger(&session).await?;
        let delete_key_trigger = prepare_delete_key_trigger(&session).await?;
        let clear_key_triggers = prepare_clear_key_triggers(&session).await?;

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
        })
    }
}

async fn prepare_insert_segment(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "insert into segments (id, name, slab_size) values (?, ?, ?)",
    )
    .await
}

async fn prepare_get_segment(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "select name, slab_size from segments where id = ? limit 1",
    )
    .await
}

async fn prepare_delete_segment(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(session, "delete from segments where id = ?").await
}

async fn prepare_get_slabs(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(session, "select slab_id from segments where id = ?").await
}

async fn prepare_get_slab_range(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "select slab_id from segments where id = ? and slab_id >= ? and slab_id <= ?",
    )
    .await
}

async fn prepare_insert_slab(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "insert into segments (id, slab_id) values (?, ?) using ttl ?",
    )
    .await
}

async fn prepare_delete_slab(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(session, "delete from segments where id = ? and slab_id = ?").await
}

async fn prepare_get_slab_triggers(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "select key, time, span from slabs where segment_id = ? and id = ?",
    )
    .await
}

async fn prepare_insert_slab_trigger(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "insert into slabs (segment_id, id, key, time, span) values (?, ?, ?, ?, ?) using ttl ?",
    )
    .await
}

async fn prepare_delete_slab_trigger(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "delete from slabs where segment_id = ? and id = ? and key = ? and time = ?",
    )
    .await
}

async fn prepare_clear_slab_triggers(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(session, "delete from slabs where segment_id = ? and id = ?").await
}

async fn prepare_get_key_times(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "select time from keys where segment_id = ? and key = ?",
    )
    .await
}

async fn prepare_get_key_triggers(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "select key, time, span from keys where segment_id = ? and key = ?",
    )
    .await
}

async fn prepare_insert_key_trigger(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "insert into keys (segment_id, key, time, span) values (?, ?, ?, ?) using ttl ?",
    )
    .await
}

async fn prepare_delete_key_trigger(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(
        session,
        "delete from keys where segment_id = ? and key = ? and time = ?",
    )
    .await
}

async fn prepare_clear_key_triggers(
    session: &Session,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    prepare(session, "delete from keys where segment_id = ? and key = ?").await
}

async fn prepare(
    session: &Session,
    statement: &str,
) -> Result<PreparedStatement, CassandraTriggerStoreError> {
    let mut statement = session.prepare(statement).await?;
    statement.set_use_cached_result_metadata(true);
    statement.set_is_idempotent(true);

    Ok(statement)
}
