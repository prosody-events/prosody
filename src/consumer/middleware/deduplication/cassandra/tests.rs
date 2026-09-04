use super::*;
use crate::cassandra::CassandraConfiguration;
use crate::test_util::TEST_KEYSPACE;

crate::dedup_store_tests!(new_store());

async fn new_store() -> color_eyre::Result<CassandraDeduplicationStore> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

    let cassandra_store = CassandraStore::new(&config).await?;
    let queries =
        Arc::new(DeduplicationQueries::new(cassandra_store.session(), TEST_KEYSPACE).await?);

    Ok(CassandraDeduplicationStore {
        store: cassandra_store,
        queries,
        ttl: 3600_i32,
        cache: Arc::new(Cache::new(128)),
    })
}

/// A read from a new cache finds durable markers once.
#[tokio::test]
async fn lookup_reports_marker_source() -> color_eyre::Result<()> {
    let mut store = new_store().await?;
    let id = Uuid::new_v4();
    assert_eq!(store.lookup(id).await?, Presence::Absent);
    store.insert(id).await?;
    assert_eq!(store.lookup(id).await?, Presence::Cached);
    store.cache = Arc::new(Cache::new(128));
    assert_eq!(store.lookup(id).await?, Presence::Durable);
    assert_eq!(store.lookup(id).await?, Presence::Cached);
    Ok(())
}
