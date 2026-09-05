use super::*;
use crate::cassandra::CassandraConfiguration;
use crate::test_util::{TEST_KEYSPACE, integration_test_count};

crate::dedup_store_tests!(new_provider(), tests = integration_test_count(25));

async fn new_provider() -> color_eyre::Result<CassandraDeduplicationStoreProvider> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

    let cassandra_store = CassandraStore::new(&config).await?;
    let queries =
        Arc::new(DeduplicationQueries::new(cassandra_store.session(), TEST_KEYSPACE).await?);

    Ok(CassandraDeduplicationStoreProvider {
        store: cassandra_store,
        queries,
        ttl: 3600_i32,
        cache: Arc::new(Cache::new(128)),
    })
}

/// A read from a new cache finds durable markers once.
#[tokio::test]
async fn lookup_reports_marker_source() -> color_eyre::Result<()> {
    let provider = new_provider().await?;
    let mut store = provider.create_store(Topic::from("test"), 0, "test");
    let id = Uuid::new_v4();
    assert_eq!(store.lookup(id).await?, Presence::Absent);
    store.insert(id).await?;
    assert_eq!(store.lookup(id).await?, Presence::Settled);
    store.cache = Arc::new(Cache::new(128));
    assert_eq!(store.lookup(id).await?, Presence::Inherited);
    assert_eq!(store.lookup(id).await?, Presence::Settled);
    Ok(())
}
