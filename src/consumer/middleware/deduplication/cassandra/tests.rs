use super::*;
use crate::cassandra::CassandraConfiguration;
use crate::test_util::TEST_KEYSPACE;

crate::dedup_store_tests!(async {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

    let cassandra_store = CassandraStore::new(&config).await?;
    let queries =
        Arc::new(DeduplicationQueries::new(cassandra_store.session(), TEST_KEYSPACE).await?);

    Ok::<_, color_eyre::Report>(CassandraDeduplicationStore {
        store: cassandra_store,
        queries,
        ttl: 3600_i32,
        cache: Arc::new(Cache::new(128)),
    })
});
