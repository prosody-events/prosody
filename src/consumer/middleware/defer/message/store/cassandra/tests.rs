//! Cassandra-backed [`CassandraMessageDeferStore`] instantiation of the shared
//! [`defer_store_tests!`] model-equivalence suite. Requires a live cluster at
//! `localhost:9042` with the `prosody_test` keyspace.

use super::*;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::defer_store_tests;
use crate::{ConsumerGroup, Partition, Topic};

defer_store_tests!(async {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace("prosody_test".to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

    let cassandra_store = CassandraStore::new(&config).await?;
    let segment_store = CassandraSegmentStore::new(cassandra_store.clone(), "prosody_test").await?;
    let queries = Arc::new(Queries::new(cassandra_store.session(), "prosody_test").await?);
    let segment = LazySegment::new(
        segment_store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
    );
    let defer_store = CassandraMessageDeferStore::new(cassandra_store, queries, segment, 1_024);
    Ok::<_, color_eyre::Report>(defer_store)
});
