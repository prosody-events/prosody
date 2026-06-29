//! Cassandra-backed [`CassandraMessageDeferStore`] instantiation of the shared
//! [`defer_store_tests!`] model-equivalence suite. Requires a live cluster at
//! `localhost:9042` with the `prosody_test` keyspace.

mod invariant;
mod legacy_repair;
mod tombstone_reverse_scan;

use super::*;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::defer_store_tests;
use crate::test_util::TEST_KEYSPACE;
use crate::{ConsumerGroup, Partition, Topic};

pub(super) async fn build_store() -> color_eyre::Result<CassandraMessageDeferStore> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;
    let cassandra_store = CassandraStore::new(&config).await?;
    let segment_store = CassandraSegmentStore::new(cassandra_store.clone(), TEST_KEYSPACE).await?;
    let queries = Arc::new(Queries::new(cassandra_store.session(), TEST_KEYSPACE).await?);
    let segment = LazySegment::new(
        segment_store,
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from(format!("test-consumer-group-{}", uuid::Uuid::new_v4())) as ConsumerGroup,
    );
    Ok(CassandraMessageDeferStore::new(
        cassandra_store,
        queries,
        segment,
        1_024,
    ))
}

defer_store_tests!(async { build_store().await });
