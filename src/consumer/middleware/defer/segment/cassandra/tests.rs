use super::*;
use crate::cassandra::CassandraConfiguration;
use crate::test_util::TEST_KEYSPACE;

#[tokio::test]
async fn test_cassandra_get_or_create_segment() -> color_eyre::Result<()> {
    let config = CassandraConfiguration::builder()
        .nodes(vec!["localhost:9042".to_owned()])
        .keyspace(TEST_KEYSPACE.to_owned())
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Config build failed: {e}"))?;

    let cassandra_store = CassandraStore::new(&config).await?;
    let segment_store = CassandraSegmentStore::new(cassandra_store, TEST_KEYSPACE).await?;

    let segment = Segment::new(
        Topic::from("test-topic"),
        Partition::from(0_i32),
        Arc::from("test-group") as ConsumerGroup,
    );
    let segment_id = segment.id();

    // Create segment
    let result = segment_store.get_or_create_segment(segment.clone()).await?;
    assert_eq!(result.id(), segment_id);

    // Verify it can be retrieved
    let retrieved = segment_store.get_segment(&segment_id).await?;
    assert_eq!(retrieved, Some(segment));

    Ok(())
}
