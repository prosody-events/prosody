use super::*;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::middleware::defer::DeferConfigurationBuilder;
use crate::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use crate::consumer::middleware::retry::RetryConfiguration;
use crate::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use crate::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use crate::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use crate::high_level::CassandraConfigurationBuilder;
use crate::high_level::mode::Mode;
use crate::producer::ProducerConfiguration;
use crate::telemetry::Telemetry;
use crate::telemetry::emitter::TelemetryEmitterConfiguration;
use color_eyre::Result;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
/// Owns the helper-produced mock cluster alongside the producer so the
/// cluster's Drop runs when the test ends (no `mem::forget` leaks).
struct ProducerFixture {
    producer: ProsodyProducer,
    _cluster: MockCluster<'static, DefaultProducerContext>,
}

/// Creates a `ProsodyProducer` connected to a mock cluster with specified
/// topics.
fn create_producer_with_topics(topics: &[&str]) -> Result<ProducerFixture> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();

    for topic in topics {
        cluster.create_topic(topic, 1, 1)?;
    }

    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec![bootstrap])
        .source_system("test")
        .build()?;

    let producer = ProsodyProducer::pipeline_producer(config, Telemetry::new().sender())?;
    Ok(ProducerFixture {
        producer,
        _cluster: cluster,
    })
}

#[test]
fn test_missing_topics_finds_missing() -> Result<()> {
    let fixture = create_producer_with_topics(&["existing-topic-1", "existing-topic-2"])?;

    let topics = vec![
        "existing-topic-1".into(),
        "missing-topic".into(),
        "existing-topic-2".into(),
        "another-missing".into(),
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    assert_eq!(result.len(), 2);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(result.contains(&Topic::from("another-missing")));
    Ok(())
}

#[test]
fn test_missing_topics_ignores_pattern_subscriptions() -> Result<()> {
    let fixture = create_producer_with_topics(&["real-topic"])?;

    let topics = vec![
        "real-topic".into(),
        "^pattern-topic.*".into(),
        "missing-topic".into(),
        "^another-pattern".into(),
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Pattern topics (starting with ^) should be filtered out
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(!result.contains(&Topic::from("^pattern-topic.*")));
    assert!(!result.contains(&Topic::from("^another-pattern")));
    Ok(())
}

#[test]
fn test_missing_topics_all_exist() -> Result<()> {
    let fixture = create_producer_with_topics(&["topic-1", "topic-2", "topic-3"])?;

    let topics = vec!["topic-1".into(), "topic-2".into(), "topic-3".into()];

    let result = missing_topics(&fixture.producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_handles_duplicates() -> Result<()> {
    let fixture = create_producer_with_topics(&["existing"])?;

    let topics = vec![
        "existing".into(),
        "missing".into(),
        "missing".into(),  // Duplicate
        "existing".into(), // Duplicate
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Should deduplicate and return only unique missing topics
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing")));
    Ok(())
}

#[test]
fn test_missing_topics_empty_list() -> Result<()> {
    let fixture = create_producer_with_topics(&["some-topic"])?;

    let topics = vec![];

    let result = missing_topics(&fixture.producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_only_patterns() -> Result<()> {
    let fixture = create_producer_with_topics(&["real-topic"])?;

    let topics = vec!["^pattern1.*".into(), "^pattern2.*".into()];

    let result = missing_topics(&fixture.producer, topics)?;

    // All pattern topics should be filtered out
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_edge_cases() -> Result<()> {
    let fixture = create_producer_with_topics(&["normal-topic"])?;

    let topics = vec![
        "normal-topic".into(),
        "^".into(),                   // Edge case: just ^
        "^a".into(),                  // Minimal pattern
        "missing^not-pattern".into(), // ^ not at start
        "".into(),                    // Empty string
    ];

    let result = missing_topics(&fixture.producer, topics)?;

    // Should filter out ^ and ^a (start with ^), but not missing^not-pattern
    // Empty string should be processed normally
    assert_eq!(result.len(), 2); // "missing^not-pattern" and ""
    assert!(result.contains(&Topic::from("missing^not-pattern")));
    assert!(result.contains(&Topic::from("")));
    assert!(!result.contains(&Topic::from("^")));
    assert!(!result.contains(&Topic::from("^a")));
    Ok(())
}

/// Owns the helper-produced mock cluster alongside the
/// `HighLevelClient` so the cluster's Drop runs at end of test.
struct ClientFixture {
    client: HighLevelClient<()>,
    _cluster: MockCluster<'static, DefaultProducerContext>,
}

/// Helper function to create a `HighLevelClient` for source system testing.
///
/// # Arguments
///
/// * `group_id` - The consumer group ID to use.
/// * `source_system` - Optional explicit source system for the producer.
///
/// # Returns
///
/// A configured `HighLevelClient` instance plus the underlying mock
/// cluster (kept alive for the test).
fn create_test_client(group_id: &str, source_system: Option<&str>) -> Result<ClientFixture> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();
    cluster.create_topic("test-topic", 1, 1)?;

    // Create producer configuration
    let mut producer_builder = ProducerConfiguration::builder();
    producer_builder
        .bootstrap_servers(vec![bootstrap.clone()])
        .mock(true);

    // Set source system if provided
    if let Some(source) = source_system {
        producer_builder.source_system(source);
    }

    // Create consumer configuration
    let mut consumer_builder = ConsumerConfiguration::builder();
    consumer_builder
        .bootstrap_servers(vec![bootstrap])
        .group_id(group_id)
        .subscribed_topics(&["test-topic".to_owned()])
        .mock(true);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        retry: RetryConfiguration::builder(),
        failure_topic: FailureTopicConfigurationBuilder::default(),
        scheduler: SchedulerConfigurationBuilder::default(),
        monopolization: MonopolizationConfigurationBuilder::default(),
        defer: DeferConfigurationBuilder::default(),
        dedup: DeduplicationConfigurationBuilder::default(),
        timeout: TimeoutConfigurationBuilder::default(),
        emitter: TelemetryEmitterConfiguration::default(),
    };
    let cassandra_builder = CassandraConfigurationBuilder::default();

    let client = HighLevelClient::new(
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
        &cassandra_builder,
    )?;
    Ok(ClientFixture {
        client,
        _cluster: cluster,
    })
}

#[test]
fn test_source_system_defaults_to_consumer_group() -> Result<()> {
    let group_id = "my-test-group";

    // Create client WITHOUT specifying source_system
    let fixture = create_test_client(group_id, None)?;

    // Verify that source_system() returns the consumer group_id
    assert_eq!(fixture.client.source_system(), group_id);
    Ok(())
}

#[test]
fn test_source_system_explicit_value_preserved() -> Result<()> {
    let explicit_source = "my-explicit-source";
    let group_id = "my-test-group";

    // Create client WITH explicit source_system
    let fixture = create_test_client(group_id, Some(explicit_source))?;

    // Verify that source_system() returns the explicit value, NOT group_id
    assert_eq!(fixture.client.source_system(), explicit_source);
    assert_ne!(fixture.client.source_system(), group_id);
    Ok(())
}
