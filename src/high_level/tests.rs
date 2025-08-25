use super::*;
use crate::producer::ProducerConfiguration;
use color_eyre::Result;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::mem::forget;

/// Creates a `ProsodyProducer` connected to a mock cluster with specified
/// topics.
fn create_producer_with_topics(topics: &[&str]) -> Result<(ProsodyProducer, String)> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();

    for topic in topics {
        cluster.create_topic(topic, 1, 1)?;
    }

    // Keep cluster alive for test duration
    forget(cluster);

    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec![bootstrap.clone()])
        .source_system("test")
        .build()?;

    let producer = ProsodyProducer::pipeline_producer(config)?;
    Ok((producer, bootstrap))
}

#[test]
fn test_missing_topics_finds_missing() -> Result<()> {
    let (producer, _bootstrap) =
        create_producer_with_topics(&["existing-topic-1", "existing-topic-2"])?;

    let topics = vec![
        "existing-topic-1".into(),
        "missing-topic".into(),
        "existing-topic-2".into(),
        "another-missing".into(),
    ];

    let result = missing_topics(&producer, topics)?;

    assert_eq!(result.len(), 2);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(result.contains(&Topic::from("another-missing")));
    Ok(())
}

#[test]
fn test_missing_topics_ignores_pattern_subscriptions() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["real-topic"])?;

    let topics = vec![
        "real-topic".into(),
        "^pattern-topic.*".into(),
        "missing-topic".into(),
        "^another-pattern".into(),
    ];

    let result = missing_topics(&producer, topics)?;

    // Pattern topics (starting with ^) should be filtered out
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing-topic")));
    assert!(!result.contains(&Topic::from("^pattern-topic.*")));
    assert!(!result.contains(&Topic::from("^another-pattern")));
    Ok(())
}

#[test]
fn test_missing_topics_all_exist() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["topic-1", "topic-2", "topic-3"])?;

    let topics = vec!["topic-1".into(), "topic-2".into(), "topic-3".into()];

    let result = missing_topics(&producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_handles_duplicates() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["existing"])?;

    let topics = vec![
        "existing".into(),
        "missing".into(),
        "missing".into(),  // Duplicate
        "existing".into(), // Duplicate
    ];

    let result = missing_topics(&producer, topics)?;

    // Should deduplicate and return only unique missing topics
    assert_eq!(result.len(), 1);
    assert!(result.contains(&Topic::from("missing")));
    Ok(())
}

#[test]
fn test_missing_topics_empty_list() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["some-topic"])?;

    let topics = vec![];

    let result = missing_topics(&producer, topics)?;

    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_only_patterns() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["real-topic"])?;

    let topics = vec!["^pattern1.*".into(), "^pattern2.*".into()];

    let result = missing_topics(&producer, topics)?;

    // All pattern topics should be filtered out
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn test_missing_topics_edge_cases() -> Result<()> {
    let (producer, _bootstrap) = create_producer_with_topics(&["normal-topic"])?;

    let topics = vec![
        "normal-topic".into(),
        "^".into(),                   // Edge case: just ^
        "^a".into(),                  // Minimal pattern
        "missing^not-pattern".into(), // ^ not at start
        "".into(),                    // Empty string
    ];

    let result = missing_topics(&producer, topics)?;

    // Should filter out ^ and ^a (start with ^), but not missing^not-pattern
    // Empty string should be processed normally
    assert_eq!(result.len(), 2); // "missing^not-pattern" and ""
    assert!(result.contains(&Topic::from("missing^not-pattern")));
    assert!(result.contains(&Topic::from("")));
    assert!(!result.contains(&Topic::from("^")));
    assert!(!result.contains(&Topic::from("^a")));
    Ok(())
}
