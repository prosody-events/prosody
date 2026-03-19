//! Unit tests for the deduplication handler.

use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerMessageValue};
use crate::consumer::middleware::deduplication::{
    DeduplicationConfiguration, DeduplicationHandler, DeduplicationMiddleware,
    MemoryDeduplicationStore, MemoryDeduplicationStoreProvider,
};
use crate::consumer::middleware::test_support::MockEventContext;
use crate::consumer::middleware::{ClassifyError, ErrorCategory, FallibleHandler};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::{Partition, Topic};
use quick_cache::UnitWeighter;
use quick_cache::sync::Cache;
use serde_json::json;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::Span;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3Default;

#[derive(Debug, Clone)]
struct TestError;

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "test error")
    }
}

impl Error for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[derive(Clone)]
struct MockHandler {
    call_count: Arc<AtomicUsize>,
    fail: bool,
}

impl MockHandler {
    fn success() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            fail: true,
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }
}

impl FallibleHandler for MockHandler {
    type Error = TestError;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        if self.fail { Err(TestError) } else { Ok(()) }
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(self) {}
}

fn create_handler(
    inner: MockHandler,
) -> DeduplicationHandler<MockHandler, MemoryDeduplicationStore> {
    DeduplicationHandler {
        inner,
        cache: Cache::with_weighter(100, 100, UnitWeighter),
        store: MemoryDeduplicationStore::new(),
        version: "1".to_owned(),
        group_id: Arc::from("test-group"),
        topic: Topic::from("test-topic"),
        partition: Partition::from(0_i32),
    }
}

fn create_test_message(key: &str, event_id: Option<&str>) -> Option<ConsumerMessage> {
    let semaphore = Arc::new(Semaphore::new(10));
    let permit = semaphore.try_acquire_owned().ok()?;
    let payload = match event_id {
        Some(id) => json!({ "id": id }),
        None => json!({}),
    };
    Some(ConsumerMessage::new(
        ConsumerMessageValue {
            key: key.into(),
            payload,
            ..Default::default()
        },
        Span::current(),
        permit,
    ))
}

#[tokio::test]
async fn local_cache_hit_skips_handler() {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        return;
    };
    let Some(msg2) = create_test_message("key1", Some("evt1")) else {
        return;
    };

    // First call processes
    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);

    // Second call deduplicates
    let result = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);
}

#[tokio::test]
async fn cache_miss_processes_and_populates() {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let Some(msg) = create_test_message("key1", Some("evt1")) else {
        return;
    };

    let result = FallibleHandler::on_message(&handler, context, msg, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);
}

#[tokio::test]
async fn different_event_ids_both_processed() {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();

    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        return;
    };
    let Some(msg2) = create_test_message("key1", Some("evt2")) else {
        return;
    };

    let _ = FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    let _ = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert_eq!(handler.inner.call_count(), 2);
}

#[tokio::test]
async fn timer_passthrough() {
    let handler = create_handler(MockHandler::success());
    let context = MockEventContext::new();
    let trigger = Trigger::for_testing(
        "test-key".into(),
        CompactDateTime::from(1000_u32),
        TimerType::default(),
    );

    let result = FallibleHandler::on_timer(&handler, context, trigger, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);
}

#[tokio::test]
async fn failed_handler_does_not_cache() {
    let handler = create_handler(MockHandler::failing());
    let context = MockEventContext::new();

    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        return;
    };
    let Some(msg2) = create_test_message("key1", Some("evt1")) else {
        return;
    };

    // First call fails
    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    assert!(result.is_err());

    // Second call should retry (not cached)
    let result = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert!(result.is_err());
    assert_eq!(handler.inner.call_count(), 2);
}

/// Test helper that computes a dedup UUID from individual components.
fn compute_dedup_uuid(
    version: &str,
    group_id: &str,
    topic: Topic,
    partition: Partition,
    key: &str,
) -> Uuid {
    let mut hasher = Xxh3Default::new();
    hasher.write_u32(version.len() as u32);
    hasher.write(version.as_bytes());
    hasher.write_u32(group_id.len() as u32);
    hasher.write(group_id.as_bytes());
    hasher.write_u32(topic.len() as u32);
    hasher.write(topic.as_bytes());
    hasher.write_i32(partition);
    hasher.write_u32(key.len() as u32);
    hasher.write(key.as_bytes());
    let hash = hasher.digest128();
    uuid::Builder::from_custom_bytes(hash.to_le_bytes()).into_uuid()
}

#[test]
fn compute_dedup_uuid_is_deterministic() {
    let uuid1 = compute_dedup_uuid("1", "group", Topic::from("topic"), 0_i32, "key");
    let uuid2 = compute_dedup_uuid("1", "group", Topic::from("topic"), 0_i32, "key");
    assert_eq!(uuid1, uuid2);
}

#[test]
fn compute_dedup_uuid_differs_by_dimension() {
    let base = compute_dedup_uuid("1", "group", Topic::from("topic"), 0_i32, "key");

    // Different version
    let diff_version = compute_dedup_uuid("2", "group", Topic::from("topic"), 0_i32, "key");
    assert_ne!(base, diff_version);

    // Different group
    let diff_group = compute_dedup_uuid("1", "other", Topic::from("topic"), 0_i32, "key");
    assert_ne!(base, diff_group);

    // Different topic
    let diff_topic = compute_dedup_uuid("1", "group", Topic::from("other"), 0_i32, "key");
    assert_ne!(base, diff_topic);

    // Different partition
    let diff_partition = compute_dedup_uuid("1", "group", Topic::from("topic"), 1_i32, "key");
    assert_ne!(base, diff_partition);

    // Different key
    let diff_key = compute_dedup_uuid("1", "group", Topic::from("topic"), 0_i32, "other");
    assert_ne!(base, diff_key);
}

#[test]
fn cache_capacity_zero_returns_none() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: 0,
        ttl: Duration::from_secs(3600),
    };
    let result =
        DeduplicationMiddleware::new(config, "group", MemoryDeduplicationStoreProvider::new());
    assert!(result.is_ok());
    assert!(result.as_ref().is_ok_and(Option::is_none));
}

#[test]
fn ttl_exceeding_max_rejected() {
    let config = DeduplicationConfiguration::builder()
        .ttl(Duration::from_secs(700_000_000))
        .build();
    assert!(config.is_ok());
    let result = DeduplicationMiddleware::new(
        config
            .as_ref()
            .ok()
            .cloned()
            .unwrap_or_else(|| DeduplicationConfiguration {
                version: "1".to_owned(),
                cache_capacity: 100,
                ttl: Duration::from_secs(700_000_000),
            }),
        "group",
        MemoryDeduplicationStoreProvider::new(),
    );
    assert!(result.is_err());
}
