//! Unit tests for the deduplication handler.

use crate::Topic;
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
use quick_cache::sync::Cache;
use serde_json::json;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::Span;

#[derive(Debug, Clone)]
enum TestError {
    Permanent,
    Transient,
}

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Permanent => write!(f, "permanent test error"),
            Self::Transient => write!(f, "transient test error"),
        }
    }
}

impl Error for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Permanent => ErrorCategory::Permanent,
            Self::Transient => ErrorCategory::Transient,
        }
    }
}

#[derive(Clone)]
struct MockHandler {
    call_count: Arc<AtomicUsize>,
    error: Option<TestError>,
}

impl MockHandler {
    fn success() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            error: None,
        }
    }

    fn failing_permanent() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            error: Some(TestError::Permanent),
        }
    }

    fn failing_transient() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            error: Some(TestError::Transient),
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
        if let Some(ref e) = self.error {
            Err(e.clone())
        } else {
            Ok(())
        }
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

fn create_handler_with(
    inner: MockHandler,
    version: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> DeduplicationHandler<MockHandler, MemoryDeduplicationStore> {
    DeduplicationHandler {
        inner,
        cache: Arc::new(Cache::new(100)),
        store: MemoryDeduplicationStore::new(),
        version: version.to_owned(),
        group_id: Arc::from(group_id),
        topic: Topic::from(topic),
        partition,
    }
}

fn create_handler(
    inner: MockHandler,
) -> DeduplicationHandler<MockHandler, MemoryDeduplicationStore> {
    create_handler_with(inner, "1", "test-group", "test-topic", 0)
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
async fn permanent_error_is_deduplicated() {
    let handler = create_handler(MockHandler::failing_permanent());
    let context = MockEventContext::new();

    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        return;
    };
    let Some(msg2) = create_test_message("key1", Some("evt1")) else {
        return;
    };

    // First call: permanent failure, should still write to dedup store
    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    assert!(result.is_err());
    assert_eq!(handler.inner.call_count(), 1);

    // Second call with same message: deduplicated — handler not called again
    let result = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert!(result.is_ok());
    assert_eq!(handler.inner.call_count(), 1);
}

#[tokio::test]
async fn transient_error_does_not_deduplicate() {
    let handler = create_handler(MockHandler::failing_transient());
    let context = MockEventContext::new();

    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        return;
    };
    let Some(msg2) = create_test_message("key1", Some("evt1")) else {
        return;
    };

    // First call: transient failure, must NOT write to dedup store
    let result =
        FallibleHandler::on_message(&handler, context.clone(), msg1, DemandType::Normal).await;
    assert!(result.is_err());

    // Second call: not deduplicated — retry reaches handler again
    let result = FallibleHandler::on_message(&handler, context, msg2, DemandType::Normal).await;
    assert!(result.is_err());
    assert_eq!(handler.inner.call_count(), 2);
}

#[test]
fn dedup_uuid_is_deterministic() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let Some(msg1) = create_test_message("key1", Some("evt1")) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    let Some(msg2) = create_test_message("key1", Some("evt1")) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    assert_eq!(
        handler.dedup_uuid_for_message(&msg1),
        handler.dedup_uuid_for_message(&msg2),
    );
    Ok(())
}

#[test]
fn dedup_uuid_differs_by_dimension() -> color_eyre::Result<()> {
    let handler = create_handler(MockHandler::success());
    let Some(base_msg) = create_test_message("key1", Some("evt1")) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    let base = handler.dedup_uuid_for_message(&base_msg);

    // Different version
    let h = create_handler_with(MockHandler::success(), "2", "test-group", "test-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different group
    let h = create_handler_with(MockHandler::success(), "1", "other-group", "test-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different topic
    let h = create_handler_with(MockHandler::success(), "1", "test-group", "other-topic", 0);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different partition
    let h = create_handler_with(MockHandler::success(), "1", "test-group", "test-topic", 1);
    assert_ne!(base, h.dedup_uuid_for_message(&base_msg));

    // Different key
    let Some(diff_key_msg) = create_test_message("key2", Some("evt1")) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    assert_ne!(base, handler.dedup_uuid_for_message(&diff_key_msg));

    // Different event_id
    let Some(diff_evt_msg) = create_test_message("key1", Some("evt2")) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    assert_ne!(base, handler.dedup_uuid_for_message(&diff_evt_msg));

    // Offset fallback (no event_id) differs from event_id path
    let Some(offset_msg) = create_test_message("key1", None) else {
        color_eyre::eyre::bail!("could not create test message");
    };
    assert_ne!(base, handler.dedup_uuid_for_message(&offset_msg));

    Ok(())
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
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: 100,
        ttl: Duration::from_secs(700_000_000),
    };
    let result =
        DeduplicationMiddleware::new(config, "group", MemoryDeduplicationStoreProvider::new());
    assert!(result.is_err());
}

#[test]
fn ttl_below_minimum_rejected() {
    let config = DeduplicationConfiguration {
        version: "1".to_owned(),
        cache_capacity: 100,
        ttl: Duration::from_secs(30),
    };
    let result =
        DeduplicationMiddleware::new(config, "group", MemoryDeduplicationStoreProvider::new());
    assert!(result.is_err());
}
