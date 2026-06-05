use super::*;
use crate::Partition;
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use crate::timers::Trigger;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Debug, Error)]
#[error("Mock error")]
struct MockError;

impl ClassifyError for MockError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[derive(Clone)]
struct MockHandler {
    invocations: Arc<AtomicUsize>,
}

impl MockHandler {
    fn new() -> Self {
        Self {
            invocations: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl FallibleHandler for MockHandler {
    type Error = MockError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(self) {}
}

#[derive(Clone)]
struct MockProvider {
    handler: MockHandler,
}

impl FallibleHandlerProvider for MockProvider {
    type Handler = MockHandler;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        self.handler.clone()
    }
}

const TEST_TOPIC: &str = "test-topic";
const TEST_PARTITION: Partition = 0;

fn test_tp_key(key: &str) -> TopicPartitionKey {
    TopicPartitionKey::new(TEST_TOPIC.into(), TEST_PARTITION, key.into())
}

fn create_key_event(
    topic: Topic,
    partition: Partition,
    key: Key,
    state: KeyState,
    timestamp: Instant,
) -> TelemetryEvent {
    TelemetryEvent {
        timestamp,
        topic,
        partition,
        data: Arc::new(Data::Key(KeyEvent {
            key,
            demand_type: DemandType::Normal,
            state,
        })),
    }
}

#[test]
fn test_configuration_validation() -> Result<()> {
    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(1.5)
        .build()?;

    assert!(config.validate().is_err(), "Should reject threshold > 1.0");

    let config = MonopolizationConfiguration::builder().build()?;
    assert!(config.validate().is_ok(), "Should accept valid defaults");

    Ok(())
}

#[test]
fn test_disabled_returns_none() -> Result<()> {
    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .enabled(false)
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    assert!(middleware.is_none(), "Disabled config should return None");

    Ok(())
}

#[test]
fn test_monopolization_error_classification() {
    let error: MonopolizationError<MockError> = MonopolizationError::Monopolization {
        topic: "test-topic".into(),
        partition: 0,
        key: "test-key".into(),
        percentage: 95.0,
        threshold: 90.0,
        window: Duration::from_mins(5),
    };

    assert!(
        matches!(error.classify_error(), ErrorCategory::Transient),
        "Monopolization errors should be transient (retry later when key is no longer \
         monopolizing)"
    );
}

#[test]
fn test_monopolization_error_message() {
    let error: MonopolizationError<MockError> = MonopolizationError::Monopolization {
        topic: "orders".into(),
        partition: 3,
        key: "user-12345".into(),
        percentage: 95.5,
        threshold: 90.0,
        window: Duration::from_mins(5),
    };

    let message = error.to_string();
    assert!(
        message.contains("user-12345"),
        "Error should include the key"
    );
    assert!(
        message.contains("orders:3"),
        "Error should include topic:partition"
    );
    assert!(
        message.contains("95.5%"),
        "Error should include the actual percentage"
    );
    assert!(message.contains("90.0%"), "Error should include threshold");
    assert!(
        message.contains("5m"),
        "Error should include window duration in human-readable format"
    );
    assert!(
        message.contains("preventing other keys from being processed efficiently"),
        "Error should include helpful explanation"
    );
}

#[tokio::test]
async fn test_non_monopolizing_key_passes_through() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_mins(5))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let tp_key = test_tp_key("test-key");
    let reference_instant = handler.reference_instant;

    let start_time = reference_instant;
    let end_time = start_time + Duration::from_secs(10);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start_time,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end_time,
    ));

    sleep(Duration::from_millis(10)).await;

    let result = handler.check_monopolization(&tp_key, end_time);
    assert!(
        result.is_none(),
        "Key using 10s of 300s window should not monopolize"
    );

    Ok(())
}

#[tokio::test]
async fn test_monopolizing_key_triggers_error() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let tp_key = test_tp_key("monopolizer");
    let reference_instant = handler.reference_instant;

    let start_time = reference_instant;
    let end_time = start_time + Duration::from_secs(95);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start_time,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end_time,
    ));

    sleep(Duration::from_millis(10)).await;

    let result = handler.check_monopolization(&tp_key, end_time);
    assert!(
        result.is_some(),
        "Key using 95s of 100s window (95%) should monopolize"
    );

    if let Some(MonopolizationError::Monopolization { percentage, .. }) = result {
        assert!(
            percentage > 90.0_f64,
            "Monopolization percentage should be > 90%"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_keys_independent_tracking() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let tp_key1 = test_tp_key("key-1");
    let tp_key2 = test_tp_key("key-2");
    let reference_instant = handler.reference_instant;

    let start1 = reference_instant;
    let end1 = start1 + Duration::from_secs(95);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key1.key.clone(),
        KeyState::HandlerInvoked,
        start1,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key1.key.clone(),
        KeyState::HandlerSucceeded,
        end1,
    ));

    let start2 = reference_instant + Duration::from_millis(100);
    let end2 = start2 + Duration::from_secs(2);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key2.key.clone(),
        KeyState::HandlerInvoked,
        start2,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key2.key.clone(),
        KeyState::HandlerSucceeded,
        end2,
    ));

    sleep(Duration::from_millis(50)).await;

    // Check key1 at the time it finished (end1 = 95s)
    let result1 = handler.check_monopolization(&tp_key1, end1);
    assert!(
        result1.is_some(),
        "Key 1 should be monopolizing (95s of 100s)"
    );

    // Check key2 at the time it finished (end2 = 2.1s)
    let result2 = handler.check_monopolization(&tp_key2, end2);
    assert!(
        result2.is_none(),
        "Key 2 should not be monopolizing (2s of 100s)"
    );

    Ok(())
}

#[tokio::test]
async fn test_window_sliding_removes_old_intervals() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(10))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let tp_key = test_tp_key("test-key");
    let reference_instant = handler.reference_instant;

    let start1 = reference_instant;
    let end1 = start1 + Duration::from_millis(9100); // 9.1 seconds to exceed 90% threshold

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start1,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end1,
    ));

    sleep(Duration::from_millis(50)).await;

    let result = handler.check_monopolization(&tp_key, end1);
    assert!(result.is_some(), "Should monopolize right after execution");

    let start2 = end1 + Duration::from_secs(11);
    let end2 = start2 + Duration::from_millis(100);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start2,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end2,
    ));

    sleep(Duration::from_millis(10)).await;

    if let Some(intervals) = handler.key_intervals.get(&tp_key) {
        let now_nanos = end2
            .saturating_duration_since(handler.reference_instant)
            .as_nanos() as u64;
        let window_nanos = Duration::from_secs(10).as_nanos() as u64;
        let window_start = now_nanos.saturating_sub(window_nanos);
        let window_interval_set = [(window_start, now_nanos)].to_interval_set();
        let windowed = intervals.intersection(&window_interval_set);

        let total_time: u64 = windowed
            .iter()
            .map(|iv| iv.upper().saturating_sub(iv.lower()))
            .sum();

        assert!(
            total_time < Duration::from_secs(1).as_nanos() as u64,
            "Old interval should be outside window"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_open_interval_closed_on_completion() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let tp_key = test_tp_key("test-key");
    let reference_instant = handler.reference_instant;

    let start = reference_instant;

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start,
    ));

    sleep(Duration::from_millis(10)).await;

    let intervals_before = handler.key_intervals.get(&tp_key);
    assert!(
        intervals_before.is_some(),
        "Should have open interval after invocation"
    );

    let end = start + Duration::from_secs(50);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end,
    ));

    sleep(Duration::from_millis(10)).await;

    let intervals_after = handler.key_intervals.get(&tp_key);
    assert!(
        intervals_after.is_some(),
        "Should have closed interval after completion"
    );

    Ok(())
}

#[tokio::test]
async fn test_boundary_execution_before_window() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let reference_instant = handler.reference_instant;

    let tp_key = test_tp_key("key-before-window");
    let execution_start = reference_instant;
    let execution_end = execution_start + Duration::from_secs(50);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        execution_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        execution_end,
    ));

    sleep(Duration::from_millis(50)).await;

    // Check at time that puts execution_start before the window
    let check_time = reference_instant + Duration::from_mins(2);
    let result = handler.check_monopolization(&tp_key, check_time);
    assert!(
        result.is_none(),
        "Execution that started before window should only count time within window"
    );

    Ok(())
}

#[tokio::test]
async fn test_boundary_execution_crosses_window_end() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let reference_instant = handler.reference_instant;

    let tp_key = test_tp_key("key-crosses-boundary");
    let execution_start = reference_instant + Duration::from_secs(10);
    let execution_end = execution_start + Duration::from_secs(95);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        execution_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        execution_end,
    ));

    sleep(Duration::from_millis(50)).await;

    let result = handler.check_monopolization(&tp_key, execution_end);
    assert!(
        result.is_some(),
        "Key using 95s of 100s window should monopolize at window end"
    );

    Ok(())
}

#[tokio::test]
async fn test_boundary_exact_threshold() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let reference_instant = handler.reference_instant;

    let tp_key = test_tp_key("key-exact-threshold");
    let execution_start = reference_instant;
    let execution_end = execution_start + Duration::from_secs(90);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        execution_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        execution_end,
    ));

    sleep(Duration::from_millis(50)).await;

    let result = handler.check_monopolization(&tp_key, execution_end);
    assert!(
        result.is_none(),
        "Key using exactly 90s of 100s window (90.0%) should not monopolize (threshold is >90%, \
         not >=90%)"
    );

    Ok(())
}

#[tokio::test]
async fn test_boundary_just_above_threshold() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let reference_instant = handler.reference_instant;

    let tp_key = test_tp_key("key-above-threshold");
    let execution_start = reference_instant;
    let execution_end = execution_start + Duration::from_millis(90_100);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        execution_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        execution_end,
    ));

    sleep(Duration::from_millis(50)).await;

    let result = handler.check_monopolization(&tp_key, execution_end);
    assert!(
        result.is_some(),
        "Key using 90.1s of 100s window (90.1%) should monopolize"
    );

    Ok(())
}

#[tokio::test]
async fn test_boundary_multiple_executions_in_window() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .monopolization_threshold(0.9)
        .window_duration(Duration::from_secs(100))
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let mock_handler = MockHandler::new();
    let provider = MockProvider {
        handler: mock_handler.clone(),
    };

    let provider = middleware.with_provider(provider);
    let handler = provider
        .handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION)
        .enabled()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected enabled handler"))?;

    let reference_instant = handler.reference_instant;
    let tp_key = test_tp_key("key-multiple-at-boundary");

    // First execution: 20s at start of window
    let first_start = reference_instant;
    let first_end = first_start + Duration::from_secs(20);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        first_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        first_end,
    ));

    // Second execution: 72s that ends at window boundary
    let second_start = first_start + Duration::from_secs(28);
    let second_end = first_start + Duration::from_secs(100);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        second_start,
    ));

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        second_end,
    ));

    sleep(Duration::from_millis(50)).await;

    // Check at end of window - should capture both executions (20s + 72s = 92s >
    // 90s)
    let check_time = first_start + Duration::from_secs(100);
    let result = handler.check_monopolization(&tp_key, check_time);
    assert!(
        result.is_some(),
        "Multiple executions totaling >90s in window should monopolize"
    );

    Ok(())
}
