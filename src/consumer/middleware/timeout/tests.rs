use super::*;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, create_test_message, create_test_trigger,
};
use crate::error::{ClassifyError, ErrorCategory};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone)]
struct TestError(&'static str);

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "test error: {}", self.0)
    }
}

impl Error for TestError {}

impl ClassifyError for TestError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}

/// Mock handler with configurable behavior including delay.
#[derive(Clone)]
struct MockHandler {
    call_count: Arc<AtomicUsize>,
    delay: Option<Duration>,
    result: Result<(), TestError>,
    /// Records whether the handler observed cancellation during execution.
    observed_cancellation: Arc<AtomicBool>,
}

impl MockHandler {
    fn success() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            delay: None,
            result: Ok(()),
            observed_cancellation: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_delay(delay: Duration) -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            delay: Some(delay),
            result: Ok(()),
            observed_cancellation: Arc::new(AtomicBool::new(false)),
        }
    }

    fn failing() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            delay: None,
            result: Err(TestError("handler failed")),
            observed_cancellation: Arc::new(AtomicBool::new(false)),
        }
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    fn observed_cancellation(&self) -> bool {
        self.observed_cancellation.load(Ordering::Relaxed)
    }

    /// Shared dispatch body: optional delay raced against cancellation, then
    /// the scripted result.
    async fn run<C>(&self, context: C) -> Result<(), TestError>
    where
        C: EventContext<Payload = serde_json::Value>,
    {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        if let Some(delay) = self.delay {
            // Wait for delay or cancellation, whichever comes first.
            select! {
                () = sleep(delay) => {}
                () = context.on_cancel() => {
                    self.observed_cancellation.store(true, Ordering::Relaxed);
                    return Err(TestError("cancelled"));
                }
            }
        }
        self.result.clone()
    }
}

impl FallibleHandler for MockHandler {
    type Error = TestError;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.run(context).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.run(context).await
    }

    async fn shutdown(self) {}
}

#[tokio::test]
async fn handler_completes_before_timeout_returns_ok() -> color_eyre::Result<()> {
    let handler = MockHandler::success();
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_secs(10),
    };
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result = timeout_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

#[tokio::test]
async fn handler_completes_before_timeout_returns_handler_error() -> color_eyre::Result<()> {
    let handler = MockHandler::failing();
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_secs(10),
    };
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result = timeout_handler
        .on_message(context, message, DemandType::Normal)
        .await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
    Ok(())
}

#[tokio::test]
async fn handler_exceeds_timeout_signals_cancellation_and_then_uncancels() -> color_eyre::Result<()>
{
    // Handler takes 100ms but timeout is 10ms
    // After timeout, cancellation is signaled and we wait for handler
    let handler = MockHandler::with_delay(Duration::from_millis(100));
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_millis(10),
    };
    let context = MockEventContext::new();
    let message = create_test_message()?;

    let result = timeout_handler
        .on_message(context.clone(), message, DemandType::Normal)
        .await;

    // Handler should return error after seeing cancellation
    assert!(result.is_err());
    // Handler was invoked and responded to cancellation
    assert_eq!(handler.call_count(), 1);
    // Handler observed the cancellation signal during execution
    assert!(handler.observed_cancellation());
    // Cancellation flag should be reset after operation completes
    assert!(!context.should_cancel());
    Ok(())
}

#[tokio::test]
async fn timer_handler_completes_before_timeout_returns_ok() {
    let handler = MockHandler::success();
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_secs(10),
    };
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result = timeout_handler
        .on_timer(context, trigger, DemandType::Normal)
        .await;

    assert!(result.is_ok());
    assert_eq!(handler.call_count(), 1);
}

#[tokio::test]
async fn timer_handler_exceeds_timeout_signals_cancellation_and_then_uncancels() {
    // Timer handler takes 100ms but timeout is 10ms
    let handler = MockHandler::with_delay(Duration::from_millis(100));
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_millis(10),
    };
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result = timeout_handler
        .on_timer(context.clone(), trigger, DemandType::Normal)
        .await;

    // Handler should return error after seeing cancellation
    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
    // Handler observed the cancellation signal during execution
    assert!(handler.observed_cancellation());
    // Cancellation flag should be reset after operation completes
    assert!(!context.should_cancel());
}

#[tokio::test]
async fn timer_handler_error_passed_through() {
    let handler = MockHandler::failing();
    let timeout_handler = TimeoutHandler {
        handler: handler.clone(),
        timeout: Duration::from_secs(10),
    };
    let context = MockEventContext::new();
    let trigger = create_test_trigger();

    let result = timeout_handler
        .on_timer(context, trigger, DemandType::Normal)
        .await;

    assert!(result.is_err());
    assert_eq!(handler.call_count(), 1);
}
