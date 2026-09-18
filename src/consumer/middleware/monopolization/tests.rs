use super::*;
use crate::Partition;
use crate::consumer::DemandType;
use crate::consumer::middleware::tests::test_support::{ScriptedHandler, TestError};
use crate::consumer::middleware::{FallibleHandlerProvider, HandlerMiddleware};
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use interval::interval_set::ToIntervalSet;
use interval::prelude::{Intersection, Union};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

const TEST_TOPIC: &str = "test-topic";
const TEST_PARTITION: Partition = 0;
const NANOS_PER_MS: u64 = 1_000_000;

#[derive(Clone)]
struct MockProvider {
    handler: ScriptedHandler,
}

/// One generated `check_monopolization` scenario: a window, an
/// integer-percent threshold, a check instant, and 1-3 keys' worth of
/// execution intervals. Everything is millisecond-granular so occupancies
/// land below, on, and above the threshold boundary.
#[derive(Clone, Debug)]
struct WindowedCase {
    window_ms: u64,
    threshold_pct: u8,
    now_ms: u64,
    /// Per-key `(start_ms, len_ms)` execution intervals; a key with no
    /// intervals is never inserted, exercising the untracked-key fast path.
    /// Start+length (not start/end) keeps every shrunk pair a valid,
    /// non-inverted interval.
    keys: Vec<Vec<(u64, u64)>>,
}

impl FallibleHandlerProvider for MockProvider {
    type Handler = ScriptedHandler;

    fn handler_for_partition(&self, _topic: Topic, _partition: Partition) -> Self::Handler {
        self.handler.clone()
    }
}

impl Arbitrary for WindowedCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let window_ms = u64::arbitrary(g) % 991_u64 + 10_u64;
        // The check instant lands before, inside, or well past the first full
        // window, so `window_start` both saturates to zero and does not.
        let now_ms = u64::arbitrary(g) % (3_u64 * window_ms);
        let key_count = usize::arbitrary(g) % 3_usize + 1_usize;
        let keys = (0..key_count)
            .map(|_| {
                let interval_count = usize::arbitrary(g) % 5_usize;
                (0..interval_count)
                    .map(|_| {
                        // Starts range past `now` and lengths up to a full
                        // window, so intervals fall before, across, and after
                        // both window edges.
                        let start = u64::arbitrary(g) % (now_ms + window_ms);
                        let len = u64::arbitrary(g) % (window_ms + 1_u64);
                        (start, len)
                    })
                    .collect()
            })
            .collect();
        Self {
            window_ms,
            threshold_pct: u8::arbitrary(g) % 99_u8 + 1_u8,
            now_ms,
            keys,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = self.clone();
        Box::new(self.keys.shrink().map(move |keys| Self {
            keys,
            ..base.clone()
        }))
    }
}

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

/// A handler over a directly seeded interval cache: the property and the
/// boundary pin drive `check_monopolization` synchronously, with no telemetry
/// event loop involved.
fn seeded_handler(threshold: f64, window: Duration) -> MonopolizationHandler<ScriptedHandler> {
    MonopolizationHandler {
        handler: ScriptedHandler::success(),
        topic: TEST_TOPIC.into(),
        partition: TEST_PARTITION,
        reference_instant: Instant::now(),
        key_intervals: Some(Arc::new(Cache::new(16_usize))),
        monopolization_threshold: threshold,
        window_duration: window,
    }
}

/// Folds `(lower, upper)` pairs into an `IntervalSet`; `None` when empty.
fn interval_set(pairs: &[(u64, u64)]) -> Option<IntervalSet<u64>> {
    let (&first, rest) = pairs.split_first()?;
    Some(rest.iter().fold([first].to_interval_set(), |set, &pair| {
        set.union(&[pair].to_interval_set())
    }))
}

/// Windowed-occupancy oracle: measures a stored interval set inside the
/// window `[window_start, now]` with the interval library's own
/// `intersection` — the allocating form the production clamp-and-sum
/// replaced, kept here as the reference it must stay equivalent to.
fn model_windowed_nanos(intervals: &IntervalSet<u64>, window_start: u64, now: u64) -> u64 {
    intervals
        .intersection(&[(window_start, now)].to_interval_set())
        .iter()
        .map(|iv| iv.upper().saturating_sub(iv.lower()))
        .sum()
}

/// Property body: for every key, `check_monopolization` rejects exactly when
/// the oracle's windowed occupancy strictly exceeds `threshold * window`, and
/// keys never observe each other's intervals.
///
/// The integer comparison below is the exact form of the production float
/// comparison: at millisecond granularity the smallest nonzero gap between
/// `occupied / window` and `pct / 100` is ~1e-8, far above f64 rounding
/// error, so the two decisions always agree.
fn matches_model(case: WindowedCase) -> bool {
    let WindowedCase {
        window_ms,
        threshold_pct,
        now_ms,
        keys,
    } = case;
    let window = Duration::from_millis(window_ms);
    let handler = seeded_handler(f64::from(threshold_pct) / 100.0_f64, window);

    let window_nanos = window_ms * NANOS_PER_MS;
    let now_nanos = now_ms * NANOS_PER_MS;
    let window_start = now_nanos.saturating_sub(window_nanos);
    let now = handler.reference_instant + Duration::from_nanos(now_nanos);
    let Some(key_intervals) = &handler.key_intervals else {
        return false;
    };

    for (i, pairs) in keys.iter().enumerate() {
        let nanos: Vec<(u64, u64)> = pairs
            .iter()
            .map(|&(start, len)| (start * NANOS_PER_MS, (start + len) * NANOS_PER_MS))
            .collect();
        if let Some(set) = interval_set(&nanos) {
            key_intervals.insert(test_tp_key(&format!("key-{i}")), set);
        }
    }

    (0..keys.len()).all(|i| {
        let tp_key = test_tp_key(&format!("key-{i}"));
        let expected = key_intervals.get(&tp_key).is_some_and(|set| {
            let occupied = model_windowed_nanos(&set, window_start, now_nanos);
            u128::from(occupied) * 100_u128 > u128::from(threshold_pct) * u128::from(window_nanos)
        });
        handler.check_monopolization(&tp_key, now).is_some() == expected
    })
}

#[test]
fn check_monopolization_matches_windowed_overlap_model() {
    init_test_logging();
    // Iteration count comes from the `QUICKCHECK_TESTS` env var.
    QuickCheck::new().quickcheck(matches_model as fn(WindowedCase) -> bool);
}

/// Pins the strict `>` in `check_monopolization`: occupancy of exactly
/// `threshold * window` is not monopolizing; one more nanosecond is.
#[test]
fn exact_threshold_occupancy_is_not_monopolizing() -> Result<()> {
    let handler = seeded_handler(0.9_f64, Duration::from_secs(100));
    let tp_key = test_tp_key("exact-threshold");
    let now = handler.reference_instant + Duration::from_secs(100);
    let ninety_secs = Duration::from_secs(90).as_nanos() as u64;
    let key_intervals = handler
        .key_intervals
        .as_ref()
        .ok_or_else(|| eyre!("seeded handler must have a key cache"))?;

    let exact = interval_set(&[(0_u64, ninety_secs)]).ok_or_else(|| eyre!("non-empty pairs"))?;
    key_intervals.insert(tp_key.clone(), exact);
    assert!(
        handler.check_monopolization(&tp_key, now).is_none(),
        "exactly 90% of the window must pass (threshold is strict >, not >=)"
    );

    let just_above =
        interval_set(&[(0_u64, ninety_secs + 1_u64)]).ok_or_else(|| eyre!("non-empty pairs"))?;
    key_intervals.insert(tp_key.clone(), just_above);
    assert!(
        handler.check_monopolization(&tp_key, now).is_some(),
        "one nanosecond above 90% must reject"
    );

    Ok(())
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
fn disabled_middleware_retains_no_key_cache() -> Result<()> {
    let telemetry = Telemetry::new();

    let config = MonopolizationConfiguration::builder()
        .enabled(false)
        .build()?;

    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    assert!(
        middleware.key_intervals.is_none(),
        "disabled middleware must not allocate a key cache"
    );

    Ok(())
}

#[test]
fn test_monopolization_error_classification() {
    let error: MonopolizationError<TestError> = MonopolizationError::Monopolization {
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
    let error: MonopolizationError<TestError> = MonopolizationError::Monopolization {
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

/// `run_event_loop` trims a key's stored intervals to the rolling window on
/// every completion event, bounding per-key memory.
///
/// Driven directly: the events are buffered in the channel, the sender is
/// dropped, and the loop is awaited to completion — it drains every event and
/// exits on channel close, so awaiting it IS the synchronization.
#[tokio::test]
async fn window_sliding_removes_old_intervals() -> Result<()> {
    init_test_logging();

    let reference = Instant::now();
    let key_intervals = Arc::new(Cache::new(16_usize));
    let window = Duration::from_secs(10);
    let tp_key = test_tp_key("test-key");
    let (tx, rx) = broadcast::channel(8_usize);

    // First execution occupies [0s, 9.1s]; the second runs [20.1s, 20.2s].
    // The completion at 20.2s trims everything before the window start
    // (10.2s), leaving only the second interval.
    for (state, at_ms) in [
        (KeyState::HandlerInvoked, 0_u64),
        (KeyState::HandlerSucceeded, 9_100_u64),
        (KeyState::HandlerInvoked, 20_100_u64),
        (KeyState::HandlerSucceeded, 20_200_u64),
    ] {
        tx.send(create_key_event(
            TEST_TOPIC.into(),
            TEST_PARTITION,
            tp_key.key.clone(),
            state,
            reference + Duration::from_millis(at_ms),
        ))
        .map_err(|_| eyre!("event loop receiver dropped"))?;
    }
    drop(tx);
    run_event_loop(reference, Arc::clone(&key_intervals), window, rx).await;

    let intervals = key_intervals
        .get(&tp_key)
        .ok_or_else(|| eyre!("expected a trimmed interval set for the key"))?;
    let stored: Vec<(u64, u64)> = intervals
        .iter()
        .map(|iv| (iv.lower(), iv.upper()))
        .collect();
    assert_eq!(
        stored,
        vec![(20_100_u64 * NANOS_PER_MS, 20_200_u64 * NANOS_PER_MS)],
        "completion must trim intervals outside the rolling window"
    );

    Ok(())
}

/// End-to-end through `MonopolizationMiddleware::new`'s spawned telemetry
/// loop: `HandlerInvoked` opens an interval reaching the far future (the key
/// reads as monopolizing arbitrarily far ahead), and the completion event
/// closes it at the completion timestamp (far-future checks come back clean).
#[tokio::test(start_paused = true)]
async fn open_interval_closed_on_completion() -> Result<()> {
    init_test_logging();

    let telemetry = Telemetry::new();
    let config = MonopolizationConfiguration::builder()
        .window_duration(Duration::from_secs(100))
        .build()?;
    let middleware = MonopolizationMiddleware::new(&config, &telemetry)?;
    let provider = middleware.with_provider(MockProvider {
        handler: ScriptedHandler::success(),
    });
    let handler = provider.handler_for_partition(TEST_TOPIC.into(), TEST_PARTITION);

    let tp_key = test_tp_key("test-key");
    let start = handler.reference_instant;
    let end = start + Duration::from_secs(50);
    let far_future = end + Duration::from_secs(1_000);

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerInvoked,
        start,
    ));
    drain_telemetry().await;

    assert!(
        handler.check_monopolization(&tp_key, far_future).is_some(),
        "an open interval extends to the far future, so the key reads as monopolizing there"
    );

    telemetry.test_emit(create_key_event(
        TEST_TOPIC.into(),
        TEST_PARTITION,
        tp_key.key.clone(),
        KeyState::HandlerSucceeded,
        end,
    ));
    drain_telemetry().await;

    assert!(
        handler.check_monopolization(&tp_key, far_future).is_none(),
        "completion must close the interval at the completion time, not leave it open"
    );
    assert!(
        handler.check_monopolization(&tp_key, end).is_none(),
        "50s of a 100s window is below the 90% threshold"
    );

    Ok(())
}

/// Deterministic barrier for the spawned telemetry loop: under paused time,
/// tokio only advances the clock once every task is idle, so this virtual
/// sleep resumes only after the loop has drained all pending events — it is
/// never a wall-clock wait.
async fn drain_telemetry() {
    sleep(Duration::from_millis(1)).await;
}

/// The settlement classification table: inner-ran rows delegate; the
/// pre-inner admission rejection is `Bypassed`. Delegation is proven against
/// a `Bypassed`-classifying probe.
#[test]
fn settlement_classification_table() {
    use crate::consumer::middleware::tests::test_support::BypassedHandler;
    use crate::consumer::middleware::{Settlement, SettlementHandler};

    type Subject = MonopolizationHandler<ScriptedHandler>;
    type Probe = MonopolizationHandler<BypassedHandler>;
    type Err_ = MonopolizationError<TestError>;

    fn monopolization() -> Err_ {
        MonopolizationError::Monopolization {
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            key: Arc::from("key"),
            percentage: 99.0_f64,
            threshold: 90.0_f64,
            window: Duration::from_mins(1),
        }
    }

    let rows: Vec<(&str, Result<(), Err_>, Settlement)> = vec![
        (
            "Ok delegates to the leaf's Final",
            Ok(()),
            Settlement::Final,
        ),
        (
            "Handler delegates to the leaf's Final",
            Err(MonopolizationError::Handler(TestError(
                ErrorCategory::Permanent,
            ))),
            Settlement::Final,
        ),
        (
            "Monopolization (pre-inner admission) is Bypassed",
            Err(monopolization()),
            Settlement::Bypassed,
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
    }

    // Delegation proof: over a Bypassed-classifying inner the delegating
    // rows stay Bypassed.
    let ok: Result<(), Err_> = Ok(());
    assert_eq!(Probe::settlement(ok.as_ref()), Settlement::Bypassed);
    let inner_err: Result<(), Err_> = Err(MonopolizationError::Handler(TestError(
        ErrorCategory::Permanent,
    )));
    assert_eq!(Probe::settlement(inner_err.as_ref()), Settlement::Bypassed);
}
