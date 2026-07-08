use super::*;
use crate::{Key, Partition};
use color_eyre::eyre::{Result, bail, ensure};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use tokio::time::timeout;

const TEST_TOPIC: &str = "test-topic";
const TEST_PARTITION: Partition = 0;

/// Failure-class service-time share pinned by [`create_selector`].
const FAILURE_WEIGHT: f64 = 0.3;

fn test_tp_key(key: &str) -> TopicPartitionKey {
    TopicPartitionKey::new(TEST_TOPIC.into(), TEST_PARTITION, Key::from(key))
}

/// Builds a selector with every tunable pinned explicitly, so ambient
/// `PROSODY_SCHEDULER_*` / `PROSODY_MAX_CONCURRENCY` environment variables
/// cannot skew test outcomes.
fn create_selector_with(failure_weight: f64) -> Result<Selector> {
    let config = SchedulerConfiguration::builder()
        .max_concurrency(32_usize)
        .failure_weight(failure_weight)
        .max_wait(Duration::from_mins(2_u64))
        .wait_weight(200.0_f64)
        .cache_size(8_192_usize)
        .build()?;
    Ok(Selector::new(&config))
}

fn create_selector() -> Result<Selector> {
    create_selector_with(FAILURE_WEIGHT)
}

fn create_task(key: &str, demand_type: DemandType, age_secs: u64) -> Task {
    let (tx, _rx) = oneshot::channel();
    Task {
        timestamp: Instant::now() - Duration::from_secs(age_secs),
        tp_key: test_tp_key(key),
        demand_type,
        key_time: None,
        tx,
    }
}

/// Records a completed selection the way telemetry would: adds `duration` to
/// the selected class's service-time ledger and to the selected key's virtual
/// time.
fn account(selector: &mut Selector, task: &Task, duration: Duration) {
    match task.demand_type {
        DemandType::Normal => selector.success_time += duration,
        DemandType::Failure => selector.failure_time += duration,
    }
    selector.increment_key_time(&task.tp_key, duration);
}

#[test]
fn empty_queue_returns_none() -> Result<()> {
    let mut selector = create_selector()?;
    assert!(selector.get_next_task().is_none());
    Ok(())
}

#[test]
fn single_task_returns_that_task() -> Result<()> {
    let mut selector = create_selector()?;
    let task = create_task("key1", DemandType::Normal, 0);
    let expected_key = task.tp_key.key.clone();
    selector.enqueue_task(task);

    let Some(task) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(task.tp_key.key, expected_key);
    Ok(())
}

#[test]
fn only_normal_tasks_selects_normal() -> Result<()> {
    let mut selector = create_selector()?;
    selector.enqueue_task(create_task("key1", DemandType::Normal, 0));
    selector.enqueue_task(create_task("key2", DemandType::Normal, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.demand_type, DemandType::Normal);
    Ok(())
}

#[test]
fn only_failure_tasks_selects_failure() -> Result<()> {
    let mut selector = create_selector()?;
    selector.enqueue_task(create_task("key1", DemandType::Failure, 0));
    selector.enqueue_task(create_task("key2", DemandType::Failure, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.demand_type, DemandType::Failure);
    Ok(())
}

#[test]
fn selects_lower_vt_within_class() -> Result<()> {
    let mut selector = create_selector()?;

    selector
        .key_times
        .insert(test_tp_key("high_vt"), Duration::from_secs(10).into());
    selector
        .key_times
        .insert(test_tp_key("low_vt"), Duration::from_millis(100).into());

    selector.enqueue_task(create_task("high_vt", DemandType::Normal, 0));
    selector.enqueue_task(create_task("low_vt", DemandType::Normal, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("low_vt"));
    Ok(())
}

#[test]
fn fifo_tiebreaking_when_vt_equal() -> Result<()> {
    let mut selector = create_selector()?;

    selector.enqueue_task(create_task("key1", DemandType::Normal, 2));
    selector.enqueue_task(create_task("key2", DemandType::Normal, 1));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("key1"));
    Ok(())
}

#[test]
fn new_key_starts_at_zero_vt() -> Result<()> {
    let mut selector = create_selector()?;

    selector
        .key_times
        .insert(test_tp_key("old_key"), Duration::from_secs(5).into());

    selector.enqueue_task(create_task("new_key", DemandType::Normal, 0));
    selector.enqueue_task(create_task("old_key", DemandType::Normal, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("new_key"));
    Ok(())
}

#[test]
fn underserved_class_wins() -> Result<()> {
    let mut selector = create_selector()?;

    selector.success_time = Duration::from_millis(700).into();
    selector.failure_time = Duration::from_millis(100).into();

    selector.enqueue_task(create_task("normal_key", DemandType::Normal, 0));
    selector.enqueue_task(create_task("failure_key", DemandType::Failure, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.demand_type, DemandType::Failure);
    Ok(())
}

#[test]
fn overserved_class_loses() -> Result<()> {
    let mut selector = create_selector()?;

    selector.success_time = Duration::from_millis(100).into();
    selector.failure_time = Duration::from_millis(700).into();

    selector.enqueue_task(create_task("normal_key", DemandType::Normal, 0));
    selector.enqueue_task(create_task("failure_key", DemandType::Failure, 0));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.demand_type, DemandType::Normal);
    Ok(())
}

/// A zero-weight class scores `f64::INFINITY` in the cross-class comparison,
/// so it is never selected while the other class has pending work — but still
/// drains once it is the only class with work (work conservation).
#[test]
fn zero_weight_class_starved_until_alone() -> Result<()> {
    for (failure_weight, served, starved) in [
        (0.0_f64, DemandType::Normal, DemandType::Failure),
        (1.0_f64, DemandType::Failure, DemandType::Normal),
    ] {
        let mut selector = create_selector_with(failure_weight)?;
        for index in 0_i32..5_i32 {
            selector.enqueue_task(create_task(&format!("n{index}"), DemandType::Normal, 0));
            selector.enqueue_task(create_task(&format!("f{index}"), DemandType::Failure, 0));
        }

        for _ in 0_i32..5_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("expected a task while the served class has work");
            };
            ensure!(
                task.demand_type == served,
                "{starved:?} selected while zero-weighted and {served:?} had pending tasks"
            );
            account(&mut selector, &task, Duration::from_millis(100));
        }

        for _ in 0_i32..5_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("expected the zero-weight class to drain once alone");
            };
            ensure!(
                task.demand_type == starved,
                "expected only {starved:?} tasks to remain"
            );
        }
        assert!(
            selector.get_next_task().is_none(),
            "queue should be fully drained"
        );
    }
    Ok(())
}

#[test]
fn wait_urgency_overrides_low_vt() -> Result<()> {
    let mut selector = create_selector()?;

    selector
        .key_times
        .insert(test_tp_key("low_vt"), Duration::from_millis(10).into());
    selector
        .key_times
        .insert(test_tp_key("high_vt"), Duration::from_secs(5).into());

    selector.enqueue_task(create_task("low_vt", DemandType::Normal, 0));
    // With WAIT_WEIGHT = 200, need 120s wait to get max boost of 200 points
    // VT difference is ~5s = 5 points, so 120s wait easily overcomes it
    selector.enqueue_task(create_task("high_vt", DemandType::Normal, 120));

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("high_vt"));
    Ok(())
}

#[test]
fn extreme_wait_guarantees_selection() -> Result<()> {
    let mut selector = create_selector()?;

    selector.enqueue_task(create_task("waiting_key", DemandType::Normal, 10));

    for i in 0_i32..100_i32 {
        let key = format!("new_{i}");
        selector
            .key_times
            .insert(test_tp_key(&key), Duration::from_millis(1).into());
        selector.enqueue_task(create_task(&key, DemandType::Normal, 0));
    }

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("waiting_key"));
    Ok(())
}

/// The wait-urgency cap (`wait_weight` = 200s) exceeds any VT distance below
/// it, so a key carrying extreme VT from a past monopoly recovers: once it has
/// waited `max_wait`, its priority (150s VT − 200s boost) beats fresh zero-VT,
/// zero-wait keys at priority 0.
#[test]
fn monopoly_recovery_high_vt_key() -> Result<()> {
    let mut selector = create_selector()?;

    selector
        .key_times
        .insert(test_tp_key("monopoly"), Duration::from_secs(150).into());
    selector.enqueue_task(create_task("monopoly", DemandType::Normal, 120));

    for i in 0_i32..10_i32 {
        selector.enqueue_task(create_task(&format!("new_{i}"), DemandType::Normal, 0));
    }

    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task but got None");
    };
    assert_eq!(selected.tp_key.key, Key::from("monopoly"));
    Ok(())
}

#[test]
fn single_key_monopoly_handled() -> Result<()> {
    let mut selector = create_selector()?;

    for _ in 0_i32..10_i32 {
        selector.enqueue_task(create_task("monopoly_key", DemandType::Normal, 0));
    }

    for _ in 0_i32..10_i32 {
        let Some(selected) = selector.get_next_task() else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.tp_key.key, Key::from("monopoly_key"));
    }
    Ok(())
}

#[test]
fn empty_class_work_conservation() -> Result<()> {
    let mut selector = create_selector()?;

    // Only enqueue tasks for normal class
    for i in 0_i32..10_i32 {
        selector.enqueue_task(create_task(&format!("n{i}"), DemandType::Normal, 0));
    }

    // Execute all normal tasks - failure class is empty
    for _ in 0_i32..10_i32 {
        let Some(task) = selector.get_next_task() else {
            bail!("Should select from non-empty class");
        };
        assert_eq!(
            task.demand_type,
            DemandType::Normal,
            "Should select from Normal class when Failure is empty"
        );
    }

    // Now queue is empty
    assert!(
        selector.get_next_task().is_none(),
        "Should return None when all classes empty"
    );

    // Add only failure tasks
    for i in 0_i32..5_i32 {
        selector.enqueue_task(create_task(&format!("f{i}"), DemandType::Failure, 0));
    }

    // Should select from failure class now
    for _ in 0_i32..5_i32 {
        let Some(task) = selector.get_next_task() else {
            bail!("Should select from non-empty class");
        };
        assert_eq!(
            task.demand_type,
            DemandType::Failure,
            "Should select from Failure class when Normal is empty"
        );
    }
    Ok(())
}

// ============================================================================
// Statistical properties
// ============================================================================

/// One adversarial prefix phase for [`prop_failure_service_share_converges`]:
/// a single-class batch of tasks drained immediately, skewing the class
/// service-time ledgers.
#[derive(Clone, Debug)]
struct Burst {
    failure_class: bool,
    tasks: usize,
    duration_ms: u64,
}

impl Arbitrary for Burst {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            failure_class: bool::arbitrary(g),
            tasks: 1_usize + usize::arbitrary(g) % 30_usize,
            duration_ms: 1_u64 + u64::arbitrary(g) % 50_u64,
        }
    }
}

/// Input for [`prop_failure_service_share_converges`]: an adversarial prefix
/// (class-ledger skews plus single-class bursts) followed by the per-class
/// steady-state task durations.
///
/// Bounds keep the worst prefix imbalance (~6.5s of one-sided service time,
/// needing ~240 catch-up selections at the 50ms duration floor) well inside
/// the 600-selection burn-in that precedes measurement.
#[derive(Clone, Debug)]
struct ProportionTrial {
    initial_success_ms: u64,
    initial_failure_ms: u64,
    bursts: Vec<Burst>,
    normal_ms: u64,
    failure_ms: u64,
}

impl Arbitrary for ProportionTrial {
    fn arbitrary(g: &mut Gen) -> Self {
        let burst_count = usize::arbitrary(g) % 4_usize;
        Self {
            initial_success_ms: u64::arbitrary(g) % 1_001_u64,
            initial_failure_ms: u64::arbitrary(g) % 1_001_u64,
            bursts: (0_usize..burst_count)
                .map(|_| Burst::arbitrary(g))
                .collect(),
            normal_ms: 50_u64 + u64::arbitrary(g) % 101_u64,
            failure_ms: 50_u64 + u64::arbitrary(g) % 101_u64,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Dropping bursts is the highest-value reduction.
        let template = self.clone();
        Box::new(self.bursts.shrink().map(move |bursts| Self {
            bursts,
            ..template.clone()
        }))
    }
}

/// Cross-class weighted fair queueing converges the failure class's share of
/// SERVICE TIME (not task count) to `failure_weight`, from any adversarial
/// starting skew.
///
/// After the prefix, a steady generator keeps both classes pending: each
/// selection is accounted with its class's duration and replaced by a
/// same-class task. The burn-in absorbs catch-up from the prefix skew; the
/// failure share of service time added during the measured settle window must
/// sit within ±0.02 of the target.
#[quickcheck]
fn prop_failure_service_share_converges(trial: ProportionTrial) -> Result<()> {
    const BURN_IN: usize = 600;
    const WINDOW: usize = 1_000;

    let ProportionTrial {
        initial_success_ms,
        initial_failure_ms,
        bursts,
        normal_ms,
        failure_ms,
    } = trial;

    let mut selector = create_selector()?;
    selector.success_time = Duration::from_millis(initial_success_ms).into();
    selector.failure_time = Duration::from_millis(initial_failure_ms).into();

    // Adversarial prefix: single-class bursts, enqueued then fully drained.
    for (burst_index, burst) in bursts.iter().enumerate() {
        let class = if burst.failure_class {
            DemandType::Failure
        } else {
            DemandType::Normal
        };
        for task_index in 0_usize..burst.tasks {
            selector.enqueue_task(create_task(
                &format!("b{burst_index}_{task_index}"),
                class,
                0,
            ));
        }
        for _ in 0_usize..burst.tasks {
            let Some(task) = selector.get_next_task() else {
                bail!("prefix drain expected a pending task");
            };
            account(
                &mut selector,
                &task,
                Duration::from_millis(burst.duration_ms),
            );
        }
    }

    // Steady generator: one pending task per class, replaced on selection.
    selector.enqueue_task(create_task("steady_n", DemandType::Normal, 0));
    selector.enqueue_task(create_task("steady_f", DemandType::Failure, 0));

    let mut window_normal = Duration::ZERO;
    let mut window_failure = Duration::ZERO;
    for selection in 0_usize..(BURN_IN + WINDOW) {
        let Some(task) = selector.get_next_task() else {
            bail!("steady phase expected a pending task at selection {selection}");
        };
        let duration = match task.demand_type {
            DemandType::Normal => Duration::from_millis(normal_ms),
            DemandType::Failure => Duration::from_millis(failure_ms),
        };
        account(&mut selector, &task, duration);
        if selection >= BURN_IN {
            match task.demand_type {
                DemandType::Normal => window_normal += duration,
                DemandType::Failure => window_failure += duration,
            }
        }
        selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));
    }

    let total = window_normal + window_failure;
    let share = window_failure.as_secs_f64() / total.as_secs_f64();
    ensure!(
        (share - FAILURE_WEIGHT).abs() <= 0.02_f64,
        "settle-window failure service share {share:.4} strayed from {FAILURE_WEIGHT}"
    );
    Ok(())
}

/// One key in a [`SpreadTrial`]: its simulated task duration, any prior
/// virtual time it arrives with, and the selection index at which it arrives
/// (staggered arrivals model a flash crowd hitting a warm scheduler).
#[derive(Clone, Debug)]
struct SpreadKey {
    duration_ms: u64,
    initial_vt_ms: u64,
    arrival: usize,
}

impl Arbitrary for SpreadKey {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            duration_ms: 20_u64 + u64::arbitrary(g) % 81_u64,
            initial_vt_ms: u64::arbitrary(g) % 1_001_u64,
            arrival: usize::arbitrary(g) % 11_usize,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrink the offset ABOVE the 20ms duration floor, never clamp:
        // clamping (`duration_ms.max(20)`) once yielded a candidate identical
        // to the failing input at the floor, which spun quickcheck's shrink
        // loop forever instead of reporting the failure. Every candidate here
        // is strictly smaller in one coordinate, so shrinking terminates.
        let extra_ms = self.duration_ms.saturating_sub(20_u64);
        Box::new((extra_ms, self.initial_vt_ms, self.arrival).shrink().map(
            |(extra_ms, initial_vt_ms, arrival)| Self {
                duration_ms: 20_u64 + extra_ms,
                initial_vt_ms,
                arrival,
            },
        ))
    }
}

/// Input for [`prop_vt_spread_bounded_and_no_starvation`]: 2-6 keys with
/// heterogeneous durations, prior-VT skews, and staggered arrivals. The first
/// key always arrives at selection 0 so the queue is never empty.
///
/// Bounds keep the worst pre-measurement imbalance (≤1s prior VT plus ≤1s of
/// pre-arrival accumulation, needing ≤600 catch-up selections at the 20ms
/// duration floor) inside the 800-selection burn-in.
#[derive(Clone, Debug)]
struct SpreadTrial {
    keys: Vec<SpreadKey>,
}

impl Arbitrary for SpreadTrial {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = 2_usize + usize::arbitrary(g) % 5_usize;
        let mut keys: Vec<SpreadKey> = (0_usize..count).map(|_| SpreadKey::arbitrary(g)).collect();
        if let Some(first) = keys.first_mut() {
            first.arrival = 0;
        }
        Self { keys }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(
            self.keys
                .shrink()
                .filter(|keys| keys.len() >= 2_usize)
                .map(|mut keys| {
                    if let Some(first) = keys.first_mut() {
                        first.arrival = 0;
                    }
                    Self { keys }
                }),
        )
    }
}

/// Within a class, min-VT selection keeps the virtual-time spread across
/// simultaneously pending keys bounded by 20× the largest task duration in the
/// trial, and starves no key: every key — including flash-crowd arrivals — is
/// selected during the settled window.
///
/// The selector is driven deterministically with simulated durations (no
/// sleeping): every selected key re-enqueues immediately, so all arrived keys
/// are pending at every sample point. Spread is sampled only after the
/// post-arrival burn-in, which absorbs prior-VT skews and pre-arrival
/// accumulation. The trial runs a fixed selection budget and asserts inside
/// it, so any violation fails an assertion — a broken selector cannot make
/// the property hang.
#[quickcheck]
fn prop_vt_spread_bounded_and_no_starvation(trial: SpreadTrial) -> Result<()> {
    const BURN_IN: usize = 800;
    const WINDOW: usize = 400;

    let SpreadTrial { keys } = trial;
    ensure!(keys.len() >= 2_usize, "generator must produce >= 2 keys");

    let mut selector = create_selector()?;
    let tp_keys: Vec<TopicPartitionKey> = (0_usize..keys.len())
        .map(|index| test_tp_key(&format!("k{index}")))
        .collect();
    let max_duration_ms = keys.iter().map(|key| key.duration_ms).fold(0_u64, u64::max);
    let bound_us = u128::from(20_u64 * max_duration_ms) * 1_000_u128;
    let last_arrival = keys.iter().map(|key| key.arrival).fold(0_usize, usize::max);
    let measure_from = last_arrival + BURN_IN;

    let mut served = vec![0_usize; keys.len()];
    for selection in 0_usize..(measure_from + WINDOW) {
        for (index, key) in keys.iter().enumerate() {
            if key.arrival == selection
                && let Some(tp_key) = tp_keys.get(index)
            {
                selector.key_times.insert(
                    tp_key.clone(),
                    Duration::from_millis(key.initial_vt_ms).into(),
                );
                selector.enqueue_task(create_task(&format!("k{index}"), DemandType::Normal, 0));
            }
        }

        let Some(task) = selector.get_next_task() else {
            bail!("expected a pending task at selection {selection}");
        };
        let Some(index) = tp_keys.iter().position(|tp_key| *tp_key == task.tp_key) else {
            bail!("selected an unknown key: {}", task.tp_key.key);
        };
        let Some(spec) = keys.get(index) else {
            bail!("no spec for key index {index}");
        };
        account(
            &mut selector,
            &task,
            Duration::from_millis(spec.duration_ms),
        );
        selector.enqueue_task(create_task(task.tp_key.key.as_ref(), DemandType::Normal, 0));

        if selection >= measure_from {
            if let Some(count) = served.get_mut(index) {
                *count += 1_usize;
            }
            let now = Instant::now();
            let vts: Vec<u128> = tp_keys
                .iter()
                .filter_map(|tp_key| selector.key_times.get(tp_key))
                .map(|vt| vt.at(now).as_micros())
                .collect();
            ensure!(
                vts.len() == keys.len(),
                "every key must have a VT entry in the settled window"
            );
            let (Some(min_vt), Some(max_vt)) = (vts.iter().min(), vts.iter().max()) else {
                bail!("no VT samples collected");
            };
            ensure!(
                max_vt - min_vt <= bound_us,
                "VT spread {}us exceeded 20x max duration ({bound_us}us) at selection {selection}",
                max_vt - min_vt
            );
        }
    }

    for (index, count) in served.iter().enumerate() {
        ensure!(
            *count > 0_usize,
            "key k{index} was starved: 0 selections in the {WINDOW}-op settled window"
        );
    }
    Ok(())
}

// ============================================================================
// Telemetry
// ============================================================================

#[test]
fn telemetry_tracks_invocation_time() -> Result<()> {
    let mut selector = create_selector()?;
    let key = Key::from("test_key");
    let tp_key = TopicPartitionKey::new(TEST_TOPIC.into(), TEST_PARTITION, key.clone());
    let now = Instant::now();

    let event = TelemetryEvent {
        timestamp: now,
        topic: TEST_TOPIC.into(),
        partition: TEST_PARTITION,
        data: Arc::new(Data::Key(KeyEvent {
            key,
            demand_type: DemandType::Normal,
            state: KeyState::HandlerInvoked,
        })),
    };

    selector.process_telemetry(event);

    assert!(selector.invocation_times.contains_key(&tp_key));
    assert_eq!(selector.invocation_times.get(&tp_key), Some(&now));
    Ok(())
}

/// Asserts that an invoke/complete telemetry pair adds the measured duration
/// to `demand_type`'s class ledger and records the key's virtual time.
fn assert_completion_updates_class_time(demand_type: DemandType, state: KeyState) -> Result<()> {
    let mut selector = create_selector()?;
    let key = Key::from("test_key");
    let tp_key = test_tp_key("test_key");
    let invoke_time = Instant::now();
    let complete_time = invoke_time + Duration::from_millis(100);

    let event = |timestamp: Instant, state: KeyState| TelemetryEvent {
        timestamp,
        topic: TEST_TOPIC.into(),
        partition: TEST_PARTITION,
        data: Arc::new(Data::Key(KeyEvent {
            key: key.clone(),
            demand_type,
            state,
        })),
    };
    let class_time = |selector: &Selector| match demand_type {
        DemandType::Normal => selector.success_time.at(complete_time),
        DemandType::Failure => selector.failure_time.at(complete_time),
    };

    selector.process_telemetry(event(invoke_time, KeyState::HandlerInvoked));
    let initial = class_time(&selector);
    selector.process_telemetry(event(complete_time, state));

    ensure!(
        class_time(&selector) > initial,
        "{demand_type:?} class time should increase after handler completion"
    );
    ensure!(
        selector.key_times.get(&tp_key).is_some(),
        "key_times should contain the key after completion"
    );
    Ok(())
}

#[test]
fn telemetry_updates_success_time_and_key_vt() -> Result<()> {
    assert_completion_updates_class_time(DemandType::Normal, KeyState::HandlerSucceeded)
}

#[test]
fn telemetry_updates_failure_time_on_handler_failed() -> Result<()> {
    assert_completion_updates_class_time(DemandType::Failure, KeyState::HandlerFailed)
}

#[test]
fn telemetry_affects_scheduling_decision() -> Result<()> {
    let mut selector = create_selector()?;

    let key1 = Key::from("key1");
    let key2 = Key::from("key2");

    let now = Instant::now();

    // Simulate key1 executing and accumulating VT
    selector.process_telemetry(TelemetryEvent {
        timestamp: now,
        topic: TEST_TOPIC.into(),
        partition: TEST_PARTITION,
        data: Arc::new(Data::Key(KeyEvent {
            key: key1.clone(),
            demand_type: DemandType::Normal,
            state: KeyState::HandlerInvoked,
        })),
    });

    selector.process_telemetry(TelemetryEvent {
        timestamp: now + Duration::from_secs(1),
        topic: TEST_TOPIC.into(),
        partition: TEST_PARTITION,
        data: Arc::new(Data::Key(KeyEvent {
            key: key1,
            demand_type: DemandType::Normal,
            state: KeyState::HandlerSucceeded,
        })),
    });

    // Enqueue tasks for both keys
    selector.enqueue_task(create_task("key1", DemandType::Normal, 0));
    selector.enqueue_task(create_task("key2", DemandType::Normal, 0));

    // key2 should be selected (lower VT)
    let Some(selected) = selector.get_next_task() else {
        bail!("Expected task");
    };
    assert_eq!(
        selected.tp_key.key, key2,
        "key2 should be selected due to lower VT"
    );

    Ok(())
}

// ============================================================================
// Dispatcher event loop
// ============================================================================

#[tokio::test]
async fn dispatcher_returns_permits() -> Result<()> {
    let telemetry = Telemetry::new();
    let config = SchedulerConfiguration::builder()
        .max_concurrency(2)
        .build()?;
    let dispatcher = Dispatcher::new(&config, &telemetry);

    let permit1 = dispatcher
        .get_permit(test_tp_key("key1"), DemandType::Normal)
        .await;
    let permit2 = dispatcher
        .get_permit(test_tp_key("key2"), DemandType::Normal)
        .await;

    assert!(permit1.is_ok(), "Should get first permit");
    assert!(permit2.is_ok(), "Should get second permit");
    Ok(())
}

#[tokio::test]
async fn dispatcher_respects_semaphore_limit() -> Result<()> {
    let telemetry = Telemetry::new();
    let config = SchedulerConfiguration::builder()
        .max_concurrency(1)
        .build()?;
    let dispatcher = Dispatcher::new(&config, &telemetry);

    // Get first permit
    let _permit1 = dispatcher
        .get_permit(test_tp_key("key1"), DemandType::Normal)
        .await?;

    // Try to get second permit (should block since max_concurrency=1)
    let permit2_future = dispatcher.get_permit(test_tp_key("key2"), DemandType::Normal);

    // Use timeout to verify it blocks
    let result = timeout(Duration::from_millis(100), permit2_future).await;

    assert!(
        result.is_err(),
        "Should timeout waiting for second permit when limit is 1"
    );

    Ok(())
}

#[tokio::test]
async fn dispatcher_releases_permits_when_dropped() -> Result<()> {
    let telemetry = Telemetry::new();
    let config = SchedulerConfiguration::builder()
        .max_concurrency(1)
        .build()?;
    let dispatcher = Dispatcher::new(&config, &telemetry);

    // Get and drop first permit
    {
        let _permit1 = dispatcher
            .get_permit(test_tp_key("key1"), DemandType::Normal)
            .await?;
        // permit1 dropped here
    }

    // Should be able to get another permit now
    let permit2 = dispatcher
        .get_permit(test_tp_key("key2"), DemandType::Normal)
        .await;

    assert!(
        permit2.is_ok(),
        "Should get permit after first was released"
    );

    Ok(())
}
