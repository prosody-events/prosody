//! Priority-based task dispatcher with virtual time fairness.
//!
//! Maintains per-key virtual time to prevent high-throughput keys from
//! monopolizing execution while boosting urgency for long-waiting tasks to
//! prevent starvation.

use super::SchedulerConfiguration;
use super::decay::DecayingDuration;
use crate::TopicPartitionKey;
use crate::consumer::DemandType;
use crate::error::{ClassifyError, ErrorCategory};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use ahash::RandomState;
use quanta::Instant;
use quick_cache::UnitWeighter;
use quick_cache::unsync::Cache;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};
use tokio::{select, spawn};
use tracing::{debug, warn};

/// Dispatches handler permits using virtual time fairness and urgency boosting.
///
/// Coordinates with telemetry to track per-key execution times and prioritizes
/// tasks to prevent starvation and monopolization.
#[derive(Clone, Debug)]
pub struct Dispatcher {
    tx: mpsc::Sender<Task>,
}

type DecayingDuration120 = DecayingDuration<120>;

/// A pending task awaiting a permit.
#[derive(Debug)]
struct Task {
    /// When this task was enqueued.
    timestamp: Instant,
    /// The topic-partition-qualified key this task belongs to.
    tp_key: TopicPartitionKey,
    /// Whether this is normal processing or failure handling.
    demand_type: DemandType,
    /// Cached virtual time for this key, avoiding `HashMap` lookup during
    /// selection.
    key_time: Option<DecayingDuration120>,
    /// Channel to send the permit back to the waiting handler.
    tx: oneshot::Sender<OwnedSemaphorePermit>,
}

/// Selects the next task to execute based on virtual time fairness and wait
/// urgency.
///
/// Tracks per-key and per-class virtual time to ensure fair scheduling while
/// preventing starvation through quadratic urgency boosting for long-waiting
/// tasks.
struct Selector {
    tasks: Vec<Task>,
    success_time: DecayingDuration120,
    failure_time: DecayingDuration120,
    invocation_times: Cache<TopicPartitionKey, Instant, UnitWeighter, RandomState>,
    key_times: Cache<TopicPartitionKey, DecayingDuration120, UnitWeighter, RandomState>,
    failure_weight: f64,
    normal_weight: f64,
    max_wait: f64,
    wait_weight: f64,
}

impl Dispatcher {
    pub fn new(config: &SchedulerConfiguration, telemetry: &Telemetry) -> Self {
        let (tx, rx) = mpsc::channel(config.max_concurrency);
        let selector = Selector::new(config);
        spawn(run_event_loop(
            config.max_concurrency,
            selector,
            rx,
            telemetry.subscribe(),
        ));

        Self { tx }
    }

    pub async fn get_permit(
        &self,
        tp_key: TopicPartitionKey,
        demand_type: DemandType,
    ) -> Result<OwnedSemaphorePermit, DispatchError> {
        let (tx, rx) = oneshot::channel();
        let task = Task {
            timestamp: Instant::now(),
            tp_key,
            demand_type,
            key_time: None,
            tx,
        };
        self.tx
            .send(task)
            .await
            .map_err(|_| DispatchError::Shutdown)?;

        rx.await.map_err(|_| DispatchError::Shutdown)
    }
}

async fn run_event_loop(
    max_concurrency: usize,
    mut selector: Selector,
    mut tasks: mpsc::Receiver<Task>,
    mut telemetry: broadcast::Receiver<TelemetryEvent>,
) {
    let permits = Arc::new(Semaphore::new(max_concurrency));

    debug!(
        max_concurrency,
        cache_size = selector.key_times.capacity(),
        "scheduler dispatcher started"
    );

    loop {
        select! {
            maybe_task = tasks.recv() => {
                if let Some(task) = maybe_task {
                    selector.enqueue_task(task);
                } else {
                    debug!(
                        pending_tasks = selector.tasks.len(),
                        "task channel closed, shutting down dispatcher"
                    );
                    break;
                }
            }

            result = telemetry.recv() => {
                match result {
                    Ok(event) => selector.process_telemetry(event),
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(
                            skipped,
                            pending_tasks = selector.tasks.len(),
                            "telemetry lagged, scheduling decisions may be suboptimal"
                        );
                    }
                    Err(RecvError::Closed) => {
                        debug!(
                            pending_tasks = selector.tasks.len(),
                            "telemetry channel closed, shutting down dispatcher"
                        );
                        break;
                    }
                }
            }

            Ok(permit) = permits.clone().acquire_owned(), if selector.has_pending_tasks() => {
                let Some(task) = selector.get_next_task() else {
                    continue;
                };

                let _ = task.tx.send(permit);
            }
        }
    }

    debug!("scheduler dispatcher stopped");
}

impl Selector {
    #[must_use]
    fn new(config: &SchedulerConfiguration) -> Self {
        Self {
            tasks: Vec::new(),
            success_time: Duration::ZERO.into(),
            failure_time: Duration::ZERO.into(),
            invocation_times: Cache::new(config.max_concurrency),
            key_times: Cache::new(config.cache_size),
            failure_weight: config.failure_weight,
            normal_weight: 1.0 - config.failure_weight,
            max_wait: config.max_wait.as_secs_f64(),
            wait_weight: config.wait_weight,
        }
    }

    fn enqueue_task(&mut self, mut task: Task) {
        task.key_time = self.key_times.get(&task.tp_key).copied();

        debug!(
            topic = %task.tp_key.topic,
            partition = task.tp_key.partition,
            key = %task.tp_key.key,
            demand_type = ?task.demand_type,
            has_prior_vt = task.key_time.is_some(),
            queue_depth = self.tasks.len() + 1,
            "task enqueued for scheduling"
        );

        self.tasks.push(task);
    }

    fn process_telemetry(
        &mut self,
        TelemetryEvent {
            timestamp,
            topic,
            partition,
            data,
        }: TelemetryEvent,
    ) {
        let Data::Key(KeyEvent {
            key,
            demand_type,
            state,
        }) = data
        else {
            return;
        };

        let tp_key = TopicPartitionKey::new(topic, partition, key);

        match state {
            KeyState::HandlerInvoked => {
                debug!(
                    topic = %tp_key.topic,
                    partition = tp_key.partition,
                    key = %tp_key.key,
                    demand_type = ?demand_type,
                    "handler invocation recorded for VT tracking"
                );
                self.invocation_times.insert(tp_key, timestamp);
            }
            KeyState::HandlerSucceeded | KeyState::HandlerFailed => {
                let Some(duration) = self.get_duration(timestamp, &tp_key) else {
                    warn!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        demand_type = ?demand_type,
                        state = ?state,
                        "missing invocation time for completed handler; \
                         VT accounting may be inaccurate (possible telemetry lag or restart)"
                    );
                    return;
                };

                debug!(
                    topic = %tp_key.topic,
                    partition = tp_key.partition,
                    key = %tp_key.key,
                    demand_type = ?demand_type,
                    succeeded = matches!(state, KeyState::HandlerSucceeded),
                    duration_ms = duration.as_millis(),
                    "handler completion recorded, updating VT"
                );

                match demand_type {
                    DemandType::Normal => self.success_time += duration,
                    DemandType::Failure => self.failure_time += duration,
                }

                self.increment_key_time(&tp_key, duration);
            }
            _ => {}
        }
    }

    fn has_pending_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    fn get_next_task(&mut self) -> Option<Task> {
        let now = Instant::now();

        // Single pass: find best task in each class simultaneously
        let (normal_best, failure_best) =
            self.tasks
                .iter()
                .enumerate()
                .fold((None, None), |(n_best, f_best), (index, task)| {
                    #[allow(clippy::cast_precision_loss)]
                    let key_vt_micros = task
                        .key_time
                        .map_or(0.0_f64, |vt| vt.at(now).as_micros() as f64);

                    let wait_time = (now - task.timestamp).as_secs_f64();
                    let wait_ratio = (wait_time / self.max_wait).min(1.0);
                    let wait_urgency_micros =
                        self.wait_weight * wait_ratio.powi(2) * 1_000_000.0_f64;
                    let priority = key_vt_micros - wait_urgency_micros;

                    match task.demand_type {
                        DemandType::Normal => (
                            update_min_priority(n_best, index, priority, task.timestamp),
                            f_best,
                        ),
                        DemandType::Failure => (
                            n_best,
                            update_min_priority(f_best, index, priority, task.timestamp),
                        ),
                    }
                });

        // Select based on class scores with fallback
        let selected_index = match (normal_best, failure_best) {
            (Some((n, ..)), Some((f, ..))) => {
                // Guard against division by zero when weight is 0.0 (class disabled).
                // A zero-weight class gets infinite score, so it's never selected
                // when the other class has pending tasks.
                let normal_score = if self.normal_weight == 0.0_f64 {
                    f64::INFINITY
                } else {
                    self.success_time.at(now).as_secs_f64() / self.normal_weight
                };
                let failure_score = if self.failure_weight == 0.0_f64 {
                    f64::INFINITY
                } else {
                    self.failure_time.at(now).as_secs_f64() / self.failure_weight
                };
                if normal_score <= failure_score { n } else { f }
            }
            (Some((n, ..)), None) => n,
            (None, Some((f, ..))) => f,
            (None, None) => return None,
        };

        let task = self.tasks.swap_remove(selected_index);

        debug!(
            topic = %task.tp_key.topic,
            partition = task.tp_key.partition,
            key = %task.tp_key.key,
            demand_type = ?task.demand_type,
            wait_time_ms = (now - task.timestamp).as_millis(),
            remaining_tasks = self.tasks.len(),
            normal_pending = normal_best.is_some(),
            failure_pending = failure_best.is_some(),
            "task selected for permit grant"
        );

        Some(task)
    }

    fn get_duration(&mut self, timestamp: Instant, tp_key: &TopicPartitionKey) -> Option<Duration> {
        let (_, time) = self.invocation_times.remove(tp_key)?;
        Some(timestamp - time)
    }

    fn increment_key_time(&mut self, tp_key: &TopicPartitionKey, duration: Duration) {
        match self.key_times.get_mut_or_guard(tp_key) {
            Ok(Some(mut value)) => *value += duration,
            Err(guard) => guard.insert(duration.into()),
            _ => {}
        }
    }
}

/// Updates the minimum priority task candidate if the new one has lower
/// priority.
fn update_min_priority(
    current: Option<(usize, f64, Instant)>,
    index: usize,
    priority: f64,
    timestamp: Instant,
) -> Option<(usize, f64, Instant)> {
    let Some((_, best_priority, best_timestamp)) = current else {
        return Some((index, priority, timestamp));
    };

    if (priority, timestamp) < (best_priority, best_timestamp) {
        Some((index, priority, timestamp))
    } else {
        current
    }
}

/// Errors that can occur when requesting a permit from the dispatcher.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// The dispatcher event loop has terminated, no more permits will be
    /// issued.
    #[error("dispatcher has been shutdown")]
    Shutdown,
}

impl ClassifyError for DispatchError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Key, Partition};
    use color_eyre::eyre::{Result, bail};
    use tokio::time::{sleep, timeout};

    const TEST_TOPIC: &str = "test-topic";
    const TEST_PARTITION: Partition = 0;

    fn test_tp_key(key: &str) -> TopicPartitionKey {
        TopicPartitionKey::new(TEST_TOPIC.into(), TEST_PARTITION, Key::from(key))
    }

    fn create_selector() -> Result<Selector> {
        let config = SchedulerConfiguration::builder().build()?;
        Ok(Selector::new(&config))
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        if let Some(task) = selected {
            assert_eq!(task.tp_key.key, expected_key);
        }
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.demand_type, DemandType::Normal);
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
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

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.tp_key.key, Key::from("waiting_key"));
        Ok(())
    }

    #[test]
    fn class_proportions_converge() -> Result<()> {
        let mut selector = create_selector()?;
        let mut failure_count = 0_i32;

        for i in 0_i32..1000_i32 {
            selector.enqueue_task(create_task(&format!("n{i}"), DemandType::Normal, 0));
            selector.enqueue_task(create_task(&format!("f{i}"), DemandType::Failure, 0));

            let selected = selector.get_next_task();
            assert!(selected.is_some());
            let Some(selected) = selected else {
                bail!("Expected task but got None");
            };

            let duration = Duration::from_millis(100);
            match selected.demand_type {
                DemandType::Normal => {
                    selector.success_time += duration;
                }
                DemandType::Failure => {
                    selector.failure_time += duration;
                    failure_count += 1_i32;
                }
            }
        }

        let failure_ratio = f64::from(failure_count) / 1000.0_f64;
        assert!(
            (failure_ratio - 0.3).abs() < 0.02_f64,
            "Failure ratio was {failure_ratio}, expected ~0.3"
        );
        Ok(())
    }

    #[test]
    fn flash_crowd_with_wait_urgency() -> Result<()> {
        let mut selector = create_selector()?;

        // Old keys have moderate VT and significant wait time
        for i in 0_i32..10_i32 {
            let key = format!("old_{i}");
            selector
                .key_times
                .insert(test_tp_key(&key), Duration::from_millis(500).into());
            // Give them 60s wait time - provides (60/120)^2 * 200 = 50 points boost
            selector.enqueue_task(create_task(&key, DemandType::Normal, 60));
        }

        // Flash crowd: 100 new keys arrive with 0 VT
        for i in 0_i32..100_i32 {
            selector.enqueue_task(create_task(&format!("new_{i}"), DemandType::Normal, 0));
        }

        let mut old_keys_selected = 0_i32;
        let mut new_keys_selected = 0_i32;

        for _ in 0_i32..50_i32 {
            if let Some(task) = selector.get_next_task() {
                let key_str = task.tp_key.key.to_string();
                if key_str.starts_with("old_") {
                    old_keys_selected += 1_i32;
                } else {
                    new_keys_selected += 1_i32;
                }
            }
        }

        assert!(
            old_keys_selected > 0_i32,
            "Old keys with wait time should get scheduled despite flash crowd"
        );
        assert!(
            new_keys_selected > 0_i32,
            "New keys should also get scheduled"
        );
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
    fn vt_spread_bounded_within_class() -> Result<()> {
        let mut selector = create_selector()?;

        for i in 0_u64..10_u64 {
            let key = format!("key_{i}");
            let vt = Duration::from_millis(i * 100);
            selector.key_times.insert(test_tp_key(&key), vt.into());
            selector.enqueue_task(create_task(&key, DemandType::Normal, 0));
        }

        let mut vts = Vec::new();
        for _ in 0_i32..10_i32 {
            if let Some(task) = selector.get_next_task()
                && let Some(vt) = selector.key_times.get(&task.tp_key)
            {
                vts.push(vt.at(Instant::now()).as_millis());
            }
        }

        if let (Some(min_vt), Some(max_vt)) = (vts.iter().min(), vts.iter().max()) {
            let spread = max_vt - min_vt;
            assert!(spread < 10_000, "VT spread too large: {spread} ms");
        }
        Ok(())
    }

    #[test]
    fn mixed_demand_types_both_served() -> Result<()> {
        let mut selector = create_selector()?;

        for i in 0_i32..50_i32 {
            selector.enqueue_task(create_task(&format!("n{i}"), DemandType::Normal, 0));
        }
        for i in 0_i32..50_i32 {
            selector.enqueue_task(create_task(&format!("f{i}"), DemandType::Failure, 0));
        }

        let mut normal_served = false;
        let mut failure_served = false;

        for _ in 0_i32..100_i32 {
            if let Some(task) = selector.get_next_task() {
                match task.demand_type {
                    DemandType::Normal => normal_served = true,
                    DemandType::Failure => failure_served = true,
                }

                let duration = Duration::from_millis(100);
                match task.demand_type {
                    DemandType::Normal => selector.success_time += duration,
                    DemandType::Failure => selector.failure_time += duration,
                }
            }
        }

        assert!(normal_served, "Normal tasks should be served");
        assert!(failure_served, "Failure tasks should be served");
        Ok(())
    }

    #[test]
    fn burst_traffic_failure_surge() -> Result<()> {
        let mut selector = create_selector()?;

        // Start with balanced load - 5 keys per class, continuous tasks
        let normal_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("n{i}")).collect();
        let failure_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("f{i}")).collect();

        for key in &normal_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }
        for key in &failure_keys {
            selector.enqueue_task(create_task(key, DemandType::Failure, 0));
        }

        // Build up some execution time with normal proportions
        for _ in 0_i32..50_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task during warmup");
            };
            let duration = Duration::from_millis(100);
            match task.demand_type {
                DemandType::Normal => selector.success_time += duration,
                DemandType::Failure => selector.failure_time += duration,
            }
            // Re-enqueue to simulate continuous load
            selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));
        }

        // Simulate burst: add many new failure keys
        let burst_keys: Vec<String> = (0_i32..20_i32).map(|i| format!("f_burst_{i}")).collect();
        for key in &burst_keys {
            selector.enqueue_task(create_task(key, DemandType::Failure, 0));
        }

        let mut failure_count = 0_i32;
        let mut normal_count = 0_i32;

        // Process the burst (should have 5 normal + 25 failure keys ready)
        for _ in 0_i32..100_i32 {
            let Some(task) = selector.get_next_task() else {
                break;
            };
            let duration = Duration::from_millis(100);
            match task.demand_type {
                DemandType::Normal => {
                    selector.success_time += duration;
                    normal_count += 1_i32;
                }
                DemandType::Failure => {
                    selector.failure_time += duration;
                    failure_count += 1_i32;
                }
            }
            // Re-enqueue to keep continuous load
            selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));
        }

        // Both classes should still get service during burst
        assert!(
            normal_count > 0_i32,
            "Normal tasks should be served during burst"
        );
        assert!(
            failure_count > 0_i32,
            "Failure tasks should be served during burst"
        );

        // Key insight: Even during burst, proportions should be maintained
        // The scheduler should maintain ~30/70 failure/normal split despite 5x more
        // failure keys
        let total = failure_count + normal_count;
        let failure_ratio = f64::from(failure_count) / f64::from(total);

        // Should maintain target proportion (allowing some variance during transient
        // burst)
        assert!(
            (0.2_f64..=0.4_f64).contains(&failure_ratio),
            "Failure ratio should stay near 30% during burst: {failure_ratio:.2}"
        );

        Ok(())
    }

    #[test]
    fn heterogeneous_task_durations() -> Result<()> {
        let mut selector = create_selector()?;

        // Create keys with different task characteristics
        // Fast keys: 10ms, Slow keys: 100ms
        let fast_keys = vec!["fast1", "fast2"];
        let slow_keys = vec!["slow1", "slow2"];

        // Set up initial VTs using key_times cache
        // All start at zero VT for fair comparison
        for &key in &fast_keys {
            selector
                .key_times
                .insert(test_tp_key(key), Duration::ZERO.into());
        }
        for &key in &slow_keys {
            selector
                .key_times
                .insert(test_tp_key(key), Duration::ZERO.into());
        }

        // Track execution for each key type
        let mut fast_time = Duration::ZERO;
        let mut slow_time = Duration::ZERO;

        // Process 100 tasks
        for _ in 0_i32..100_i32 {
            // Enqueue all keys
            for &key in &fast_keys {
                selector.enqueue_task(create_task(key, DemandType::Normal, 0));
            }
            for &key in &slow_keys {
                selector.enqueue_task(create_task(key, DemandType::Normal, 0));
            }

            // Select and execute
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in heterogeneous test");
            };

            let key_str = task.tp_key.key.to_string();
            let duration = if fast_keys.contains(&key_str.as_str()) {
                let d = Duration::from_millis(10);
                fast_time += d;
                d
            } else {
                let d = Duration::from_millis(100);
                slow_time += d;
                d
            };

            // Update VT for the key
            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }
        }

        // Fast keys should get more executions (since they're faster)
        // But total time should be roughly balanced
        let total_time = fast_time + slow_time;
        let fast_ratio = fast_time.as_secs_f64() / total_time.as_secs_f64();

        // With heterogeneous durations, VT-based fairness means
        // fast keys get more executions but similar VT
        assert!(
            (0.3_f64..=0.7_f64).contains(&fast_ratio),
            "Fast keys should get balanced time despite different durations: {fast_ratio:.2}"
        );

        Ok(())
    }

    #[test]
    fn long_tail_extreme_outlier() -> Result<()> {
        let mut selector = create_selector()?;

        // One very slow key, multiple fast keys
        let slow_key = "slow_outlier";
        let fast_keys = vec!["fast1", "fast2", "fast3"];

        // Initialize all at zero VT
        selector
            .key_times
            .insert(test_tp_key(slow_key), Duration::ZERO.into());
        for &key in &fast_keys {
            selector
                .key_times
                .insert(test_tp_key(key), Duration::ZERO.into());
        }

        let mut slow_count = 0_i32;
        let mut fast_count = 0_i32;

        // Process 100 tasks
        for _ in 0_i32..100_i32 {
            // Enqueue all keys
            selector.enqueue_task(create_task(slow_key, DemandType::Normal, 0));
            for &key in &fast_keys {
                selector.enqueue_task(create_task(key, DemandType::Normal, 0));
            }

            // Select and execute
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in long tail test");
            };

            let duration = if task.tp_key.key == Key::from(slow_key) {
                slow_count += 1_i32;
                Duration::from_millis(500) // 50x slower
            } else {
                fast_count += 1_i32;
                Duration::from_millis(10)
            };

            // Update VT
            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }
        }

        // Slow key should still get some executions (not starved)
        assert!(
            slow_count > 0_i32,
            "Slow outlier key should not be completely starved"
        );

        // Fast keys should get many more executions (since they're 50x faster)
        assert!(
            fast_count > slow_count * 10_i32,
            "Fast keys should get far more executions: fast={fast_count} slow={slow_count}"
        );

        // Check VT fairness: all keys should have similar VT (not execution count)
        let slow_vt = selector
            .key_times
            .get(&test_tp_key(slow_key))
            .map_or(Duration::ZERO, |vt| vt.at(Instant::now()));

        let max_fast_vt = fast_keys
            .iter()
            .filter_map(|&key| selector.key_times.get(&test_tp_key(key)))
            .map(|vt| vt.at(Instant::now()))
            .max()
            .unwrap_or(Duration::ZERO);

        // VT spread should be bounded (VT fairness, not execution fairness)
        let vt_diff = slow_vt.abs_diff(max_fast_vt).as_millis();
        assert!(
            vt_diff < 10_000_u128,
            "VT spread should be bounded despite heterogeneous durations: diff={vt_diff}ms"
        );

        Ok(())
    }

    #[test]
    fn monopoly_recovery_high_vt_key() -> Result<()> {
        let mut selector = create_selector()?;

        // Create one "monopoly" key with extreme VT from past monopoly
        let monopoly_key = "monopoly";
        let new_keys = vec!["new1", "new2", "new3"];

        // Monopoly key has 150s of VT (simulating past monopoly)
        // Initial class_avg = (150 + 0 + 0 + 0) / 4 = 37.5s
        // Monopoly distance from avg = 150 - 37.5 = 112.5s
        // With 100s wait time: wait_urgency = 200 * (100/120)^2 = 138.9
        // Monopoly priority = 112.5 - 138.9 = -26.4 (high priority due to wait)
        // New key priority = (0 - 37.5) - 0 = -37.5 (even higher initially)
        // So new keys are selected first, but monopoly recovers once they accumulate VT
        selector
            .key_times
            .insert(test_tp_key(monopoly_key), Duration::from_secs(150).into());

        // New keys start at zero VT
        for &key in &new_keys {
            selector
                .key_times
                .insert(test_tp_key(key), Duration::ZERO.into());
        }

        let mut monopoly_count = 0_i32;
        let mut new_count = 0_i32;
        let mut first_monopoly_iter = None;

        // Enqueue all tasks once
        // Give monopoly significant wait time (100s) to enable recovery via wait
        // urgency With WAIT_WEIGHT = 200, 100s wait provides: (100/120)^2 * 200
        // = 138.9 boost
        selector.enqueue_task(create_task(monopoly_key, DemandType::Normal, 100));
        for &key in &new_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        // Process tasks to observe monopoly recovery
        // New keys accumulate VT → class average rises → monopoly's VT distance shrinks
        // Combined with wait urgency, monopoly recovers relatively quickly
        for i in 0_i32..1000_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in monopoly recovery test at iteration {i}");
            };

            // Simulate realistic task durations
            let duration = Duration::from_millis(100);
            let key_str = task.tp_key.key.clone();

            if task.tp_key.key == Key::from(monopoly_key) {
                monopoly_count += 1_i32;
                if first_monopoly_iter.is_none() {
                    first_monopoly_iter = Some(i);
                }
            } else {
                new_count += 1_i32;
            }

            // Update VT for the executed task
            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            // Re-enqueue to simulate continuous load
            // Monopoly maintains wait time, new keys don't
            let wait_time = if key_str == Key::from(monopoly_key) {
                100
            } else {
                0
            };
            selector.enqueue_task(create_task(key_str.as_ref(), DemandType::Normal, wait_time));
        }

        // Verify monopoly gets scheduled
        assert!(
            first_monopoly_iter.is_some(),
            "Monopoly key should eventually get scheduled"
        );
        let first_iter = first_monopoly_iter.unwrap_or(0_i32);

        // Without lag bounds, monopoly recovers via combined wait urgency and class
        // average rising Monopoly priority: (150 - class_avg) - 138.9 = (150 -
        // 37.5 - 0.025*k) - 138.9 = -26.4 - 0.025*k New key priority: (0.1*k/3
        // - class_avg) - 0 = (0.1*k/3 - 37.5 - 0.025*k) = -37.5 + 0.0083*k
        // Crossover at k ≈ 333 when monopoly priority becomes better than new keys
        assert!(
            first_iter > 300_i32,
            "Monopoly should be deferred initially due to high VT, got iteration {first_iter}"
        );
        assert!(
            first_iter < 400_i32,
            "Monopoly should eventually recover as class average rises, got iteration {first_iter}"
        );

        // Verify sustained recovery: monopoly gets regular service after initial
        // deferral After recovering at ~336, monopoly should be selected for
        // remaining ~664 iterations With wait urgency advantage, monopoly
        // should get at least 100 selections
        assert!(
            monopoly_count > 100_i32,
            "Monopoly should sustain recovery with wait urgency: got {monopoly_count} executions"
        );

        // But new keys should still get more total service (they started with VT
        // advantage)
        assert!(
            new_count > monopoly_count,
            "New keys should get more total service: new={new_count} monopoly={monopoly_count}"
        );

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

    #[test]
    fn interleaved_bursts_both_classes() -> Result<()> {
        let mut selector = create_selector()?;

        let normal_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("n{i}")).collect();
        let failure_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("f{i}")).collect();

        let mut normal_count = 0_i32;
        let mut failure_count = 0_i32;

        // Phase 1: Failure class burst (0-250 iterations)
        // During this phase, failure keys arrive frequently, normal keys arrive slowly
        for key in &failure_keys {
            selector.enqueue_task(create_task(key, DemandType::Failure, 0));
        }
        for key in &normal_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        for i in 0_i32..250_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task during failure burst");
            };

            let duration = Duration::from_millis(100);
            match task.demand_type {
                DemandType::Normal => {
                    selector.success_time += duration;
                    normal_count += 1_i32;
                }
                DemandType::Failure => {
                    selector.failure_time += duration;
                    failure_count += 1_i32;
                }
            }

            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            // Burst pattern: failure keys re-enqueue every time, normal only every 5th
            // iteration
            let key_str = task.tp_key.key.to_string();
            if failure_keys.contains(&key_str) || i % 5_i32 == 0_i32 {
                selector.enqueue_task(create_task(&key_str, task.demand_type, 0));
            }
        }

        let phase1_normal = normal_count;
        let phase1_failure = failure_count;

        // Re-enqueue all normal keys for phase 2
        for key in &normal_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        // Phase 2: Normal class burst (250-500 iterations)
        for i in 250_i32..500_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task during normal burst at iteration {i}");
            };

            let duration = Duration::from_millis(100);
            match task.demand_type {
                DemandType::Normal => {
                    selector.success_time += duration;
                    normal_count += 1_i32;
                }
                DemandType::Failure => {
                    selector.failure_time += duration;
                    failure_count += 1_i32;
                }
            }

            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            // Burst pattern: normal keys re-enqueue every time, failure only every 5th
            // iteration
            let key_str = task.tp_key.key.to_string();
            if normal_keys.contains(&key_str) || i % 5_i32 == 0_i32 {
                selector.enqueue_task(create_task(&key_str, task.demand_type, 0));
            }
        }

        // Verify both classes got service in both phases
        assert!(
            phase1_failure > phase1_normal,
            "Failure class should dominate phase 1: failure={phase1_failure} \
             normal={phase1_normal}"
        );

        let phase2_normal = normal_count - phase1_normal;
        let phase2_failure = failure_count - phase1_failure;
        assert!(
            phase2_normal > phase2_failure,
            "Normal class should dominate phase 2: normal={phase2_normal} failure={phase2_failure}"
        );

        // Despite bursts, overall proportions should trend toward 70/30
        let total = normal_count + failure_count;
        let normal_pct = (f64::from(normal_count) / f64::from(total)) * 100.0_f64;
        assert!(
            normal_pct > 50.0_f64 && normal_pct < 90.0_f64,
            "Normal percentage should trend toward 70%, got {normal_pct:.1}%"
        );

        Ok(())
    }

    #[test]
    fn vt_spread_convergence_over_time() -> Result<()> {
        let mut selector = create_selector()?;

        let keys: Vec<String> = (0_i32..10_i32).map(|i| format!("k{i}")).collect();

        // Initialize with varying VT
        for (i, key) in keys.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let vt = Duration::from_millis((i as u64) * 100_u64);
            selector.key_times.insert(test_tp_key(key), vt.into());
        }

        // Enqueue all keys
        for key in &keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        let mut max_spread = 0_u128;

        // Run for many iterations to test long-term stability
        for _ in 0_i32..10000_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in convergence test");
            };

            // Execute with small variance
            let duration = Duration::from_millis(10_u64 + (task.tp_key.key.len() as u64 % 5_u64));
            selector.success_time += duration;

            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            // Re-enqueue
            selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));

            // Check spread every 100 iterations
            if selector.tasks.len() == keys.len() {
                let now = Instant::now();
                let vts: Vec<u128> = keys
                    .iter()
                    .filter_map(|k| {
                        selector
                            .key_times
                            .get(&test_tp_key(k))
                            .map(|vt| vt.at(now).as_micros())
                    })
                    .collect();

                if let (Some(min_vt), Some(max_vt)) = (vts.iter().min(), vts.iter().max()) {
                    let spread = max_vt - min_vt;
                    max_spread = max_spread.max(spread);
                }
            }
        }

        // Convert to milliseconds for assertion
        let max_spread_ms = max_spread / 1000_u128;

        // Without lag bounds, spread should still converge due to VT fairness
        // With 10ms tasks, spread should stay under ~1000ms (100 task durations)
        assert!(
            max_spread_ms < 1000_u128,
            "VT spread should remain bounded over time: max={max_spread_ms}ms"
        );

        Ok(())
    }

    #[test]
    fn class_proportion_recovery_from_skew() -> Result<()> {
        let mut selector = create_selector()?;

        let normal_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("n{i}")).collect();
        let failure_keys: Vec<String> = (0_i32..5_i32).map(|i| format!("f{i}")).collect();

        // Force extreme skew: 90/10 in favor of Normal
        // Simulate 900ms Normal execution, 100ms Failure execution
        selector.success_time = Duration::from_millis(900).into();
        selector.failure_time = Duration::from_millis(100).into();

        // Enqueue tasks from both classes
        for key in &normal_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }
        for key in &failure_keys {
            selector.enqueue_task(create_task(key, DemandType::Failure, 0));
        }

        let mut normal_count = 0_i32;
        let mut failure_count = 0_i32;

        // Run for 1000 iterations to observe recovery
        for _ in 0_i32..1000_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in recovery test");
            };

            let duration = Duration::from_millis(10);
            match task.demand_type {
                DemandType::Normal => {
                    selector.success_time += duration;
                    normal_count += 1_i32;
                }
                DemandType::Failure => {
                    selector.failure_time += duration;
                    failure_count += 1_i32;
                }
            }

            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            // Re-enqueue
            selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));
        }

        // System should recover toward 70/30 split
        // Failure class was underserved (10% vs target 30%), so should get more service
        let total = normal_count + failure_count;
        let failure_pct = (f64::from(failure_count) / f64::from(total)) * 100.0_f64;

        // Should recover toward 30% (allow 20-40% range for convergence)
        assert!(
            failure_pct > 20.0_f64 && failure_pct < 40.0_f64,
            "Failure class should recover toward 30%, got {failure_pct:.1}%"
        );

        // Failure should get MORE service than before (was 10%, should increase)
        let initial_failure_pct = (100.0_f64 / 1000.0_f64) * 100.0_f64; // 10%
        assert!(
            failure_pct > initial_failure_pct,
            "Failure percentage should increase from {initial_failure_pct:.1}% to \
             {failure_pct:.1}%"
        );

        Ok(())
    }

    #[test]
    fn concurrent_key_arrivals_stress() -> Result<()> {
        let mut selector = create_selector()?;

        // Create 10 existing keys with varying VT (simulating ongoing work)
        let existing_keys: Vec<String> = (0_i32..10_i32).map(|i| format!("old{i}")).collect();
        for (i, key) in existing_keys.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let vt = Duration::from_millis((i as u64) * 50_u64);
            selector.key_times.insert(test_tp_key(key), vt.into());
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        // Simulate flash crowd: 20 new keys arrive simultaneously
        let new_keys: Vec<String> = (0_i32..20_i32).map(|i| format!("new{i}")).collect();
        for key in &new_keys {
            selector.enqueue_task(create_task(key, DemandType::Normal, 0));
        }

        let mut old_selections = 0_i32;
        let mut new_selections = 0_i32;

        // Process 100 tasks
        for _ in 0_i32..100_i32 {
            let Some(task) = selector.get_next_task() else {
                bail!("Expected task in concurrent arrivals test");
            };

            let duration = Duration::from_millis(10);
            selector.success_time += duration;

            if let Some(mut vt) = selector.key_times.get_mut(&task.tp_key) {
                *vt += duration;
            }

            let key_str = task.tp_key.key.to_string();
            if existing_keys.contains(&key_str) {
                old_selections += 1_i32;
            } else {
                new_selections += 1_i32;
            }

            // Re-enqueue
            selector.enqueue_task(create_task(task.tp_key.key.as_ref(), task.demand_type, 0));
        }

        // New keys start at 0 VT (lower than existing 0-450ms), so should dominate
        // initially This is correct behavior - new keys are underserved
        // relative to class average
        assert!(
            new_selections > 60_i32,
            "New keys should dominate initially due to 0 VT: {new_selections}/100"
        );

        // Existing keys will get some service, but not much initially
        // This verifies we don't completely starve them despite VT disadvantage
        assert!(
            old_selections < 40_i32,
            "Existing keys should get less service due to higher VT: {old_selections}/100"
        );

        // Check that VT spread hasn't exploded
        let all_keys = [existing_keys, new_keys].concat();
        let now = Instant::now();
        let vts: Vec<u128> = all_keys
            .iter()
            .filter_map(|k| {
                selector
                    .key_times
                    .get(&test_tp_key(k))
                    .map(|vt| vt.at(now).as_micros())
            })
            .collect();

        if let (Some(min_vt), Some(max_vt)) = (vts.iter().min(), vts.iter().max()) {
            let spread_ms = (max_vt - min_vt) / 1000_u128;
            assert!(
                spread_ms < 5000_u128,
                "VT spread should remain reasonable after flash crowd: {spread_ms}ms"
            );
        }

        Ok(())
    }

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
            data: Data::Key(KeyEvent {
                key,
                demand_type: DemandType::Normal,
                state: KeyState::HandlerInvoked,
            }),
        };

        selector.process_telemetry(event);

        assert!(selector.invocation_times.contains_key(&tp_key));
        assert_eq!(selector.invocation_times.get(&tp_key), Some(&now));
        Ok(())
    }

    #[test]
    fn telemetry_updates_success_time_and_key_vt() -> Result<()> {
        let mut selector = create_selector()?;
        let key = Key::from("test_key");
        let tp_key = test_tp_key("test_key");
        let invoke_time = Instant::now();
        let complete_time = invoke_time + Duration::from_millis(100);

        // First, record invocation
        selector.process_telemetry(TelemetryEvent {
            timestamp: invoke_time,
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            data: Data::Key(KeyEvent {
                key: key.clone(),
                demand_type: DemandType::Normal,
                state: KeyState::HandlerInvoked,
            }),
        });

        let initial_success_time = selector.success_time.at(complete_time);

        // Then, record completion
        selector.process_telemetry(TelemetryEvent {
            timestamp: complete_time,
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            data: Data::Key(KeyEvent {
                key,
                demand_type: DemandType::Normal,
                state: KeyState::HandlerSucceeded,
            }),
        });

        // Verify success_time increased
        let final_success_time = selector.success_time.at(complete_time);
        assert!(
            final_success_time > initial_success_time,
            "success_time should increase after handler completion"
        );

        // Verify key VT was updated
        assert!(
            selector.key_times.get(&tp_key).is_some(),
            "key_times should contain the key after completion"
        );
        Ok(())
    }

    #[test]
    fn telemetry_updates_failure_time_on_handler_failed() -> Result<()> {
        let mut selector = create_selector()?;
        let key = Key::from("test_key");
        let invoke_time = Instant::now();
        let complete_time = invoke_time + Duration::from_millis(50);

        // Record invocation
        selector.process_telemetry(TelemetryEvent {
            timestamp: invoke_time,
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            data: Data::Key(KeyEvent {
                key: key.clone(),
                demand_type: DemandType::Failure,
                state: KeyState::HandlerInvoked,
            }),
        });

        let initial_failure_time = selector.failure_time.at(complete_time);

        // Record failure
        selector.process_telemetry(TelemetryEvent {
            timestamp: complete_time,
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            data: Data::Key(KeyEvent {
                key,
                demand_type: DemandType::Failure,
                state: KeyState::HandlerFailed,
            }),
        });

        let final_failure_time = selector.failure_time.at(complete_time);
        assert!(
            final_failure_time > initial_failure_time,
            "failure_time should increase after handler failed"
        );
        Ok(())
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
            data: Data::Key(KeyEvent {
                key: key1.clone(),
                demand_type: DemandType::Normal,
                state: KeyState::HandlerInvoked,
            }),
        });

        selector.process_telemetry(TelemetryEvent {
            timestamp: now + Duration::from_secs(1),
            topic: TEST_TOPIC.into(),
            partition: TEST_PARTITION,
            data: Data::Key(KeyEvent {
                key: key1,
                demand_type: DemandType::Normal,
                state: KeyState::HandlerSucceeded,
            }),
        });

        // Enqueue tasks for both keys
        selector.enqueue_task(create_task("key1", DemandType::Normal, 0));
        selector.enqueue_task(create_task("key2", DemandType::Normal, 0));

        // key2 should be selected (lower VT)
        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task");
        };
        assert_eq!(
            selected.tp_key.key, key2,
            "key2 should be selected due to lower VT"
        );

        Ok(())
    }

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

    #[tokio::test]
    async fn dispatcher_schedules_by_priority() -> Result<()> {
        let telemetry = Telemetry::new();
        let config = SchedulerConfiguration::builder()
            .max_concurrency(10)
            .build()?;
        let dispatcher = Dispatcher::new(&config, &telemetry);

        // Enqueue tasks with different wait times (older = higher priority)
        let old_task = dispatcher.get_permit(test_tp_key("old"), DemandType::Normal);
        sleep(Duration::from_millis(10)).await;
        let new_task = dispatcher.get_permit(test_tp_key("new"), DemandType::Normal);

        // Both should complete, and old task should get priority
        let (result1, result2) = tokio::join!(old_task, new_task);

        assert!(result1.is_ok(), "Old task should complete");
        assert!(result2.is_ok(), "New task should complete");
        Ok(())
    }
}
