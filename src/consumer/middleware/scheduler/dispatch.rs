#![allow(
    dead_code,
    missing_docs,
    unused_variables,
    clippy::missing_errors_doc,
    clippy::same_name_method
)] //todo: remove

use super::decay::DecayingDuration;
use crate::Key;
use crate::consumer::DemandType;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};
use ahash::RandomState;
use quanta::Instant;
use quick_cache::unsync::Cache;
use soa_derive::{StructOfArray, soa_zip};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};
use tokio::{select, spawn};
use tracing::warn;

#[derive(Clone, Debug)]
pub struct Dispatcher {
    tx: mpsc::Sender<Task>,
}

type DecayingDuration120 = DecayingDuration<120>;

const NORMAL_WEIGHT: f64 = 0.7;
const FAILURE_WEIGHT: f64 = 0.3;

const MAX_WAIT_SECS: f64 = 120.0;
const WAIT_WEIGHT: f64 = 200.0;
const FAIRNESS_WEIGHT: f64 = 1.0;

#[derive(Debug, StructOfArray)]
#[soa_derive(Debug)]
#[allow(clippy::multiple_inherent_impl)]
struct Task {
    timestamp: Instant,
    key: Key,
    demand_type: DemandType,
    tx: oneshot::Sender<OwnedSemaphorePermit>,
}

struct Selector {
    tasks: TaskVec,
    invocation_times: HashMap<Key, Instant, RandomState>,
    success_time: DecayingDuration120,
    failure_time: DecayingDuration120,
    key_times: Cache<Key, DecayingDuration120>,
}

impl Dispatcher {
    pub fn new(max_permits: usize, telemetry: &Telemetry) -> Self {
        let (tx, rx) = mpsc::channel(max_permits);
        let selector = Selector::new();
        spawn(run_event_loop(
            max_permits,
            selector,
            rx,
            telemetry.subscribe(),
        ));

        Self { tx }
    }

    pub async fn get_permit(
        &self,
        key: Key,
        demand_type: DemandType,
    ) -> Result<OwnedSemaphorePermit, DispatchError> {
        let (tx, rx) = oneshot::channel();
        let task = Task {
            timestamp: Instant::now(),
            key,
            demand_type,
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
    max_permits: usize,
    mut selector: Selector,
    mut tasks: mpsc::Receiver<Task>,
    mut telemetry: broadcast::Receiver<TelemetryEvent>,
) {
    let permits = Arc::new(Semaphore::new(max_permits));

    loop {
        select! {
            maybe_task = tasks.recv() => {
                match maybe_task {
                    Some(task) => selector.enqueue_task(task),
                    None => break,
                }
            }

            result = telemetry.recv() => {
                match result {
                    Ok(event) => selector.process_telemetry(event),
                    Err(RecvError::Lagged(skipped)) => {
                        warn!("telemetry lagged by {skipped} events");
                    }
                    Err(RecvError::Closed) => break,
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
}

impl Selector {
    fn new() -> Self {
        Self {
            tasks: TaskVec::default(),
            invocation_times: HashMap::default(),
            success_time: Duration::ZERO.into(),
            failure_time: Duration::ZERO.into(),
            key_times: Cache::new(8_192),
        }
    }

    fn enqueue_task(&mut self, task: Task) {
        self.tasks.push(task);
    }

    fn process_telemetry(
        &mut self,
        TelemetryEvent {
            timestamp, data, ..
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

        match state {
            KeyState::HandlerInvoked => {
                self.invocation_times.insert(key, timestamp);
            }
            KeyState::HandlerSucceeded | KeyState::HandlerFailed => {
                let Some(duration) = self.get_duration(timestamp, &key) else {
                    warn!("failed to get execution duration for key {key}");
                    return;
                };

                match demand_type {
                    DemandType::Normal => self.success_time += duration,
                    DemandType::Failure => self.failure_time += duration,
                }

                self.increment_key_time(&key, duration);
            }
            _ => {}
        }
    }

    fn has_pending_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    fn get_next_task(&mut self) -> Option<Task> {
        if self.tasks.is_empty() {
            return None;
        }

        let now = Instant::now();

        // Collect task metadata and compute class statistics in one pass
        let task_data: Vec<_> = soa_zip!(&self.tasks, [timestamp, key, demand_type])
            .enumerate()
            .map(|(index, (timestamp, key, demand_type))| {
                let key_vt = self
                    .key_times
                    .get(key)
                    .map_or(Duration::ZERO, |vt| vt.at(now));

                #[allow(clippy::cast_precision_loss)]
                let key_vt_micros = key_vt.as_micros() as f64;

                (index, key_vt_micros, *timestamp, *demand_type)
            })
            .collect();

        // Calculate class averages
        let (normal_count, normal_sum, failure_count, failure_sum) = task_data.iter().fold(
            (0_i32, 0.0_f64, 0_i32, 0.0_f64),
            |acc, &(_, vt, _, demand_type)| match demand_type {
                DemandType::Normal => (acc.0 + 1_i32, acc.1 + vt, acc.2, acc.3),
                DemandType::Failure => (acc.0, acc.1, acc.2 + 1_i32, acc.3 + vt),
            },
        );

        if normal_count == 0_i32 && failure_count == 0_i32 {
            return None;
        }

        let normal_avg_vt = if normal_count > 0_i32 {
            #[allow(clippy::cast_precision_loss)]
            {
                normal_sum / f64::from(normal_count)
            }
        } else {
            0.0_f64
        };

        let failure_avg_vt = if failure_count > 0_i32 {
            #[allow(clippy::cast_precision_loss)]
            {
                failure_sum / f64::from(failure_count)
            }
        } else {
            0.0_f64
        };

        // Find best task from each class
        let normal_best =
            Self::select_best_from_class(&task_data, DemandType::Normal, normal_avg_vt, now);

        let failure_best =
            Self::select_best_from_class(&task_data, DemandType::Failure, failure_avg_vt, now);

        // Decide which class to serve
        let selected_index = match (normal_best, failure_best) {
            (Some(n), Some(f)) => {
                // Both classes have eligible tasks - compare class scores
                let normal_score = self.success_time.at(now).as_secs_f64() / NORMAL_WEIGHT;
                let failure_score = self.failure_time.at(now).as_secs_f64() / FAILURE_WEIGHT;
                Some(if normal_score <= failure_score { n } else { f })
            }
            (Some(n), None) => Some(n),
            (None, Some(f)) => Some(f),
            (None, None) => None,
        }?;

        Some(self.tasks.swap_remove(selected_index))
    }

    /// Selects the best task from a specific demand class.
    ///
    /// Returns the index of the best task, or `None` if no eligible tasks
    /// exist.
    #[allow(clippy::cast_precision_loss)]
    fn select_best_from_class(
        task_data: &[(usize, f64, Instant, DemandType)],
        target_class: DemandType,
        class_avg_vt: f64,
        now: Instant,
    ) -> Option<usize> {
        task_data
            .iter()
            .filter(|(_, _, _, demand_type)| *demand_type == target_class)
            .map(|(index, key_vt_micros, timestamp, _)| {
                let vt_distance_micros = key_vt_micros - class_avg_vt;
                let vt_distance_secs = vt_distance_micros / 1_000_000.0_f64;
                let wait_time = (now - *timestamp).as_secs_f64();
                let wait_ratio = (wait_time / MAX_WAIT_SECS).min(1.0);
                let priority =
                    vt_distance_secs * FAIRNESS_WEIGHT - WAIT_WEIGHT * wait_ratio * wait_ratio;

                (*index, priority, *timestamp)
            })
            .min_by(|(_, p1, t1), (_, p2, t2)| {
                p1.partial_cmp(p2)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| t1.cmp(t2))
            })
            .map(|(index, ..)| index)
    }

    fn get_duration(&mut self, timestamp: Instant, key: &Key) -> Option<Duration> {
        let time = self.invocation_times.remove(key)?;
        Some(timestamp - time)
    }

    fn increment_key_time(&mut self, key: &Key, duration: Duration) {
        match self.key_times.get_mut_or_guard(key) {
            Ok(Some(mut value)) => *value += duration,
            Err(guard) => guard.insert(duration.into()),
            _ => {}
        }
    }
}

#[derive(Debug, Error)]
pub enum DispatchError {
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
    use color_eyre::eyre::{Result, bail};

    fn create_selector() -> Selector {
        Selector::new()
    }

    fn create_task(key: &str, demand_type: DemandType, age_secs: u64) -> Task {
        let (tx, _rx) = oneshot::channel();
        Task {
            timestamp: Instant::now() - Duration::from_secs(age_secs),
            key: Key::from(key),
            demand_type,
            tx,
        }
    }

    #[test]
    fn empty_queue_returns_none() {
        let mut selector = create_selector();
        assert!(selector.get_next_task().is_none());
    }

    #[test]
    fn single_task_returns_that_task() {
        let mut selector = create_selector();
        let task = create_task("key1", DemandType::Normal, 0);
        let expected_key = task.key.clone();
        selector.enqueue_task(task);

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        if let Some(task) = selected {
            assert_eq!(task.key, expected_key);
        }
    }

    #[test]
    fn only_normal_tasks_selects_normal() -> Result<()> {
        let mut selector = create_selector();
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
        let mut selector = create_selector();
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
        let mut selector = create_selector();

        selector
            .key_times
            .insert(Key::from("high_vt"), Duration::from_secs(10).into());
        selector
            .key_times
            .insert(Key::from("low_vt"), Duration::from_millis(100).into());

        selector.enqueue_task(create_task("high_vt", DemandType::Normal, 0));
        selector.enqueue_task(create_task("low_vt", DemandType::Normal, 0));

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.key, Key::from("low_vt"));
        Ok(())
    }

    #[test]
    fn fifo_tiebreaking_when_vt_equal() -> Result<()> {
        let mut selector = create_selector();

        selector.enqueue_task(create_task("key1", DemandType::Normal, 2));
        selector.enqueue_task(create_task("key2", DemandType::Normal, 1));

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.key, Key::from("key1"));
        Ok(())
    }

    #[test]
    fn new_key_starts_at_zero_vt() -> Result<()> {
        let mut selector = create_selector();

        selector
            .key_times
            .insert(Key::from("old_key"), Duration::from_secs(5).into());

        selector.enqueue_task(create_task("new_key", DemandType::Normal, 0));
        selector.enqueue_task(create_task("old_key", DemandType::Normal, 0));

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.key, Key::from("new_key"));
        Ok(())
    }

    #[test]
    fn underserved_class_wins() -> Result<()> {
        let mut selector = create_selector();

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
        let mut selector = create_selector();

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
        let mut selector = create_selector();

        selector
            .key_times
            .insert(Key::from("low_vt"), Duration::from_millis(10).into());
        selector
            .key_times
            .insert(Key::from("high_vt"), Duration::from_secs(5).into());

        selector.enqueue_task(create_task("low_vt", DemandType::Normal, 0));
        // With WAIT_WEIGHT = 200, need 120s wait to get max boost of 200 points
        // VT difference is ~5s = 5 points, so 120s wait easily overcomes it
        selector.enqueue_task(create_task("high_vt", DemandType::Normal, 120));

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.key, Key::from("high_vt"));
        Ok(())
    }

    #[test]
    fn extreme_wait_guarantees_selection() -> Result<()> {
        let mut selector = create_selector();

        selector.enqueue_task(create_task("waiting_key", DemandType::Normal, 10));

        for i in 0_i32..100_i32 {
            let key = format!("new_{i}");
            selector
                .key_times
                .insert(Key::from(key.as_str()), Duration::from_millis(1).into());
            selector.enqueue_task(create_task(&key, DemandType::Normal, 0));
        }

        let selected = selector.get_next_task();
        assert!(selected.is_some());
        let Some(selected) = selected else {
            bail!("Expected task but got None");
        };
        assert_eq!(selected.key, Key::from("waiting_key"));
        Ok(())
    }

    #[test]
    fn class_proportions_converge() -> Result<()> {
        let mut selector = create_selector();
        let mut normal_count = 0_i32;
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
                    normal_count += 1_i32;
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
    fn flash_crowd_with_wait_urgency() {
        let mut selector = create_selector();

        // Old keys have moderate VT and significant wait time
        for i in 0_i32..10_i32 {
            let key = format!("old_{i}");
            selector
                .key_times
                .insert(Key::from(key.as_str()), Duration::from_millis(500).into());
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
                let key_str = task.key.to_string();
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
    }

    #[test]
    fn single_key_monopoly_handled() -> Result<()> {
        let mut selector = create_selector();

        for _ in 0_i32..10_i32 {
            selector.enqueue_task(create_task("monopoly_key", DemandType::Normal, 0));
        }

        for _ in 0_i32..10_i32 {
            let Some(selected) = selector.get_next_task() else {
                bail!("Expected task but got None");
            };
            assert_eq!(selected.key, Key::from("monopoly_key"));
        }
        Ok(())
    }

    #[test]
    fn vt_spread_bounded_within_class() {
        let mut selector = create_selector();

        for i in 0_u64..10_u64 {
            let key = format!("key_{i}");
            let vt = Duration::from_millis(i * 100);
            selector
                .key_times
                .insert(Key::from(key.as_str()), vt.into());
            selector.enqueue_task(create_task(&key, DemandType::Normal, 0));
        }

        let mut vts = Vec::new();
        for _ in 0_i32..10_i32 {
            if let Some(task) = selector.get_next_task()
                && let Some(vt) = selector.key_times.get(&task.key)
            {
                vts.push(vt.at(Instant::now()).as_millis());
            }
        }

        if let (Some(min_vt), Some(max_vt)) = (vts.iter().min(), vts.iter().max()) {
            let spread = max_vt - min_vt;
            assert!(spread < 10_000, "VT spread too large: {spread} ms");
        }
    }

    #[test]
    fn mixed_demand_types_both_served() {
        let mut selector = create_selector();

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
    }

    #[test]
    fn burst_traffic_failure_surge() -> Result<()> {
        let mut selector = create_selector();

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
            selector.enqueue_task(create_task(task.key.as_ref(), task.demand_type, 0));
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
            selector.enqueue_task(create_task(task.key.as_ref(), task.demand_type, 0));
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
        let mut selector = create_selector();

        // Create keys with different task characteristics
        // Fast keys: 10ms, Slow keys: 100ms
        let fast_keys = vec!["fast1", "fast2"];
        let slow_keys = vec!["slow1", "slow2"];

        // Set up initial VTs using key_times cache
        // All start at zero VT for fair comparison
        for &key in &fast_keys {
            selector
                .key_times
                .insert(Key::from(key), Duration::ZERO.into());
        }
        for &key in &slow_keys {
            selector
                .key_times
                .insert(Key::from(key), Duration::ZERO.into());
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

            let key_str = task.key.to_string();
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
            if let Some(mut vt) = selector.key_times.get_mut(&task.key) {
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
        let mut selector = create_selector();

        // One very slow key, multiple fast keys
        let slow_key = "slow_outlier";
        let fast_keys = vec!["fast1", "fast2", "fast3"];

        // Initialize all at zero VT
        selector
            .key_times
            .insert(Key::from(slow_key), Duration::ZERO.into());
        for &key in &fast_keys {
            selector
                .key_times
                .insert(Key::from(key), Duration::ZERO.into());
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

            let duration = if task.key == Key::from(slow_key) {
                slow_count += 1_i32;
                Duration::from_millis(500) // 50x slower
            } else {
                fast_count += 1_i32;
                Duration::from_millis(10)
            };

            // Update VT
            if let Some(mut vt) = selector.key_times.get_mut(&task.key) {
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
            .get(&Key::from(slow_key))
            .map_or(Duration::ZERO, |vt| vt.at(Instant::now()));

        let max_fast_vt = fast_keys
            .iter()
            .filter_map(|&key| selector.key_times.get(&Key::from(key)))
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
        let mut selector = create_selector();

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
            .insert(Key::from(monopoly_key), Duration::from_secs(150).into());

        // New keys start at zero VT
        for &key in &new_keys {
            selector
                .key_times
                .insert(Key::from(key), Duration::ZERO.into());
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
            let key_str = task.key.clone();

            if task.key == Key::from(monopoly_key) {
                monopoly_count += 1_i32;
                if first_monopoly_iter.is_none() {
                    first_monopoly_iter = Some(i);
                }
            } else {
                new_count += 1_i32;
            }

            // Update VT for the executed task
            if let Some(mut vt) = selector.key_times.get_mut(&task.key) {
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
}
