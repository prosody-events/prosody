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
        }) = &*data
        else {
            return;
        };

        let tp_key = TopicPartitionKey::new(topic, partition, key.clone());

        match *state {
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
                    let key_vt_secs = task.key_time.map_or(0.0_f64, |vt| vt.at(now).as_secs_f64());

                    let wait_time = (now - task.timestamp).as_secs_f64();
                    let wait_ratio = (wait_time / self.max_wait).min(1.0);
                    let wait_urgency_secs = self.wait_weight * wait_ratio.powi(2);
                    let priority = key_vt_secs - wait_urgency_secs;

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
mod tests;
