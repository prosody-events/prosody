use crate::{
    AttemptFrame, AttemptModel, AttemptParameters, ConcurrencyLatencyCurve, EventOutcome,
    EventSpec, FinalOutcome, Plant, PlantConfiguration, PlantError, PlantSnapshot, RetryCount,
    RetryCountError, RetryOutcome, ScaleChange, Settlement, SimulationResult,
};
use std::num::NonZeroU32;

use prosody_scale_core::{CalendarArtifactId, CalendarRateSegment, ScheduledRelease};

const CALENDAR_FORECAST_SEGMENT_COUNT_MAX: usize = 8;

/// Current time and prior tick columns for one regime calculation.
#[derive(Clone, Copy)]
pub struct TickContext<'a> {
    /// Current virtual time.
    pub now_micros: u64,
    /// Zero-based tick index.
    pub tick_index: u32,
    /// Plant state before new demand enters.
    pub plant: PlantSnapshot,
    /// Current owner for each partition.
    pub partition_owners: &'a [u32],
    /// Prior tick values in newest-first order.
    pub history: TickHistoryView<'a>,
    /// Current observable Normal backlog.
    pub normal_backlog: NormalBacklogView<'a>,
    /// Current known deferred Failure backlog.
    pub failure_backlog: FailureBacklogView<'a>,
    /// Settlements completed before this controller tick.
    pub completed_settlements: &'a [Settlement],
    /// Exact handler-slot transitions through this tick.
    pub attempt_transitions: &'a [crate::AttemptTransition],
}

/// Paired Normal backlog columns for all partitions.
#[derive(Clone, Copy)]
pub struct NormalBacklogView<'a> {
    counts: &'a [u32],
    oldest_release_micros: &'a [u64],
}

/// Observable Normal backlog for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NormalBacklog {
    /// Observable event count.
    pub count: u32,
    /// Oldest observable release time.
    pub oldest_release_micros: u64,
}

impl NormalBacklogView<'_> {
    /// Returns the observable backlog for one partition.
    #[must_use]
    pub fn get(self, partition: usize) -> Option<NormalBacklog> {
        Some(NormalBacklog {
            count: *self.counts.get(partition)?,
            oldest_release_micros: *self.oldest_release_micros.get(partition)?,
        })
    }
}

/// Paired Failure backlog columns for all partitions.
#[derive(Clone, Copy)]
pub struct FailureBacklogView<'a> {
    counts: &'a [u32],
    release_micros: &'a [u64],
}

/// Known Failure backlog for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureBacklog {
    /// Known event count.
    pub count: u32,
    /// Earliest known release time.
    pub release_micros: u64,
}

impl FailureBacklogView<'_> {
    /// Returns the known backlog for one partition.
    #[must_use]
    pub fn get(self, partition: usize) -> Option<FailureBacklog> {
        Some(FailureBacklog {
            count: *self.counts.get(partition)?,
            release_micros: *self.release_micros.get(partition)?,
        })
    }
}

/// All plant inputs calculated for one virtual-time tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TickInputs {
    /// New Kafka messages at this tick.
    pub message_count: u32,
    /// New timer events at this tick.
    pub timer_count: u32,
    /// Base handler duration for each new event.
    pub handler_micros: u64,
    /// Dependency calls made by each attempt.
    pub dependency_operations: u32,
    /// Duration of one dependency call at this tick.
    pub dependency_operation_micros: u64,
    /// Added handler duration at this tick.
    pub handler_added_micros: u64,
    /// Complete rule for generated event outcomes.
    pub outcome: EventOutcomeRule,
    /// Delay from a desired scale change to ready replicas.
    pub launch_delay_micros: u64,
    /// Desired replica action and its calculated actuator delay.
    pub scale: ScaleDirective,
}

/// Bounded frozen calendar input for one controller tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalendarForecastInput {
    artifact: CalendarArtifactId,
    prior_probability: f64,
    segments: [CalendarRateSegment; CALENDAR_FORECAST_SEGMENT_COUNT_MAX],
    segment_count: u8,
}

/// Bounded known future releases for one controller tick.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledReleasesInput {
    releases: Vec<ScheduledRelease>,
}

impl ScheduledReleasesInput {
    /// Returns an empty schedule.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            releases: Vec::new(),
        }
    }

    /// Copies one bounded release schedule.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule exceeds the certified producer bound.
    pub fn new(
        releases: Vec<ScheduledRelease>,
        certified_count_max: u32,
    ) -> Result<Self, PlantError> {
        let certified_count_max =
            usize::try_from(certified_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if certified_count_max == 0 {
            return Err(PlantError::ZeroBound {
                name: "scheduled_release_count_max",
            });
        }
        if releases.len() > certified_count_max {
            return Err(PlantError::ScheduledReleaseCapacity);
        }
        Ok(Self { releases })
    }

    /// Returns the known releases.
    #[must_use]
    pub fn releases(&self) -> &[ScheduledRelease] {
        &self.releases
    }
}

impl CalendarForecastInput {
    /// Copies one nonempty bounded calendar path.
    ///
    /// # Errors
    ///
    /// Returns an error when the path exceeds the simulator bound.
    pub fn new(
        artifact: CalendarArtifactId,
        prior_probability: f64,
        segments: &[CalendarRateSegment],
    ) -> Result<Self, PlantError> {
        let Some(&first) = segments.first() else {
            return Err(PlantError::ZeroBound {
                name: "calendar_forecast_segments",
            });
        };
        if segments.len() > CALENDAR_FORECAST_SEGMENT_COUNT_MAX {
            return Err(PlantError::CalendarCapacity);
        }
        let mut values = [first; CALENDAR_FORECAST_SEGMENT_COUNT_MAX];
        values[..segments.len()].copy_from_slice(segments);
        let segment_count = u8::try_from(segments.len()).map_err(|_| PlantError::PlatformLimit)?;
        Ok(Self {
            artifact,
            prior_probability,
            segments: values,
            segment_count,
        })
    }

    pub(crate) const fn artifact(self) -> CalendarArtifactId {
        self.artifact
    }

    pub(crate) const fn prior_probability(self) -> f64 {
        self.prior_probability
    }

    pub(crate) fn segments(&self) -> &[CalendarRateSegment] {
        &self.segments[..usize::from(self.segment_count)]
    }
}

/// Complete outcome rule for events generated during one tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventOutcomeRule {
    /// Every event succeeds on its first attempt.
    Success,
    /// Every event retries before it succeeds.
    TransientThenSuccess(RetryCount),
    /// Each selected event ends with a permanent failure.
    PermanentEvery(NonZeroU32),
    /// Each event retries before the selected events fail permanently.
    TransientThenPermanentEvery {
        /// Number of retry-producing attempts.
        count: RetryCount,
        /// Positive interval between permanent failures.
        interval: NonZeroU32,
    },
}

impl EventOutcomeRule {
    /// Constructs a transient retry rule.
    ///
    /// # Errors
    ///
    /// Returns an error when the retry count exceeds the simulator bound.
    pub fn transient_then_success(count: u8) -> Result<Self, RetryCountError> {
        Ok(Self::TransientThenSuccess(RetryCount::new(count)?))
    }

    /// Constructs a periodic permanent-failure rule.
    ///
    /// Returns `None` when the interval is zero.
    #[must_use]
    pub fn permanent_every(interval: u32) -> Option<Self> {
        NonZeroU32::new(interval).map(Self::PermanentEvery)
    }

    fn outcome(self, event_index: u32) -> EventOutcome {
        match self {
            Self::Success => EventOutcome::Final(FinalOutcome::Success),
            Self::TransientThenSuccess(count) => EventOutcome::Retry {
                outcome: RetryOutcome::Transient,
                count,
                final_outcome: FinalOutcome::Success,
            },
            Self::PermanentEvery(interval) => {
                EventOutcome::Final(periodic_final_outcome(event_index, interval))
            }
            Self::TransientThenPermanentEvery { count, interval } => EventOutcome::Retry {
                outcome: RetryOutcome::Transient,
                count,
                final_outcome: periodic_final_outcome(event_index, interval),
            },
        }
    }
}

fn periodic_final_outcome(event_index: u32, interval: NonZeroU32) -> FinalOutcome {
    if event_index.is_multiple_of(interval.get()) {
        FinalOutcome::PermanentFailure
    } else {
        FinalOutcome::Success
    }
}

/// A desired replica action calculated by a regime function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScaleDirective {
    /// Keep the prior desired replica count.
    Hold,
    /// Keep an externally owned desired replica count.
    ExternalHold,
    /// Publish one desired replica count.
    Request {
        /// Desired replica count.
        replicas: u32,
    },
}

/// Reporter and aggregator action for one controller tick.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReporterDirective {
    /// Send the current cumulative snapshot.
    #[default]
    Send,
    /// Do not send a snapshot at this tick.
    Missing,
    /// Start a new reporter incarnation with fresh counters.
    Restart,
    /// Replace the aggregator and discard its live table.
    ReplaceAggregator,
}

/// Current tick values for one generated event function.
#[derive(Clone, Copy)]
pub struct EventContext<'a> {
    /// Current tick context and historical columns.
    pub tick: TickContext<'a>,
    /// Values calculated by the tick graph.
    pub inputs: TickInputs,
    /// Zero-based event offset within the tick source.
    pub event_offset: u32,
    /// Stable event index across the complete simulation.
    pub event_index: u32,
    /// Configured partition count.
    pub partition_count: u32,
    /// Configured key count.
    pub key_count: u32,
    /// Durable source that generated this event.
    pub source: crate::EventSource,
}

/// Per-event values calculated after the tick graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventInputs {
    /// Virtual time when this event becomes available.
    pub release_micros: u64,
    /// Kafka partition for this event.
    pub partition: u32,
    /// Serialization key for this event.
    pub key: u32,
    /// Base handler duration for each attempt.
    pub handler_micros: u64,
    /// Dependency calls made by each attempt.
    pub dependency_operations: u32,
    /// Complete sequence of attempt outcomes.
    pub outcome: EventOutcome,
}

/// A regime-owned graph that calculates every tick input.
pub trait TickGenerator {
    /// Returns the certified maximum future releases in one observation.
    fn scheduled_release_count_max(&self) -> u32 {
        1
    }

    /// Calculates one row from time, plant state, history, and graph
    /// dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error when a generated value violates a plant bound.
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError>;

    /// Applies observations after the plant reaches this tick.
    ///
    /// # Errors
    ///
    /// Returns an error when an observed value violates a plant bound.
    fn observe(
        &mut self,
        _: TickContext<'_>,
        inputs: TickInputs,
    ) -> Result<TickInputs, PlantError> {
        Ok(inputs)
    }

    /// Records one desired-replica metric value consumed by the actuator.
    ///
    /// # Errors
    ///
    /// Returns an error when the observation exceeds a fixed bound.
    fn metric_polled(&mut self, _: TickContext<'_>, _: u32, _: u32) -> Result<(), PlantError> {
        Ok(())
    }

    /// Calculates the frozen calendar input for this tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the forecast violates a simulator bound.
    fn calendar_forecast(
        &self,
        _: TickContext<'_>,
    ) -> Result<Option<CalendarForecastInput>, PlantError> {
        Ok(None)
    }

    /// Returns known pending releases for this tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule exceeds a simulator bound.
    fn scheduled_releases(&self, _: TickContext<'_>) -> Result<ScheduledReleasesInput, PlantError> {
        Ok(ScheduledReleasesInput::empty())
    }

    /// Calculates the reporter lifecycle action for this tick.
    fn reporter(&self, _: TickContext<'_>) -> ReporterDirective {
        ReporterDirective::Send
    }

    /// Calculates one event from the tick row and its dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error when the outcome exceeds a simulator bound.
    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        Ok(EventInputs {
            release_micros: context.tick.now_micros,
            partition: context.event_index % context.partition_count,
            key: context.event_index % context.key_count,
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            outcome: context.inputs.outcome.outcome(context.event_index),
        })
    }
}

/// Fixed-capacity structure-of-arrays history for graph functions.
pub struct TickHistory {
    now_micros: Vec<u64>,
    message_count: Vec<u32>,
    timer_count: Vec<u32>,
    replicas: Vec<u32>,
    physical_slots: Vec<u32>,
    released: Vec<u32>,
    settled: Vec<u32>,
    backlog: Vec<u32>,
    active_handlers: Vec<u32>,
    available_attempts: Vec<u32>,
    handler_occupancy_micros: Vec<u64>,
    attempt_transition_count: Vec<usize>,
    useful_completions: Vec<u32>,
    completed_attempts: Vec<u32>,
    started_attempts: Vec<u32>,
    rebalance_pause_micros: Vec<u64>,
    normal_attempts: Vec<u32>,
    normal_successes: Vec<u32>,
    normal_transient_failures: Vec<u32>,
    normal_terminal_failures: Vec<u32>,
    normal_permanent_failures: Vec<u32>,
    failure_attempts: Vec<u32>,
    failure_successes: Vec<u32>,
    failure_transient_failures: Vec<u32>,
    failure_terminal_failures: Vec<u32>,
    failure_permanent_failures: Vec<u32>,
    partitions_ready: Vec<bool>,
    handler_micros: Vec<u64>,
    dependency_operation_micros: Vec<u64>,
    desired_replicas: Vec<u32>,
    partition_normal_backlog: Vec<u32>,
    partition_count: usize,
    cursor: usize,
    length: usize,
}

/// Read-only access to prior tick columns.
#[derive(Clone, Copy)]
pub struct TickHistoryView<'a> {
    history: &'a TickHistory,
}

impl TickHistory {
    /// Allocates every historical column at one fixed capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the capacity is zero or exceeds the platform.
    pub fn new(sample_count_max: u32, partition_count: u32) -> Result<Self, PlantError> {
        let capacity = usize::try_from(sample_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "tick_history_count_max",
            });
        }
        let partition_count =
            usize::try_from(partition_count).map_err(|_| PlantError::PlatformLimit)?;
        if partition_count == 0 {
            return Err(PlantError::ZeroBound {
                name: "partition_count",
            });
        }
        let partition_cell_count = capacity
            .checked_mul(partition_count)
            .ok_or(PlantError::PlatformLimit)?;
        Ok(Self {
            now_micros: vec![0; capacity],
            message_count: vec![0; capacity],
            timer_count: vec![0; capacity],
            replicas: vec![0; capacity],
            physical_slots: vec![0; capacity],
            released: vec![0; capacity],
            settled: vec![0; capacity],
            backlog: vec![0; capacity],
            active_handlers: vec![0; capacity],
            available_attempts: vec![0; capacity],
            handler_occupancy_micros: vec![0; capacity],
            attempt_transition_count: vec![0; capacity],
            useful_completions: vec![0; capacity],
            completed_attempts: vec![0; capacity],
            started_attempts: vec![0; capacity],
            rebalance_pause_micros: vec![0; capacity],
            normal_attempts: vec![0; capacity],
            normal_successes: vec![0; capacity],
            normal_transient_failures: vec![0; capacity],
            normal_terminal_failures: vec![0; capacity],
            normal_permanent_failures: vec![0; capacity],
            failure_attempts: vec![0; capacity],
            failure_successes: vec![0; capacity],
            failure_transient_failures: vec![0; capacity],
            failure_terminal_failures: vec![0; capacity],
            failure_permanent_failures: vec![0; capacity],
            partitions_ready: vec![false; capacity],
            handler_micros: vec![0; capacity],
            dependency_operation_micros: vec![0; capacity],
            desired_replicas: vec![0; capacity],
            partition_normal_backlog: vec![0; partition_cell_count],
            partition_count,
            cursor: 0,
            length: 0,
        })
    }

    fn view(&self) -> TickHistoryView<'_> {
        TickHistoryView { history: self }
    }

    fn push(
        &mut self,
        now_micros: u64,
        plant: PlantSnapshot,
        inputs: TickInputs,
        partition_normal_backlog: &[u32],
    ) {
        let index = self.cursor;
        self.now_micros[index] = now_micros;
        self.message_count[index] = inputs.message_count;
        self.timer_count[index] = inputs.timer_count;
        self.replicas[index] = plant.replicas;
        self.physical_slots[index] = plant.physical_slots;
        self.released[index] = plant.released;
        self.settled[index] = plant.settled;
        self.backlog[index] = plant.backlog;
        self.active_handlers[index] = plant.active_handlers;
        self.available_attempts[index] = plant.available_attempts;
        self.handler_occupancy_micros[index] = plant.handler_occupancy_micros;
        self.attempt_transition_count[index] = plant.attempt_transition_count;
        self.useful_completions[index] = plant.useful_completions;
        self.completed_attempts[index] = plant.completed_attempts;
        self.started_attempts[index] = plant.started_attempts;
        self.rebalance_pause_micros[index] = plant.rebalance_pause_micros;
        self.normal_attempts[index] = plant.normal_attempts;
        self.normal_successes[index] = plant.normal_successes;
        self.normal_transient_failures[index] = plant.normal_transient_failures;
        self.normal_terminal_failures[index] = plant.normal_terminal_failures;
        self.normal_permanent_failures[index] = plant.normal_permanent_failures;
        self.failure_attempts[index] = plant.failure_attempts;
        self.failure_successes[index] = plant.failure_successes;
        self.failure_transient_failures[index] = plant.failure_transient_failures;
        self.failure_terminal_failures[index] = plant.failure_terminal_failures;
        self.failure_permanent_failures[index] = plant.failure_permanent_failures;
        self.partitions_ready[index] = plant.partitions_ready;
        self.handler_micros[index] = inputs.handler_micros;
        self.dependency_operation_micros[index] = inputs.dependency_operation_micros;
        self.desired_replicas[index] = match inputs.scale {
            ScaleDirective::Hold | ScaleDirective::ExternalHold => {
                self.latest_desired_replicas(plant.replicas)
            }
            ScaleDirective::Request { replicas } => replicas,
        };
        let partition_start = index * self.partition_count;
        let partition_end = partition_start + self.partition_count;
        self.partition_normal_backlog[partition_start..partition_end]
            .copy_from_slice(partition_normal_backlog);
        self.cursor = (self.cursor + 1) % self.now_micros.len();
        self.length = (self.length + 1).min(self.now_micros.len());
    }

    fn latest_desired_replicas(&self, fallback: u32) -> u32 {
        self.view().desired_replicas(0).unwrap_or(fallback)
    }
}

impl<'a> TickHistoryView<'a> {
    /// Returns the retained sample count.
    #[must_use]
    pub const fn len(self) -> usize {
        self.history.length
    }

    /// Returns true when the history has no sample.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.history.length == 0
    }

    /// Returns virtual time for one newest-first offset.
    #[must_use]
    pub fn now_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.now_micros[index])
    }

    /// Returns message demand for one newest-first offset.
    #[must_use]
    pub fn message_count(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.message_count[index])
    }

    /// Returns timer demand for one newest-first offset.
    #[must_use]
    pub fn timer_count(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.timer_count[index])
    }

    /// Returns actual replicas for one newest-first offset.
    #[must_use]
    pub fn replicas(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.replicas[index])
    }

    /// Returns slots on ready replicas and replicas that still drain handlers.
    #[must_use]
    pub fn physical_slots(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.physical_slots[index])
    }

    /// Returns released events for one newest-first offset.
    #[must_use]
    pub fn released(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.released[index])
    }

    /// Returns settled events for one newest-first offset.
    #[must_use]
    pub fn settled(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.settled[index])
    }

    /// Returns backlog for one newest-first offset.
    #[must_use]
    pub fn backlog(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.backlog[index])
    }

    /// Returns active handlers for one newest-first offset.
    #[must_use]
    pub fn active_handlers(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.active_handlers[index])
    }

    /// Returns available attempts for one newest-first offset.
    #[must_use]
    pub fn available_attempts(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.available_attempts[index])
    }

    /// Returns cumulative handler occupancy for one newest-first offset.
    #[must_use]
    pub fn handler_occupancy_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.handler_occupancy_micros[index])
    }

    /// Returns the recorded transition count for one newest-first offset.
    #[must_use]
    pub fn attempt_transition_count(self, steps_back: usize) -> Option<usize> {
        self.index(steps_back)
            .map(|index| self.history.attempt_transition_count[index])
    }

    /// Returns cumulative useful completions for one newest-first offset.
    #[must_use]
    pub fn useful_completions(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.useful_completions[index])
    }

    /// Returns cumulative completed attempts for one newest-first offset.
    #[must_use]
    pub fn completed_attempts(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.completed_attempts[index])
    }

    /// Returns cumulative started attempts for one newest-first offset.
    #[must_use]
    pub fn started_attempts(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.started_attempts[index])
    }

    /// Returns cumulative rebalance pause time for one newest-first offset.
    #[must_use]
    pub fn rebalance_pause_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.rebalance_pause_micros[index])
    }

    /// Returns cumulative normal attempts for one newest-first offset.
    #[must_use]
    pub fn normal_attempts(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.normal_attempts[index])
    }

    /// Returns cumulative successes from normal demand.
    #[must_use]
    pub fn normal_successes(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.normal_successes[index])
    }

    /// Returns cumulative transient failures from normal demand.
    #[must_use]
    pub fn normal_transient_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.normal_transient_failures[index])
    }

    /// Returns cumulative terminal failures from normal demand.
    #[must_use]
    pub fn normal_terminal_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.normal_terminal_failures[index])
    }

    /// Returns cumulative permanent failures from normal demand.
    #[must_use]
    pub fn normal_permanent_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.normal_permanent_failures[index])
    }

    /// Returns cumulative failure attempts for one newest-first offset.
    #[must_use]
    pub fn failure_attempts(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.failure_attempts[index])
    }

    /// Returns cumulative successes from failure demand.
    #[must_use]
    pub fn failure_successes(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.failure_successes[index])
    }

    /// Returns cumulative transient failures from failure demand.
    #[must_use]
    pub fn failure_transient_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.failure_transient_failures[index])
    }

    /// Returns cumulative terminal failures from failure demand.
    #[must_use]
    pub fn failure_terminal_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.failure_terminal_failures[index])
    }

    /// Returns cumulative permanent failures from failure demand.
    #[must_use]
    pub fn failure_permanent_failures(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.failure_permanent_failures[index])
    }

    /// Returns whether partitions were ready for one newest-first offset.
    #[must_use]
    pub fn partitions_ready(self, steps_back: usize) -> Option<bool> {
        self.index(steps_back)
            .map(|index| self.history.partitions_ready[index])
    }

    /// Returns handler duration for one newest-first offset.
    #[must_use]
    pub fn handler_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.handler_micros[index])
    }

    /// Returns dependency duration for one newest-first offset.
    #[must_use]
    pub fn dependency_operation_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.dependency_operation_micros[index])
    }

    /// Returns desired replicas for one newest-first offset.
    #[must_use]
    pub fn desired_replicas(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.desired_replicas[index])
    }

    /// Returns partition backlog for one newest-first offset.
    #[must_use]
    pub fn partition_normal_backlog(self, steps_back: usize) -> Option<&'a [u32]> {
        let index = self.index(steps_back)?;
        let start = index * self.history.partition_count;
        let end = start + self.history.partition_count;
        Some(&self.history.partition_normal_backlog[start..end])
    }

    fn index(self, steps_back: usize) -> Option<usize> {
        if steps_back >= self.history.length {
            return None;
        }
        let capacity = self.history.now_micros.len();
        Some((self.history.cursor + capacity - 1 - steps_back) % capacity)
    }
}

/// One graph-driven simulator with bounded historical columns.
pub struct SimulationHarness<Graph, Model = DefaultTickAttemptModel> {
    plant: Plant<Model>,
    graph: Graph,
    history: TickHistory,
    partition_count: u32,
    key_count: u32,
    event_count: u32,
    tick_index: u32,
    published_replicas: u32,
    next_metric_poll_micros: u64,
    metric_poll_interval_micros: u64,
    partition_normal_backlog: Vec<u32>,
    partition_normal_oldest_release_micros: Vec<u64>,
    partition_failure_backlog: Vec<u32>,
    partition_failure_release_micros: Vec<u64>,
}

impl<Graph: TickGenerator> SimulationHarness<Graph, DefaultTickAttemptModel> {
    /// Allocates one plant and its bounded graph history.
    ///
    /// # Errors
    ///
    /// Returns an error when any fixed bound is invalid.
    pub fn new(
        configuration: PlantConfiguration,
        initial_replicas: u32,
        history_count_max: u32,
        graph: Graph,
    ) -> Result<Self, PlantError> {
        let attempt_model = DefaultTickAttemptModel {
            parameters: AttemptParameters::default(),
            dependency_latency_curve: configuration.dependency_latency_curve.clone(),
            handler_latency_curve: configuration.handler_latency_curve.clone(),
        };
        Self::with_attempt_model(
            configuration,
            initial_replicas,
            history_count_max,
            graph,
            attempt_model,
        )
    }
}

impl<Graph: TickGenerator, Model: TickDrivenAttemptModel> SimulationHarness<Graph, Model> {
    /// Allocates one plant with a regime-owned attempt equation.
    ///
    /// # Errors
    ///
    /// Returns an error when any fixed bound is invalid.
    pub(crate) fn with_attempt_model(
        configuration: PlantConfiguration,
        initial_replicas: u32,
        history_count_max: u32,
        graph: Graph,
        attempt_model: Model,
    ) -> Result<Self, PlantError> {
        let partition_count = configuration.partition_count;
        let key_count = configuration.key_count;
        let metric_poll_interval_micros = configuration.metric_poll_interval_micros();
        if metric_poll_interval_micros == 0 {
            return Err(PlantError::ZeroBound {
                name: "metric_poll_interval_micros",
            });
        }
        let plant = Plant::with_attempt_model(configuration, initial_replicas, attempt_model)?;
        let partition_capacity =
            usize::try_from(partition_count).map_err(|_| PlantError::PlatformLimit)?;
        Ok(Self {
            plant,
            graph,
            history: TickHistory::new(history_count_max, partition_count)?,
            partition_count,
            key_count,
            event_count: 0,
            tick_index: 0,
            published_replicas: initial_replicas,
            next_metric_poll_micros: 0,
            metric_poll_interval_micros,
            partition_normal_backlog: vec![0; partition_capacity],
            partition_normal_oldest_release_micros: vec![0; partition_capacity],
            partition_failure_backlog: vec![0; partition_capacity],
            partition_failure_release_micros: vec![0; partition_capacity],
        })
    }

    fn poll_metric(
        &mut self,
        now_micros: u64,
        inputs: TickInputs,
        plant: PlantSnapshot,
    ) -> Result<(), PlantError> {
        if now_micros < self.next_metric_poll_micros {
            return Ok(());
        }
        self.next_metric_poll_micros = now_micros.saturating_add(self.metric_poll_interval_micros);
        self.plant.replace_scale_target(ScaleChange {
            at_micros: now_micros.saturating_add(inputs.launch_delay_micros),
            replicas: self.published_replicas,
        })?;
        self.graph.metric_polled(
            TickContext {
                now_micros,
                tick_index: self.tick_index,
                plant,
                partition_owners: &[],
                history: self.history.view(),
                normal_backlog: NormalBacklogView {
                    counts: &self.partition_normal_backlog,
                    oldest_release_micros: &self.partition_normal_oldest_release_micros,
                },
                failure_backlog: FailureBacklogView {
                    counts: &self.partition_failure_backlog,
                    release_micros: &self.partition_failure_release_micros,
                },
                completed_settlements: self.plant.completed_settlements(),
                attempt_transitions: self.plant.attempt_transitions(),
            },
            self.published_replicas,
            self.plant.in_flight_replicas(),
        )
    }

    /// Calculates one input row and advances the plant at that time.
    ///
    /// # Errors
    ///
    /// Returns an error when generated demand exceeds a fixed plant bound.
    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "harness_tick"))]
    pub fn tick(&mut self, now_micros: u64) -> Result<PlantSnapshot, PlantError> {
        let previous_micros = self.history.view().now_micros(0).unwrap_or(0);
        let before = self.plant.advance_until(previous_micros);
        let schedule_context = TickContext {
            now_micros,
            tick_index: self.tick_index,
            plant: before,
            partition_owners: &[],
            history: self.history.view(),
            normal_backlog: NormalBacklogView {
                counts: &self.partition_normal_backlog,
                oldest_release_micros: &self.partition_normal_oldest_release_micros,
            },
            failure_backlog: FailureBacklogView {
                counts: &self.partition_failure_backlog,
                release_micros: &self.partition_failure_release_micros,
            },
            completed_settlements: self.plant.completed_settlements(),
            attempt_transitions: &[],
        };
        let inputs = self.graph.calculate(schedule_context)?;
        self.plant.attempt_model.update(inputs);
        let event_context = TickContext {
            now_micros,
            tick_index: self.tick_index,
            plant: before,
            partition_owners: &[],
            history: self.history.view(),
            normal_backlog: NormalBacklogView {
                counts: &self.partition_normal_backlog,
                oldest_release_micros: &self.partition_normal_oldest_release_micros,
            },
            failure_backlog: FailureBacklogView {
                counts: &self.partition_failure_backlog,
                release_micros: &self.partition_failure_release_micros,
            },
            completed_settlements: &[],
            attempt_transitions: &[],
        };
        let mut event_sink = EventSink {
            graph: &self.graph,
            plant: &mut self.plant,
            event_count: &mut self.event_count,
            partition_count: self.partition_count,
            key_count: self.key_count,
        };
        event_sink.add(
            &event_context,
            inputs,
            crate::EventSource::Message,
            inputs.message_count,
        )?;
        event_sink.add(
            &event_context,
            inputs,
            crate::EventSource::Timer,
            inputs.timer_count,
        )?;
        let after = self.plant.advance_until(now_micros);
        self.plant.write_partition_backlogs(
            now_micros,
            &mut self.partition_normal_backlog,
            &mut self.partition_normal_oldest_release_micros,
            &mut self.partition_failure_backlog,
            &mut self.partition_failure_release_micros,
        )?;
        let observed_inputs = self.graph.observe(
            TickContext {
                now_micros,
                tick_index: self.tick_index,
                plant: after,
                partition_owners: self.plant.partition_owners(),
                history: self.history.view(),
                normal_backlog: NormalBacklogView {
                    counts: &self.partition_normal_backlog,
                    oldest_release_micros: &self.partition_normal_oldest_release_micros,
                },
                failure_backlog: FailureBacklogView {
                    counts: &self.partition_failure_backlog,
                    release_micros: &self.partition_failure_release_micros,
                },
                completed_settlements: self.plant.completed_settlements(),
                attempt_transitions: self.plant.attempt_transitions(),
            },
            inputs,
        )?;
        if let ScaleDirective::Request { replicas } = observed_inputs.scale {
            self.published_replicas = replicas;
        }
        self.poll_metric(now_micros, inputs, after)?;
        self.history.push(
            now_micros,
            after,
            observed_inputs,
            &self.partition_normal_backlog,
        );
        self.tick_index = self.tick_index.saturating_add(1);
        Ok(after)
    }

    /// Returns the latest desired replica count.
    #[must_use]
    pub fn desired_replicas(&self) -> u32 {
        let fallback = self.history.view().replicas(0).unwrap_or(1);
        self.history.latest_desired_replicas(fallback)
    }

    /// Returns the regime graph.
    #[must_use]
    pub const fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Runs all generated work to settlement.
    #[must_use]
    pub fn finish(self) -> SimulationResult {
        self.plant.run()
    }

    /// Runs all work and returns the regime graph with the result.
    #[must_use]
    pub fn finish_with_graph(self) -> (SimulationResult, Graph) {
        (self.plant.run(), self.graph)
    }
}

struct EventSink<'a, Graph, Model> {
    graph: &'a Graph,
    plant: &'a mut Plant<Model>,
    event_count: &'a mut u32,
    partition_count: u32,
    key_count: u32,
}

impl<Graph: TickGenerator, Model: AttemptModel> EventSink<'_, Graph, Model> {
    fn add(
        &mut self,
        context: &TickContext<'_>,
        inputs: TickInputs,
        source: crate::EventSource,
        count: u32,
    ) -> Result<(), PlantError> {
        for event_offset in 0..count {
            let event_index = *self.event_count;
            let event = self.graph.event(EventContext {
                tick: *context,
                inputs,
                event_offset,
                event_index,
                partition_count: self.partition_count,
                key_count: self.key_count,
                source,
            })?;
            self.plant.add_event(EventSpec {
                release_micros: event.release_micros,
                partition: event.partition,
                key: event.key,
                handler_micros: event.handler_micros,
                dependency_operations: event.dependency_operations,
                outcome: event.outcome,
                source,
            })?;
            *self.event_count = self.event_count.saturating_add(1);
        }
        Ok(())
    }
}

/// One attempt model that accepts each calculated tick row.
pub trait TickDrivenAttemptModel: AttemptModel {
    /// Replaces the current tick inputs before the plant advances.
    fn update(&mut self, inputs: TickInputs);
}

/// Default attempt equation for generated tick inputs.
pub struct DefaultTickAttemptModel {
    parameters: AttemptParameters,
    dependency_latency_curve: ConcurrencyLatencyCurve,
    handler_latency_curve: ConcurrencyLatencyCurve,
}

impl TickDrivenAttemptModel for DefaultTickAttemptModel {
    fn update(&mut self, inputs: TickInputs) {
        self.parameters = AttemptParameters {
            dependency_operation_micros: inputs.dependency_operation_micros,
            handler_added_micros: inputs.handler_added_micros,
        };
    }
}

impl AttemptModel for DefaultTickAttemptModel {
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters {
        AttemptParameters {
            dependency_operation_micros: self
                .parameters
                .dependency_operation_micros
                .saturating_add(
                    self.dependency_latency_curve
                        .added_micros(frame.active_handlers),
                ),
            handler_added_micros: self.parameters.handler_added_micros.saturating_add(
                self.handler_latency_curve
                    .added_micros(frame.active_handlers),
            ),
        }
    }
}
