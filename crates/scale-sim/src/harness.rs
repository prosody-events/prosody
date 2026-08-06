use crate::{
    AttemptFrame, AttemptModel, AttemptParameters, ConcurrencyLatencyCurve, EventSpec, Plant,
    PlantConfiguration, PlantError, PlantSnapshot, ScaleChange, Settlement, SimulationResult,
};

/// Current time and prior tick columns for one regime calculation.
#[derive(Clone, Copy)]
pub struct TickContext<'a> {
    /// Current virtual time.
    pub now_micros: u64,
    /// Zero-based tick index.
    pub tick_index: u32,
    /// Plant state before new demand enters.
    pub plant: PlantSnapshot,
    /// Prior tick values in newest-first order.
    pub history: TickHistoryView<'a>,
    /// Current released backlog by partition.
    pub partition_backlog: &'a [u32],
    /// Current oldest release time by partition.
    pub partition_oldest_release_micros: &'a [u64],
    /// Settlements completed before this controller tick.
    pub completed_settlements: &'a [Settlement],
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
    /// Transient failures before each event settles.
    pub transient_failures: u8,
    /// Reject each Nth new event permanently.
    pub permanent_rejection_every: u32,
    /// Delay from a desired scale change to ready replicas.
    pub launch_delay_micros: u64,
    /// Desired replica action and its calculated actuator delay.
    pub scale: ScaleDirective,
}

/// A desired replica action calculated by a regime function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScaleDirective {
    /// Keep the prior desired replica count.
    Hold,
    /// Keep an externally owned desired replica count.
    ExternalHold,
    /// Request a replica count after one actuator delay.
    Request {
        /// Desired replica count.
        replicas: u32,
        /// Delay until the new replicas become ready.
        delay_micros: u64,
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
    /// Whether a timer source generated this event.
    pub timer: bool,
}

/// Per-event values calculated after the tick graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventInputs {
    /// Kafka partition for this event.
    pub partition: u32,
    /// Serialization key for this event.
    pub key: u32,
    /// Base handler duration for each attempt.
    pub handler_micros: u64,
    /// Dependency calls made by each attempt.
    pub dependency_operations: u32,
    /// Transient failures before the final result.
    pub transient_failures: u8,
    /// Whether the final result is a permanent rejection.
    pub permanent_rejection: bool,
}

/// A regime-owned graph that calculates every tick input.
pub trait TickGenerator {
    /// Calculates one row from time, plant state, history, and graph
    /// dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error when a generated value violates a plant bound.
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError>;

    /// Calculates the reporter lifecycle action for this tick.
    fn reporter(&self, _context: TickContext<'_>) -> ReporterDirective {
        ReporterDirective::Send
    }

    /// Calculates one event from the tick row and its dependencies.
    fn event(&self, context: EventContext<'_>) -> EventInputs {
        let permanent_rejection = context.inputs.permanent_rejection_every > 0
            && context
                .event_index
                .is_multiple_of(context.inputs.permanent_rejection_every);
        EventInputs {
            partition: context.event_index % context.partition_count,
            key: context.event_index % context.key_count,
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            transient_failures: context.inputs.transient_failures,
            permanent_rejection,
        }
    }
}

/// Fixed-capacity structure-of-arrays history for graph functions.
pub struct TickHistory {
    now_micros: Vec<u64>,
    message_count: Vec<u32>,
    timer_count: Vec<u32>,
    replicas: Vec<u32>,
    released: Vec<u32>,
    settled: Vec<u32>,
    backlog: Vec<u32>,
    active_handlers: Vec<u32>,
    handler_occupancy_micros: Vec<u64>,
    useful_completions: Vec<u32>,
    partitions_ready: Vec<bool>,
    handler_micros: Vec<u64>,
    dependency_operation_micros: Vec<u64>,
    desired_replicas: Vec<u32>,
    partition_backlog: Vec<u32>,
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
            released: vec![0; capacity],
            settled: vec![0; capacity],
            backlog: vec![0; capacity],
            active_handlers: vec![0; capacity],
            handler_occupancy_micros: vec![0; capacity],
            useful_completions: vec![0; capacity],
            partitions_ready: vec![false; capacity],
            handler_micros: vec![0; capacity],
            dependency_operation_micros: vec![0; capacity],
            desired_replicas: vec![0; capacity],
            partition_backlog: vec![0; partition_cell_count],
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
        partition_backlog: &[u32],
    ) {
        let index = self.cursor;
        self.now_micros[index] = now_micros;
        self.message_count[index] = inputs.message_count;
        self.timer_count[index] = inputs.timer_count;
        self.replicas[index] = plant.replicas;
        self.released[index] = plant.released;
        self.settled[index] = plant.settled;
        self.backlog[index] = plant.backlog;
        self.active_handlers[index] = plant.active_handlers;
        self.handler_occupancy_micros[index] = plant.handler_occupancy_micros;
        self.useful_completions[index] = plant.useful_completions;
        self.partitions_ready[index] = plant.partitions_ready;
        self.handler_micros[index] = inputs.handler_micros;
        self.dependency_operation_micros[index] = inputs.dependency_operation_micros;
        self.desired_replicas[index] = match inputs.scale {
            ScaleDirective::Hold | ScaleDirective::ExternalHold => {
                self.latest_desired_replicas(plant.replicas)
            }
            ScaleDirective::Request { replicas, .. } => replicas,
        };
        let partition_start = index * self.partition_count;
        let partition_end = partition_start + self.partition_count;
        self.partition_backlog[partition_start..partition_end].copy_from_slice(partition_backlog);
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

    /// Returns cumulative handler occupancy for one newest-first offset.
    #[must_use]
    pub fn handler_occupancy_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.handler_occupancy_micros[index])
    }

    /// Returns cumulative useful completions for one newest-first offset.
    #[must_use]
    pub fn useful_completions(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.useful_completions[index])
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
    pub fn partition_backlog(self, steps_back: usize) -> Option<&'a [u32]> {
        let index = self.index(steps_back)?;
        let start = index * self.history.partition_count;
        let end = start + self.history.partition_count;
        Some(&self.history.partition_backlog[start..end])
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
    partition_backlog: Vec<u32>,
    partition_oldest_release_micros: Vec<u64>,
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
            partition_backlog: vec![0; partition_capacity],
            partition_oldest_release_micros: vec![0; partition_capacity],
        })
    }

    /// Calculates one input row and advances the plant at that time.
    ///
    /// # Errors
    ///
    /// Returns an error when generated demand exceeds a fixed plant bound.
    pub fn tick(&mut self, now_micros: u64) -> Result<PlantSnapshot, PlantError> {
        let before = self.plant.advance_until(now_micros);
        self.plant
            .write_partition_backlog(now_micros, &mut self.partition_backlog)?;
        self.plant.write_partition_oldest_release(
            now_micros,
            &mut self.partition_oldest_release_micros,
        )?;
        let inputs = self.graph.calculate(TickContext {
            now_micros,
            tick_index: self.tick_index,
            plant: before,
            history: self.history.view(),
            partition_backlog: &self.partition_backlog,
            partition_oldest_release_micros: &self.partition_oldest_release_micros,
            completed_settlements: self.plant.completed_settlements(),
        })?;
        self.plant.attempt_model.update(inputs);
        if let ScaleDirective::Request {
            replicas,
            delay_micros,
        } = inputs.scale
        {
            self.plant.replace_scale_target(ScaleChange {
                at_micros: now_micros.saturating_add(delay_micros),
                replicas,
            })?;
        }
        let context = TickContext {
            now_micros,
            tick_index: self.tick_index,
            plant: before,
            history: self.history.view(),
            partition_backlog: &self.partition_backlog,
            partition_oldest_release_micros: &self.partition_oldest_release_micros,
            completed_settlements: &[],
        };
        let mut event_sink = EventSink {
            graph: &self.graph,
            plant: &mut self.plant,
            event_count: &mut self.event_count,
            partition_count: self.partition_count,
            key_count: self.key_count,
        };
        event_sink.add(context, inputs, false, inputs.message_count)?;
        event_sink.add(context, inputs, true, inputs.timer_count)?;
        let after = self.plant.advance_until(now_micros);
        self.plant
            .write_partition_backlog(now_micros, &mut self.partition_backlog)?;
        self.history
            .push(now_micros, after, inputs, &self.partition_backlog);
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
        context: TickContext<'_>,
        inputs: TickInputs,
        timer: bool,
        count: u32,
    ) -> Result<(), PlantError> {
        for event_offset in 0..count {
            let event_index = *self.event_count;
            let event = self.graph.event(EventContext {
                tick: context,
                inputs,
                event_offset,
                event_index,
                partition_count: self.partition_count,
                key_count: self.key_count,
                timer,
            });
            self.plant.add_event(EventSpec {
                release_micros: context.now_micros,
                partition: event.partition,
                key: event.key,
                handler_micros: event.handler_micros,
                dependency_operations: event.dependency_operations,
                transient_failures: event.transient_failures,
                permanent_rejection: event.permanent_rejection,
                timer,
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
