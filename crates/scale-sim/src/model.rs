use crate::series::{OutputFunction, SeriesContext, SeriesFunction, series_graph};
use crate::{ConcurrencyLatencyCurve, PlantError, StepSeries};

/// Values available to a regime calculation for one handler attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptFrame {
    /// Current virtual time.
    pub now_micros: u64,
    /// Stable event index in insertion order.
    pub event_index: u32,
    /// One-based attempt count for the event.
    pub attempt: u32,
    /// Current ready replica count.
    pub replicas: u32,
    /// Active handler count, including this attempt.
    pub active_handlers: u32,
    /// Active shared-resource operation count.
    pub dependency_concurrency: u32,
    /// Released events that wait for a handler.
    pub queued_events: u32,
}

/// Plant parameters calculated for one handler attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptParameters {
    /// Duration of one dependency operation.
    pub dependency_operation_micros: u64,
    /// Latency added to the event's sampled handler duration.
    pub handler_added_micros: u64,
}

/// Read-only time and history for one regime function evaluation.
#[derive(Clone, Copy)]
pub struct AttemptContext<'a> {
    /// Current plant values.
    pub frame: AttemptFrame,
    /// Prior calculated values in newest-first order.
    pub history: AttemptHistoryView<'a>,
}

/// A regime-owned calculation graph for handler attempts.
pub trait AttemptGenerator {
    /// Calculates all attempt parameters from time, history, and dependencies.
    fn calculate(&self, context: AttemptContext<'_>) -> AttemptParameters;
}

/// A simulation model that can retain bounded state between attempts.
pub trait AttemptModel {
    /// Calculates plant parameters for one immutable attempt frame.
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters;
}

/// Bounded historical columns for prior attempt calculations.
pub struct AttemptHistory {
    now_micros: Vec<u64>,
    replicas: Vec<u32>,
    active_handlers: Vec<u32>,
    dependency_concurrency: Vec<u32>,
    queued_events: Vec<u32>,
    dependency_operation_micros: Vec<u64>,
    handler_added_micros: Vec<u64>,
    cursor: usize,
    length: usize,
}

/// Read-only access to bounded historical columns.
#[derive(Clone, Copy)]
pub struct AttemptHistoryView<'a> {
    history: &'a AttemptHistory,
}

impl AttemptHistory {
    /// Allocates each historical column at one fixed capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the capacity is zero or exceeds the platform.
    pub fn new(sample_count_max: u32) -> Result<Self, PlantError> {
        let capacity = usize::try_from(sample_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "attempt_history_count_max",
            });
        }
        Ok(Self {
            now_micros: vec![0; capacity],
            replicas: vec![0; capacity],
            active_handlers: vec![0; capacity],
            dependency_concurrency: vec![0; capacity],
            queued_events: vec![0; capacity],
            dependency_operation_micros: vec![0; capacity],
            handler_added_micros: vec![0; capacity],
            cursor: 0,
            length: 0,
        })
    }

    fn view(&self) -> AttemptHistoryView<'_> {
        AttemptHistoryView { history: self }
    }

    fn push(&mut self, frame: AttemptFrame, parameters: AttemptParameters) {
        let index = self.cursor;
        self.now_micros[index] = frame.now_micros;
        self.replicas[index] = frame.replicas;
        self.active_handlers[index] = frame.active_handlers;
        self.dependency_concurrency[index] = frame.dependency_concurrency;
        self.queued_events[index] = frame.queued_events;
        self.dependency_operation_micros[index] = parameters.dependency_operation_micros;
        self.handler_added_micros[index] = parameters.handler_added_micros;
        self.cursor = (self.cursor + 1) % self.now_micros.len();
        self.length = (self.length + 1).min(self.now_micros.len());
    }
}

impl AttemptHistoryView<'_> {
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

    /// Returns the virtual time for one newest-first sample offset.
    #[must_use]
    pub fn now_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.now_micros[index])
    }

    /// Returns the replica count for one newest-first sample offset.
    #[must_use]
    pub fn replicas(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.replicas[index])
    }

    /// Returns the active handler count for one newest-first sample offset.
    #[must_use]
    pub fn active_handlers(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.active_handlers[index])
    }

    /// Returns dependency concurrency for one newest-first sample offset.
    #[must_use]
    pub fn dependency_concurrency(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.dependency_concurrency[index])
    }

    /// Returns queued demand for one newest-first sample offset.
    #[must_use]
    pub fn queued_events(self, steps_back: usize) -> Option<u32> {
        self.index(steps_back)
            .map(|index| self.history.queued_events[index])
    }

    /// Returns dependency service time for one newest-first sample offset.
    #[must_use]
    pub fn dependency_operation_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.dependency_operation_micros[index])
    }

    /// Returns added handler time for one newest-first sample offset.
    #[must_use]
    pub fn handler_added_micros(self, steps_back: usize) -> Option<u64> {
        self.index(steps_back)
            .map(|index| self.history.handler_added_micros[index])
    }

    fn index(self, steps_back: usize) -> Option<usize> {
        if steps_back >= self.history.length {
            return None;
        }
        let capacity = self.history.now_micros.len();
        Some((self.history.cursor + capacity - 1 - steps_back) % capacity)
    }
}

/// Adds bounded history to one pure regime calculation graph.
pub struct HistoricalAttemptModel<Graph> {
    graph: Graph,
    history: AttemptHistory,
}

impl<Graph> HistoricalAttemptModel<Graph> {
    /// Constructs one graph with fixed historical storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the history capacity is invalid.
    pub fn new(graph: Graph, history_count_max: u32) -> Result<Self, PlantError> {
        Ok(Self {
            graph,
            history: AttemptHistory::new(history_count_max)?,
        })
    }
}

impl<Graph: AttemptGenerator> AttemptModel for HistoricalAttemptModel<Graph> {
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters {
        let parameters = self.graph.calculate(AttemptContext {
            frame,
            history: self.history.view(),
        });
        self.history.push(frame, parameters);
        parameters
    }
}

/// Piecewise input tables used by deterministic regimes.
pub struct SeriesAttemptModel {
    model: SeriesAttemptGraph,
}

impl SeriesAttemptModel {
    pub(crate) fn new(
        dependency_operation_micros: StepSeries<u64>,
        dependency_latency_curve: ConcurrencyLatencyCurve,
        handler_latency_curve: ConcurrencyLatencyCurve,
        history_count_max: u32,
    ) -> Result<Self, PlantError> {
        Ok(Self {
            model: SeriesAttemptGraph::new(
                dependency_operation_micros,
                dependency_latency_curve,
                handler_latency_curve,
                history_count_max,
            )?,
        })
    }
}

impl AttemptModel for SeriesAttemptModel {
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters {
        self.model.evaluate(frame.now_micros, frame)
    }
}

series_graph! {
    struct SeriesAttemptGraph(AttemptFrame) with (
        dependency_operation_micros: StepSeries<u64>,
        dependency_latency_curve: ConcurrencyLatencyCurve,
        handler_latency_curve: ConcurrencyLatencyCurve,
    ) {
        series dependency_series: u64 ["dependency base time", Microseconds, Input] =
            DependencySeries(dependency_operation_micros) => ();
        series dependency_contention: u64 ["dependency contention", Microseconds, Input] =
            DependencyContention(dependency_latency_curve) => ();
        series dependency_total: u64 ["dependency time", Microseconds, State] = DependencyTotal {} =>
            (dependency_series, dependency_contention);
        series handler_contention: u64 ["handler contention", Microseconds, Input] =
            HandlerContention(handler_latency_curve) => ();
        output output: AttemptParameters = AttemptOutput {} =>
            (dependency_total, handler_contention);
    }
}

struct DependencySeries(StepSeries<u64>);

impl SeriesFunction<AttemptFrame, ()> for DependencySeries {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.at(context.frame.now_micros)
    }
}

struct DependencyContention(ConcurrencyLatencyCurve);

impl SeriesFunction<AttemptFrame, ()> for DependencyContention {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.added_micros(context.frame.active_handlers)
    }
}

struct DependencyTotal;

impl SeriesFunction<AttemptFrame, (u64, u64)> for DependencyTotal {
    type Output = u64;

    fn calculate(&self, _: SeriesContext<'_, AttemptFrame>, values: (u64, u64)) -> Self::Output {
        values.0.saturating_add(values.1)
    }
}

struct HandlerContention(ConcurrencyLatencyCurve);

impl SeriesFunction<AttemptFrame, ()> for HandlerContention {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.added_micros(context.frame.active_handlers)
    }
}

struct AttemptOutput;

impl OutputFunction<AttemptFrame, (u64, u64)> for AttemptOutput {
    type Output = AttemptParameters;

    fn calculate(&self, _: SeriesContext<'_, AttemptFrame>, values: (u64, u64)) -> Self::Output {
        AttemptParameters {
            dependency_operation_micros: values.0,
            handler_added_micros: values.1,
        }
    }
}
