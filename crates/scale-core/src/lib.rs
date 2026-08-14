//! Bounded predictive autoscaling algorithm.
//!
//! [`controller::step`] is the only state transition. Construction allocates
//! all retained state and scratch memory. A transition performs bounded work
//! and does not allocate.

mod arrival;
mod capacity;
mod change_point;
/// The bounded controller state, scratch columns, and transition function.
pub mod controller;
mod edf;
mod lead_time;
mod partition;
mod planning;
mod random;
mod reliability;
mod types;

pub use arrival::{
    ArrivalCountPredictive, ArrivalEvidence, ArrivalPredictiveError, ArrivalPrior,
    ArrivalPriorError,
};
pub use capacity::{
    CapacityCurve, CapacityGrid, CapacityGridError, CapacityModelError, CapacityPrior,
    CompletionPosteriorCell, PosteriorError, ResourceWindow, ResourceWindowError,
    ThroughputPosteriorCell,
};
pub use controller::{
    DecisionActionColumns, DecisionColumnSummary, DecisionCurveError, DecisionRejection,
    ScaleScratch, ScaleState, step,
};
pub use lead_time::{
    DurationCell, LaunchEvidence, LaunchEvidenceError, LaunchPrior, LaunchPriorGrid,
    LeadTimePriorError, PredictiveQuantileError, ReadinessGroupId, ReadinessLump,
    ReadinessObservation, RebalanceEvidence, RebalancePrior, TransitionDirection,
};
pub use partition::{PartitionPriorPredictiveCheck, partition_prior_predictive_check};
pub use random::RandomStream;
pub use reliability::ReliabilityPrior;
pub use types::{
    ActuationCommitment, ApplyDecision, AttemptOutcomeCounts, AttemptOutcomeEvidence,
    BacklogCohort, CalendarArtifactId, CalendarRateSegment, Cohort, Configuration,
    ConfigurationError, DecisionDiagnostics, DemandClass, GroupObservation, HoldDecision,
    HoldReason, ModelTime, ObservationBuffer, ObservationError, OccupancyTraceEvidence,
    OccupancyTransition, PosteriorQuery, PriorArtifactBudget, PriorArtifactIdentity,
    PriorCoverageRecord, ScaleDecision, ScheduledRelease, ServiceObjective,
};

#[cfg(test)]
mod tests;
