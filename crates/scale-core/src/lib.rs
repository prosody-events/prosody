//! Bounded predictive autoscaling algorithm.
//!
//! [`step`] is the only state transition. Construction allocates all retained
//! state and scratch memory. A call to [`step`] performs bounded work and does
//! not allocate.

mod arrival;
mod capacity;
mod edf;
mod lead_time;
mod model;
mod partition;
mod random;
mod reliability;
mod types;

pub use arrival::{ArrivalEvidence, ArrivalPrior, ArrivalPriorError};
pub use capacity::{
    CapacityCurve, CapacityGrid, CapacityGridError, CapacityPrior, PosteriorError, ResourceWindow,
    ResourceWindowError, ThroughputPosteriorCell,
};
pub use lead_time::{
    TransitionDirection, TransitionEvidence, TransitionEvidenceError, TransitionPrior,
    TransitionPriorError,
};
pub use model::{DecisionCurveError, ScaleScratch, ScaleState, step};
pub use random::RandomStream;
pub use reliability::ReliabilityPrior;
pub use types::{
    ActuationCommitment, ApplyDecision, ArrivalPosterior, AttemptOutcomeCounts,
    AttemptOutcomeEvidence, BacklogCohort, CalendarArtifactId, CalendarRateSegment, Cohort,
    Configuration, ConfigurationError, DecisionDiagnostics, DemandClass, GroupObservation,
    HoldDecision, HoldReason, ModelTime, ObservationBuffer, ObservationError, PosteriorQuery,
    ScaleDecision, ServiceObjective,
};

#[cfg(test)]
mod tests;
