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
mod types;

pub use arrival::ArrivalEvidence;
pub use capacity::{
    CapacityCurve, CapacityGrid, CapacityGridError, CapacityPrior, PosteriorError, ResourceWindow,
    ResourceWindowError, ThroughputPosteriorCell,
};
pub use lead_time::{TransitionDirection, TransitionEvidence, TransitionEvidenceError};
pub use model::{DecisionCurveError, ScaleScratch, ScaleState, step};
pub use random::RandomStream;
pub use types::{
    ApplyDecision, ArrivalPosterior, Cohort, Configuration, ConfigurationError,
    DecisionDiagnostics, GroupObservation, HoldDecision, HoldReason, ModelTime, ObservationBuffer,
    ObservationError, PosteriorQuery, ScaleDecision, ServiceObjective,
};

#[cfg(test)]
mod tests;
