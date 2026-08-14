use std::{f64::consts::E, mem::size_of, time::Duration};

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use statrs::distribution::{Beta, ContinuousCDF, Gamma, LogNormal};
#[cfg(test)]
use statrs::function::gamma::ln_gamma;
use thiserror::Error;

use crate::arrival::ArrivalPrior;
use crate::change_point::ChangePointKernel;
use crate::types::prior_artifact_contract_holds;
use crate::{
    OccupancyTraceEvidence, PriorArtifact, PriorArtifactBudget, PriorArtifactIdentity,
    PriorCoverageRecord,
};

const CAPACITY_MODEL_STORAGE_BYTES_MAX: usize = 512 * 1_024 * 1_024;
const CAPACITY_MODEL_ARTIFACT_SOURCE: u64 = 0x4341_5041_4349_5459;
const CAPACITY_MODEL_ARTIFACT_VERSION: u32 = 2;
const PATH_SOLVER_PROBABILITY_ERROR_MAX: f64 = 1.0e-10_f64;
const START_HISTORY_SLOT_MAX: usize = 4_096;
const REPORT_CLOCK_ERROR_SECONDS: f64 = 1.0e-6_f64;
const RESIDUAL_BIN_COUNT: usize = 16;
const RESIDUAL_BIN_COUNT_F64: f64 = 16.0_f64;
const OBSERVATION_PROBABILITY_ERROR_MAX: f64 = 1.0_f64 / 16.0_f64;
const HAZARD_TRANSITION_PROBABILITY_ERROR_MAX: f64 = 1.0_f64 / 8.0_f64;
const OBSERVATION_COVERAGE_INDEX: usize = 0;
const HAZARD_COVERAGE_INDEX: usize = 1;
const SOLVER_COVERAGE_INDEX: usize = 2;
const CAPACITY_MODEL_BUDGET: PriorArtifactBudget = PriorArtifactBudget::new(
    (CAPACITY_MODEL_STORAGE_BYTES_MAX / size_of::<f64>()) as u32,
    CAPACITY_MODEL_STORAGE_BYTES_MAX as u64,
    1_u64 << 56,
    1.0e-6_f64,
    REPORT_CLOCK_ERROR_SECONDS,
    HAZARD_TRANSITION_PROBABILITY_ERROR_MAX,
);

/// Versioned prior and approximation limits for the capacity model.
///
/// Version 2 uses the certified event-path sampling model. It assigns one
/// affected window and nine clean windows to observation quality. It assigns
/// equal prior odds to the bounded knee family and the no-knee family. Within
/// the knee family, it assigns equal odds to no collapse and positive collapse.
/// The quadratic collapse law represents pairwise contention after the knee.
/// A labelled plant corpus can replace these judgments under a new version.
#[derive(Clone)]
struct CapacityModelArtifact {
    identity: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: Vec<PriorCoverageRecord>,
    hazard_mean_per_second: f64,
    hazard_shape: f64,
    observation_quality_alpha: f64,
    observation_quality_beta: f64,
    no_knee_probability: f64,
    no_collapse_probability: f64,
    markov_clock_assumption: MarkovClockAssumption,
    start_delay_evidence: StartDelayEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkovClockAssumption {
    MemorylessAggregateCompletions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartDelayEvidence {
    DiscardedByAcceptedStartConditioning,
}

/// Time-rescaled residual evidence for the aggregate completion clock.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CapacityClockCheck {
    /// Completion residuals included in the check.
    pub sample_count: u32,
    /// Largest empirical CDF distance from the uniform clock.
    pub maximum_distance: f64,
    /// DKW and report-clock rejection threshold.
    pub rejection_threshold: f64,
    /// Whether the residual evidence rejects the declared clock.
    pub rejected: bool,
}

/// One point on the passive throughput curve.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CapacityCurve {
    /// Throughput stays linear across the supported concurrency range.
    NoKnee {
        /// Uncongested operation time in seconds.
        service_time_seconds: f64,
    },
    /// Throughput reaches one finite knee.
    Knee {
        /// Uncongested operation time in seconds.
        service_time_seconds: f64,
        /// Peak completed-attempt rate available to this group.
        capacity_per_second: f64,
        /// Post-knee collapse strength.
        collapse: f64,
    },
}

/// One weighted throughput value from the joint capacity posterior.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ThroughputPosteriorCell {
    /// Predicted completed attempts per second.
    pub throughput_per_second: f64,
    /// Lowest throughput in this cell's parameter region.
    pub throughput_low_per_second: f64,
    /// Highest throughput in this cell's parameter region.
    pub throughput_high_per_second: f64,
    /// Joint posterior probability for this curve.
    pub probability: f64,
}

/// One weighted completion prediction from the joint capacity posterior.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CompletionPosteriorCell {
    /// Predicted completed attempts in the window.
    pub mean: f64,
    /// Joint posterior probability for this curve.
    pub probability: f64,
}

impl CapacityCurve {
    pub(crate) const fn service_time_seconds(self) -> f64 {
        match self {
            Self::NoKnee {
                service_time_seconds,
            }
            | Self::Knee {
                service_time_seconds,
                ..
            } => service_time_seconds,
        }
    }

    /// Returns completed-attempt throughput at one live concurrency.
    ///
    /// This is the physical curve. Concurrency past the knee collapses
    /// throughput. Use it to explain observed windows, never to plan supply.
    #[must_use]
    pub fn throughput(self, concurrency: f64) -> f64 {
        if concurrency <= 0.0_f64 {
            return 0.0;
        }
        match self {
            Self::NoKnee {
                service_time_seconds,
            } => concurrency / service_time_seconds,
            Self::Knee {
                service_time_seconds,
                capacity_per_second,
                collapse,
            } => {
                let knee = capacity_per_second * service_time_seconds;
                if concurrency <= knee {
                    return concurrency / service_time_seconds;
                }
                let excess = (concurrency - knee) / knee;
                capacity_per_second / (1.0 + collapse * excess * excess)
            }
        }
    }

    /// Returns the deliverable event rate for one slot allowance.
    ///
    /// A work-conserving consumer operates at the demand-driven concurrency,
    /// not at its slot allowance. Idle slots therefore never push the plant
    /// past its knee: the deliverable rate is the curve peak inside the
    /// allowance, `min(slots / service_time, capacity)`. The knee ceiling on
    /// the replica target itself comes from [`ScaleState`] through the
    /// decision cap, not from this rate.
    ///
    /// [`ScaleState`]: crate::ScaleState
    #[must_use]
    pub fn sustainable_throughput(self, concurrency: f64) -> f64 {
        if concurrency <= 0.0_f64 {
            return 0.0;
        }
        match self {
            Self::NoKnee {
                service_time_seconds,
            } => concurrency / service_time_seconds,
            Self::Knee {
                service_time_seconds,
                capacity_per_second,
                ..
            } => (concurrency / service_time_seconds).min(capacity_per_second),
        }
    }
}

/// Prior family for positive capacity scale parameters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CapacityPrior {
    /// Equal prior mass for equal intervals on each logarithmic axis.
    LogUniform,
    /// Independent normal priors on logarithmic service time and capacity.
    LogNormal {
        /// Median handler service time.
        service_time_median_seconds: f64,
        /// Median aggregate peak capacity.
        capacity_median_per_second: f64,
        /// Standard deviation on both natural-log axes.
        log_standard_deviation: f64,
    },
}

impl CapacityPrior {
    fn validate(self) -> Result<(), CapacityGridError> {
        match self {
            Self::LogUniform => Ok(()),
            Self::LogNormal {
                service_time_median_seconds,
                capacity_median_per_second,
                log_standard_deviation,
            } if service_time_median_seconds.is_finite()
                && service_time_median_seconds > 0.0_f64
                && capacity_median_per_second.is_finite()
                && capacity_median_per_second > 0.0_f64
                && log_standard_deviation.is_finite()
                && log_standard_deviation >= f64::EPSILON =>
            {
                Ok(())
            }
            Self::LogNormal { .. } => Err(CapacityGridError::InvalidPrior),
        }
    }
}

/// Cartesian grid for one resource channel.
#[derive(Clone, Debug)]
pub struct CapacityGrid {
    service_times_seconds: Vec<f64>,
    service_time_lows: Vec<f64>,
    service_time_highs: Vec<f64>,
    capacities_per_second: Vec<f64>,
    capacity_lows: Vec<f64>,
    capacity_highs: Vec<f64>,
    collapse_values: Vec<f64>,
    collapse_lows: Vec<f64>,
    collapse_highs: Vec<f64>,
    no_knee: Vec<f64>,
    knee_values: Vec<f64>,
    knee_indexes: Vec<u32>,
    knee_cell_count: u32,
    service_time_count: u32,
    capacity_count: u32,
    collapse_count: u32,
    prior: CapacityPrior,
}

impl CapacityGrid {
    /// Constructs a bounded grid from three explicit axes.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, invalid, or oversized axes.
    pub fn new(
        service_times_seconds: &[f64],
        capacities_per_second: &[f64],
        collapse_values: &[f64],
    ) -> Result<Self, CapacityGridError> {
        Self::new_with_prior(
            service_times_seconds,
            capacities_per_second,
            collapse_values,
            CapacityPrior::LogUniform,
        )
    }

    /// Constructs a bounded grid with an explicit scale prior.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, invalid, or oversized axes or prior values.
    pub fn new_with_prior(
        service_times_seconds: &[f64],
        capacities_per_second: &[f64],
        collapse_values: &[f64],
        prior: CapacityPrior,
    ) -> Result<Self, CapacityGridError> {
        validate_axis(service_times_seconds, false)?;
        validate_axis(capacities_per_second, false)?;
        validate_axis(collapse_values, true)?;
        prior.validate()?;

        let knee_cell_count = service_times_seconds
            .len()
            .checked_mul(capacities_per_second.len())
            .and_then(|count| count.checked_mul(collapse_values.len()))
            .ok_or(CapacityGridError::TooLarge)?;
        let cell_count = knee_cell_count
            .checked_add(service_times_seconds.len())
            .ok_or(CapacityGridError::TooLarge)?;
        if capacity_grid_storage_bytes(cell_count, knee_cell_count)
            .is_none_or(|bytes| bytes > CAPACITY_MODEL_BUDGET.storage_bytes_max() as usize)
        {
            return Err(CapacityGridError::TooLarge);
        }

        let service_bounds = log_axis_bounds(service_times_seconds);
        let capacity_bounds = log_axis_bounds(capacities_per_second);
        let collapse_bounds = collapse_axis_bounds(collapse_values);
        let mut service_time_cells = Vec::with_capacity(cell_count);
        let mut service_time_lows = Vec::with_capacity(cell_count);
        let mut service_time_highs = Vec::with_capacity(cell_count);
        let mut capacity_cells = Vec::with_capacity(cell_count);
        let mut capacity_lows = Vec::with_capacity(cell_count);
        let mut capacity_highs = Vec::with_capacity(cell_count);
        let mut collapse_cells = Vec::with_capacity(cell_count);
        let mut collapse_lows = Vec::with_capacity(cell_count);
        let mut collapse_highs = Vec::with_capacity(cell_count);
        for (service_index, &service_time_seconds) in service_times_seconds.iter().enumerate() {
            for (capacity_index, &capacity_per_second) in capacities_per_second.iter().enumerate() {
                for (collapse_index, &collapse) in collapse_values.iter().enumerate() {
                    service_time_cells.push(service_time_seconds);
                    service_time_lows.push(service_bounds[service_index].0);
                    service_time_highs.push(service_bounds[service_index].1);
                    capacity_cells.push(capacity_per_second);
                    capacity_lows.push(capacity_bounds[capacity_index].0);
                    capacity_highs.push(capacity_bounds[capacity_index].1);
                    collapse_cells.push(collapse);
                    collapse_lows.push(collapse_bounds[collapse_index].0);
                    collapse_highs.push(collapse_bounds[collapse_index].1);
                }
            }
        }
        let mut no_knee = vec![0.0_f64; knee_cell_count];
        for (service_index, &service_time_seconds) in service_times_seconds.iter().enumerate() {
            service_time_cells.push(service_time_seconds);
            service_time_lows.push(service_bounds[service_index].0);
            service_time_highs.push(service_bounds[service_index].1);
            capacity_cells.push(0.0_f64);
            capacity_lows.push(0.0_f64);
            capacity_highs.push(0.0_f64);
            collapse_cells.push(0.0_f64);
            collapse_lows.push(0.0_f64);
            collapse_highs.push(0.0_f64);
            no_knee.push(1.0_f64);
        }
        let mut knee_values = service_time_cells
            .iter()
            .take(knee_cell_count)
            .zip(capacity_cells.iter().take(knee_cell_count))
            .map(|(service_time, capacity)| service_time * capacity)
            .collect::<Vec<_>>();
        knee_values.sort_by(f64::total_cmp);
        knee_values.dedup_by(|left, right| left.total_cmp(right).is_eq());
        let mut knee_indexes = Vec::with_capacity(knee_cell_count);
        for (&service_time, &capacity) in service_time_cells
            .iter()
            .take(knee_cell_count)
            .zip(capacity_cells.iter().take(knee_cell_count))
        {
            let knee = service_time * capacity;
            let index = knee_values
                .binary_search_by(|candidate| candidate.total_cmp(&knee))
                .map_err(|_| CapacityGridError::KneeIndex)?;
            knee_indexes.push(u32::try_from(index).map_err(|_| CapacityGridError::TooLarge)?);
        }
        Ok(Self {
            service_times_seconds: service_time_cells,
            service_time_lows,
            service_time_highs,
            capacities_per_second: capacity_cells,
            capacity_lows,
            capacity_highs,
            collapse_values: collapse_cells,
            collapse_lows,
            collapse_highs,
            no_knee,
            knee_values,
            knee_indexes,
            knee_cell_count: u32::try_from(knee_cell_count)
                .map_err(|_| CapacityGridError::TooLarge)?,
            service_time_count: u32::try_from(service_times_seconds.len())
                .map_err(|_| CapacityGridError::TooLarge)?,
            capacity_count: u32::try_from(capacities_per_second.len())
                .map_err(|_| CapacityGridError::TooLarge)?,
            collapse_count: u32::try_from(collapse_values.len())
                .map_err(|_| CapacityGridError::TooLarge)?,
            prior,
        })
    }

    /// Returns the number of grid cells.
    #[must_use]
    pub fn cell_count(&self) -> u32 {
        self.service_times_seconds.len() as u32
    }

    pub(crate) const fn capacity_value_count(&self) -> u32 {
        self.capacity_count
    }

    fn throughput_interval(&self, index: usize, concurrency: f64) -> (f64, f64) {
        let service_low = self.service_time_lows[index];
        let service_high = self.service_time_highs[index];
        if self.no_knee[index] > 0.0_f64 {
            return (concurrency / service_high, concurrency / service_low);
        }
        let capacity_low = self.capacity_lows[index];
        let capacity_high = self.capacity_highs[index];
        let collapse_low = self.collapse_lows[index];
        let collapse_high = self.collapse_highs[index];
        let low = throughput(service_low, capacity_low, collapse_high, false, concurrency).min(
            throughput(
                service_high,
                capacity_low,
                collapse_high,
                false,
                concurrency,
            ),
        );
        let high = if concurrency <= capacity_high * service_high {
            (concurrency / service_low).min(capacity_high)
        } else {
            throughput(
                service_high,
                capacity_high,
                collapse_low,
                false,
                concurrency,
            )
        };
        (low, high)
    }
}

/// One passive resource observation window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResourceWindow {
    concurrency: f64,
    exposure_micros: u64,
    completed_attempts: u32,
    started_attempts: Option<u32>,
}

impl ResourceWindow {
    /// Constructs one eligible resource window.
    ///
    /// # Errors
    ///
    /// Returns an error when concurrency is negative or not finite.
    ///
    /// Returns an error when exposure is not positive and finite.
    pub fn new(
        concurrency: f64,
        exposure_seconds: f64,
        completed_attempts: u32,
    ) -> Result<Self, ResourceWindowError> {
        if !concurrency.is_finite() || concurrency < 0.0_f64 {
            return Err(ResourceWindowError::InvalidValue {
                name: "concurrency",
                value: concurrency,
            });
        }
        validate_positive(exposure_seconds, "exposure_seconds")?;
        let exposure = Duration::try_from_secs_f64(exposure_seconds)
            .map_err(|_| ResourceWindowError::ClockResolution)?;
        if exposure.subsec_nanos() % 1_000 != 0 {
            return Err(ResourceWindowError::ClockResolution);
        }
        let exposure_micros = u64::try_from(exposure.as_micros())
            .map_err(|_| ResourceWindowError::ClockResolution)?;
        if exposure_micros == 0 {
            return Err(ResourceWindowError::ClockResolution);
        }
        Ok(Self {
            concurrency,
            exposure_micros,
            completed_attempts,
            started_attempts: None,
        })
    }

    /// Constructs one resource window with an observed start count.
    ///
    /// # Errors
    ///
    /// Returns an error when concurrency is negative or not finite.
    ///
    /// Returns an error when exposure is not positive and finite.
    pub fn new_with_starts(
        concurrency: f64,
        exposure_seconds: f64,
        completed_attempts: u32,
        started_attempts: u32,
    ) -> Result<Self, ResourceWindowError> {
        let mut window = Self::new(concurrency, exposure_seconds, completed_attempts)?;
        window.started_attempts = Some(started_attempts);
        Ok(window)
    }

    pub(crate) const fn concurrency(&self) -> f64 {
        self.concurrency
    }

    pub(crate) fn exposure_seconds(&self) -> f64 {
        Duration::from_micros(self.exposure_micros).as_secs_f64()
    }

    pub(crate) const fn exposure_micros(&self) -> u64 {
        self.exposure_micros
    }

    pub(crate) const fn completed_attempts(&self) -> u32 {
        self.completed_attempts
    }

    pub(crate) const fn started_attempts(&self) -> Option<u32> {
        self.started_attempts
    }
}

#[derive(Clone, Copy)]
struct StartWindow {
    exposure_seconds: f64,
    started_attempts: Option<u32>,
}

#[derive(Clone, Copy)]
struct RetainedHistory<'a> {
    windows: &'a [StartWindow],
    head: usize,
    length: usize,
}

#[derive(Clone, Copy)]
struct DeathBand {
    low: usize,
    high: usize,
    exposure_seconds: f64,
}

#[derive(Clone, Copy)]
struct ErrorLedger {
    group_budget: f64,
    charged: f64,
}

impl ErrorLedger {
    /// Splits the path error across all solver groups in one cell-window.
    ///
    /// Each group receives `PATH_SOLVER_PROBABILITY_ERROR_MAX / group_count`.
    /// A group divides its grant across its uniformization steps and
    /// contraction.
    fn new(group_count: usize) -> Option<Self> {
        Self::with_budget(group_count, PATH_SOLVER_PROBABILITY_ERROR_MAX)
    }

    fn with_budget(group_count: usize, budget: f64) -> Option<Self> {
        let count = u32::try_from(group_count).ok()?;
        (count > 0).then_some(Self {
            group_budget: budget / f64::from(count),
            charged: 0.0_f64,
        })
    }

    fn charge(&mut self, amount: f64) {
        self.charged += amount;
        debug_assert!(
            self.charged <= PATH_SOLVER_PROBABILITY_ERROR_MAX,
            "the path solver must not exceed its error budget"
        );
    }
}

#[derive(Clone, Copy)]
struct CapacityAllocation {
    cell_count: usize,
    state_count: usize,
    filter_count: usize,
    filter_curve_count: usize,
    transition_count: usize,
    start_history_capacity: usize,
}

#[cfg(test)]
struct CompletionScratch<'a> {
    coefficients: &'a mut [f64],
    convolution: &'a mut [f64],
    binomial: &'a mut [f64],
}

pub(crate) struct CapacityFactor {
    grid: CapacityGrid,
    arrival_shape: f64,
    arrival_rate_seconds: f64,
    concurrency_max: f64,
    exposure_min_seconds: f64,
    prior_weights: Vec<f64>,
    weights: Vec<f64>,
    likelihoods: Vec<f64>,
    hazard_rates_per_second: Vec<f64>,
    contamination_probabilities: Vec<f64>,
    filter_weights: Vec<f64>,
    filter_log_weights: Vec<f64>,
    filter_curve_weights: Vec<f64>,
    start_history: Vec<StartWindow>,
    start_history_head: usize,
    start_history_len: usize,
    predictive_start_history: Vec<StartWindow>,
    state_exposure_seconds: Vec<f64>,
    state_completion_counts: Vec<u32>,
    forward_probabilities: Vec<f64>,
    state_rates: Vec<f64>,
    forward_coefficients: Vec<f64>,
    forward_work: Vec<f64>,
    residual_counts: [u32; RESIDUAL_BIN_COUNT],
    residual_sample_count: u32,
    residual_integrated_hazard: f64,
    markov_clock_rejected: bool,
}

impl CapacityFactor {
    pub(crate) fn new_with_prior(
        grid: CapacityGrid,
        change_rate_per_second: f64,
        arrival_prior: &ArrivalPrior,
        concurrency_max: f64,
        exposure_min_seconds: f64,
        attempt_count_max: u32,
    ) -> Result<Self, CapacityModelError> {
        validate_capacity_observation_contract(
            concurrency_max,
            exposure_min_seconds,
            attempt_count_max,
        )?;
        let cell_count = grid.service_times_seconds.len();
        let state_count = concurrency_max as usize + 1;
        let history_coverage_seconds = (0..cell_count)
            .map(|index| effective_service_time(&grid, index, concurrency_max))
            .fold(0.0_f64, f64::max);
        if !history_coverage_seconds.is_finite() {
            return Err(CapacityModelError::InvalidObservationContract);
        }
        // The ring keeps exact start windows for completion-only
        // prediction. Exposure older than the ring passes to the
        // Gamma-Poisson prehistory marginal, so this bound changes report
        // sharpness, never correctness. The trace likelihood reads the
        // certified trace, not this ring. A deep-collapse tail cell can
        // demand centuries of coverage at maximum concurrency; the clamp
        // keeps such hypotheses affordable instead of rejecting the
        // configuration.
        let history_count = (history_coverage_seconds / exposure_min_seconds).ceil() as usize;
        let start_history_capacity = history_count.saturating_add(1).min(START_HISTORY_SLOT_MAX);
        let artifact = capacity_model_artifact(
            change_rate_per_second,
            arrival_prior.shape(),
            concurrency_max as u32,
        )?;
        let prior_weights = capacity_prior(&grid, &artifact);
        let (hazard_rates_per_second, hazard_weights) = hazard_prior(&artifact)?;
        let (contamination_probabilities, contamination_weights) = contamination_prior(&artifact)?;
        let filter_count = hazard_weights
            .len()
            .checked_mul(contamination_weights.len())
            .ok_or(CapacityModelError::StorageBound)?;
        let filter_curve_count = filter_count
            .checked_mul(cell_count)
            .ok_or(CapacityModelError::StorageBound)?;
        let transition_count = usize::try_from(attempt_count_max)
            .map_err(|_| CapacityModelError::StorageBound)?
            .checked_mul(2)
            .and_then(|count| count.checked_add(1))
            .ok_or(CapacityModelError::StorageBound)?;
        validate_capacity_allocation(
            &grid,
            &artifact,
            CapacityAllocation {
                cell_count,
                state_count,
                filter_count,
                filter_curve_count,
                transition_count,
                start_history_capacity,
            },
            attempt_count_max,
        )?;
        let mut filter_weights = Vec::with_capacity(filter_count);
        let mut filter_curve_weights = Vec::with_capacity(filter_curve_count);
        for hazard_weight in hazard_weights {
            for &contamination_weight in &contamination_weights {
                filter_weights.push(hazard_weight * contamination_weight);
                filter_curve_weights.extend_from_slice(&prior_weights);
            }
        }
        Ok(Self {
            grid,
            arrival_shape: arrival_prior.shape(),
            arrival_rate_seconds: arrival_prior.rate_seconds(),
            concurrency_max,
            exposure_min_seconds,
            weights: prior_weights.clone(),
            prior_weights,
            likelihoods: vec![0.0_f64; cell_count],
            hazard_rates_per_second,
            contamination_probabilities,
            filter_log_weights: vec![0.0_f64; filter_count],
            filter_weights,
            filter_curve_weights,
            start_history: vec![
                StartWindow {
                    exposure_seconds: 0.0_f64,
                    started_attempts: None,
                };
                start_history_capacity
            ],
            start_history_head: 0,
            start_history_len: 0,
            predictive_start_history: vec![
                StartWindow {
                    exposure_seconds: 0.0_f64,
                    started_attempts: None,
                };
                start_history_capacity
            ],
            state_exposure_seconds: vec![0.0_f64; state_count],
            state_completion_counts: vec![0; state_count],
            forward_probabilities: vec![0.0_f64; state_count],
            state_rates: vec![0.0_f64; state_count],
            forward_coefficients: vec![0.0_f64; state_count],
            forward_work: vec![0.0_f64; state_count],
            residual_counts: [0; RESIDUAL_BIN_COUNT],
            residual_sample_count: 0,
            residual_integrated_hazard: 0.0_f64,
            markov_clock_rejected: false,
        })
    }

    pub(crate) const fn posterior_value_count(&self) -> u32 {
        self.grid.capacity_value_count()
    }

    pub(crate) fn curve_posterior_value_count(&self) -> u32 {
        self.grid.cell_count()
    }

    pub(crate) fn artifact(
        &self,
        change_rate_per_second: f64,
    ) -> Result<PriorArtifact, CapacityModelError> {
        let artifact = capacity_model_artifact(
            change_rate_per_second,
            self.arrival_shape,
            self.concurrency_max as u32,
        )?;
        Ok(PriorArtifact::new(
            artifact.identity,
            artifact.budget,
            artifact.coverage.into_boxed_slice(),
        ))
    }

    pub(crate) fn contamination_posterior_value_count(&self) -> u32 {
        self.contamination_probabilities.len() as u32
    }

    pub(crate) fn write_contamination_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let quality_count = self.contamination_probabilities.len();
        if values.len() != quality_count || probabilities.len() != quality_count {
            return Err(PosteriorError::BufferLength {
                expected: self.contamination_posterior_value_count(),
            });
        }
        values.copy_from_slice(&self.contamination_probabilities);
        probabilities.fill(0.0_f64);
        for filter_weights in self.filter_weights.chunks_exact(quality_count) {
            for (probability, weight) in probabilities.iter_mut().zip(filter_weights) {
                *probability += *weight;
            }
        }
        Ok(())
    }

    pub(crate) fn curve_and_probability(&self, index: usize) -> (CapacityCurve, f64) {
        let curve = if self.grid.no_knee[index] > 0.0_f64 {
            CapacityCurve::NoKnee {
                service_time_seconds: self.grid.service_times_seconds[index],
            }
        } else {
            CapacityCurve::Knee {
                service_time_seconds: self.grid.service_times_seconds[index],
                capacity_per_second: self.grid.capacities_per_second[index],
                collapse: self.grid.collapse_values[index],
            }
        };
        (curve, self.weights[index])
    }

    pub(crate) fn write_throughput_posterior(
        &self,
        concurrency: f64,
        cells: &mut [ThroughputPosteriorCell],
    ) -> Result<(), PosteriorError> {
        if cells.len() != self.weights.len() {
            return Err(PosteriorError::BufferLength {
                expected: self.grid.cell_count(),
            });
        }
        for (index, cell) in cells.iter_mut().enumerate() {
            let (low, high) = self.grid.throughput_interval(index, concurrency);
            cell.throughput_per_second = throughput(
                self.grid.service_times_seconds[index],
                self.grid.capacities_per_second[index],
                self.grid.collapse_values[index],
                self.grid.no_knee[index] > 0.0_f64,
                concurrency,
            );
            cell.throughput_low_per_second = low;
            cell.throughput_high_per_second = high;
            cell.probability = self.weights[index];
        }
        Ok(())
    }

    pub(crate) fn write_completion_posterior(
        &mut self,
        window: &ResourceWindow,
        cells: &mut [CompletionPosteriorCell],
    ) -> Result<(), PosteriorError> {
        if cells.len() != self.weights.len() {
            return Err(PosteriorError::BufferLength {
                expected: self.grid.cell_count(),
            });
        }
        self.predictive_start_history
            .copy_from_slice(&self.start_history);
        let mut head = self.start_history_head;
        let mut length = self.start_history_len;
        record_start_window(
            &mut self.predictive_start_history,
            &mut head,
            &mut length,
            window,
        );
        for (index, cell) in cells.iter_mut().enumerate() {
            cell.mean = completion_expectation(
                &self.grid,
                index,
                RetainedHistory {
                    windows: &self.predictive_start_history,
                    head,
                    length,
                },
                window,
                self.arrival_shape,
                self.arrival_rate_seconds,
            );
            cell.probability = self.weights[index];
        }
        Ok(())
    }

    pub(crate) const fn service_time_posterior_value_count(&self) -> u32 {
        self.grid.service_time_count
    }

    pub(crate) const fn collapse_posterior_value_count(&self) -> u32 {
        self.grid.collapse_count
    }

    pub(crate) fn knee_posterior_value_count(&self) -> u32 {
        self.grid.knee_values.len() as u32
    }

    pub(crate) fn transition(&mut self, elapsed: Duration) {
        let cell_count = self.weights.len();
        let quality_count = self.contamination_probabilities.len();
        for (hazard_index, hazard) in self.hazard_rates_per_second.iter().enumerate() {
            let transition = ChangePointKernel::new(*hazard).probabilities(elapsed);
            for quality_index in 0..quality_count {
                let filter = hazard_index * quality_count + quality_index;
                let start = filter * cell_count;
                let end = start + cell_count;
                for (weight, prior) in self.filter_curve_weights[start..end]
                    .iter_mut()
                    .zip(&self.prior_weights)
                {
                    *weight = transition.retained * *weight + transition.redrawn * prior;
                }
            }
        }
        self.mix_filters();
    }

    /// Applies one certified occupancy-trace observation.
    ///
    /// The update costs `O(G (C + J (C N + D)) + G F)`. `G` is the curve count.
    /// `J` is the start-group count. `C` is the state count. `N` is the
    /// largest certified Poisson term count. `D` is the contracted death count.
    /// `D` is zero when contraction cannot occur. `F` is the filter count.
    /// An accepted window also enters the start-history ring that
    /// feeds [`Self::write_completion_posterior`].
    pub(crate) fn update(&mut self, evidence: OccupancyTraceEvidence<'_>) {
        let window = evidence.window();
        debug_assert!(
            evidence.mean_concurrency() <= self.concurrency_max,
            "the observation buffer enforces maximum resource concurrency"
        );
        debug_assert!(
            window.exposure_seconds() >= self.exposure_min_seconds,
            "the observation buffer enforces minimum resource exposure"
        );
        fold_trace(
            evidence,
            &mut self.state_exposure_seconds,
            &mut self.state_completion_counts,
        );
        self.update_residual_check(evidence);
        for index in 0..self.likelihoods.len() {
            fill_state_rates(&self.grid, index, &mut self.state_rates);
            let raw = path_log_score_with_rates(
                &self.state_rates,
                &self.state_exposure_seconds,
                &self.state_completion_counts,
            );
            let normalizer = feasibility_probability_with_rates(
                &self.state_rates,
                evidence,
                &mut self.forward_probabilities,
                &mut self.forward_coefficients,
                &mut self.forward_work,
            );
            self.likelihoods[index] = normalizer
                .filter(|value| *value > 0.0_f64)
                .map_or(f64::NEG_INFINITY, |value| raw - value.ln());
        }
        let prior_predictive = log_weighted_sum(&self.prior_weights, &self.likelihoods);
        if prior_predictive.is_finite() && self.update_filters(prior_predictive) {
            record_start_window(
                &mut self.start_history,
                &mut self.start_history_head,
                &mut self.start_history_len,
                window,
            );
        }
    }

    pub(crate) fn omit_observation(&mut self) {
        self.residual_integrated_hazard = 0.0_f64;
    }

    pub(crate) const fn markov_clock_rejected(&self) -> bool {
        self.markov_clock_rejected
    }

    pub(crate) fn clock_check(&self) -> CapacityClockCheck {
        if self.residual_sample_count == 0 {
            return CapacityClockCheck {
                sample_count: 0,
                maximum_distance: 0.0_f64,
                rejection_threshold: f64::INFINITY,
                rejected: false,
            };
        }
        let sample_count = f64::from(self.residual_sample_count);
        let dkw_bound = (-(CAPACITY_MODEL_BUDGET.boundary_probability_max() * 0.5_f64).ln()
            / (2.0_f64 * sample_count))
            .sqrt();
        let lattice_bound = 1.0_f64 / RESIDUAL_BIN_COUNT_F64;
        let mut cumulative = 0_u32;
        let mut maximum_distance = 0.0_f64;
        let mut expected = lattice_bound;
        for (index, count) in self.residual_counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            let empirical = f64::from(cumulative) / sample_count;
            maximum_distance = maximum_distance.max((empirical - expected).abs());
            expected = if index + 1 == RESIDUAL_BIN_COUNT {
                1.0_f64
            } else {
                expected + lattice_bound
            };
        }
        CapacityClockCheck {
            sample_count: self.residual_sample_count,
            maximum_distance,
            rejection_threshold: dkw_bound + lattice_bound,
            rejected: self.markov_clock_rejected,
        }
    }

    fn update_residual_check(&mut self, evidence: OccupancyTraceEvidence<'_>) {
        let mut state = evidence.initial_busy_slots() as usize;
        let mut previous_offset = 0_u64;
        for ((&offset, &completed), &started) in evidence
            .offsets_micros()
            .iter()
            .zip(evidence.completion_groups())
            .zip(evidence.start_groups())
        {
            let exposure = Duration::from_micros(offset - previous_offset).as_secs_f64();
            self.residual_integrated_hazard += exposure * self.posterior_state_rate(state);
            for _ in 0..completed {
                let uniform = 1.0_f64 - (-self.residual_integrated_hazard).exp();
                let bin = ((uniform * RESIDUAL_BIN_COUNT_F64) as usize).min(RESIDUAL_BIN_COUNT - 1);
                self.residual_counts[bin] = self.residual_counts[bin].saturating_add(1);
                self.residual_sample_count = self.residual_sample_count.saturating_add(1);
                self.residual_integrated_hazard = 0.0_f64;
                state -= 1;
            }
            state += started as usize;
            previous_offset = offset;
        }
        let tail = Duration::from_micros(evidence.window().exposure_micros() - previous_offset)
            .as_secs_f64();
        self.residual_integrated_hazard += tail * self.posterior_state_rate(state);
        if self.residual_sample_count == 0 {
            return;
        }
        let alpha = CAPACITY_MODEL_BUDGET.boundary_probability_max();
        let sample_count = f64::from(self.residual_sample_count);
        let dkw_bound = (-(alpha * 0.5_f64).ln() / (2.0_f64 * sample_count)).sqrt();
        let lattice_bound = 1.0_f64 / RESIDUAL_BIN_COUNT_F64;
        let mut cumulative = 0_u32;
        let mut distance = 0.0_f64;
        let mut expected = lattice_bound;
        for (index, count) in self.residual_counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            let empirical = f64::from(cumulative) / sample_count;
            distance = distance.max((empirical - expected).abs());
            expected = if index + 1 == RESIDUAL_BIN_COUNT {
                1.0_f64
            } else {
                expected + lattice_bound
            };
        }
        self.markov_clock_rejected = distance > dkw_bound + lattice_bound;
    }

    fn posterior_state_rate(&self, state: usize) -> f64 {
        self.weights
            .iter()
            .enumerate()
            .map(|(index, weight)| weight * state_rate(&self.grid, index, state))
            .sum()
    }

    pub(crate) fn expected_capacity(&self, simd_level: Level) -> f64 {
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return 0.0_f64;
        }
        dispatch!(simd_level, simd => weighted_sum(simd, &self.weights, &self.grid.capacities_per_second))
            / knee_probability
    }

    pub(crate) fn expected_service_time(&self, simd_level: Level) -> f64 {
        dispatch!(simd_level, simd => weighted_sum(
            simd,
            &self.weights,
            &self.grid.service_times_seconds,
        ))
    }

    pub(crate) fn capacity_quantile(&self, probability: f64) -> f64 {
        let collapse_count = self.grid.collapse_count as usize;
        let capacity_count = self.grid.capacity_count as usize;
        let service_count = self.grid.service_time_count as usize;
        let service_stride = capacity_count * collapse_count;
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return 0.0_f64;
        }
        let mut cumulative = 0.0_f64;
        for capacity in 0..capacity_count {
            for service in 0..service_count {
                let start = service * service_stride + capacity * collapse_count;
                let end = start + collapse_count;
                cumulative += self.weights[start..end].iter().sum::<f64>() / knee_probability;
            }
            if cumulative >= probability {
                return self.grid.capacities_per_second[capacity * collapse_count];
            }
        }
        self.grid.capacities_per_second[(capacity_count - 1) * collapse_count]
    }

    pub(crate) fn write_capacity_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let capacity_count = self.grid.capacity_count as usize;
        if values.len() != capacity_count || probabilities.len() != capacity_count {
            return Err(PosteriorError::BufferLength {
                expected: self.grid.capacity_count,
            });
        }
        probabilities.fill(0.0_f64);
        let collapse_count = self.grid.collapse_count as usize;
        let service_stride = capacity_count * collapse_count;
        for capacity in 0..capacity_count {
            values[capacity] = self.grid.capacities_per_second[capacity * collapse_count];
            for service in 0..self.grid.service_time_count as usize {
                let start = service * service_stride + capacity * collapse_count;
                let end = start + collapse_count;
                probabilities[capacity] += self.weights[start..end].iter().sum::<f64>();
            }
        }
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return Ok(());
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(())
    }

    pub(crate) fn write_service_time_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let service_count = self.grid.service_time_count as usize;
        if values.len() != service_count || probabilities.len() != service_count {
            return Err(PosteriorError::BufferLength {
                expected: self.grid.service_time_count,
            });
        }
        let service_stride = self.grid.capacity_count as usize * self.grid.collapse_count as usize;
        for service in 0..service_count {
            let start = service * service_stride;
            values[service] = self.grid.service_times_seconds[start];
            probabilities[service] = self.weights[start..start + service_stride]
                .iter()
                .sum::<f64>()
                + self.weights[self.grid.knee_cell_count as usize + service];
        }
        Ok(())
    }

    pub(crate) fn write_collapse_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let collapse_count = self.grid.collapse_count as usize;
        if values.len() != collapse_count || probabilities.len() != collapse_count {
            return Err(PosteriorError::BufferLength {
                expected: self.grid.collapse_count,
            });
        }
        probabilities.fill(0.0_f64);
        values.copy_from_slice(&self.grid.collapse_values[..collapse_count]);
        for (cell, weight) in self
            .weights
            .iter()
            .take(self.grid.knee_cell_count as usize)
            .enumerate()
        {
            probabilities[cell % collapse_count] += weight;
        }
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return Ok(());
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(())
    }

    pub(crate) fn write_knee_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let knee_count = self.grid.knee_values.len();
        if values.len() != knee_count || probabilities.len() != knee_count {
            return Err(PosteriorError::BufferLength {
                expected: self.knee_posterior_value_count(),
            });
        }
        values.copy_from_slice(&self.grid.knee_values);
        probabilities.fill(0.0_f64);
        for (&index, &weight) in self.grid.knee_indexes.iter().zip(&self.weights) {
            probabilities[index as usize] += weight;
        }
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return Ok(());
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(())
    }

    pub(crate) fn saturation_probability(&self, simd_level: Level, concurrency: f64) -> f64 {
        dispatch!(simd_level, simd => saturation_probability(
            simd,
            &self.weights,
            &self.grid.service_times_seconds,
            &self.grid.capacities_per_second,
            &self.grid.no_knee,
            concurrency,
        ))
    }

    pub(crate) fn fill_throughput(
        simd_level: Level,
        curve: CapacityCurve,
        concurrency: &[f64],
        output: &mut [f64],
    ) {
        assert_eq!(
            concurrency.len(),
            output.len(),
            "each candidate concurrency must have one throughput output"
        );
        dispatch!(simd_level, simd => curve_throughput(simd, curve, concurrency, output));
    }

    fn knee_probability(&self) -> f64 {
        self.weights[..self.grid.knee_cell_count as usize]
            .iter()
            .sum()
    }

    pub(crate) fn no_knee_probability(&self) -> f64 {
        self.weights[self.grid.knee_cell_count as usize..]
            .iter()
            .sum()
    }

    /// Updates the joint hazard, quality, and persistent-curve posterior.
    ///
    /// An identifiable persistent curve has positive divergence from the
    /// prior predictive. Its run evidence then grows in expectation.
    fn update_filters(&mut self, prior_predictive: f64) -> bool {
        let cell_count = self.weights.len();
        let quality_count = self.contamination_probabilities.len();
        for (filter, curve_weights) in self
            .filter_curve_weights
            .chunks_exact_mut(cell_count)
            .enumerate()
        {
            let contamination = self.contamination_probabilities[filter % quality_count];
            let maximum = self
                .likelihoods
                .iter()
                .map(|likelihood| {
                    log_contamination_mixture(*likelihood, prior_predictive, contamination)
                })
                .fold(f64::NEG_INFINITY, f64::max);
            let predictive = curve_weights
                .iter()
                .zip(&self.likelihoods)
                .map(|(weight, likelihood)| {
                    weight
                        * (log_contamination_mixture(*likelihood, prior_predictive, contamination)
                            - maximum)
                            .exp()
                })
                .sum::<f64>();
            if predictive <= 0.0_f64 {
                return false;
            }
            self.filter_log_weights[filter] =
                self.filter_weights[filter].ln() + maximum + predictive.ln();
            for (weight, likelihood) in curve_weights.iter_mut().zip(&self.likelihoods) {
                let mixture =
                    log_contamination_mixture(*likelihood, prior_predictive, contamination);
                *weight *= (mixture - maximum).exp() / predictive;
            }
        }
        let maximum = self
            .filter_log_weights
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let total = self
            .filter_log_weights
            .iter()
            .map(|weight| (*weight - maximum).exp())
            .sum::<f64>();
        if total <= 0.0_f64 {
            return false;
        }
        for (weight, log_weight) in self.filter_weights.iter_mut().zip(&self.filter_log_weights) {
            *weight = (*log_weight - maximum).exp() / total;
        }
        self.mix_filters();
        true
    }

    fn mix_filters(&mut self) {
        self.weights.fill(0.0_f64);
        let cell_count = self.weights.len();
        for (filter_weight, curve_weights) in self
            .filter_weights
            .iter()
            .zip(self.filter_curve_weights.chunks_exact(cell_count))
        {
            for (weight, curve_weight) in self.weights.iter_mut().zip(curve_weights) {
                *weight += filter_weight * curve_weight;
            }
        }
    }
}

fn capacity_prior(grid: &CapacityGrid, artifact: &CapacityModelArtifact) -> Vec<f64> {
    let mut weights = match grid.prior {
        CapacityPrior::LogUniform => log_uniform_capacity_prior(grid, artifact),
        CapacityPrior::LogNormal {
            service_time_median_seconds,
            capacity_median_per_second,
            log_standard_deviation,
        } => log_normal_capacity_prior(
            grid,
            service_time_median_seconds,
            capacity_median_per_second,
            log_standard_deviation,
            artifact,
        ),
    };
    let service_stride = grid.capacity_count as usize * grid.collapse_count as usize;
    let mut no_knee_weights = Vec::with_capacity(grid.service_time_count as usize);
    for service in 0..grid.service_time_count as usize {
        let start = service * service_stride;
        let service_mass = weights[start..start + service_stride].iter().sum::<f64>();
        no_knee_weights.push(service_mass * artifact.no_knee_probability);
    }
    for weight in &mut weights {
        *weight *= 1.0_f64 - artifact.no_knee_probability;
    }
    weights.extend(no_knee_weights);
    weights
}

fn validate_capacity_observation_contract(
    concurrency_max: f64,
    exposure_min_seconds: f64,
    attempt_count_max: u32,
) -> Result<(), CapacityModelError> {
    if concurrency_max.is_finite()
        && concurrency_max > 0.0_f64
        && exposure_min_seconds.is_finite()
        && exposure_min_seconds > 0.0_f64
        && attempt_count_max > 0
    {
        Ok(())
    } else {
        Err(CapacityModelError::InvalidObservationContract)
    }
}

fn validate_capacity_allocation(
    grid: &CapacityGrid,
    artifact: &CapacityModelArtifact,
    allocation: CapacityAllocation,
    attempt_count_max: u32,
) -> Result<(), CapacityModelError> {
    let storage_bytes = capacity_storage_bytes(
        allocation.filter_curve_count,
        allocation.cell_count,
        allocation.filter_count,
        allocation.state_count,
        allocation.transition_count,
        allocation.start_history_capacity,
    )?
    .checked_add(
        capacity_grid_storage_bytes(allocation.cell_count, grid.knee_cell_count as usize)
            .ok_or(CapacityModelError::StorageBound)?,
    )
    .ok_or(CapacityModelError::StorageBound)?;
    let storage_bytes = artifact
        .coverage
        .len()
        .checked_mul(size_of::<PriorCoverageRecord>())
        .and_then(|coverage_bytes| storage_bytes.checked_add(coverage_bytes))
        .ok_or(CapacityModelError::StorageBound)?;
    let band_count = allocation.state_count.min(allocation.transition_count);
    let cells =
        u64::try_from(allocation.cell_count).map_err(|_| CapacityModelError::StorageBound)?;
    let bands = u64::try_from(band_count).map_err(|_| CapacityModelError::StorageBound)?;
    let update_operation_count = u64::from(attempt_count_max)
        .checked_mul(bands)
        .and_then(|value| value.checked_mul(bands))
        .and_then(|value| value.checked_mul(cells))
        .ok_or(CapacityModelError::StorageBound)?;
    if prior_artifact_contract_holds(
        artifact.identity,
        artifact.budget,
        &artifact.coverage,
        allocation.filter_curve_count,
        storage_bytes,
        update_operation_count,
    ) {
        Ok(())
    } else {
        Err(CapacityModelError::StorageBound)
    }
}

fn capacity_storage_bytes(
    filter_curve_count: usize,
    cell_count: usize,
    filter_count: usize,
    state_count: usize,
    transition_count: usize,
    start_history_capacity: usize,
) -> Result<usize, CapacityModelError> {
    filter_curve_count
        .checked_add(
            cell_count
                .checked_mul(3)
                .ok_or(CapacityModelError::StorageBound)?,
        )
        .and_then(|count| {
            filter_count
                .checked_mul(2)
                .and_then(|filter_bytes| count.checked_add(filter_bytes))
        })
        .and_then(|count| count.checked_mul(size_of::<f64>()))
        .and_then(|bytes| {
            state_count
                .checked_mul(5 * size_of::<f64>() + size_of::<u32>())
                .and_then(|state_bytes| bytes.checked_add(state_bytes))
        })
        .and_then(|bytes| {
            transition_count
                .checked_mul(size_of::<u64>() + 2 * size_of::<u32>())
                .and_then(|transition_bytes| bytes.checked_add(transition_bytes))
        })
        .and_then(|bytes| {
            start_history_capacity
                .checked_mul(2)
                .and_then(|count| count.checked_mul(size_of::<StartWindow>()))
                .and_then(|history_bytes| bytes.checked_add(history_bytes))
        })
        .ok_or(CapacityModelError::StorageBound)
}

fn capacity_grid_storage_bytes(cell_count: usize, knee_cell_count: usize) -> Option<usize> {
    cell_count
        .checked_mul(10)
        .and_then(|count| count.checked_mul(size_of::<f64>()))
        .and_then(|bytes| {
            knee_cell_count
                .checked_mul(size_of::<f64>() + size_of::<u32>())
                .and_then(|knee_bytes| bytes.checked_add(knee_bytes))
        })
}

/// Discretizes the Gamma hazard prior with exact linear-rate cell masses.
///
/// Log spacing only sets cell boundaries. Gamma integration includes the
/// log-cell-width Jacobian, so this function does not change the exponent.
fn capacity_model_artifact(
    mean_per_second: f64,
    shape: f64,
    busy_slot_max: u32,
) -> Result<CapacityModelArtifact, CapacityModelError> {
    if !mean_per_second.is_finite()
        || mean_per_second <= 0.0_f64
        || !shape.is_finite()
        || shape <= 0.0_f64
    {
        return Err(CapacityModelError::InvalidHazardPrior);
    }
    let distribution = Gamma::new(shape, mean_per_second / shape)
        .map_err(|_| CapacityModelError::InvalidHazardPrior)?;
    // Keep half of the tail budget for inverse-CDF and CDF roundoff.
    let tail = CAPACITY_MODEL_BUDGET.boundary_probability_max() * 0.25_f64;
    let low = distribution.inverse_cdf(tail);
    let high = distribution.inverse_cdf(1.0_f64 - tail);
    if !low.is_finite() || low <= 0.0_f64 || !high.is_finite() || high <= low {
        return Err(CapacityModelError::InvalidHazardPrior);
    }
    let lower_tail = distribution.cdf(low);
    let upper_tail = 1.0_f64 - distribution.cdf(high);
    let random_stream = mean_per_second.to_bits() ^ shape.to_bits().rotate_left(32) | 1;
    let mut coverage = Vec::with_capacity(busy_slot_max as usize + 4);
    coverage.extend([
        PriorCoverageRecord::new(
            0.0_f64,
            1.0_f64,
            0.0_f64,
            0.0_f64,
            OBSERVATION_PROBABILITY_ERROR_MAX,
        ),
        PriorCoverageRecord::new(
            low,
            high,
            lower_tail,
            upper_tail,
            HAZARD_TRANSITION_PROBABILITY_ERROR_MAX,
        ),
        PriorCoverageRecord::new(
            0.0_f64,
            1.0_f64,
            0.0_f64,
            PATH_SOLVER_PROBABILITY_ERROR_MAX,
            PATH_SOLVER_PROBABILITY_ERROR_MAX,
        ),
    ]);
    for state in 0..=busy_slot_max {
        coverage.push(PriorCoverageRecord::new(
            f64::from(state),
            f64::from(state) + 1.0_f64,
            0.0_f64,
            0.0_f64,
            0.0_f64,
        ));
    }
    let artifact = CapacityModelArtifact {
        identity: PriorArtifactIdentity::new(
            CAPACITY_MODEL_ARTIFACT_SOURCE,
            CAPACITY_MODEL_ARTIFACT_VERSION,
            random_stream ^ 0x5736_4556_454e_5453,
        ),
        budget: CAPACITY_MODEL_BUDGET,
        coverage,
        hazard_mean_per_second: mean_per_second,
        hazard_shape: shape,
        observation_quality_alpha: 1.0_f64,
        observation_quality_beta: 9.0_f64,
        no_knee_probability: 0.5_f64,
        no_collapse_probability: 0.5_f64,
        markov_clock_assumption: MarkovClockAssumption::MemorylessAggregateCompletions,
        start_delay_evidence: StartDelayEvidence::DiscardedByAcceptedStartConditioning,
    };
    if !prior_artifact_contract_holds(
        artifact.identity,
        artifact.budget,
        &artifact.coverage,
        1,
        1,
        1,
    ) {
        return Err(CapacityModelError::InvalidHazardPrior);
    }
    if artifact.coverage[SOLVER_COVERAGE_INDEX].tail_probability()
        > PATH_SOLVER_PROBABILITY_ERROR_MAX
        || artifact.budget.path_time_error_seconds() != REPORT_CLOCK_ERROR_SECONDS
        || artifact.markov_clock_assumption != MarkovClockAssumption::MemorylessAggregateCompletions
        || artifact.start_delay_evidence != StartDelayEvidence::DiscardedByAcceptedStartConditioning
    {
        return Err(CapacityModelError::InvalidObservationContract);
    }
    Ok(artifact)
}

fn hazard_prior(
    artifact: &CapacityModelArtifact,
) -> Result<(Vec<f64>, Vec<f64>), CapacityModelError> {
    let distribution = Gamma::new(
        artifact.hazard_shape,
        artifact.hazard_mean_per_second / artifact.hazard_shape,
    )
    .map_err(|_| CapacityModelError::InvalidHazardPrior)?;
    let coverage = artifact.coverage[HAZARD_COVERAGE_INDEX];
    let low = coverage.lower_endpoint();
    let high = coverage.upper_endpoint();
    // The maximum derivative of exp(-x) with respect to log(x) is 1/e.
    let log_width_max = E * coverage.decision_cost_error();
    let interval_count = ((high / low).ln() / log_width_max).ceil() as usize;
    let count = interval_count
        .checked_add(1)
        .ok_or(CapacityModelError::StorageBound)?;
    if count
        .checked_mul(2 * size_of::<f64>())
        .is_none_or(|bytes| bytes > CAPACITY_MODEL_STORAGE_BYTES_MAX)
    {
        return Err(CapacityModelError::StorageBound);
    }
    let log_low = low.ln();
    let interval_count_f64 =
        f64::from(u32::try_from(interval_count).map_err(|_| CapacityModelError::StorageBound)?);
    let log_step = (high.ln() - log_low) / interval_count_f64;
    let rates = (0..count)
        .map(|index| {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            (log_low + f64::from(index) * log_step).exp()
        })
        .collect::<Vec<_>>();
    let lower_tail = distribution.cdf(rates[0]);
    let upper_tail = 1.0_f64 - distribution.cdf(rates[count - 1]);
    if lower_tail + upper_tail > artifact.budget.boundary_probability_max() {
        return Err(CapacityModelError::HazardTailMass);
    }
    let mut weights = Vec::with_capacity(count);
    for index in 0..count {
        let lower = if index == 0 {
            0.0_f64
        } else {
            (rates[index - 1] * rates[index]).sqrt()
        };
        let upper = if index + 1 == count {
            f64::INFINITY
        } else {
            (rates[index] * rates[index + 1]).sqrt()
        };
        weights.push(distribution.cdf(upper) - distribution.cdf(lower));
    }
    normalize(&mut weights);
    Ok((rates, weights))
}

fn contamination_prior(
    artifact: &CapacityModelArtifact,
) -> Result<(Vec<f64>, Vec<f64>), CapacityModelError> {
    let distribution = Beta::new(
        artifact.observation_quality_alpha,
        artifact.observation_quality_beta,
    )
    .map_err(|_| CapacityModelError::InvalidObservationQualityPrior)?;
    let count = (0.5_f64 / artifact.coverage[OBSERVATION_COVERAGE_INDEX].decision_cost_error())
        .ceil() as usize;
    if count == 0
        || count
            .checked_mul(2 * size_of::<f64>())
            .is_none_or(|bytes| bytes > CAPACITY_MODEL_STORAGE_BYTES_MAX)
    {
        return Err(CapacityModelError::InvalidObservationQualityPrior);
    }
    let count_u32 =
        u32::try_from(count).map_err(|_| CapacityModelError::InvalidObservationQualityPrior)?;
    let width = 1.0_f64 / f64::from(count_u32);
    let probabilities = (0..count)
        .map(|index| {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            (f64::from(index) + 0.5_f64) * width
        })
        .collect::<Vec<_>>();
    let mut weights = (0..count)
        .map(|index| {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            let lower = f64::from(index) * width;
            let upper = f64::from(index + 1) * width;
            distribution.cdf(upper) - distribution.cdf(lower)
        })
        .collect::<Vec<_>>();
    normalize(&mut weights);
    Ok((probabilities, weights))
}

fn normalize(weights: &mut [f64]) {
    let total = weights.iter().sum::<f64>();
    if total > 0.0_f64 {
        for weight in weights {
            *weight /= total;
        }
    }
}

fn record_start_window(
    history: &mut [StartWindow],
    head: &mut usize,
    length: &mut usize,
    window: &ResourceWindow,
) {
    history[*head] = StartWindow {
        exposure_seconds: window.exposure_seconds(),
        started_attempts: window.started_attempts,
    };
    *head = (*head + 1) % history.len();
    *length = (*length + 1).min(history.len());
}

fn effective_service_time(grid: &CapacityGrid, index: usize, concurrency: f64) -> f64 {
    concurrency
        / throughput(
            grid.service_times_seconds[index],
            grid.capacities_per_second[index],
            grid.collapse_values[index],
            grid.no_knee[index] > 0.0_f64,
            concurrency,
        )
}

fn state_rate(grid: &CapacityGrid, index: usize, state: usize) -> f64 {
    let state = u32::try_from(state).map_or(f64::from(u32::MAX), f64::from);
    throughput(
        grid.service_times_seconds[index],
        grid.capacities_per_second[index],
        grid.collapse_values[index],
        grid.no_knee[index] > 0.0_f64,
        state,
    )
}

fn fill_state_rates(grid: &CapacityGrid, index: usize, rates: &mut [f64]) {
    for (state, rate) in rates.iter_mut().enumerate() {
        *rate = state_rate(grid, index, state);
    }
}

fn fold_trace(
    evidence: OccupancyTraceEvidence<'_>,
    exposure_seconds: &mut [f64],
    completion_counts: &mut [u32],
) {
    exposure_seconds.fill(0.0_f64);
    completion_counts.fill(0);
    let mut state = evidence.initial_busy_slots() as usize;
    let mut previous_offset = 0_u64;
    for ((&offset, &completed), &started) in evidence
        .offsets_micros()
        .iter()
        .zip(evidence.completion_groups())
        .zip(evidence.start_groups())
    {
        exposure_seconds[state] += Duration::from_micros(offset - previous_offset).as_secs_f64();
        for _ in 0..completed {
            completion_counts[state] = completion_counts[state].saturating_add(1);
            state -= 1;
        }
        state += started as usize;
        previous_offset = offset;
    }
    exposure_seconds[state] +=
        Duration::from_micros(evidence.window().exposure_micros() - previous_offset).as_secs_f64();
}

fn path_log_score_with_rates(
    rates: &[f64],
    exposure_seconds: &[f64],
    completion_counts: &[u32],
) -> f64 {
    let mut score = 0.0_f64;
    for state in 1..exposure_seconds.len() {
        let rate = rates[state];
        score -= exposure_seconds[state] * rate;
        if completion_counts[state] > 0 {
            if rate <= 0.0_f64 {
                return f64::NEG_INFINITY;
            }
            score += f64::from(completion_counts[state]) * rate.ln();
        }
    }
    score
}

fn feasibility_probability_with_rates(
    rates: &[f64],
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<f64> {
    feasibility_probability_and_charge(rates, evidence, probabilities, coefficients, work)
        .map(|(probability, _)| probability)
}

fn feasibility_probability_and_charge(
    rates: &[f64],
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<(f64, f64)> {
    feasibility_probability_with_budget(
        rates,
        evidence,
        probabilities,
        coefficients,
        work,
        PATH_SOLVER_PROBABILITY_ERROR_MAX,
    )
}

fn feasibility_probability_with_budget(
    rates: &[f64],
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
    budget: f64,
) -> Option<(f64, f64)> {
    let c_max = probabilities.len() - 1;
    let total_starts = evidence.window().started_attempts()? as usize;
    let initial = evidence.initial_busy_slots() as usize;
    if initial
        .checked_add(total_starts)
        .is_some_and(|value| value <= c_max)
    {
        return Some((1.0_f64, 0.0_f64));
    }
    let group_count = evidence
        .start_groups()
        .iter()
        .filter(|starts| **starts > 0)
        .count();
    let mut ledger = ErrorLedger::with_budget(group_count, budget)?;
    probabilities.fill(0.0_f64);
    probabilities[initial] = 1.0_f64;
    let mut safe_mass = 0.0_f64;
    let mut remaining_starts = total_starts;
    let mut low = if remaining_starts > c_max {
        0
    } else {
        c_max - remaining_starts + 1
    };
    let mut high = initial;
    let mut previous_offset = 0_u64;
    for (&offset, &starts) in evidence
        .offsets_micros()
        .iter()
        .zip(evidence.start_groups())
    {
        if starts == 0 {
            continue;
        }
        let band_mass = probabilities[low..=high].iter().sum::<f64>();
        let contraction_budget = pure_death_step_with_rates(
            rates,
            DeathBand {
                low,
                high,
                exposure_seconds: Duration::from_micros(offset - previous_offset).as_secs_f64(),
            },
            probabilities,
            coefficients,
            work,
            &mut ledger,
        )?;
        safe_mass += (band_mass - probabilities[low..=high].iter().sum::<f64>()).max(0.0_f64);
        high = contract_death_band(
            rates,
            DeathBand {
                low,
                high,
                exposure_seconds: Duration::from_micros(offset - previous_offset).as_secs_f64(),
            },
            probabilities,
            contraction_budget,
            &mut ledger,
        );
        let starts = starts as usize;
        for state in (low..=high).rev() {
            let shifted = state.saturating_add(starts);
            if shifted <= c_max {
                probabilities[shifted] = probabilities[state];
            }
            probabilities[state] = 0.0_f64;
        }
        high = high.saturating_add(starts).min(c_max);
        remaining_starts = remaining_starts.saturating_sub(starts);
        if remaining_starts <= c_max {
            let safe_high = c_max - remaining_starts;
            for probability in &mut probabilities[..=safe_high.min(high)] {
                safe_mass += *probability;
                *probability = 0.0_f64;
            }
            low = safe_high + 1;
        } else {
            low = 0;
        }
        previous_offset = offset;
        if low > high {
            return Some((safe_mass.min(1.0_f64), ledger.charged));
        }
    }
    Some((
        (safe_mass + probabilities[low..=high].iter().sum::<f64>()).clamp(0.0_f64, 1.0_f64),
        ledger.charged,
    ))
}

#[cfg(test)]
fn path_log_score(
    grid: &CapacityGrid,
    index: usize,
    exposure_seconds: &[f64],
    completion_counts: &[u32],
) -> f64 {
    let mut rates = vec![0.0_f64; exposure_seconds.len()];
    fill_state_rates(grid, index, &mut rates);
    path_log_score_with_rates(&rates, exposure_seconds, completion_counts)
}

#[cfg(test)]
fn feasibility_probability(
    grid: &CapacityGrid,
    index: usize,
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<f64> {
    let mut rates = vec![0.0_f64; probabilities.len()];
    fill_state_rates(grid, index, &mut rates);
    feasibility_probability_with_rates(&rates, evidence, probabilities, coefficients, work)
}

#[cfg(test)]
fn completion_marginal_probability(
    grid: &CapacityGrid,
    index: usize,
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<f64> {
    let mut rates = vec![0.0_f64; probabilities.len()];
    fill_state_rates(grid, index, &mut rates);
    let rough_normalizer =
        feasibility_probability_with_rates(&rates, evidence, probabilities, coefficients, work)?;
    // The ratio uses two solver results. Give each result one quarter of the
    // target times the denominator, so their combined ratio error stays bound.
    let solver_budget = PATH_SOLVER_PROBABILITY_ERROR_MAX * rough_normalizer / 4.0_f64;
    let (normalizer, _) = feasibility_probability_with_budget(
        &rates,
        evidence,
        probabilities,
        coefficients,
        work,
        solver_budget,
    )?;
    if normalizer <= 0.0_f64 {
        return None;
    }
    let final_state = evidence.final_busy_slots() as usize;
    let mut remaining_starts = evidence.window().started_attempts()? as usize;
    let mut low = final_state.saturating_sub(remaining_starts);
    let mut high = evidence.initial_busy_slots() as usize;
    probabilities.fill(0.0_f64);
    probabilities[high] = 1.0_f64;
    let group_count = evidence
        .start_groups()
        .iter()
        .filter(|starts| **starts > 0)
        .count()
        + 1;
    let mut ledger = ErrorLedger::with_budget(group_count, solver_budget)?;
    let mut previous_offset = 0_u64;
    for (&offset, &starts) in evidence
        .offsets_micros()
        .iter()
        .zip(evidence.start_groups())
    {
        if starts == 0 {
            continue;
        }
        pure_death_step_with_rates(
            &rates,
            DeathBand {
                low,
                high,
                exposure_seconds: Duration::from_micros(offset - previous_offset).as_secs_f64(),
            },
            probabilities,
            coefficients,
            work,
            &mut ledger,
        )?;
        let starts = starts as usize;
        for state in (low..=high).rev() {
            let shifted = state.saturating_add(starts);
            if shifted < probabilities.len() {
                probabilities[shifted] = probabilities[state];
            }
            probabilities[state] = 0.0_f64;
        }
        high = high.saturating_add(starts).min(probabilities.len() - 1);
        remaining_starts = remaining_starts.saturating_sub(starts);
        low = final_state.saturating_sub(remaining_starts);
        probabilities[..low].fill(0.0_f64);
        previous_offset = offset;
    }
    pure_death_step_with_rates(
        &rates,
        DeathBand {
            low: final_state,
            high,
            exposure_seconds: Duration::from_micros(
                evidence.window().exposure_micros() - previous_offset,
            )
            .as_secs_f64(),
        },
        probabilities,
        coefficients,
        work,
        &mut ledger,
    )?;
    Some((probabilities[final_state] / normalizer).clamp(0.0_f64, 1.0_f64))
}

/// Evolves one reachable pure-death band with a certified Poisson term count.
///
/// Equal-rate bands use the Erlang limit. All other bands use uniformization.
fn pure_death_step_with_rates(
    rates: &[f64],
    band: DeathBand,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
    ledger: &mut ErrorLedger,
) -> Option<f64> {
    let DeathBand {
        low,
        high,
        exposure_seconds,
    } = band;
    if exposure_seconds == 0.0_f64 || low > high {
        return Some(ledger.group_budget);
    }
    let first_rate = rates[low];
    let all_equal = rates[low..=high]
        .iter()
        .all(|rate| rate.to_bits() == first_rate.to_bits());
    if all_equal {
        equal_rate_death_step(first_rate, low, high, exposure_seconds, probabilities, work)?;
        return Some(ledger.group_budget);
    }
    uniformized_death_step(rates, band, probabilities, coefficients, work, ledger)
}

#[cfg(test)]
fn pure_death_step(
    grid: &CapacityGrid,
    index: usize,
    band: DeathBand,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<()> {
    let mut rates = vec![0.0_f64; probabilities.len()];
    fill_state_rates(grid, index, &mut rates);
    let mut ledger = ErrorLedger::new(1)?;
    pure_death_step_with_rates(&rates, band, probabilities, coefficients, work, &mut ledger)?;
    Some(())
}

fn equal_rate_death_step(
    rate: f64,
    low: usize,
    high: usize,
    exposure_seconds: f64,
    probabilities: &mut [f64],
    work: &mut [f64],
) -> Option<()> {
    work[low..=high].fill(0.0_f64);
    let mean = rate * exposure_seconds;
    let zero_deaths = (-mean).exp();
    for source in low..=high {
        let mut probability = zero_deaths;
        for deaths in 0..=source - low {
            work[source - deaths] += probabilities[source] * probability;
            let Ok(divisor) = u32::try_from(deaths + 1) else {
                return None;
            };
            probability *= mean / f64::from(divisor);
        }
    }
    probabilities[low..=high].copy_from_slice(&work[low..=high]);
    Some(())
}

fn uniformized_death_step(
    rates: &[f64],
    band: DeathBand,
    probabilities: &mut [f64],
    current: &mut [f64],
    next: &mut [f64],
    ledger: &mut ErrorLedger,
) -> Option<f64> {
    let DeathBand {
        low,
        high,
        exposure_seconds,
    } = band;
    let rate = (low..=high)
        .map(|state| rates[state])
        .fold(0.0_f64, f64::max);
    if rate == 0.0_f64 {
        return Some(ledger.group_budget);
    }
    let mean_limit = -ledger.group_budget.ln();
    let step_count = (rate * exposure_seconds / mean_limit).ceil().max(1.0_f64) as u64;
    let step_count_f64 = Duration::from_secs(step_count).as_secs_f64();
    let step_seconds = exposure_seconds / step_count_f64;
    let charge_count = step_count.checked_add(1)?;
    let charge_count_f64 = Duration::from_secs(charge_count).as_secs_f64();
    let tail_bound = ledger.group_budget / charge_count_f64;
    for _ in 0..step_count {
        current[low..=high].copy_from_slice(&probabilities[low..=high]);
        probabilities[low..=high].fill(0.0_f64);
        let mean = rate * step_seconds;
        let mut poisson = (-mean).exp();
        for state in low..=high {
            probabilities[state] += poisson * current[state];
        }
        let mut term = 0_u32;
        while poisson_upper_tail_bound(mean, term, poisson) > tail_bound {
            next[low..=high].fill(0.0_f64);
            for state in low..=high {
                let death = rates[state] / rate;
                next[state] += current[state] * (1.0_f64 - death);
                if state > low {
                    next[state - 1] += current[state] * death;
                }
            }
            current[low..=high].copy_from_slice(&next[low..=high]);
            term = term.checked_add(1)?;
            poisson *= mean / f64::from(term);
            for state in low..=high {
                probabilities[state] += poisson * current[state];
            }
        }
        if current[low + 1..=high].iter().all(|mass| *mass == 0.0_f64) {
            probabilities[low] += poisson_upper_tail(mean, term, poisson);
        }
        ledger.charge(tail_bound);
    }
    Some(tail_bound)
}

fn poisson_upper_tail(mean: f64, term: u32, probability: f64) -> f64 {
    let mut term = term;
    let mut probability = probability;
    let mut tail = 0.0_f64;
    loop {
        let Some(next_term) = term.checked_add(1) else {
            return tail;
        };
        probability *= mean / f64::from(next_term);
        if probability == 0.0_f64 {
            return tail;
        }
        tail += probability;
        term = next_term;
    }
}

fn poisson_upper_tail_bound(mean: f64, term: u32, probability: f64) -> f64 {
    let next_term = term.saturating_add(1);
    let next = probability * mean / f64::from(next_term);
    let following = next_term.saturating_add(1);
    if f64::from(following) <= mean {
        return 1.0_f64;
    }
    next / (1.0_f64 - mean / f64::from(following))
}

/// Removes upper states only when their total mass has a charged bound.
///
/// Reaching `state` needs `high - state` deaths. The collapse curve is
/// unimodal, so the minimum intermediate rate is at one of the two endpoints.
/// The one-death bound at `high - 1` is the smallest candidate bound.
/// Lower states use no larger minimum rate and require more deaths.
fn contract_death_band(
    rates: &[f64],
    band: DeathBand,
    probabilities: &mut [f64],
    budget: f64,
    ledger: &mut ErrorLedger,
) -> usize {
    let DeathBand {
        low,
        high,
        exposure_seconds,
    } = band;
    if (-rates[high] * exposure_seconds).exp() > budget {
        return high;
    }
    for state in low..high {
        let rate = rates[state + 1].min(rates[high]);
        let death_count = high - state;
        if poisson_lower_tail(rate * exposure_seconds, death_count, budget) <= budget {
            probabilities[state + 1..=high].fill(0.0_f64);
            ledger.charge(budget);
            return state;
        }
    }
    high
}

fn poisson_lower_tail(mean: f64, term_count: usize, budget: f64) -> f64 {
    let mut probability = (-mean).exp();
    let mut sum = probability;
    for term in 1..term_count {
        if sum > budget {
            return sum;
        }
        let term = u32::try_from(term).map_or(f64::INFINITY, f64::from);
        probability *= mean / term;
        sum += probability;
    }
    sum
}

fn completion_expectation(
    grid: &CapacityGrid,
    index: usize,
    history: RetainedHistory<'_>,
    window: &ResourceWindow,
    arrival_shape: f64,
    arrival_rate_seconds: f64,
) -> f64 {
    let delay = effective_service_time(grid, index, window.concurrency);
    let target_end = delay + window.exposure_seconds();
    let mut age = 0.0_f64;
    let mut known_mean = 0.0_f64;
    let mut known_overlap = 0.0_f64;
    let mut posterior_shape = arrival_shape;
    let mut posterior_rate = arrival_rate_seconds;
    for offset in 0..history.length {
        let index = (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[index];
        let window_end = age + start_window.exposure_seconds;
        if let Some(starts) = start_window.started_attempts {
            posterior_shape += f64::from(starts);
            posterior_rate += start_window.exposure_seconds;
            let overlap = (window_end.min(target_end) - age.max(delay)).max(0.0_f64);
            known_overlap += overlap;
            known_mean += f64::from(starts) * overlap / start_window.exposure_seconds;
        }
        age = window_end;
    }
    let missing = (window.exposure_seconds() - known_overlap).max(0.0_f64);
    known_mean + posterior_shape / posterior_rate * missing
}

#[cfg(test)]
fn completion_log_likelihood(
    grid: &CapacityGrid,
    index: usize,
    history: RetainedHistory<'_>,
    window: &ResourceWindow,
    arrival_shape: f64,
    arrival_rate_seconds: f64,
    scratch: CompletionScratch<'_>,
) -> f64 {
    let CompletionScratch {
        coefficients,
        convolution,
        binomial,
    } = scratch;
    let completed = window.completed_attempts as usize;
    if completed >= coefficients.len() {
        return f64::NEG_INFINITY;
    }
    let delay = effective_service_time(grid, index, window.concurrency);
    let target_end = delay + window.exposure_seconds();
    let mut age = 0.0_f64;
    let mut known_overlap = 0.0_f64;
    let mut posterior_shape = arrival_shape;
    let mut posterior_rate = arrival_rate_seconds;
    let mut deterministic = 0_usize;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let window_end = age + start_window.exposure_seconds;
        if let Some(starts) = start_window.started_attempts {
            posterior_shape += f64::from(starts);
            posterior_rate += start_window.exposure_seconds;
            let overlap = (window_end.min(target_end) - age.max(delay)).max(0.0_f64);
            known_overlap += overlap;
            if overlap >= start_window.exposure_seconds {
                deterministic = deterministic.saturating_add(starts as usize);
            }
        }
        age = window_end;
    }
    if deterministic > completed {
        return f64::NEG_INFINITY;
    }
    let target = completed - deterministic;
    coefficients[..=target].fill(0.0_f64);
    coefficients[0] = 1.0_f64;
    let mut degree = 0_usize;
    let mut coefficient_log_scale = 0.0_f64;
    age = 0.0_f64;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let window_end = age + start_window.exposure_seconds;
        if let Some(starts) = start_window.started_attempts {
            let overlap = (window_end.min(target_end) - age.max(delay)).max(0.0_f64);
            let probability = overlap / start_window.exposure_seconds;
            if probability > 0.0_f64 && probability < 1.0_f64 {
                let group_degree = (starts as usize).min(target);
                let maximum = (0..=group_degree)
                    .map(|count| binomial_log_probability(starts, count, probability))
                    .fold(f64::NEG_INFINITY, f64::max);
                for (count, value) in binomial[..=group_degree].iter_mut().enumerate() {
                    *value = (binomial_log_probability(starts, count, probability) - maximum).exp();
                }
                convolution[..=target].fill(0.0_f64);
                for known in 0..=degree {
                    let remaining = target - known;
                    for added in 0..=group_degree.min(remaining) {
                        convolution[known + added] += coefficients[known] * binomial[added];
                    }
                }
                degree = target.min(degree + group_degree);
                let scale = convolution[..=degree]
                    .iter()
                    .copied()
                    .fold(0.0_f64, f64::max);
                if scale <= 0.0_f64 {
                    return f64::NEG_INFINITY;
                }
                for (coefficient, value) in coefficients[..=degree]
                    .iter_mut()
                    .zip(&convolution[..=degree])
                {
                    *coefficient = *value / scale;
                }
                coefficient_log_scale += maximum + scale.ln();
            }
        }
        age = window_end;
    }
    let missing = (window.exposure_seconds() - known_overlap).max(0.0_f64);
    completion_probability_from_coefficients(
        coefficients,
        degree,
        target,
        coefficient_log_scale,
        posterior_shape,
        posterior_rate,
        missing,
    )
}

#[cfg(test)]
fn completion_probability_from_coefficients(
    coefficients: &[f64],
    degree: usize,
    target: usize,
    coefficient_log_scale: f64,
    posterior_shape: f64,
    posterior_rate: f64,
    missing: f64,
) -> f64 {
    if missing == 0.0_f64 {
        return if target <= degree && coefficients[target] > 0.0_f64 {
            coefficients[target].ln() + coefficient_log_scale
        } else {
            f64::NEG_INFINITY
        };
    }
    let success = posterior_rate / (posterior_rate + missing);
    let mut likelihood = f64::NEG_INFINITY;
    for (known, coefficient) in coefficients.iter().enumerate().take(degree.min(target) + 1) {
        if *coefficient > 0.0_f64 {
            let missing_count = target - known;
            likelihood = log_add_exp(
                likelihood,
                coefficient.ln()
                    + coefficient_log_scale
                    + negative_binomial_log_probability(posterior_shape, success, missing_count),
            );
        }
    }
    likelihood
}

#[cfg(test)]
fn binomial_log_probability(trials: u32, count: usize, probability: f64) -> f64 {
    let trials = f64::from(trials);
    let count = f64::from(u32::try_from(count).map_or(u32::MAX, |value| value));
    ln_gamma(trials + 1.0_f64) - ln_gamma(count + 1.0_f64) - ln_gamma(trials - count + 1.0_f64)
        + count * probability.ln()
        + (trials - count) * (-probability).ln_1p()
}

#[cfg(test)]
fn negative_binomial_log_probability(shape: f64, success: f64, count: usize) -> f64 {
    let count = f64::from(u32::try_from(count).map_or(u32::MAX, |value| value));
    ln_gamma(count + shape) - ln_gamma(shape) - ln_gamma(count + 1.0_f64)
        + shape * success.ln()
        + count * (-success).ln_1p()
}

fn log_weighted_sum(weights: &[f64], log_values: &[f64]) -> f64 {
    let maximum = log_values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !maximum.is_finite() {
        return maximum;
    }
    maximum
        + weights
            .iter()
            .zip(log_values)
            .map(|(weight, value)| weight * (*value - maximum).exp())
            .sum::<f64>()
            .ln()
}

fn log_contamination_mixture(clean: f64, prior_predictive: f64, probability: f64) -> f64 {
    log_add_exp(
        (-probability).ln_1p() + clean,
        probability.ln() + prior_predictive,
    )
}

fn log_add_exp(left: f64, right: f64) -> f64 {
    let maximum = left.max(right);
    if maximum == f64::NEG_INFINITY {
        maximum
    } else {
        maximum + ((left - maximum).exp() + (right - maximum).exp()).ln()
    }
}

fn log_uniform_capacity_prior(grid: &CapacityGrid, artifact: &CapacityModelArtifact) -> Vec<f64> {
    let service_count = grid.service_time_count as usize;
    let capacity_count = grid.capacity_count as usize;
    let collapse_count = grid.collapse_count as usize;
    let service_stride = capacity_count * collapse_count;
    let mut weights = Vec::with_capacity(grid.service_times_seconds.len());
    for service_index in 0..service_count {
        let service_mass = bounded_log_mass(service_index, service_count, |index| {
            grid.service_times_seconds[index * service_stride]
        });
        for capacity_index in 0..capacity_count {
            let capacity_mass = bounded_log_mass(capacity_index, capacity_count, |index| {
                grid.capacities_per_second[index * collapse_count]
            });
            for collapse_index in 0..collapse_count {
                let collapse_mass = collapse_mass(
                    &grid.collapse_values[..collapse_count],
                    collapse_index,
                    artifact,
                );
                weights.push(service_mass * capacity_mass * collapse_mass);
            }
        }
    }
    let total = weights.iter().sum::<f64>();
    for weight in &mut weights {
        *weight /= total;
    }
    weights
}

fn log_normal_capacity_prior(
    grid: &CapacityGrid,
    service_median: f64,
    capacity_median: f64,
    log_standard_deviation: f64,
    artifact: &CapacityModelArtifact,
) -> Vec<f64> {
    let service_count = grid.service_time_count as usize;
    let capacity_count = grid.capacity_count as usize;
    let collapse_count = grid.collapse_count as usize;
    let service_stride = capacity_count * collapse_count;
    let service_values = (0..service_count)
        .map(|index| grid.service_times_seconds[index * service_stride])
        .collect::<Vec<_>>();
    let capacity_values = (0..capacity_count)
        .map(|index| grid.capacities_per_second[index * collapse_count])
        .collect::<Vec<_>>();
    let service_masses =
        log_normal_axis_masses(&service_values, service_median, log_standard_deviation);
    let capacity_masses =
        log_normal_axis_masses(&capacity_values, capacity_median, log_standard_deviation);
    let mut weights = Vec::with_capacity(grid.service_times_seconds.len());
    for &service_mass in &service_masses {
        for &capacity_mass in &capacity_masses {
            for collapse_index in 0..collapse_count {
                weights.push(
                    service_mass
                        * capacity_mass
                        * collapse_mass(
                            &grid.collapse_values[..collapse_count],
                            collapse_index,
                            artifact,
                        ),
                );
            }
        }
    }
    let total = weights.iter().sum::<f64>();
    for weight in &mut weights {
        *weight /= total;
    }
    weights
}

fn log_normal_axis_masses(values: &[f64], median: f64, log_standard_deviation: f64) -> Vec<f64> {
    let Ok(distribution) = LogNormal::new(median.ln(), log_standard_deviation) else {
        return Vec::new();
    };
    (0..values.len())
        .map(|index| {
            let lower = if index == 0 {
                0.0_f64
            } else {
                (values[index - 1] * values[index]).sqrt()
            };
            let upper = if index + 1 == values.len() {
                f64::INFINITY
            } else {
                (values[index] * values[index + 1]).sqrt()
            };
            distribution.cdf(upper) - distribution.cdf(lower)
        })
        .collect()
}

fn bounded_log_mass<Value>(index: usize, count: usize, value: Value) -> f64
where
    Value: Fn(usize) -> f64,
{
    if count == 1 {
        return 1.0_f64;
    }
    let minimum = value(0).ln();
    let maximum = value(count - 1).ln();
    let center = value(index).ln();
    let lower = if index == 0 {
        minimum
    } else {
        value(index - 1).ln().midpoint(center)
    };
    let upper = if index + 1 == count {
        maximum
    } else {
        center.midpoint(value(index + 1).ln())
    };
    (upper - lower) / (maximum - minimum)
}

fn log_axis_bounds(values: &[f64]) -> Vec<(f64, f64)> {
    (0..values.len())
        .map(|index| {
            let low = if index == 0 {
                values[0]
            } else {
                (values[index - 1] * values[index]).sqrt()
            };
            let high = if index + 1 == values.len() {
                values[values.len() - 1]
            } else {
                (values[index] * values[index + 1]).sqrt()
            };
            (low, high)
        })
        .collect()
}

fn collapse_axis_bounds(values: &[f64]) -> Vec<(f64, f64)> {
    if values[0] != 0.0_f64 {
        return linear_axis_bounds(values);
    }
    let mut bounds = Vec::with_capacity(values.len());
    bounds.push((0.0_f64, 0.0_f64));
    bounds.extend(linear_axis_bounds(&values[1..]));
    bounds
}

fn linear_axis_bounds(values: &[f64]) -> Vec<(f64, f64)> {
    (0..values.len())
        .map(|index| {
            let low = if index == 0 {
                values[0]
            } else {
                values[index - 1].midpoint(values[index])
            };
            let high = if index + 1 == values.len() {
                values[values.len() - 1]
            } else {
                values[index].midpoint(values[index + 1])
            };
            (low, high)
        })
        .collect()
}

fn collapse_mass(values: &[f64], index: usize, artifact: &CapacityModelArtifact) -> f64 {
    if values.len() == 1 {
        return 1.0_f64;
    }
    if values[0] != 0.0_f64 {
        return bounded_linear_mass(values, index);
    }
    if index == 0 {
        return artifact.no_collapse_probability;
    }
    (1.0_f64 - artifact.no_collapse_probability) * bounded_linear_mass(&values[1..], index - 1)
}

fn bounded_linear_mass(values: &[f64], index: usize) -> f64 {
    if values.len() == 1 {
        return 1.0_f64;
    }
    let lower = if index == 0 {
        values[0]
    } else {
        values[index - 1].midpoint(values[index])
    };
    let upper = if index + 1 == values.len() {
        values[values.len() - 1]
    } else {
        values[index].midpoint(values[index + 1])
    };
    (upper - lower) / (values[values.len() - 1] - values[0])
}

/// Error from a caller-owned posterior buffer.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PosteriorError {
    /// A value or probability buffer has the wrong fixed length.
    #[error("capacity posterior buffers must contain {expected} values")]
    BufferLength {
        /// Required value count.
        expected: u32,
    },
    /// A lead-time query contains no replica change.
    #[error("a lead-time posterior query must change replicas")]
    ZeroReplicaDelta,
    /// Reliability posterior parameters are invalid.
    #[error("reliability posterior parameters must be positive and finite")]
    ReliabilityDistribution,
}

fn weighted_sum<S: Simd>(simd: S, weights: &[f64], values: &[f64]) -> f64 {
    let lane_count = S::f64s::N;
    let vector_count = weights.len() / lane_count;
    let mut sum = S::f64s::splat(simd, 0.0_f64);
    for vector_index in 0..vector_count {
        let start = vector_index * lane_count;
        let end = start + lane_count;
        let weight = S::f64s::from_slice(simd, &weights[start..end]);
        let value = S::f64s::from_slice(simd, &values[start..end]);
        sum += weight * value;
    }
    let mut total = sum.as_slice().iter().sum::<f64>();
    for index in vector_count * lane_count..weights.len() {
        total += weights[index] * values[index];
    }
    total
}

fn saturation_probability<S: Simd>(
    simd: S,
    weights: &[f64],
    service_times_seconds: &[f64],
    capacities_per_second: &[f64],
    no_knee_values: &[f64],
    concurrency: f64,
) -> f64 {
    let lane_count = S::f64s::N;
    let vector_count = weights.len() / lane_count;
    let concurrency = S::f64s::splat(simd, concurrency);
    let zero = S::f64s::splat(simd, 0.0_f64);
    let mut sum = zero;
    for vector_index in 0..vector_count {
        let start = vector_index * lane_count;
        let end = start + lane_count;
        let weight = S::f64s::from_slice(simd, &weights[start..end]);
        let service_time = S::f64s::from_slice(simd, &service_times_seconds[start..end]);
        let capacity = S::f64s::from_slice(simd, &capacities_per_second[start..end]);
        let no_knee = S::f64s::from_slice(simd, &no_knee_values[start..end]);
        sum += ((service_time * capacity).simd_le(concurrency) & no_knee.simd_eq(zero))
            .select(weight, zero);
    }
    let mut total = sum.as_slice().iter().sum::<f64>();
    for index in vector_count * lane_count..weights.len() {
        if no_knee_values[index] == 0.0_f64
            && service_times_seconds[index] * capacities_per_second[index]
                <= concurrency.as_slice()[0]
        {
            total += weights[index];
        }
    }
    total
}

fn curve_throughput<S: Simd>(
    simd: S,
    curve: CapacityCurve,
    concurrency: &[f64],
    output: &mut [f64],
) {
    let lane_count = S::f64s::N;
    let vector_count = concurrency.len() / lane_count;
    let (service_time_seconds, ceiling) = match curve {
        CapacityCurve::NoKnee {
            service_time_seconds,
        } => (service_time_seconds, f64::INFINITY),
        CapacityCurve::Knee {
            service_time_seconds,
            capacity_per_second,
            ..
        } => (service_time_seconds, capacity_per_second),
    };
    let service_time = S::f64s::splat(simd, service_time_seconds);
    let capacity = S::f64s::splat(simd, ceiling);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let concurrency = S::f64s::from_slice(simd, &concurrency[start..end]);
        let sustainable = (concurrency / service_time).min(capacity);
        sustainable.store_slice(&mut output[start..end]);
    }
    for candidate in vector_count * lane_count..concurrency.len() {
        output[candidate] = curve.sustainable_throughput(concurrency[candidate]);
    }
}

fn throughput(
    service_time_seconds: f64,
    capacity_per_second: f64,
    collapse: f64,
    no_knee: bool,
    concurrency: f64,
) -> f64 {
    let curve = if no_knee {
        CapacityCurve::NoKnee {
            service_time_seconds,
        }
    } else {
        CapacityCurve::Knee {
            service_time_seconds,
            capacity_per_second,
            collapse,
        }
    };
    curve.throughput(concurrency)
}

fn validate_axis(values: &[f64], permits_zero: bool) -> Result<(), CapacityGridError> {
    if values.is_empty() {
        return Err(CapacityGridError::EmptyAxis);
    }
    for &value in values {
        if !value.is_finite() {
            return Err(CapacityGridError::InvalidAxisValue { value });
        }
        if value < 0.0_f64 || (!permits_zero && value == 0.0_f64) {
            return Err(CapacityGridError::InvalidAxisValue { value });
        }
    }
    if values
        .windows(2)
        .any(|pair| pair[0].total_cmp(&pair[1]).is_ge())
    {
        return Err(CapacityGridError::AxisOrder);
    }
    Ok(())
}

fn validate_positive(value: f64, name: &'static str) -> Result<(), ResourceWindowError> {
    if !value.is_finite() || value <= 0.0_f64 {
        return Err(ResourceWindowError::InvalidValue { name, value });
    }
    Ok(())
}

/// Invalid capacity-grid configuration.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum CapacityGridError {
    /// An axis contains no cells.
    #[error("each capacity-grid axis must contain a value")]
    EmptyAxis,
    /// An axis contains an invalid value.
    #[error("capacity-grid value {value} is invalid")]
    InvalidAxisValue {
        /// Invalid axis value.
        value: f64,
    },
    /// A scale prior value is invalid or narrower than machine precision.
    #[error("a capacity scale prior value is invalid")]
    InvalidPrior,
    /// An axis does not increase strictly.
    #[error("each capacity-grid axis must increase strictly")]
    AxisOrder,
    /// The Cartesian grid exceeds its fixed bound.
    #[error("the capacity grid exceeds its storage budget")]
    TooLarge,
    /// The grid could not map a cell to its knee value.
    #[error("a capacity-grid knee value has no axis index")]
    KneeIndex,
}

/// Invalid capacity-model construction.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum CapacityModelError {
    /// A plant observation contract is invalid.
    #[error("a capacity observation contract is invalid")]
    InvalidObservationContract,
    /// Capacity state exceeds the artifact storage bound.
    #[error("capacity state exceeds the artifact storage bound")]
    StorageBound,
    /// The hazard prior is invalid or cannot form a finite grid.
    #[error("the capacity hazard prior is invalid")]
    InvalidHazardPrior,
    /// Hazard rates leave too much prior mass outside their centers.
    #[error("the capacity hazard grid exceeds its tail-mass budget")]
    HazardTailMass,
    /// The observation-quality prior is invalid.
    #[error("the capacity observation-quality prior is invalid")]
    InvalidObservationQualityPrior,
}

/// Invalid passive resource window.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ResourceWindowError {
    /// A required value is outside its finite range.
    #[error("{name} value {value} is outside its finite range")]
    InvalidValue {
        /// Name of the invalid field.
        name: &'static str,
        /// Invalid field value.
        value: f64,
    },
    /// Exposure does not resolve to the certified microsecond clock.
    #[error("resource exposure must resolve to whole microseconds")]
    ClockResolution,
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
