use std::{
    array,
    f64::consts::{E, LOG2_E},
    mem::size_of,
    time::Duration,
};

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use statrs::distribution::{Beta, ContinuousCDF, Gamma, LogNormal};
use statrs::function::gamma::{gamma_lr, gamma_ur, ln_gamma};
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
/// The scalar kernel completes at least this many simple operations per second.
///
/// This authored rate is conservative for supported production processors.
const CAPACITY_KERNEL_OPERATIONS_PER_SECOND_MIN: u64 = 100_000_000;
/// One capacity update can use at most 400 billion simple operations.
///
/// The honest regime table has a maximum of 158,367,561,327 operations.
/// Two-times headroom gives 316,735,122,654 operations. Rounding this value
/// up to one significant figure gives 400,000,000,000 operations. At the
/// certified minimum rate, this limit permits 4,000 seconds for one update.
const CAPACITY_UPDATE_OPERATION_COUNT_MAX: u64 = 4_000 * CAPACITY_KERNEL_OPERATIONS_PER_SECOND_MIN;
const REPORT_CLOCK_ERROR_SECONDS: f64 = 1.0e-6_f64;
/// The residual check has a one-percent test size.
///
/// The check reports a diagnostic and gates no decision. A false rejection
/// misreports the clock assumption to the operator.
const RESIDUAL_REJECTION_PROBABILITY: f64 = 0.01_f64;
/// A contamination midpoint changes the affine likelihood mixture by at most
/// half its grid-cell width. Eight cells bound that probability error at 1/16.
const OBSERVATION_PROBABILITY_ERROR_MAX: f64 = 1.0_f64 / 16.0_f64;
/// The hazard enters decisions only through `exp(-h * t)`.
///
/// The authored transition-probability tolerance is 12.5 percent. The arrival
/// and capacity models use the same value.
const HAZARD_TRANSITION_PROBABILITY_ERROR_MAX: f64 = 1.0_f64 / 8.0_f64;
/// One curve-axis cell can span at most two octaves.
///
/// A cell point can differ from a region member by a factor of two. The
/// authored relative-error bound is therefore 1.0. This point substitution is
/// the capacity model's largest authorized approximation.
const CURVE_GRID_RELATIVE_ERROR_MAX: f64 = 1.0_f64;
const OBSERVATION_COVERAGE_INDEX: usize = 0;
const HAZARD_COVERAGE_INDEX: usize = 1;
const SOLVER_COVERAGE_INDEX: usize = 2;
const CAPACITY_MODEL_BUDGET: PriorArtifactBudget = PriorArtifactBudget::new(
    (CAPACITY_MODEL_STORAGE_BYTES_MAX / size_of::<f64>()) as u32,
    CAPACITY_MODEL_STORAGE_BYTES_MAX as u64,
    CAPACITY_UPDATE_OPERATION_COUNT_MAX,
    1.0e-6_f64,
    REPORT_CLOCK_ERROR_SECONDS,
    CURVE_GRID_RELATIVE_ERROR_MAX,
);
const EMPTY_START_WINDOW: StartWindow = StartWindow {
    end_micros: 0,
    exposure_seconds: 0.0_f64,
    started_attempts: None,
};

/// Versioned prior and approximation limits for the capacity model.
///
/// Version 2 uses the certified event-path sampling model. It assigns one
/// affected window and nine clean windows to observation quality. It assigns
/// equal prior odds to the bounded knee family and the no-knee family. Within
/// the knee family, it assigns equal odds to no collapse and positive collapse.
/// The quadratic collapse law represents pairwise contention after the knee.
/// Busy-state coverage is unverified. The artifact records no per-busy-state
/// coverage. A labelled plant corpus can replace these judgments under a new
/// version.
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
    resource_window_group_count_max: u32,
}

impl CapacityModelArtifact {
    fn with_coverage(
        mut self,
        grid: &CapacityGrid,
        history_coverage_seconds: f64,
    ) -> Result<Self, CapacityModelError> {
        record_grid_coverage(grid, &mut self)?;
        self.coverage.push(PriorCoverageRecord::new(
            0.0_f64,
            history_coverage_seconds,
            0.0_f64,
            0.0_f64,
            0.0_f64,
        ));
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkovClockAssumption {
    MemorylessAggregateCompletions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartDelayEvidence {
    /// The model discards start-delay evidence. No factor recovers this
    /// evidence.
    DiscardedWithoutRecovery,
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

/// A capacity summary conditioned on the model having a finite knee.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ConditionalCapacity {
    /// Posterior probability of the conditioning event.
    pub(crate) conditioning_probability: f64,
    /// Conditional value. Infinity represents the unbounded no-knee case.
    pub(crate) value: f64,
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

/// Predictive completion counts and the CDF interval around one observation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompletionPredictiveSummary {
    /// Smallest counts whose CDF reaches each requested probability.
    pub quantile_counts: [u32; 3],
    /// CDF immediately below the observed count.
    pub lower: f64,
    /// CDF at the observed count.
    pub upper: f64,
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
    /// past its knee. The deliverable rate is the curve peak inside the
    /// allowance: `min(slots / service_time, capacity)`.
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
                && log_standard_deviation > 0.0_f64 =>
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
    linear_state_max: Vec<usize>,
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
        let mut linear_state_max = Vec::with_capacity(cell_count);
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
                    let knee = capacity_per_second * service_time_seconds;
                    linear_state_max.push(knee.floor() as usize);
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
            linear_state_max.push(usize::MAX);
        }
        let (knee_values, knee_indexes) =
            knee_metadata(&service_time_cells, &capacity_cells, knee_cell_count)?;
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
            linear_state_max,
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

fn knee_metadata(
    service_times: &[f64],
    capacities: &[f64],
    knee_cell_count: usize,
) -> Result<(Vec<f64>, Vec<u32>), CapacityGridError> {
    let cells = service_times
        .iter()
        .take(knee_cell_count)
        .zip(capacities.iter().take(knee_cell_count));
    let mut values = cells
        .clone()
        .map(|(service, capacity)| service * capacity)
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values.dedup_by(|left, right| left.total_cmp(right).is_eq());
    let mut indexes = Vec::with_capacity(knee_cell_count);
    for (&service, &capacity) in cells {
        let knee = service * capacity;
        let index = values
            .binary_search_by(|candidate| candidate.total_cmp(&knee))
            .map_err(|_| CapacityGridError::KneeIndex)?;
        indexes.push(u32::try_from(index).map_err(|_| CapacityGridError::TooLarge)?);
    }
    Ok((values, indexes))
}

/// One passive resource observation window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResourceWindow {
    concurrency: f64,
    exposure_micros: u64,
    completed_attempts: u32,
    started_attempts: u32,
}

impl ResourceWindow {
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
            started_attempts,
        })
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

    pub(crate) const fn started_attempts(&self) -> u32 {
        self.started_attempts
    }
}

#[derive(Clone, Copy)]
struct StartWindow {
    end_micros: u64,
    exposure_seconds: f64,
    started_attempts: Option<u32>,
}

#[derive(Clone, Copy)]
struct RetainedHistory<'a> {
    windows: &'a [StartWindow],
    head: usize,
    length: usize,
    end_micros: u64,
}

#[derive(Clone, Copy)]
struct DeathBand {
    low: usize,
    high: usize,
    exposure_seconds: f64,
}

#[derive(Clone, Copy)]
struct LinearRateBand {
    service_time_seconds: f64,
    state_max: usize,
}

#[derive(Clone, Copy)]
enum SpreadTruncation {
    #[cfg(test)]
    Disabled,
    Charged(f64),
}

#[derive(Clone, Copy)]
struct ErrorLedger {
    group_budget: f64,
    charged: f64,
}

impl ErrorLedger {
    /// Splits the path error budget across all solver groups in one
    /// cell-window.
    ///
    /// Each group receives `budget / group_count`. A group divides its grant
    /// across its uniformization steps and contraction.
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
    ln_gamma_integer_count: usize,
    start_history_capacity: usize,
    group_count: usize,
}

struct CompletionScratch<'a> {
    simd_level: Level,
    coefficients: &'a mut [f64],
    convolution: &'a mut [f64],
    binomial: &'a mut [f64],
    ln_gamma_integers: &'a [f64],
}

#[derive(Clone, Copy)]
struct CompletionTail<'a> {
    coefficient_log_scale: f64,
    posterior_shape: f64,
    posterior_rate: f64,
    missing: f64,
    ln_gamma_integers: &'a [f64],
}

pub(crate) struct CapacityFactor {
    simd_level: Level,
    grid: CapacityGrid,
    arrival_shape: f64,
    arrival_rate_seconds: f64,
    concurrency_max: f64,
    exposure_min_seconds: f64,
    history_coverage_seconds: f64,
    resource_window_group_count_max: u32,
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
    observation_clock_micros: u64,
    predictive_start_history: Vec<StartWindow>,
    previous_window_concurrency: Option<f64>,
    completion_coefficients: Vec<f64>,
    completion_convolution: Vec<f64>,
    completion_binomial: Vec<f64>,
    completion_cell_cdfs: Vec<f64>,
    ln_gamma_integers: Vec<f64>,
    state_exposure_seconds: Vec<f64>,
    state_completion_counts: Vec<u32>,
    forward_probabilities: Vec<f64>,
    state_rates: Vec<f64>,
    forward_coefficients: Vec<f64>,
    forward_work: Vec<f64>,
    residuals: Vec<f64>,
    residual_head: usize,
    residual_len: usize,
    residual_sort_scratch: Vec<f64>,
    residual_sample_count: u32,
    residual_maximum_distance: f64,
    residual_integrated_hazards: Vec<f64>,
    discard_next_residual: bool,
    markov_clock_rejected: bool,
}

impl CapacityFactor {
    pub(crate) fn new_with_prior_with_groups(
        grid: CapacityGrid,
        change_rate_per_second: f64,
        arrival_prior: &ArrivalPrior,
        concurrency_max: f64,
        exposure_min_seconds: f64,
        attempt_count_max: u32,
        group_count_max: u32,
    ) -> Result<Self, CapacityModelError> {
        validate_capacity_observation_contract(
            concurrency_max,
            exposure_min_seconds,
            attempt_count_max,
            group_count_max,
        )?;
        let cell_count = grid.service_times_seconds.len();
        let state_count = concurrency_max as usize + 1;
        let (history_coverage_seconds, start_history_capacity) =
            start_history_contract(&grid, concurrency_max, exposure_min_seconds)?;
        let artifact = capacity_model_artifact_with_groups(
            change_rate_per_second,
            arrival_prior.shape(),
            group_count_max,
        )?
        .with_coverage(&grid, history_coverage_seconds)?;
        let prior_weights = capacity_prior(&grid, &artifact)?;
        let (hazard_rates_per_second, hazard_weights) = hazard_prior(&artifact)?;
        let (contamination_probabilities, contamination_weights) = contamination_prior(&artifact)?;
        let filter_count = hazard_weights
            .len()
            .checked_mul(contamination_weights.len())
            .ok_or(CapacityModelError::StorageBound)?;
        let filter_curve_count = filter_count
            .checked_mul(cell_count)
            .ok_or(CapacityModelError::StorageBound)?;
        let (transition_count, ln_gamma_integer_count) = attempt_buffer_counts(attempt_count_max)?;
        validate_capacity_allocation(
            &grid,
            &artifact,
            CapacityAllocation {
                cell_count,
                state_count,
                filter_count,
                filter_curve_count,
                transition_count,
                ln_gamma_integer_count,
                start_history_capacity,
                group_count: group_count_max as usize,
            },
            exposure_min_seconds,
        )?;
        let (filter_weights, filter_curve_weights) = filter_prior(
            &hazard_weights,
            &contamination_weights,
            &prior_weights,
            filter_count,
        );
        Ok(Self {
            simd_level: Level::new(),
            grid,
            arrival_shape: arrival_prior.shape(),
            arrival_rate_seconds: arrival_prior.rate_seconds(),
            concurrency_max,
            exposure_min_seconds,
            history_coverage_seconds,
            resource_window_group_count_max: group_count_max,
            weights: prior_weights.clone(),
            prior_weights,
            likelihoods: vec![0.0_f64; cell_count],
            hazard_rates_per_second,
            contamination_probabilities,
            filter_log_weights: vec![0.0_f64; filter_count],
            filter_weights,
            filter_curve_weights,
            start_history: vec![EMPTY_START_WINDOW; start_history_capacity],
            start_history_head: 0,
            start_history_len: 0,
            observation_clock_micros: 0,
            predictive_start_history: vec![EMPTY_START_WINDOW; start_history_capacity],
            previous_window_concurrency: None,
            completion_coefficients: vec![0.0_f64; attempt_count_max as usize + 1],
            completion_convolution: vec![0.0_f64; attempt_count_max as usize + 1],
            completion_binomial: vec![0.0_f64; attempt_count_max as usize + 1],
            completion_cell_cdfs: vec![0.0_f64; cell_count],
            ln_gamma_integers: integer_ln_gamma_table(ln_gamma_integer_count)?,
            state_exposure_seconds: vec![0.0_f64; state_count],
            state_completion_counts: vec![0; state_count],
            forward_probabilities: vec![0.0_f64; state_count],
            state_rates: vec![0.0_f64; state_count],
            forward_coefficients: vec![0.0_f64; state_count],
            forward_work: vec![0.0_f64; state_count],
            residuals: vec![0.0_f64; attempt_count_max as usize],
            residual_head: 0,
            residual_len: 0,
            residual_sort_scratch: vec![0.0_f64; attempt_count_max as usize],
            residual_sample_count: 0,
            residual_maximum_distance: 0.0_f64,
            residual_integrated_hazards: vec![0.0_f64; cell_count],
            discard_next_residual: false,
            markov_clock_rejected: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_prior(
        grid: CapacityGrid,
        change_rate_per_second: f64,
        arrival_prior: &ArrivalPrior,
        concurrency_max: f64,
        exposure_min_seconds: f64,
        attempt_count_max: u32,
    ) -> Result<Self, CapacityModelError> {
        Self::new_with_prior_with_groups(
            grid,
            change_rate_per_second,
            arrival_prior,
            concurrency_max,
            exposure_min_seconds,
            attempt_count_max,
            attempt_count_max,
        )
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
        let artifact = capacity_model_artifact_with_groups(
            change_rate_per_second,
            self.arrival_shape,
            self.resource_window_group_count_max,
        )?
        .with_coverage(&self.grid, self.history_coverage_seconds)?;
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

    #[cfg_attr(
        feature = "hotpath",
        hotpath::measure(label = "completion_predictive_sweep")
    )]
    fn completion_predictive_sweep(
        &mut self,
        window: &ResourceWindow,
        count_max: u32,
        mut visit: impl FnMut(u32, f64) -> bool,
    ) {
        self.predictive_start_history
            .copy_from_slice(&self.start_history);
        let mut head = self.start_history_head;
        let mut length = self.start_history_len;
        let end_micros = self
            .observation_clock_micros
            .saturating_add(window.exposure_micros());
        record_start_window(
            &mut self.predictive_start_history,
            &mut head,
            &mut length,
            window,
            end_micros,
            Some(window.started_attempts()),
        );
        let predictive_concurrency = self
            .previous_window_concurrency
            .unwrap_or(window.concurrency);
        self.completion_cell_cdfs.fill(0.0_f64);
        for count in 0..=count_max {
            for index in 0..self.weights.len() {
                let predictive_window = ResourceWindow {
                    concurrency: predictive_concurrency,
                    exposure_micros: window.exposure_micros,
                    completed_attempts: count,
                    started_attempts: window.started_attempts,
                };
                self.completion_cell_cdfs[index] += completion_log_likelihood(
                    &self.grid,
                    index,
                    RetainedHistory {
                        windows: &self.predictive_start_history,
                        head,
                        length,
                        end_micros,
                    },
                    &predictive_window,
                    self.arrival_shape,
                    self.arrival_rate_seconds,
                    CompletionScratch {
                        simd_level: self.simd_level,
                        coefficients: &mut self.completion_coefficients,
                        convolution: &mut self.completion_convolution,
                        binomial: &mut self.completion_binomial,
                        ln_gamma_integers: &self.ln_gamma_integers,
                    },
                )
                .exp();
            }
            let mut cumulative = 0.0_f64;
            for (weight, cell_cdf) in self.weights.iter().zip(&self.completion_cell_cdfs) {
                cumulative += *weight * *cell_cdf;
            }
            if !visit(count, cumulative.clamp(0.0_f64, 1.0_f64)) {
                break;
            }
        }
    }

    pub(crate) fn completion_predictive_summary(
        &mut self,
        window: &ResourceWindow,
        observed: u32,
        thresholds: [f64; 3],
    ) -> CompletionPredictiveSummary {
        let count_max = self.completion_coefficients.len().saturating_sub(1) as u32;
        let mut quantile_counts = [count_max; 3];
        let mut quantile_found = [false; 3];
        let mut lower = if observed.saturating_sub(1) > count_max {
            1.0_f64
        } else {
            0.0_f64
        };
        let mut upper = if observed > count_max {
            1.0_f64
        } else {
            0.0_f64
        };
        self.completion_predictive_sweep(window, count_max, |count, cdf| {
            for index in 0..thresholds.len() {
                if !quantile_found[index] && cdf >= thresholds[index] {
                    quantile_counts[index] = count;
                    quantile_found[index] = true;
                }
            }
            if observed > 0 && count == observed - 1 {
                lower = cdf;
            }
            if count == observed {
                upper = cdf;
            }
            !(quantile_found.iter().all(|found| *found) && count >= observed)
        });
        CompletionPredictiveSummary {
            quantile_counts,
            lower,
            upper,
        }
    }

    #[cfg(test)]
    pub(crate) fn completion_predictive_cdf(
        &mut self,
        window: &ResourceWindow,
        completed_attempts: u32,
    ) -> f64 {
        if completed_attempts as usize >= self.completion_coefficients.len() {
            return 1.0_f64;
        }
        self.predictive_start_history
            .copy_from_slice(&self.start_history);
        let mut head = self.start_history_head;
        let mut length = self.start_history_len;
        let end_micros = self
            .observation_clock_micros
            .saturating_add(window.exposure_micros());
        record_start_window(
            &mut self.predictive_start_history,
            &mut head,
            &mut length,
            window,
            end_micros,
            Some(window.started_attempts()),
        );
        let predictive_concurrency = self
            .previous_window_concurrency
            .unwrap_or(window.concurrency);
        let mut cumulative = 0.0_f64;
        for index in 0..self.weights.len() {
            let mut cell_cdf = 0.0_f64;
            for count in 0..=completed_attempts {
                let predictive_window = ResourceWindow {
                    concurrency: predictive_concurrency,
                    exposure_micros: window.exposure_micros,
                    completed_attempts: count,
                    started_attempts: window.started_attempts,
                };
                cell_cdf += completion_log_likelihood(
                    &self.grid,
                    index,
                    RetainedHistory {
                        windows: &self.predictive_start_history,
                        head,
                        length,
                        end_micros,
                    },
                    &predictive_window,
                    self.arrival_shape,
                    self.arrival_rate_seconds,
                    CompletionScratch {
                        simd_level: self.simd_level,
                        coefficients: &mut self.completion_coefficients,
                        convolution: &mut self.completion_convolution,
                        binomial: &mut self.completion_binomial,
                        ln_gamma_integers: &self.ln_gamma_integers,
                    },
                )
                .exp();
            }
            cumulative += self.weights[index] * cell_cdf;
        }
        cumulative.clamp(0.0_f64, 1.0_f64)
    }

    #[cfg(test)]
    pub(crate) fn write_completion_predictive_cdfs(
        &mut self,
        window: &ResourceWindow,
        output: &mut [f64],
    ) {
        let count_max = output.len().saturating_sub(1) as u32;
        self.completion_predictive_sweep(window, count_max, |count, cdf| {
            output[count as usize] = cdf;
            true
        });
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
    /// [`validate_capacity_allocation`] bounds this cost at construction.
    /// Every report enters the start-history ring for completion prediction.
    ///
    /// The update applies evidence to its interval start. It then advances
    /// the change process across the evidence interval.
    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "capacity_update"))]
    pub(crate) fn update(&mut self, evidence: OccupancyTraceEvidence<'_>, elapsed: Duration) {
        let window = evidence.window();
        debug_assert!(
            evidence.mean_concurrency() <= self.concurrency_max,
            "the observation buffer enforces maximum resource concurrency"
        );
        let exposure = Duration::from_micros(window.exposure_micros());
        self.transition(elapsed.saturating_sub(exposure));
        self.observation_clock_micros = self
            .observation_clock_micros
            .saturating_add(elapsed.as_micros() as u64);
        record_start_window(
            &mut self.start_history,
            &mut self.start_history_head,
            &mut self.start_history_len,
            window,
            self.observation_clock_micros,
            Some(window.started_attempts()),
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
        for index in 0..self.grid.knee_cell_count as usize {
            fill_knee_state_rates(&self.grid, index, &mut self.state_rates);
            self.update_cell_likelihood(index, evidence);
        }
        for index in self.grid.knee_cell_count as usize..self.likelihoods.len() {
            fill_no_knee_state_rates(&self.grid, index, &mut self.state_rates);
            self.update_cell_likelihood(index, evidence);
        }
        let prior_predictive = log_weighted_sum(&self.prior_weights, &self.likelihoods);
        if posterior_update_eligible(prior_predictive) {
            self.update_filters(prior_predictive);
        }
        self.previous_window_concurrency = Some(evidence.mean_concurrency());
        self.transition(exposure);
    }

    fn update_cell_likelihood(&mut self, index: usize, evidence: OccupancyTraceEvidence<'_>) {
        let linear_rate_band = linear_rate_band(&self.grid, index);
        let raw = path_log_score_with_rates(
            &self.state_rates,
            &self.state_exposure_seconds,
            &self.state_completion_counts,
        );
        let normalizer = feasibility_probability_with_rates(
            &self.state_rates,
            linear_rate_band,
            evidence,
            &mut self.forward_probabilities,
            &mut self.forward_coefficients,
            &mut self.forward_work,
        );
        self.likelihoods[index] = normalizer
            .filter(|value| *value > 0.0_f64)
            .map_or(f64::NEG_INFINITY, |value| raw - value.ln());
    }

    pub(crate) fn omit_observation(&mut self, elapsed: Duration) {
        self.transition(elapsed);
        self.observation_clock_micros = self
            .observation_clock_micros
            .saturating_add(elapsed.as_micros() as u64);
        record_start_gap(
            &mut self.start_history,
            &mut self.start_history_head,
            &mut self.start_history_len,
            self.observation_clock_micros,
            elapsed,
        );
        self.residual_integrated_hazards.fill(0.0_f64);
        self.discard_next_residual = true;
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
        let dkw_bound =
            (-(RESIDUAL_REJECTION_PROBABILITY * 0.5_f64).ln() / (2.0_f64 * sample_count)).sqrt();
        CapacityClockCheck {
            sample_count: self.residual_sample_count,
            maximum_distance: self.residual_maximum_distance,
            rejection_threshold: dkw_bound,
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
            self.add_residual_exposure(state, exposure);
            if completed > 0 {
                if self.discard_next_residual {
                    self.discard_next_residual = false;
                } else {
                    let residual = self.predictive_residual(completed);
                    self.record_residual(residual);
                }
                self.residual_integrated_hazards.fill(0.0_f64);
            }
            // One group is a simultaneous batch; see `fold_trace`.
            state = state + started as usize - completed as usize;
            previous_offset = offset;
        }
        let tail = Duration::from_micros(evidence.window().exposure_micros() - previous_offset)
            .as_secs_f64();
        self.add_residual_exposure(state, tail);
        if self.residual_sample_count > 0 {
            let sample_count = f64::from(self.residual_sample_count);
            let alpha = RESIDUAL_REJECTION_PROBABILITY;
            let dkw_bound = (-(alpha * 0.5_f64).ln() / (2.0_f64 * sample_count)).sqrt();
            self.refresh_residual_check(sample_count);
            // The capacity certification ruling makes this check advisory.
            // Decision gating waits for a calibrated rejection rule.
            self.markov_clock_rejected = self.residual_maximum_distance > dkw_bound;
        }
    }

    fn add_residual_exposure(&mut self, state: usize, exposure: f64) {
        for (index, hazard) in self.residual_integrated_hazards.iter_mut().enumerate() {
            *hazard += exposure * state_rate(&self.grid, index, state);
        }
    }

    fn predictive_residual(&self, completed: u32) -> f64 {
        let shape = f64::from(completed);
        let survival = self
            .weights
            .iter()
            .zip(&self.residual_integrated_hazards)
            .map(|(weight, hazard)| {
                let cell_survival = if *hazard <= 0.0_f64 {
                    1.0_f64
                } else if *hazard < shape {
                    1.0_f64 - gamma_lr(shape, *hazard)
                } else {
                    gamma_ur(shape, *hazard)
                };
                weight * cell_survival
            })
            .sum::<f64>();
        -survival.ln()
    }

    fn record_residual(&mut self, residual: f64) {
        self.residuals[self.residual_head] = residual;
        self.residual_head = (self.residual_head + 1) % self.residuals.len();
        self.residual_len = (self.residual_len + 1).min(self.residuals.len());
        self.residual_sample_count = self.residual_len as u32;
    }

    fn refresh_residual_check(&mut self, sample_count: f64) {
        // Keep the priced sort. Incremental sorted maintenance adds index code for
        // negligible work.
        self.residual_sort_scratch[..self.residual_len]
            .copy_from_slice(&self.residuals[..self.residual_len]);
        self.residual_sort_scratch[..self.residual_len].sort_unstable_by(f64::total_cmp);
        let mut maximum = 0.0_f64;
        for (index, residual) in self.residual_sort_scratch[..self.residual_len]
            .iter()
            .copied()
            .enumerate()
        {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            let lower = f64::from(index) / sample_count;
            let upper = f64::from(index.saturating_add(1)) / sample_count;
            let cdf = -(-residual).exp_m1();
            maximum = maximum.max((cdf - lower).abs());
            maximum = maximum.max((upper - cdf).abs());
        }
        self.residual_maximum_distance = maximum;
    }

    pub(crate) fn expected_capacity(&self, simd_level: Level) -> ConditionalCapacity {
        let knee_probability = self.knee_probability();
        let value = if knee_probability == 0.0_f64 {
            f64::INFINITY
        } else {
            dispatch!(simd_level, simd => weighted_sum(simd, &self.weights, &self.grid.capacities_per_second))
                / knee_probability
        };
        ConditionalCapacity {
            conditioning_probability: knee_probability,
            value,
        }
    }

    pub(crate) fn expected_service_time(&self, simd_level: Level) -> f64 {
        dispatch!(simd_level, simd => weighted_sum(
            simd,
            &self.weights,
            &self.grid.service_times_seconds,
        ))
    }

    pub(crate) fn capacity_quantile(&self, probability: f64) -> ConditionalCapacity {
        let collapse_count = self.grid.collapse_count as usize;
        let capacity_count = self.grid.capacity_count as usize;
        let service_count = self.grid.service_time_count as usize;
        let service_stride = capacity_count * collapse_count;
        let knee_probability = self.knee_probability();
        if knee_probability == 0.0_f64 {
            return ConditionalCapacity {
                conditioning_probability: 0.0_f64,
                value: f64::INFINITY,
            };
        }
        let mut cumulative = 0.0_f64;
        for capacity in 0..capacity_count {
            for service in 0..service_count {
                let start = service * service_stride + capacity * collapse_count;
                let end = start + collapse_count;
                cumulative += self.weights[start..end].iter().sum::<f64>() / knee_probability;
            }
            if cumulative >= probability {
                return ConditionalCapacity {
                    conditioning_probability: knee_probability,
                    value: self.grid.capacities_per_second[capacity * collapse_count],
                };
            }
        }
        ConditionalCapacity {
            conditioning_probability: knee_probability,
            value: self.grid.capacities_per_second[(capacity_count - 1) * collapse_count],
        }
    }

    pub(crate) fn write_capacity_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<f64, PosteriorError> {
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
        if knee_probability == 0.0_f64 {
            return Ok(0.0_f64);
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(knee_probability)
    }

    pub(crate) fn write_service_time_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<f64, PosteriorError> {
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
        Ok(1.0_f64)
    }

    pub(crate) fn write_collapse_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<f64, PosteriorError> {
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
        if knee_probability == 0.0_f64 {
            return Ok(0.0_f64);
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(knee_probability)
    }

    pub(crate) fn write_knee_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<f64, PosteriorError> {
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
        if knee_probability == 0.0_f64 {
            return Ok(0.0_f64);
        }
        for value in probabilities {
            *value /= knee_probability;
        }
        Ok(knee_probability)
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

    #[cfg(test)]
    pub(crate) fn fill_throughput(
        simd_level: Level,
        curve: CapacityCurve,
        concurrency: &[f64],
        output: &mut [f64],
    ) -> Result<(), PosteriorError> {
        if concurrency.len() != output.len() {
            return Err(PosteriorError::BufferLength {
                expected: concurrency.len() as u32,
            });
        }
        dispatch!(simd_level, simd => curve_throughput(simd, curve, concurrency, output));
        Ok(())
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
    fn update_filters(&mut self, prior_predictive: f64) {
        let cell_count = self.weights.len();
        let quality_count = self.contamination_probabilities.len();
        for (filter, curve_weights) in self
            .filter_curve_weights
            .chunks_exact(cell_count)
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
            if !predictive.is_finite() || predictive <= 0.0_f64 {
                return;
            }
            self.filter_log_weights[filter] =
                self.filter_weights[filter].ln() + maximum + predictive.ln();
        }
        let maximum = self
            .filter_log_weights
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let total = dispatch!(self.simd_level, simd => sum_shifted_exponentials(
            simd,
            &self.filter_log_weights,
            maximum,
        ));
        if !total.is_finite() || total <= 0.0_f64 {
            return;
        }
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
            let predictive =
                (self.filter_log_weights[filter] - self.filter_weights[filter].ln() - maximum)
                    .exp();
            for (weight, likelihood) in curve_weights.iter_mut().zip(&self.likelihoods) {
                let mixture =
                    log_contamination_mixture(*likelihood, prior_predictive, contamination);
                *weight *= (mixture - maximum).exp() / predictive;
            }
        }
        dispatch!(self.simd_level, simd => write_normalized_exponentials(
            simd,
            &self.filter_log_weights,
            &mut self.filter_weights,
            maximum,
            total,
        ));
        self.mix_filters();
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

fn capacity_prior(
    grid: &CapacityGrid,
    artifact: &CapacityModelArtifact,
) -> Result<Vec<f64>, CapacityModelError> {
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
        )?,
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
    Ok(weights)
}

fn validate_capacity_observation_contract(
    concurrency_max: f64,
    exposure_min_seconds: f64,
    attempt_count_max: u32,
    group_count_max: u32,
) -> Result<(), CapacityModelError> {
    if concurrency_max.is_finite()
        && concurrency_max > 0.0_f64
        && exposure_min_seconds.is_finite()
        && exposure_min_seconds > 0.0_f64
        && attempt_count_max > 0
        && group_count_max > 0
    {
        Ok(())
    } else {
        Err(CapacityModelError::InvalidObservationContract)
    }
}

fn start_history_contract(
    grid: &CapacityGrid,
    concurrency_max: f64,
    exposure_min_seconds: f64,
) -> Result<(f64, usize), CapacityModelError> {
    let coverage = (0..grid.service_times_seconds.len())
        .map(|index| effective_service_time(grid, index, concurrency_max))
        .fold(0.0_f64, f64::max);
    if !coverage.is_finite() {
        return Err(CapacityModelError::InvalidObservationContract);
    }
    let count = (coverage / exposure_min_seconds).ceil() as usize;
    let capacity = count
        .checked_add(1)
        .ok_or(CapacityModelError::StorageBound)?;
    Ok((coverage, capacity))
}

fn validate_capacity_allocation(
    grid: &CapacityGrid,
    artifact: &CapacityModelArtifact,
    allocation: CapacityAllocation,
    exposure_seconds: f64,
) -> Result<(), CapacityModelError> {
    let storage_bytes = capacity_storage_bytes(allocation)?
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
    let update_operation_count =
        capacity_update_operation_count(grid, allocation, exposure_seconds)
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

fn capacity_update_operation_count(
    grid: &CapacityGrid,
    allocation: CapacityAllocation,
    exposure_seconds: f64,
) -> Option<u64> {
    let band_count = allocation.state_count.min(allocation.transition_count);
    let states = u64::try_from(band_count).ok()?;
    let groups = u64::try_from(allocation.group_count).ok()?;
    let groups_u32 = u32::try_from(groups).ok()?;
    let group_budget = PATH_SOLVER_PROBABILITY_ERROR_MAX / f64::from(groups_u32);
    let source_count = states.checked_add(1)?;
    let source_count_u32 = u32::try_from(source_count).ok()?;
    let source_charge = group_budget / f64::from(source_count_u32);
    let states_u32 = u32::try_from(states).ok()?;
    let half_width = (f64::from(states_u32) * (2.0_f64 / source_charge).ln() / 2.0_f64).sqrt();
    let spread_width = (2.0_f64 * half_width).ceil() as u64 + 1;
    let equal_rate_cost = groups
        .checked_mul(states)?
        .checked_mul(states.checked_add(1)?)?
        / 2;
    let contraction_cost = groups.checked_mul(states)?;
    let mut kernel_cost = 0_u64;
    for index in 0..allocation.cell_count {
        let knee =
            (grid.capacities_per_second[index] * grid.service_times_seconds[index]).floor() as u64;
        let wholly_linear = grid.no_knee[index] > 0.0_f64 || states.saturating_sub(1) <= knee;
        let linear_states = if wholly_linear {
            states
        } else {
            states.min(knee.saturating_add(1))
        };
        let linear_cost = groups
            .checked_mul(linear_states)?
            .checked_mul(spread_width)?;
        let cell_cost = if wholly_linear {
            linear_cost
        } else {
            let mean_limit = -group_budget.ln();
            let exposure_steps = (grid.capacities_per_second[index] * exposure_seconds / mean_limit)
                .ceil()
                .max(1.0_f64) as u64;
            let steps = exposure_steps.checked_add(groups)?;
            let steps_u32 = u32::try_from(exposure_steps).ok()?;
            let tail_bound = group_budget / f64::from(steps_u32.saturating_add(1));
            let terms = u64::from(uniformization_term_count_bound(mean_limit, tail_bound)?);
            let uniform_cost = steps
                .checked_mul(terms)?
                .checked_mul(states)?
                .checked_mul(4)?;
            let mixed_cost = linear_cost.max(uniform_cost);
            if grid.collapse_values[index] == 0.0_f64 {
                mixed_cost.max(equal_rate_cost)
            } else {
                mixed_cost
            }
        };
        kernel_cost = kernel_cost
            .checked_add(cell_cost)?
            .checked_add(contraction_cost)?;
    }
    let cells = u64::try_from(allocation.cell_count).ok()?;
    let allocated_states = u64::try_from(allocation.state_count).ok()?;
    // Each cell fills its rate vector and then scans the same vector for the
    // path score. Kernel work is priced separately above.
    let path_cost = cells.checked_mul(allocated_states)?.checked_mul(2)?;
    let Ok(attempts) = u64::try_from(allocation.transition_count.saturating_sub(1) / 2) else {
        return None;
    };
    let residual_batches = groups.min(attempts);
    // Each trace group adds exposure for every curve. Each completion batch
    // then scans the curve posterior and clears its integrated hazards.
    let residual_grid_cost = groups
        .checked_add(1)?
        .checked_add(residual_batches.checked_mul(2)?)?
        .checked_mul(cells)?;
    let residual_ring_cost = residual_batches;
    let sort_levels = u64::from(attempts.max(1).ilog2().saturating_add(1));
    // The diagnostic copies and scans the ring once. The unstable sort has an
    // O(N log N) comparison bound; two operations price each compare and move.
    let residual_sort_cost = attempts
        .checked_mul(sort_levels)?
        .checked_mul(2)?
        .checked_add(attempts.checked_mul(2)?)?;
    let trace_cost = groups.checked_mul(5)?;
    let filter_cost = u64::try_from(allocation.filter_curve_count).ok()?;
    kernel_cost
        .checked_add(path_cost)?
        .checked_add(residual_grid_cost)?
        .checked_add(residual_ring_cost)?
        .checked_add(residual_sort_cost)?
        .checked_add(trace_cost)?
        .checked_add(filter_cost)
}

fn uniformization_term_count_bound(mean: f64, tail_bound: f64) -> Option<u32> {
    let mut probability = (-mean).exp();
    let mut term = 0_u32;
    while poisson_upper_tail_bound(mean, term, probability) > tail_bound {
        term = term.checked_add(1)?;
        probability *= mean / f64::from(term);
    }
    term.checked_add(1)
}

fn capacity_storage_bytes(allocation: CapacityAllocation) -> Result<usize, CapacityModelError> {
    let CapacityAllocation {
        filter_curve_count,
        cell_count,
        filter_count,
        state_count,
        transition_count,
        ln_gamma_integer_count,
        start_history_capacity,
        ..
    } = allocation;
    filter_curve_count
        .checked_add(
            cell_count
                .checked_mul(4)
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
        .and_then(|bytes| {
            ln_gamma_integer_count
                .checked_mul(size_of::<f64>())
                .and_then(|table_bytes| bytes.checked_add(table_bytes))
        })
        .and_then(|bytes| bytes.checked_add(size_of::<u32>()))
        .ok_or(CapacityModelError::StorageBound)
}

/// Derives attempt-sized buffer lengths from the attempt cap.
fn attempt_buffer_counts(attempt_count_max: u32) -> Result<(usize, usize), CapacityModelError> {
    let attempt_count =
        usize::try_from(attempt_count_max).map_err(|_| CapacityModelError::StorageBound)?;
    let transition_count = attempt_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(1))
        .ok_or(CapacityModelError::StorageBound)?;
    let ln_gamma_integer_count = attempt_count
        .checked_add(1)
        .ok_or(CapacityModelError::StorageBound)?;
    Ok((transition_count, ln_gamma_integer_count))
}

/// Index `n` stores `ln_gamma(n + 1)` for the certified attempt range.
fn integer_ln_gamma_table(count: usize) -> Result<Vec<f64>, CapacityModelError> {
    (0..count)
        .map(|index| {
            let integer = u32::try_from(index).map_err(|_| CapacityModelError::StorageBound)?;
            Ok(ln_gamma(f64::from(integer) + 1.0_f64))
        })
        .collect()
}

fn capacity_grid_storage_bytes(cell_count: usize, knee_cell_count: usize) -> Option<usize> {
    cell_count
        .checked_mul(10)
        .and_then(|count| count.checked_mul(size_of::<f64>()))
        .and_then(|bytes| {
            cell_count
                .checked_mul(size_of::<usize>())
                .and_then(|metadata_bytes| bytes.checked_add(metadata_bytes))
        })
        .and_then(|bytes| {
            knee_cell_count
                .checked_mul(size_of::<f64>() + size_of::<u32>())
                .and_then(|knee_bytes| bytes.checked_add(knee_bytes))
        })
}

fn record_grid_coverage(
    grid: &CapacityGrid,
    artifact: &mut CapacityModelArtifact,
) -> Result<(), CapacityModelError> {
    let service_count = grid.service_time_count as usize;
    let capacity_count = grid.capacity_count as usize;
    let collapse_count = grid.collapse_count as usize;
    let service_stride = capacity_count * collapse_count;
    let services = (0..service_count)
        .map(|index| grid.service_times_seconds[index * service_stride])
        .collect::<Vec<_>>();
    let capacities = (0..capacity_count)
        .map(|index| grid.capacities_per_second[index * collapse_count])
        .collect::<Vec<_>>();
    let collapses = &grid.collapse_values[..collapse_count];
    let (service_lower, service_upper, capacity_lower, capacity_upper) = match grid.prior {
        CapacityPrior::LogUniform => (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64),
        CapacityPrior::LogNormal {
            service_time_median_seconds,
            capacity_median_per_second,
            log_standard_deviation,
        } => {
            let service = LogNormal::new(service_time_median_seconds.ln(), log_standard_deviation)
                .map_err(|_| CapacityModelError::InvalidCapacityPrior)?;
            let capacity = LogNormal::new(capacity_median_per_second.ln(), log_standard_deviation)
                .map_err(|_| CapacityModelError::InvalidCapacityPrior)?;
            (
                service.cdf(services[0]),
                1.0_f64 - service.cdf(services[services.len() - 1]),
                capacity.cdf(capacities[0]),
                1.0_f64 - capacity.cdf(capacities[capacities.len() - 1]),
            )
        }
    };
    let axis_error = |values: &[f64], bounds: &[(f64, f64)]| {
        values
            .iter()
            .zip(bounds)
            .map(|(&value, &(low, high))| ((value - low) / value).max((high - value) / value))
            .fold(0.0_f64, f64::max)
    };
    let service_error = axis_error(&services, &log_axis_bounds(&services));
    let capacity_error = axis_error(&capacities, &log_axis_bounds(&capacities));
    // Collapse is dimensionless and centered near one. Its record uses the
    // absolute half width instead of a relative error at the zero endpoint.
    let collapse_error = collapses
        .iter()
        .zip(collapse_axis_bounds(collapses))
        .map(|(&value, (low, high))| (value - low).max(high - value))
        .fold(0.0_f64, f64::max);
    let curve_coverage_start = artifact.coverage.len();
    artifact.coverage.extend([
        PriorCoverageRecord::new(
            services[0],
            services[services.len() - 1],
            service_lower,
            service_upper,
            service_error,
        ),
        PriorCoverageRecord::new(
            capacities[0],
            capacities[capacities.len() - 1],
            capacity_lower,
            capacity_upper,
            capacity_error,
        ),
        PriorCoverageRecord::new(
            collapses[0],
            collapses[collapses.len() - 1],
            0.0_f64,
            0.0_f64,
            collapse_error,
        ),
    ]);
    let curve_coverage = &artifact.coverage[curve_coverage_start..];
    if curve_coverage.iter().all(|record| {
        record.tail_probability() <= artifact.budget.boundary_probability_max()
            && record.decision_cost_error() <= CURVE_GRID_RELATIVE_ERROR_MAX
    }) {
        Ok(())
    } else {
        Err(CapacityModelError::GridCoverage)
    }
}

/// Discretizes the Gamma hazard prior with exact linear-rate cell masses.
///
/// Log spacing only sets cell boundaries. Gamma integration includes the
/// log-cell-width Jacobian, so this function does not change the exponent.
fn capacity_model_artifact_with_groups(
    mean_per_second: f64,
    shape: f64,
    resource_window_group_count_max: u32,
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
    let mut coverage = Vec::with_capacity(6);
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
        start_delay_evidence: StartDelayEvidence::DiscardedWithoutRecovery,
        resource_window_group_count_max,
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
    if artifact.coverage[OBSERVATION_COVERAGE_INDEX].decision_cost_error()
        != OBSERVATION_PROBABILITY_ERROR_MAX
        || artifact.coverage[HAZARD_COVERAGE_INDEX].decision_cost_error()
            != HAZARD_TRANSITION_PROBABILITY_ERROR_MAX
        || artifact.coverage[SOLVER_COVERAGE_INDEX].decision_cost_error()
            != PATH_SOLVER_PROBABILITY_ERROR_MAX
        || artifact.coverage[SOLVER_COVERAGE_INDEX].tail_probability()
            > PATH_SOLVER_PROBABILITY_ERROR_MAX
        || artifact.budget.path_time_error_seconds() != REPORT_CLOCK_ERROR_SECONDS
        || artifact.markov_clock_assumption != MarkovClockAssumption::MemorylessAggregateCompletions
        || artifact.start_delay_evidence != StartDelayEvidence::DiscardedWithoutRecovery
        || artifact.resource_window_group_count_max == 0
    {
        return Err(CapacityModelError::InvalidObservationContract);
    }
    Ok(artifact)
}

#[cfg(test)]
fn capacity_model_artifact(
    mean_per_second: f64,
    shape: f64,
) -> Result<CapacityModelArtifact, CapacityModelError> {
    capacity_model_artifact_with_groups(mean_per_second, shape, 1)
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

/// Builds the joint filter prior as the outer product of the hazard and
/// contamination weights, with one copy of the capacity prior per pair.
fn filter_prior(
    hazard_weights: &[f64],
    contamination_weights: &[f64],
    prior_weights: &[f64],
    filter_count: usize,
) -> (Vec<f64>, Vec<f64>) {
    let mut filter_weights = Vec::with_capacity(filter_count);
    let mut filter_curve_weights = Vec::with_capacity(filter_count * prior_weights.len());
    for &hazard_weight in hazard_weights {
        for &contamination_weight in contamination_weights {
            filter_weights.push(hazard_weight * contamination_weight);
            filter_curve_weights.extend_from_slice(prior_weights);
        }
    }
    (filter_weights, filter_curve_weights)
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
    end_micros: u64,
    started_attempts: Option<u32>,
) {
    history[*head] = StartWindow {
        end_micros,
        exposure_seconds: window.exposure_seconds(),
        started_attempts,
    };
    *head = (*head + 1) % history.len();
    *length = (*length + 1).min(history.len());
}

fn record_start_gap(
    history: &mut [StartWindow],
    head: &mut usize,
    length: &mut usize,
    end_micros: u64,
    exposure: Duration,
) {
    history[*head] = StartWindow {
        end_micros,
        exposure_seconds: exposure.as_secs_f64(),
        started_attempts: None,
    };
    *head = (*head + 1) % history.len();
    *length = (*length + 1).min(history.len());
}

fn posterior_update_eligible(prior_predictive: f64) -> bool {
    prior_predictive.is_finite()
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

fn fill_knee_state_rates(grid: &CapacityGrid, index: usize, rates: &mut [f64]) {
    let service_time_seconds = grid.service_times_seconds[index];
    let capacity_per_second = grid.capacities_per_second[index];
    let collapse = grid.collapse_values[index];
    let knee = capacity_per_second * service_time_seconds;
    for (state, rate) in rates.iter_mut().enumerate() {
        let concurrency = u32::try_from(state).map_or(f64::from(u32::MAX), f64::from);
        *rate = if concurrency <= 0.0_f64 {
            0.0_f64
        } else if concurrency <= knee {
            concurrency / service_time_seconds
        } else {
            let excess = (concurrency - knee) / knee;
            capacity_per_second / (1.0_f64 + collapse * excess * excess)
        };
    }
}

fn fill_no_knee_state_rates(grid: &CapacityGrid, index: usize, rates: &mut [f64]) {
    let service_time_seconds = grid.service_times_seconds[index];
    for (state, rate) in rates.iter_mut().enumerate() {
        let concurrency = u32::try_from(state).map_or(f64::from(u32::MAX), f64::from);
        *rate = if concurrency <= 0.0_f64 {
            0.0_f64
        } else {
            concurrency / service_time_seconds
        };
    }
}

#[cfg(test)]
fn fill_state_rates(grid: &CapacityGrid, index: usize, rates: &mut [f64]) {
    if index < grid.knee_cell_count as usize {
        fill_knee_state_rates(grid, index, rates);
    } else {
        fill_no_knee_state_rates(grid, index, rates);
    }
}

fn linear_rate_band(grid: &CapacityGrid, index: usize) -> LinearRateBand {
    LinearRateBand {
        service_time_seconds: grid.service_times_seconds[index],
        state_max: grid.linear_state_max[index],
    }
}

/// Folds a boundary trace into per-state exposure and completion counts.
///
/// One trace group is a simultaneous batch: the state path is defined at
/// group boundaries only, and a group's completions are attributed to the
/// state that accrued the exposure they came from.
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
        completion_counts[state] = completion_counts[state].saturating_add(completed);
        state = state + started as usize - completed as usize;
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
    linear_rate_band: LinearRateBand,
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<f64> {
    feasibility_probability_and_charge(
        rates,
        linear_rate_band,
        evidence,
        probabilities,
        coefficients,
        work,
    )
    .map(|(probability, _)| probability)
}

fn feasibility_probability_and_charge(
    rates: &[f64],
    linear_rate_band: LinearRateBand,
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
) -> Option<(f64, f64)> {
    feasibility_probability_with_budget(
        rates,
        linear_rate_band,
        evidence,
        probabilities,
        coefficients,
        work,
        PATH_SOLVER_PROBABILITY_ERROR_MAX,
    )
}

fn feasibility_probability_with_budget(
    rates: &[f64],
    linear_rate_band: LinearRateBand,
    evidence: OccupancyTraceEvidence<'_>,
    probabilities: &mut [f64],
    coefficients: &mut [f64],
    work: &mut [f64],
    budget: f64,
) -> Option<(f64, f64)> {
    let c_max = probabilities.len() - 1;
    let total_starts = evidence.window().started_attempts() as usize;
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
    // `safe_mass` owns paths that cannot exceed `c_max` after all remaining
    // starts. `low` is the first path that can still exceed that limit.
    // Each death step drops below-`low` mass so the caller can add it once.
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
            linear_rate_band,
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
    None
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
    feasibility_probability_with_rates(
        &rates,
        linear_rate_band(grid, index),
        evidence,
        probabilities,
        coefficients,
        work,
    )
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
    let rough_normalizer = feasibility_probability_with_rates(
        &rates,
        linear_rate_band(grid, index),
        evidence,
        probabilities,
        coefficients,
        work,
    )?;
    // The ratio uses two solver results. Give each result one quarter of the
    // target times the denominator, so their combined ratio error stays bound.
    let solver_budget = PATH_SOLVER_PROBABILITY_ERROR_MAX * rough_normalizer / 4.0_f64;
    let (normalizer, _) = feasibility_probability_with_budget(
        &rates,
        linear_rate_band(grid, index),
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
    let mut remaining_starts = evidence.window().started_attempts() as usize;
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
            linear_rate_band(grid, index),
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
        linear_rate_band(grid, index),
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

/// Evolves one reachable pure-death band with its exact structural kernel.
///
/// Linear-rate bands use binomial thinning. Equal-rate bands use the Erlang
/// limit. Mixed bands use uniformization with a charged Poisson tail.
fn pure_death_step_with_rates(
    rates: &[f64],
    linear_rate_band: LinearRateBand,
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
    if high <= linear_rate_band.state_max {
        let source_count = high.checked_sub(low)?.checked_add(1)?;
        let charge_count = u32::try_from(source_count.checked_add(1)?).ok()?;
        let source_charge = ledger.group_budget / f64::from(charge_count);
        linear_rate_death_step(
            linear_rate_band.service_time_seconds,
            band,
            probabilities,
            work,
            SpreadTruncation::Charged(source_charge),
            ledger,
        )?;
        return Some(source_charge);
    }
    let first_rate = rates[low];
    // This bitwise scan is semantic; knee thresholds diverge at integer-knee ULP
    // corners and width-one collapse bands, while its linear cost is negligible.
    if rates[low..=high]
        .iter()
        .all(|rate| rate.to_bits() == first_rate.to_bits())
    {
        equal_rate_death_step(
            first_rate,
            low,
            high,
            exposure_seconds,
            probabilities,
            coefficients,
            work,
        )?;
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
    let mut ledger = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX)?;
    pure_death_step_with_rates(
        &rates,
        linear_rate_band(grid, index),
        band,
        probabilities,
        coefficients,
        work,
        &mut ledger,
    )?;
    Some(())
}

/// Evolves one band whose members have independent exponential lifetimes.
///
/// The branch retains survivors at or above `low` inside the certified spread.
/// It drops exact below-`low` mass and charged spread tails. The caller reads
/// both losses through `safe_mass`. The binomial values inside the spread have
/// zero truncation error. The ledger charges only the omitted spread tails.
fn linear_rate_death_step(
    service_time_seconds: f64,
    band: DeathBand,
    probabilities: &mut [f64],
    work: &mut [f64],
    truncation: SpreadTruncation,
    ledger: &mut ErrorLedger,
) -> Option<()> {
    let DeathBand {
        low,
        high,
        exposure_seconds,
    } = band;
    work[low..=high].fill(0.0_f64);
    let survival = (-exposure_seconds / service_time_seconds).exp();
    for (source, &source_probability) in probabilities.iter().enumerate().take(high + 1).skip(low) {
        let source_u32 = u32::try_from(source).ok()?;
        let (window_low, window_high) = match truncation {
            #[cfg(test)]
            SpreadTruncation::Disabled => (low, source),
            SpreadTruncation::Charged(charge) => {
                let source_count = f64::from(source_u32);
                let mean = source_count * survival;
                let half_width = (source_count * (2.0_f64 / charge).ln() / 2.0_f64).sqrt();
                let spread_low = (mean - half_width).ceil().max(0.0_f64) as usize;
                let spread_high = (mean + half_width).floor().min(source_count) as usize;
                ledger.charge(charge);
                (low.max(spread_low), spread_high)
            }
        };
        if window_low > window_high {
            continue;
        }
        if survival == 0.0_f64 {
            if window_low == 0 {
                work[0] += source_probability;
            }
            continue;
        }
        for (survivors, destination) in work
            .iter_mut()
            .enumerate()
            .take(window_high + 1)
            .skip(window_low)
        {
            *destination += source_probability
                * binomial_log_probability(source_u32, survivors, survival).exp();
        }
    }
    probabilities[low..=high].copy_from_slice(&work[low..=high]);
    Some(())
}

/// Evolves an equal-rate band from independent log-space Poisson terms.
///
/// `log_factorials` is construction-sized scratch. Each term that underflows
/// is below `f64::MIN_POSITIVE`. The total omitted mass is at most the band
/// width times that value for each source state.
fn equal_rate_death_step(
    rate: f64,
    low: usize,
    high: usize,
    exposure_seconds: f64,
    probabilities: &mut [f64],
    log_factorials: &mut [f64],
    work: &mut [f64],
) -> Option<()> {
    work[low..=high].fill(0.0_f64);
    let mean = rate * exposure_seconds;
    let width = high.checked_sub(low)?;
    log_factorials[0] = 0.0_f64;
    for deaths in 1..=width {
        let Ok(deaths_u32) = u32::try_from(deaths) else {
            return None;
        };
        log_factorials[deaths] = log_factorials[deaths - 1] + f64::from(deaths_u32).ln();
    }
    let log_mean = mean.ln();
    for source in low..=high {
        for deaths in 0..=source - low {
            let Ok(deaths_u32) = u32::try_from(deaths) else {
                return None;
            };
            let probability = if mean == 0.0_f64 {
                f64::from(u8::from(deaths == 0))
            } else {
                (-mean + f64::from(deaths_u32) * log_mean - log_factorials[deaths]).exp()
            };
            work[source - deaths] += probabilities[source] * probability;
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
    let step_count = (rate * exposure_seconds / mean_limit).ceil().max(1.0_f64) as u32;
    let step_count_f64 = f64::from(step_count);
    let step_seconds = exposure_seconds / step_count_f64;
    let charge_count = step_count.checked_add(1)?;
    let charge_count_f64 = f64::from(charge_count);
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

#[cfg(test)]
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
    let mut known_mean = 0.0_f64;
    let mut known_overlap = 0.0_f64;
    let mut posterior_shape = arrival_shape;
    let mut posterior_rate = arrival_rate_seconds;
    for offset in 0..history.length {
        let index = (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[index];
        let age = Duration::from_micros(history.end_micros.saturating_sub(start_window.end_micros))
            .as_secs_f64();
        let window_end = age + start_window.exposure_seconds;
        if let Some(starts) = start_window.started_attempts {
            posterior_shape += f64::from(starts);
            posterior_rate += start_window.exposure_seconds;
            let overlap = (window_end.min(target_end) - age.max(delay)).max(0.0_f64);
            known_overlap += overlap;
            known_mean += f64::from(starts) * overlap / start_window.exposure_seconds;
        }
    }
    let missing = (window.exposure_seconds() - known_overlap).max(0.0_f64);
    known_mean + posterior_shape / posterior_rate * missing
}

#[cfg_attr(
    feature = "hotpath",
    hotpath::measure(label = "completion_log_likelihood")
)]
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
        simd_level,
        coefficients,
        convolution,
        binomial,
        ln_gamma_integers,
    } = scratch;
    let completed = window.completed_attempts as usize;
    if completed >= coefficients.len() {
        return f64::NEG_INFINITY;
    }
    let delay = effective_service_time(grid, index, window.concurrency);
    let target_end = delay + window.exposure_seconds();
    let mut known_overlap = 0.0_f64;
    let mut posterior_shape = arrival_shape;
    let mut posterior_rate = arrival_rate_seconds;
    let mut deterministic = 0_usize;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let age = Duration::from_micros(history.end_micros.saturating_sub(start_window.end_micros))
            .as_secs_f64();
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
    }
    if deterministic > completed {
        return f64::NEG_INFINITY;
    }
    let target = completed - deterministic;
    coefficients[..=target].fill(0.0_f64);
    coefficients[0] = 1.0_f64;
    let mut degree = 0_usize;
    let mut coefficient_log_scale = 0.0_f64;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let age = Duration::from_micros(history.end_micros.saturating_sub(start_window.end_micros))
            .as_secs_f64();
        let window_end = age + start_window.exposure_seconds;
        if let Some(starts) = start_window.started_attempts {
            let overlap = (window_end.min(target_end) - age.max(delay)).max(0.0_f64);
            let probability = overlap / start_window.exposure_seconds;
            if probability > 0.0_f64 && probability < 1.0_f64 {
                let group_degree = (starts as usize).min(target);
                write_binomial_log_masses(
                    starts,
                    probability,
                    group_degree,
                    binomial,
                    ln_gamma_integers,
                );
                let (maximum, scale, next_degree) = dispatch!(simd_level, simd => completion_group_convolution(
                    simd,
                    coefficients,
                    convolution,
                    binomial,
                    degree,
                    group_degree,
                    target,
                ));
                if scale <= 0.0_f64 {
                    return f64::NEG_INFINITY;
                }
                degree = next_degree;
                coefficient_log_scale += maximum + scale.ln();
            }
        }
    }
    let missing = (window.exposure_seconds() - known_overlap).max(0.0_f64);
    completion_probability_from_coefficients(
        coefficients,
        degree,
        target,
        CompletionTail {
            coefficient_log_scale,
            posterior_shape,
            posterior_rate,
            missing,
            ln_gamma_integers,
        },
    )
}

#[cfg_attr(
    feature = "hotpath",
    hotpath::measure(label = "binomial_mass_generation")
)]
fn write_binomial_log_masses(
    starts: u32,
    probability: f64,
    group_degree: usize,
    output: &mut [f64],
    ln_gamma_integers: &[f64],
) {
    // Observation intake rejects windows whose started attempts exceed the
    // attempt cap, so `trials` and both derived indexes stay inside the table.
    let trials = starts as usize;
    let trials_float = f64::from(starts);
    let trials_log_gamma = ln_gamma_integers[trials];
    let log_probability = probability.ln();
    let log_failure_probability = (-probability).ln_1p();
    for (mass_index, value) in output[..=group_degree].iter_mut().enumerate() {
        let count = f64::from(u32::try_from(mass_index).map_or(u32::MAX, |value| value));
        let log_combination = trials_log_gamma
            - ln_gamma_integers[mass_index]
            - ln_gamma_integers[trials - mass_index];
        *value = (trials_float - count).mul_add(
            log_failure_probability,
            count.mul_add(log_probability, log_combination),
        );
    }
}

#[cfg_attr(
    feature = "hotpath",
    hotpath::measure(label = "completion_group_convolution")
)]
fn completion_group_convolution<S: Simd>(
    simd: S,
    coefficients: &mut [f64],
    convolution: &mut [f64],
    binomial: &mut [f64],
    degree: usize,
    group_degree: usize,
    target: usize,
) -> (f64, f64, usize) {
    let next_degree = target.min(degree + group_degree);
    // Log masses are finite or negative infinity, and convolutions contain
    // non-negative products. Neither maximum scan can contain NaN.
    let maximum = exponentiate_log_masses(simd, &mut binomial[..=group_degree]);
    convolution[..=next_degree].fill(0.0_f64);
    let mut known = 0_usize;
    while known + 7 <= degree && group_degree >= 7 {
        let full_added_count = group_degree + 1;
        let eighth_added_count = group_degree.min(target - known - 7) + 1;
        if eighth_added_count != full_added_count {
            break;
        }
        convolution_axpy_eight(
            simd,
            &coefficients[known..known + 8],
            &binomial[..full_added_count],
            &mut convolution[known..known + group_degree + 8],
        );
        known += 8;
    }
    while known + 3 <= degree && group_degree >= 3 {
        let full_added_count = group_degree + 1;
        let fourth_added_count = group_degree.min(target - known - 3) + 1;
        if fourth_added_count != full_added_count {
            break;
        }
        convolution_axpy_four(
            simd,
            &coefficients[known..known + 4],
            &binomial[..full_added_count],
            &mut convolution[known..known + group_degree + 4],
        );
        known += 4;
    }
    for known in known..=degree {
        let added_count = group_degree.min(target - known) + 1;
        convolution_axpy(
            simd,
            coefficients[known],
            &binomial[..added_count],
            &mut convolution[known..known + added_count],
        );
    }
    let scale = vector_max(simd, &convolution[..=next_degree], 0.0_f64);
    vector_divide(
        simd,
        &convolution[..=next_degree],
        &mut coefficients[..=next_degree],
        scale,
    );
    (maximum, scale, next_degree)
}

fn convolution_axpy_eight<S: Simd>(
    simd: S,
    coefficients: &[f64],
    values: &[f64],
    output: &mut [f64],
) {
    for row in 0..8 {
        convolution_axpy(
            simd,
            coefficients[row],
            &values[..7 - row],
            &mut output[row..7],
        );
        let suffix_start = values.len() - row;
        convolution_axpy(
            simd,
            coefficients[row],
            &values[suffix_start..],
            &mut output[values.len()..values.len() + row],
        );
    }

    let lane_count = S::f64s::N;
    let shared_count = values.len() - 7;
    let vector_count = shared_count / lane_count;
    let coefficient_vectors: [S::f64s; 8] =
        array::from_fn(|row| S::f64s::splat(simd, coefficients[row]));
    for vector in 0..vector_count {
        let added = vector * lane_count;
        let output_start = added + 7;
        let output_end = output_start + lane_count;
        let mut current = S::f64s::from_slice(simd, &output[output_start..output_end]);
        for row in 0..8 {
            let value = S::f64s::from_slice(simd, &values[added + 7 - row..output_end - row]);
            current = coefficient_vectors[row].mul_add(value, current);
        }
        current.store_slice(&mut output[output_start..output_end]);
    }
    for added in vector_count * lane_count..shared_count {
        let output_index = added + 7;
        for row in 0..8 {
            output[output_index] =
                coefficients[row].mul_add(values[added + 7 - row], output[output_index]);
        }
    }
}

fn convolution_axpy_four<S: Simd>(
    simd: S,
    coefficients: &[f64],
    values: &[f64],
    output: &mut [f64],
) {
    for row in 0..4 {
        convolution_axpy(
            simd,
            coefficients[row],
            &values[..3 - row],
            &mut output[row..3],
        );
        let suffix_start = values.len() - row;
        convolution_axpy(
            simd,
            coefficients[row],
            &values[suffix_start..],
            &mut output[values.len()..values.len() + row],
        );
    }

    let lane_count = S::f64s::N;
    let shared_count = values.len() - 3;
    let vector_count = shared_count / lane_count;
    let coefficient_vectors = [
        S::f64s::splat(simd, coefficients[0]),
        S::f64s::splat(simd, coefficients[1]),
        S::f64s::splat(simd, coefficients[2]),
        S::f64s::splat(simd, coefficients[3]),
    ];
    for vector in 0..vector_count {
        let added = vector * lane_count;
        let output_start = added + 3;
        let output_end = output_start + lane_count;
        let mut current = S::f64s::from_slice(simd, &output[output_start..output_end]);
        for row in 0..4 {
            let value = S::f64s::from_slice(simd, &values[added + 3 - row..output_end - row]);
            current = coefficient_vectors[row].mul_add(value, current);
        }
        current.store_slice(&mut output[output_start..output_end]);
    }
    for added in vector_count * lane_count..shared_count {
        let output_index = added + 3;
        for row in 0..4 {
            output[output_index] =
                coefficients[row].mul_add(values[added + 3 - row], output[output_index]);
        }
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure(label = "mass_exponentiation"))]
fn exponentiate_log_masses<S: Simd>(simd: S, values: &mut [f64]) -> f64 {
    let maximum = vector_max(simd, values, f64::NEG_INFINITY);
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let maximum_vector = S::f64s::splat(simd, maximum);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        vector_exp(
            simd,
            S::f64s::from_slice(simd, &values[start..end]) - maximum_vector,
        )
        .store_slice(&mut values[start..end]);
    }
    let tail = &mut values[vector_count * lane_count..];
    if !tail.is_empty() {
        let input = S::f64s::from_fn(simd, |lane| {
            tail.get(lane).map_or(0.0_f64, |value| *value - maximum)
        });
        let output = vector_exp(simd, input);
        tail.copy_from_slice(&output.as_slice()[..tail.len()]);
    }
    maximum
}

fn vector_exp<S: Simd>(simd: S, value: S::f64s) -> S::f64s {
    const LN_2_HIGH: f64 = 6.933_593_75e-1_f64;
    const LN_2_LOW: f64 = -2.121_944_400_546_905_8e-4_f64;
    const UNDERFLOW_CUTOFF: f64 = -745.133_219_101_941_1_f64;
    const P: [f64; 3] = [
        1.261_771_930_748_105_8e-4_f64,
        3.029_944_077_074_419_6e-2_f64,
        1.0_f64,
    ];
    const Q: [f64; 4] = [
        3.001_985_051_386_644_7e-6_f64,
        2.524_483_403_496_841e-3_f64,
        2.272_655_482_081_550_2e-1_f64,
        2.0_f64,
    ];

    let exponent = (value * LOG2_E).round_ties_even();
    let reduced = exponent
        .mul_add(S::f64s::splat(simd, -LN_2_HIGH), value)
        .mul_add(S::f64s::splat(simd, 1.0_f64), exponent * -LN_2_LOW);
    let square = reduced * reduced;
    let numerator = (square * P[0] + P[1]) * square + P[2];
    let numerator = numerator * reduced;
    let denominator = ((square * Q[0] + Q[1]) * square + Q[2]) * square + Q[3];
    let polynomial = numerator.mul_add(
        S::f64s::splat(simd, 2.0_f64) / (denominator - numerator),
        S::f64s::splat(simd, 1.0_f64),
    );
    let scale = S::f64s::from_fn(simd, |lane| {
        let integer_exponent = exponent.as_slice()[lane] as i64;
        // The clamp keeps `power_of_two` arithmetic in range when a
        // discarded padding lane carries an infinite input.
        power_of_two(integer_exponent.clamp(-1_074, 1_024))
    });
    let subnormal_adjustment = S::f64s::from_fn(simd, |lane| {
        if (exponent.as_slice()[lane] as i64) < -1_074 {
            0.5_f64
        } else {
            1.0_f64
        }
    });
    let result = polynomial * subnormal_adjustment * scale;
    value
        .simd_lt(S::f64s::splat(simd, UNDERFLOW_CUTOFF))
        .select(S::f64s::splat(simd, 0.0_f64), result)
}

fn power_of_two(exponent: i64) -> f64 {
    if exponent >= -1_022 {
        let biased = u64::try_from(exponent + 1_023).map_or(u64::MAX, |value| value);
        f64::from_bits(biased << 52)
    } else {
        let shift = u32::try_from(exponent + 1_074).map_or(0, |value| value);
        f64::from_bits(1_u64 << shift)
    }
}

fn sum_shifted_exponentials<S: Simd>(simd: S, values: &[f64], maximum: f64) -> f64 {
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let maximum_vector = S::f64s::splat(simd, maximum);
    let mut total = 0.0_f64;
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let exponential = vector_exp(
            simd,
            S::f64s::from_slice(simd, &values[start..end]) - maximum_vector,
        );
        for value in exponential.as_slice() {
            total += *value;
        }
    }
    let tail = &values[vector_count * lane_count..];
    if !tail.is_empty() {
        let input = S::f64s::from_fn(simd, |lane| {
            tail.get(lane).map_or(0.0_f64, |value| *value) - maximum
        });
        let exponential = vector_exp(simd, input);
        for value in &exponential.as_slice()[..tail.len()] {
            total += *value;
        }
    }
    total
}

fn write_normalized_exponentials<S: Simd>(
    simd: S,
    values: &[f64],
    output: &mut [f64],
    maximum: f64,
    total: f64,
) {
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let maximum_vector = S::f64s::splat(simd, maximum);
    let total = S::f64s::splat(simd, total);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        (vector_exp(
            simd,
            S::f64s::from_slice(simd, &values[start..end]) - maximum_vector,
        ) / total)
            .store_slice(&mut output[start..end]);
    }
    let tail = &values[vector_count * lane_count..];
    if !tail.is_empty() {
        let input = S::f64s::from_fn(simd, |lane| {
            tail.get(lane).map_or(0.0_f64, |value| *value) - maximum
        });
        let exponential = vector_exp(simd, input) / total;
        output[vector_count * lane_count..].copy_from_slice(&exponential.as_slice()[..tail.len()]);
    }
}

fn convolution_axpy<S: Simd>(simd: S, coefficient: f64, values: &[f64], output: &mut [f64]) {
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let coefficient = S::f64s::splat(simd, coefficient);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let value = S::f64s::from_slice(simd, &values[start..end]);
        let current = S::f64s::from_slice(simd, &output[start..end]);
        coefficient
            .mul_add(value, current)
            .store_slice(&mut output[start..end]);
    }
    for index in vector_count * lane_count..values.len() {
        output[index] = coefficient.as_slice()[0].mul_add(values[index], output[index]);
    }
}

fn vector_max<S: Simd>(simd: S, values: &[f64], initial: f64) -> f64 {
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let mut maximum = S::f64s::splat(simd, initial);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        maximum = maximum.max(S::f64s::from_slice(simd, &values[start..end]));
    }
    let mut result = maximum.as_slice().iter().copied().fold(initial, f64::max);
    for value in &values[vector_count * lane_count..] {
        result = result.max(*value);
    }
    result
}

fn vector_divide<S: Simd>(simd: S, values: &[f64], output: &mut [f64], divisor: f64) {
    let lane_count = S::f64s::N;
    let vector_count = values.len() / lane_count;
    let divisor = S::f64s::splat(simd, divisor);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let value = S::f64s::from_slice(simd, &values[start..end]);
        (value / divisor).store_slice(&mut output[start..end]);
    }
    for index in vector_count * lane_count..values.len() {
        output[index] = values[index] / divisor.as_slice()[0];
    }
}

#[cfg(test)]
fn completion_log_likelihood_reference(
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
        ..
    } = scratch;
    let completed = window.completed_attempts as usize;
    if completed >= coefficients.len() {
        return f64::NEG_INFINITY;
    }
    let delay = effective_service_time(grid, index, window.concurrency);
    let target_end = delay + window.exposure_seconds();
    let mut known_overlap = 0.0_f64;
    let mut posterior_shape = arrival_shape;
    let mut posterior_rate = arrival_rate_seconds;
    let mut deterministic = 0_usize;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let age = Duration::from_micros(history.end_micros.saturating_sub(start_window.end_micros))
            .as_secs_f64();
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
    }
    if deterministic > completed {
        return f64::NEG_INFINITY;
    }
    let target = completed - deterministic;
    coefficients[..=target].fill(0.0_f64);
    coefficients[0] = 1.0_f64;
    let mut degree = 0_usize;
    let mut coefficient_log_scale = 0.0_f64;
    for offset in 0..history.length {
        let history_index =
            (history.head + history.windows.len() - 1 - offset) % history.windows.len();
        let start_window = history.windows[history_index];
        let age = Duration::from_micros(history.end_micros.saturating_sub(start_window.end_micros))
            .as_secs_f64();
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
    }
    let missing = (window.exposure_seconds() - known_overlap).max(0.0_f64);
    completion_probability_from_coefficients_reference(
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
fn completion_probability_from_coefficients_reference(
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

#[cfg_attr(
    feature = "hotpath",
    hotpath::measure(label = "completion_probability_from_coefficients")
)]
fn completion_probability_from_coefficients(
    coefficients: &[f64],
    degree: usize,
    target: usize,
    tail: CompletionTail<'_>,
) -> f64 {
    let CompletionTail {
        coefficient_log_scale,
        posterior_shape,
        posterior_rate,
        missing,
        ln_gamma_integers,
    } = tail;
    if missing == 0.0_f64 {
        return if target <= degree && coefficients[target] > 0.0_f64 {
            coefficients[target].ln() + coefficient_log_scale
        } else {
            f64::NEG_INFINITY
        };
    }
    let success = posterior_rate / (posterior_rate + missing);
    let shape_log_gamma = ln_gamma(posterior_shape);
    let log_success = success.ln();
    let log_failure = (-success).ln_1p();
    let mut likelihood = f64::NEG_INFINITY;
    for (known, coefficient) in coefficients.iter().enumerate().take(degree.min(target) + 1) {
        if *coefficient > 0.0_f64 {
            let missing_count = target - known;
            likelihood = log_add_exp(
                likelihood,
                coefficient.ln()
                    + coefficient_log_scale
                    + negative_binomial_log_probability_hoisted(
                        posterior_shape,
                        missing_count,
                        shape_log_gamma,
                        log_success,
                        log_failure,
                        ln_gamma_integers,
                    ),
            );
        }
    }
    likelihood
}

fn negative_binomial_log_probability_hoisted(
    shape: f64,
    count: usize,
    shape_log_gamma: f64,
    log_success: f64,
    log_failure: f64,
    ln_gamma_integers: &[f64],
) -> f64 {
    let count_float = f64::from(u32::try_from(count).map_or(u32::MAX, |value| value));
    ln_gamma(count_float + shape) - shape_log_gamma - ln_gamma_integers[count]
        + shape * log_success
        + count_float * log_failure
}

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
) -> Result<Vec<f64>, CapacityModelError> {
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
        log_normal_axis_masses(&service_values, service_median, log_standard_deviation)?;
    let capacity_masses =
        log_normal_axis_masses(&capacity_values, capacity_median, log_standard_deviation)?;
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
    Ok(weights)
}

pub(crate) fn log_normal_axis_masses(
    values: &[f64],
    median: f64,
    log_standard_deviation: f64,
) -> Result<Vec<f64>, CapacityModelError> {
    let distribution = LogNormal::new(median.ln(), log_standard_deviation)
        .map_err(|_| CapacityModelError::InvalidCapacityPrior)?;
    let masses = (0..values.len())
        .map(|index| {
            let lower = if index == 0 {
                values[0]
            } else {
                (values[index - 1] * values[index]).sqrt()
            };
            let upper = if index + 1 == values.len() {
                values[values.len() - 1]
            } else {
                (values[index] * values[index + 1]).sqrt()
            };
            distribution.cdf(upper) - distribution.cdf(lower)
        })
        .collect::<Vec<_>>();
    let total = masses.iter().sum::<f64>();
    if !total.is_finite() || total <= 0.0_f64 {
        return Err(CapacityModelError::InvalidCapacityPrior);
    }
    Ok(masses.into_iter().map(|mass| mass / total).collect())
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

pub(super) fn curve_throughput<S: Simd>(
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
        let concurrency = concurrency[candidate];
        output[candidate] = if concurrency <= 0.0_f64 {
            0.0_f64
        } else {
            (concurrency / service_time_seconds).min(ceiling)
        };
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
    /// The capacity scale prior cannot form finite truncated masses.
    #[error("the capacity scale prior is invalid")]
    InvalidCapacityPrior,
    /// A curve cell exceeds the tail or decision-cost error budget.
    #[error("the capacity grid exceeds its coverage budget")]
    GridCoverage,
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
