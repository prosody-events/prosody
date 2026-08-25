use std::{
    borrow::Borrow,
    collections::HashMap,
    f64::consts::{E, LOG2_E, PI},
    iter::repeat_n,
    mem::size_of,
    time::Duration,
};

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use rand_distr::{Binomial, Distribution};
use statrs::distribution::{Beta, ContinuousCDF, Gamma, LogNormal, Normal};
use statrs::function::gamma::{gamma_lr, gamma_ur, ln_gamma};
use thiserror::Error;

use crate::change_point::ChangePointKernel;
use crate::random::{PoissonMean, RandomStream, sample_gamma, sample_poisson};
use crate::types::prior_artifact_contract_holds;
use crate::{
    OccupancyTraceEvidence, PriorArtifact, PriorArtifactBudget, PriorArtifactIdentity,
    PriorCoverageRecord,
};

const CAPACITY_MODEL_STORAGE_BYTES_MAX: usize = 512 * 1_024 * 1_024;
const CAPACITY_MODEL_ARTIFACT_SOURCE: u64 = 0x4341_5041_4349_5459;
const CAPACITY_MODEL_ARTIFACT_VERSION: u32 = 5;
const SERVICE_SHAPES: [u32; 6] = [1, 2, 4, 8, 16, 32];
const SERVICE_CLOCKS: [ServiceClock; 7] = [
    ServiceClock::Erlang(SERVICE_SHAPES[0]),
    ServiceClock::Erlang(SERVICE_SHAPES[1]),
    ServiceClock::Erlang(SERVICE_SHAPES[2]),
    ServiceClock::Erlang(SERVICE_SHAPES[3]),
    ServiceClock::Erlang(SERVICE_SHAPES[4]),
    ServiceClock::Erlang(SERVICE_SHAPES[5]),
    ServiceClock::Deterministic,
];
/// The completion predictive permits at most `1 / 64` rank error.
const COMPLETION_PREDICTIVE_RANK_ERROR_MAX: f64 = 1.0_f64 / 64.0_f64;
/// One predictive sweep uses at most 8,192 Monte Carlo draws.
///
/// The median has the largest rank standard error. The order-statistic normal
/// approximation gives `sqrt(0.5 * 0.5 / 8,192) < 1 / 128`. This gives a
/// factor-two margin against [`COMPLETION_PREDICTIVE_RANK_ERROR_MAX`].
/// A smaller attempt contract keeps one stratum for each possible count.
const COMPLETION_PREDICTIVE_DRAW_COUNT: u32 = 8_192;
const _: () = assert!(
    0.25_f64 / COMPLETION_PREDICTIVE_DRAW_COUNT as f64
        <= (COMPLETION_PREDICTIVE_RANK_ERROR_MAX / 2.0_f64)
            * (COMPLETION_PREDICTIVE_RANK_ERROR_MAX / 2.0_f64),
    "the predictive draw count must satisfy the rank error bound"
);
const WITHIN_CELL_NEWTON_STEPS: u64 = 5;
/// The within-cell Laplace evidence has relative error at most `8 / n`.
///
/// This Bernstein-von-Mises bound applies when the mode is interior, the
/// curvature is positive, and `n` completed durations are available.
const WITHIN_CELL_LAPLACE_ERROR_CONSTANT: f64 = 8.0_f64;
/// One capacity update can use at most 1.6 billion simple operations.
///
/// The duration price table has a maximum below 800 million operations.
/// Two-times headroom fits the rounded bound.
const CAPACITY_UPDATE_OPERATION_COUNT_MAX: u64 = 1_600_000_000;
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
const CAPACITY_MODEL_BUDGET: PriorArtifactBudget = PriorArtifactBudget::new(
    (CAPACITY_MODEL_STORAGE_BYTES_MAX / size_of::<f64>()) as u32,
    CAPACITY_MODEL_STORAGE_BYTES_MAX as u64,
    CAPACITY_UPDATE_OPERATION_COUNT_MAX,
    1.0e-6_f64,
    REPORT_CLOCK_ERROR_SECONDS,
    CURVE_GRID_RELATIVE_ERROR_MAX,
);
/// Versioned prior and approximation limits for the capacity model.
///
/// Version 5 uses at most 8,192 completion-predictive draws. The median rank
/// error has a factor-two margin below the authorized `1 / 64`
/// error. This version uses labeled key queues for generative completion
/// sampling. One
/// fleet clock applies fleet-wide state rates to all owner queues. It uses
/// observed arrivals as exogenous events. Sampled completions close keys.
/// The update still conditions on the observed occupancy and duration paths.
/// It assigns one
/// affected window and nine clean windows to observation quality. It assigns
/// equal prior odds to the bounded knee family and the no-knee family. Within
/// the knee family, it assigns equal odds to no collapse and positive collapse.
/// The clock family gives equal prior odds to six Erlang clocks and one
/// deterministic clock. The deterministic clock is the exact infinite-shape
/// Erlang limit. The quadratic collapse law represents pairwise contention.
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
    service_clock_assumption: ServiceClockAssumption,
    service_duration_evidence: ServiceDurationEvidence,
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
enum ServiceClockAssumption {
    ErlangAndDeterministicRenewal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceClock {
    Erlang(u32),
    Deterministic,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum OwnerKeyLayout {
    Unique,
    Serialized,
    General,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceDurationEvidence {
    ObservedAttemptDurationsAndBoundaryAges,
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
struct CapacityAllocation {
    cell_count: usize,
    state_count: usize,
    filter_count: usize,
    filter_curve_count: usize,
    transition_count: usize,
    group_limit: usize,
}

struct GeneratedWindow {
    now_seconds: f64,
    busy: u32,
    available: u32,
    completed: u32,
}

#[derive(Clone, Copy)]
struct DurationStatistics {
    completion_count: u32,
    duration_sum_seconds: f64,
    log_duration_sum_seconds: f64,
}

struct WithinCellPosterior<'a, 'b> {
    shape: u32,
    low: f64,
    high: f64,
    evidence: &'a OccupancyTraceEvidence<'b>,
    statistics: DurationStatistics,
    aggregate_count: u32,
    prior: CapacityPrior,
}

struct DeterministicWithinCellPosterior<'a, 'b> {
    low: f64,
    high: f64,
    evidence: &'a OccupancyTraceEvidence<'b>,
    aggregate_count: u32,
    prior: CapacityPrior,
}

pub(crate) struct CapacityFactor {
    simd_level: Level,
    grid: CapacityGrid,
    change_hazard_shape: f64,
    concurrency_max: f64,
    exposure_min_seconds: f64,
    history_coverage_seconds: f64,
    resource_window_group_count_max: u32,
    prior_weights: Vec<f64>,
    weights: Vec<f64>,
    likelihoods: Vec<f64>,
    shape_weights: [f64; SERVICE_CLOCKS.len()],
    shape_scores: Vec<f64>,
    shape_cell_weights: Vec<f64>,
    hazard_rates_per_second: Vec<f64>,
    contamination_probabilities: Vec<f64>,
    /// Provides the normalized linear view of `filter_log_weights`.
    filter_weights: Vec<f64>,
    /// Stores the authoritative normalized filter posterior in the log domain.
    filter_log_weights: Vec<f64>,
    filter_curve_weights: Vec<f64>,
    observation_clock_micros: u64,
    previous_window_concurrency: Option<f64>,
    completion_coefficients: Vec<f64>,
    predictive_stage_scratch: Vec<f64>,
    predictive_owner_snapshot: Vec<OwnerGeneratedWindow>,
    predictive_owner_scratch: Vec<OwnerGeneratedWindow>,
    predictive_arrival_key_scratch: Vec<usize>,
    predictive_owner_key_index_scratch: HashMap<u64, usize>,
    predictive_owner_count_scratch: Vec<OwnerCount>,
    predictive_owner_key_scratch: Vec<u64>,
    predictive_owner_slot_scratch: Vec<u64>,
    duration_rates: Vec<f64>,
    duration_log_rate_modes: Vec<f64>,
    duration_log_rate_curvatures: Vec<f64>,
    duration_log_rate_lows: Vec<f64>,
    duration_log_rate_highs: Vec<f64>,
    duration_draw_cdf_lows: Vec<f64>,
    duration_draw_cdf_highs: Vec<f64>,
    state_exposure_seconds: Vec<f64>,
    state_completion_counts: Vec<u32>,
    state_rates: Vec<f64>,
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
        change_hazard_shape: f64,
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
        let history_coverage_seconds = history_coverage(&grid, concurrency_max)?;
        let artifact = capacity_model_artifact_with_groups(
            change_rate_per_second,
            change_hazard_shape,
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
        let transition_count = attempt_buffer_count(attempt_count_max)?;
        validate_capacity_allocation(
            &grid,
            &artifact,
            CapacityAllocation {
                cell_count,
                state_count,
                filter_count,
                filter_curve_count,
                transition_count,
                group_limit: group_count_max as usize,
            },
        )?;
        let (filter_weights, filter_curve_weights) = filter_prior(
            &hazard_weights,
            &contamination_weights,
            &prior_weights,
            filter_count,
        );
        let filter_log_weights = filter_weights.iter().map(|weight| weight.ln()).collect();
        let (duration_log_rate_modes, duration_log_rate_lows, duration_log_rate_highs) =
            duration_log_rate_bounds(&grid);
        Ok(Self {
            simd_level: Level::new(),
            grid,
            change_hazard_shape,
            concurrency_max,
            exposure_min_seconds,
            history_coverage_seconds,
            resource_window_group_count_max: group_count_max,
            weights: prior_weights.clone(),
            prior_weights,
            likelihoods: vec![0.0_f64; cell_count],
            shape_weights: [1.0_f64 / f64::from(SERVICE_CLOCKS.len() as u32); SERVICE_CLOCKS.len()],
            shape_scores: vec![0.0_f64; cell_count * SERVICE_CLOCKS.len()],
            shape_cell_weights: vec![
                1.0_f64 / f64::from(SERVICE_CLOCKS.len() as u32);
                cell_count * SERVICE_CLOCKS.len()
            ],
            hazard_rates_per_second,
            contamination_probabilities,
            filter_log_weights,
            filter_weights,
            filter_curve_weights,
            observation_clock_micros: 0,
            previous_window_concurrency: None,
            completion_coefficients: vec![0.0_f64; attempt_count_max as usize + 1],
            predictive_stage_scratch: Vec::with_capacity(attempt_count_max as usize),
            predictive_owner_snapshot: Vec::with_capacity(group_count_max as usize),
            predictive_owner_scratch: Vec::with_capacity(group_count_max as usize),
            predictive_arrival_key_scratch: Vec::with_capacity(attempt_count_max as usize),
            predictive_owner_key_index_scratch: HashMap::with_capacity(attempt_count_max as usize),
            predictive_owner_count_scratch: Vec::with_capacity(group_count_max as usize),
            predictive_owner_key_scratch: Vec::with_capacity(attempt_count_max as usize),
            predictive_owner_slot_scratch: Vec::with_capacity(attempt_count_max as usize),
            duration_rates: vec![0.0_f64; cell_count],
            duration_log_rate_modes,
            duration_log_rate_curvatures: vec![1.0e24_f64; cell_count * SERVICE_CLOCKS.len()],
            duration_log_rate_lows,
            duration_log_rate_highs,
            duration_draw_cdf_lows: vec![0.0_f64; cell_count * SERVICE_CLOCKS.len()],
            duration_draw_cdf_highs: vec![1.0_f64; cell_count * SERVICE_CLOCKS.len()],
            state_exposure_seconds: vec![0.0_f64; state_count],
            state_completion_counts: vec![0; state_count],
            state_rates: vec![0.0_f64; state_count],
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

    fn fill_duration_draw_cdfs(&mut self) {
        for cell in 0..self.weights.len() {
            let cell_low = -self.grid.service_time_highs[cell].ln();
            let cell_high = -self.grid.service_time_lows[cell].ln();
            for (shape, clock) in SERVICE_CLOCKS.iter().copied().enumerate() {
                let index = cell * SERVICE_CLOCKS.len() + shape;
                let (low, high) = if clock == ServiceClock::Deterministic {
                    (
                        self.duration_log_rate_lows[index],
                        self.duration_log_rate_highs[index],
                    )
                } else {
                    (cell_low, cell_high)
                };
                if let Ok(normal) = Normal::new(
                    self.duration_log_rate_modes[index],
                    self.duration_log_rate_curvatures[index].sqrt().recip(),
                ) {
                    self.duration_draw_cdf_lows[index] = normal.cdf(low);
                    self.duration_draw_cdf_highs[index] = normal.cdf(high);
                }
            }
        }
    }

    fn prepare_completion_owners(
        &mut self,
        evidence: &OccupancyTraceEvidence<'_>,
    ) -> (OwnerKeyLayout, bool) {
        let layout = owner_key_layout(evidence, &mut self.predictive_owner_key_scratch);
        let saturated = saturated_owner_window(
            evidence,
            layout,
            &mut self.predictive_owner_key_scratch,
            &mut self.predictive_owner_slot_scratch,
        );
        build_owner_depth_snapshot(
            evidence,
            &mut self.predictive_owner_snapshot,
            &mut self.predictive_arrival_key_scratch,
            &mut self.predictive_owner_key_index_scratch,
        );
        self.predictive_owner_scratch
            .clone_from(&self.predictive_owner_snapshot);
        (layout, saturated)
    }

    #[cfg(test)]
    pub(crate) fn new_with_prior(
        grid: CapacityGrid,
        change_rate_per_second: f64,
        change_hazard_shape: f64,
        concurrency_max: f64,
        exposure_min_seconds: f64,
        attempt_count_max: u32,
    ) -> Result<Self, CapacityModelError> {
        Self::new_with_prior_with_groups(
            grid,
            change_rate_per_second,
            change_hazard_shape,
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
            self.change_hazard_shape,
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
    /// Generates the joint completion predictive from pre-window state.
    ///
    /// Each draw starts with the pre-window busy and available work counts.
    /// Demand arrivals and generated completions determine all later starts.
    /// Live-attempt ages do not change the model's memoryless completion draw.
    fn completion_predictive_sweep(
        &mut self,
        evidence: &OccupancyTraceEvidence<'_>,
        seed: u64,
        count_max: u32,
        mut visit: impl FnMut(u32, f64) -> bool,
    ) {
        self.completion_coefficients.fill(0.0_f64);
        let (owner_key_layout, saturated_owner_window) = self.prepare_completion_owners(evidence);
        self.fill_duration_draw_cdfs();
        let sample_count =
            COMPLETION_PREDICTIVE_DRAW_COUNT.min(self.completion_coefficients.len() as u32);
        let sample_count_reciprocal = f64::from(sample_count).recip();
        let mut cell = 0_usize;
        let mut cumulative = self.weights[0];
        for scenario in 0..sample_count {
            let mut random = RandomStream::new(seed)
                .domain(0x636f_6d70_6c65_7465)
                .domain(u64::from(scenario));
            let posterior_draw = (f64::from(scenario) + 0.5_f64) * sample_count_reciprocal;
            while posterior_draw > cumulative && cell + 1 < self.weights.len() {
                cell += 1;
                cumulative += self.weights[cell];
            }
            let shape_scenario = scenario.wrapping_mul(2_653_443_761) % sample_count;
            let shape_draw = (f64::from(shape_scenario) + 0.5_f64) * sample_count_reciprocal;
            let shape_start = cell * SERVICE_CLOCKS.len();
            let shape_weights =
                &self.shape_cell_weights[shape_start..shape_start + SERVICE_CLOCKS.len()];
            let mut shape_index = 0_usize;
            let mut shape_cumulative = shape_weights[0];
            while shape_draw > shape_cumulative && shape_index + 1 < SERVICE_CLOCKS.len() {
                shape_index += 1;
                shape_cumulative += shape_weights[shape_index];
            }
            let rate_index = cell * SERVICE_CLOCKS.len() + shape_index;
            let mut rate_random = RandomStream::new(seed)
                .domain(0x7261_7465_5f64_7261)
                .domain(u64::from(scenario));
            let slot_rate = match SERVICE_CLOCKS[shape_index] {
                ServiceClock::Erlang(_) => truncated_log_rate_draw(
                    self.duration_log_rate_modes[rate_index],
                    self.duration_log_rate_curvatures[rate_index],
                    -self.grid.service_time_highs[cell].ln(),
                    -self.grid.service_time_lows[cell].ln(),
                    self.duration_draw_cdf_lows[rate_index],
                    self.duration_draw_cdf_highs[rate_index],
                    rate_random.open_unit_f64(),
                ),
                ServiceClock::Deterministic => deterministic_log_rate_draw(DeterministicRateDraw {
                    mode: self.duration_log_rate_modes[rate_index],
                    curvature: self.duration_log_rate_curvatures[rate_index],
                    low: self.duration_log_rate_lows[rate_index],
                    high: self.duration_log_rate_highs[rate_index],
                    cell: (
                        -self.grid.service_time_highs[cell].ln(),
                        -self.grid.service_time_lows[cell].ln(),
                    ),
                    prior: self.grid.prior,
                    lower_cdf: self.duration_draw_cdf_lows[rate_index],
                    upper_cdf: self.duration_draw_cdf_highs[rate_index],
                    draw: rate_random.open_unit_f64(),
                }),
            };
            let completed = generate_completion_count(
                evidence,
                &CompletionWalk {
                    grid: &self.grid,
                    cell,
                    clock: SERVICE_CLOCKS[shape_index],
                    slot_rate,
                },
                evidence
                    .slot_count()
                    .min(evidence.dispatchable_demand_ceiling()),
                &mut random,
                CompletionGeneration {
                    stages: &mut self.predictive_stage_scratch,
                    owners: &mut self.predictive_owner_scratch,
                    owner_snapshot: &self.predictive_owner_snapshot,
                    arrival_keys: &self.predictive_arrival_key_scratch,
                    owner_counts: &mut self.predictive_owner_count_scratch,
                    owner_key_layout,
                    saturated_owner_window,
                    owner_keys: &self.predictive_owner_key_scratch,
                    owner_slots: &self.predictive_owner_slot_scratch,
                },
            );
            let bucket = match usize::try_from(completed) {
                Ok(count) => count,
                Err(_) => usize::MAX,
            }
            .min(self.completion_coefficients.len() - 1);
            self.completion_coefficients[bucket] += 1.0_f64;
        }
        visit_completion_cdf(
            &self.completion_coefficients,
            sample_count_reciprocal,
            count_max,
            &mut visit,
        );
    }

    pub(crate) fn completion_predictive_summary<'a>(
        &mut self,
        evidence: impl Borrow<OccupancyTraceEvidence<'a>>,
        seed: u64,
        observed: u32,
        thresholds: [f64; 3],
    ) -> CompletionPredictiveSummary {
        let evidence = evidence.borrow();
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
        self.completion_predictive_sweep(evidence, seed, count_max, |count, cdf| {
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
    pub(crate) fn completion_predictive_cdf<'a>(
        &mut self,
        evidence: impl Borrow<OccupancyTraceEvidence<'a>>,
        seed: u64,
        completed_attempts: u32,
    ) -> f64 {
        let evidence = evidence.borrow();
        if completed_attempts as usize >= self.completion_coefficients.len() {
            return 1.0_f64;
        }
        let mut result = 0.0_f64;
        self.completion_predictive_sweep(evidence, seed, completed_attempts, |_, cdf| {
            result = cdf;
            true
        });
        result
    }

    #[cfg(test)]
    pub(crate) fn write_completion_predictive_cdfs<'a>(
        &mut self,
        evidence: impl Borrow<OccupancyTraceEvidence<'a>>,
        seed: u64,
        output: &mut [f64],
    ) {
        let evidence = evidence.borrow();
        let count_max = output.len().saturating_sub(1) as u32;
        self.completion_predictive_sweep(evidence, seed, count_max, |count, cdf| {
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
    ///
    /// The update applies evidence to its interval start. It then advances
    /// the change process across the evidence interval.
    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "capacity_update"))]
    pub(crate) fn update<'a>(
        &mut self,
        evidence: impl Borrow<OccupancyTraceEvidence<'a>>,
        elapsed: Duration,
    ) {
        let evidence = evidence.borrow();
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
        let (offsets, durations, ..) = evidence.service_durations();
        if offsets.is_empty() && !evidence.completion_groups().iter().all(|count| *count == 0) {
            for index in 0..self.grid.knee_cell_count as usize {
                fill_knee_state_rates(&self.grid, index, &mut self.state_rates);
                self.update_path_likelihood(index);
            }
            for index in self.grid.knee_cell_count as usize..self.likelihoods.len() {
                fill_no_knee_state_rates(&self.grid, index, &mut self.state_rates);
                self.update_path_likelihood(index);
            }
        } else {
            let statistics = duration_statistics(durations);
            self.update_duration_likelihoods(evidence, statistics);
        }
        self.update_shape_weights();
        let prior_predictive = log_weighted_sum(&self.prior_weights, &self.likelihoods);
        if posterior_update_eligible(prior_predictive) {
            self.update_filters(prior_predictive);
        }
        self.previous_window_concurrency = Some(evidence.mean_concurrency());
        self.transition(exposure);
    }

    /// Scores an admission-gated occupancy path.
    ///
    /// The observed path comes from an admission-controlled system. Every
    /// certified trace is feasible with probability one. The raw Markov path
    /// score is the exact conditional likelihood.
    fn update_path_likelihood(&mut self, index: usize) {
        self.likelihoods[index] = path_log_score_with_rates(
            &self.state_rates,
            &self.state_exposure_seconds,
            &self.state_completion_counts,
        );
        let start = index * SERVICE_CLOCKS.len();
        self.shape_scores[start..start + SERVICE_CLOCKS.len()].fill(self.likelihoods[index]);
        self.shape_cell_weights[start..start + SERVICE_CLOCKS.len()]
            .copy_from_slice(&self.shape_weights);
    }

    fn update_duration_likelihoods(
        &mut self,
        evidence: &OccupancyTraceEvidence<'_>,
        statistics: DurationStatistics,
    ) {
        debug_assert!(
            statistics.completion_count == 0
                || WITHIN_CELL_LAPLACE_ERROR_CONSTANT / f64::from(statistics.completion_count)
                    > 0.0_f64,
            "the priced Laplace error bound must stay positive"
        );
        let concurrency = evidence.mean_concurrency().max(1.0_f64);
        for (index, rate) in self.duration_rates.iter_mut().enumerate() {
            *rate = throughput(
                self.grid.service_times_seconds[index],
                self.grid.capacities_per_second[index],
                self.grid.collapse_values[index],
                self.grid.no_knee[index] > 0.0_f64,
                concurrency,
            ) / concurrency;
        }
        for index in 0..self.duration_rates.len() {
            let point_rate = self.duration_rates[index];
            let low = -self.grid.service_time_highs[index].ln();
            let high = -self.grid.service_time_lows[index].ln();
            let aggregate_score =
                aggregate_completion_log_score(&self.grid, index, &self.state_completion_counts);
            let aggregate_count = self.state_completion_counts.iter().copied().sum::<u32>();
            let start = index * SERVICE_CLOCKS.len();
            for (shape_index, clock) in SERVICE_CLOCKS.iter().copied().enumerate() {
                let mode_index = start + shape_index;
                if clock == ServiceClock::Deterministic {
                    let posterior = DeterministicWithinCellPosterior {
                        low,
                        high,
                        evidence,
                        aggregate_count,
                        prior: self.grid.prior,
                    };
                    let (mode, curvature, score) =
                        deterministic_within_cell_log_evidence(&posterior);
                    self.duration_log_rate_modes[mode_index] = mode;
                    self.duration_log_rate_curvatures[mode_index] = curvature;
                    let (feasible_low, feasible_high) =
                        deterministic_feasible_interval(evidence, low, high);
                    self.duration_log_rate_lows[mode_index] = feasible_low;
                    self.duration_log_rate_highs[mode_index] = feasible_high;
                    self.shape_scores[mode_index] =
                        score + aggregate_score - f64::from(aggregate_count) * point_rate.ln();
                    continue;
                }
                let shape = match clock {
                    ServiceClock::Erlang(shape) => shape,
                    ServiceClock::Deterministic => continue,
                };
                if low.to_bits() == high.to_bits() {
                    self.duration_log_rate_modes[mode_index] = low;
                    self.duration_log_rate_curvatures[mode_index] = f64::INFINITY;
                    self.shape_scores[mode_index] =
                        duration_log_likelihood(shape, point_rate, evidence, statistics)
                            + f64::from(aggregate_count) * low;
                    self.shape_scores[mode_index] +=
                        aggregate_score - f64::from(aggregate_count) * point_rate.ln();
                    continue;
                }
                let posterior = WithinCellPosterior {
                    shape,
                    low,
                    high,
                    evidence,
                    statistics,
                    aggregate_count,
                    prior: self.grid.prior,
                };
                let (mode, curvature) =
                    within_cell_mode(point_rate.ln().clamp(low, high), &posterior);
                self.duration_log_rate_modes[mode_index] = mode;
                self.duration_log_rate_curvatures[mode_index] = curvature;
                self.shape_scores[mode_index] =
                    within_cell_laplace_log_evidence(mode, curvature, &posterior);
                self.shape_scores[mode_index] +=
                    aggregate_score - f64::from(aggregate_count) * point_rate.ln();
            }
            self.likelihoods[index] = log_weighted_sum(
                &self.shape_weights,
                &self.shape_scores[start..start + SERVICE_CLOCKS.len()],
            );
            normalize_log_weights(
                &self.shape_weights,
                &self.shape_scores[start..start + SERVICE_CLOCKS.len()],
                &mut self.shape_cell_weights[start..start + SERVICE_CLOCKS.len()],
            );
        }
    }

    fn update_shape_weights(&mut self) {
        let mut predictive = [f64::NEG_INFINITY; SERVICE_CLOCKS.len()];
        for (shape, score) in predictive.iter_mut().enumerate() {
            let mut maximum = f64::NEG_INFINITY;
            for cell in 0..self.weights.len() {
                maximum = maximum.max(
                    self.weights[cell].ln()
                        + self.shape_scores[cell * SERVICE_CLOCKS.len() + shape],
                );
            }
            let mut sum = 0.0_f64;
            for cell in 0..self.weights.len() {
                sum += (self.weights[cell].ln()
                    + self.shape_scores[cell * SERVICE_CLOCKS.len() + shape]
                    - maximum)
                    .exp();
            }
            *score = maximum + sum.ln();
        }
        let maximum = self
            .shape_weights
            .iter()
            .zip(predictive)
            .filter(|(weight, score)| **weight > 0.0_f64 && score.is_finite())
            .map(|(weight, score)| weight.ln() + score)
            .fold(f64::NEG_INFINITY, f64::max);
        let normalizer = self
            .shape_weights
            .iter()
            .zip(predictive)
            .filter(|(weight, score)| **weight > 0.0_f64 && score.is_finite())
            .map(|(weight, score)| (weight.ln() + score - maximum).exp())
            .sum::<f64>();
        let previous = self.shape_weights;
        for shape in 0..SERVICE_CLOCKS.len() {
            self.shape_weights[shape] =
                if previous[shape] > 0.0_f64 && predictive[shape].is_finite() {
                    (previous[shape].ln() + predictive[shape] - maximum).exp() / normalizer
                } else {
                    0.0_f64
                };
        }
    }

    pub(crate) fn omit_observation(&mut self, elapsed: Duration) {
        self.transition(elapsed);
        self.observation_clock_micros = self
            .observation_clock_micros
            .saturating_add(elapsed.as_micros() as u64);
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

    fn update_residual_check(&mut self, evidence: &OccupancyTraceEvidence<'_>) {
        let (offsets, durations, ..) = evidence.service_durations();
        if !durations.is_empty() {
            for (&offset, &duration) in offsets.iter().zip(durations) {
                let pit = self.duration_predictive_pit(evidence, offset, duration);
                self.record_residual(pit);
            }
            if self.residual_sample_count > 0 {
                let sample_count = f64::from(self.residual_sample_count);
                let alpha = RESIDUAL_REJECTION_PROBABILITY;
                let dkw_bound = (-(alpha * 0.5_f64).ln() / (2.0_f64 * sample_count)).sqrt();
                self.refresh_residual_check(sample_count);
                self.markov_clock_rejected = self.residual_maximum_distance > dkw_bound;
            }
            return;
        }
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
                    self.record_residual(-(-residual).exp_m1());
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

    fn duration_predictive_pit(
        &self,
        evidence: &OccupancyTraceEvidence<'_>,
        offset_micros: u64,
        duration_micros: u64,
    ) -> f64 {
        let entry_micros = duration_micros.saturating_sub(offset_micros);
        let concurrency = evidence.mean_concurrency().max(1.0_f64);
        let mut predictive = 0.0_f64;
        for cell in 0..self.weights.len() {
            let rate = throughput(
                self.grid.service_times_seconds[cell],
                self.grid.capacities_per_second[cell],
                self.grid.collapse_values[cell],
                self.grid.no_knee[cell] > 0.0_f64,
                concurrency,
            ) / concurrency;
            let total = Duration::from_micros(duration_micros).as_secs_f64() * rate;
            let entry = Duration::from_micros(entry_micros).as_secs_f64() * rate;
            for (shape_index, clock) in SERVICE_CLOCKS.iter().copied().enumerate() {
                let (total_cdf, entry_cdf) = match clock {
                    ServiceClock::Erlang(shape) => {
                        let shape = f64::from(shape);
                        (
                            if total <= 0.0_f64 {
                                0.0_f64
                            } else {
                                gamma_lr(shape, shape * total)
                            },
                            if entry <= 0.0_f64 {
                                0.0_f64
                            } else {
                                gamma_lr(shape, shape * entry)
                            },
                        )
                    }
                    ServiceClock::Deterministic => {
                        (f64::from(total >= 1.0_f64), f64::from(entry >= 1.0_f64))
                    }
                };
                let conditional =
                    (total_cdf - entry_cdf) / (1.0_f64 - entry_cdf).max(f64::MIN_POSITIVE);
                predictive += self.weights[cell]
                    * self.shape_weights[shape_index]
                    * conditional.clamp(0.0_f64, 1.0_f64);
            }
        }
        predictive.clamp(0.0_f64, 1.0_f64)
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
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            let lower = f64::from(index) / sample_count;
            let upper = f64::from(index.saturating_add(1)) / sample_count;
            let cdf = residual;
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
            if !predictive.is_finite() || predictive <= 0.0_f64 {
                return;
            }
            self.filter_log_weights[filter] += maximum + predictive.ln();
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
        let total = dispatch!(self.simd_level, simd => sum_shifted_exponentials(
            simd,
            &self.filter_log_weights,
            maximum,
        ));
        if !total.is_finite() || total <= 0.0_f64 {
            return;
        }
        dispatch!(self.simd_level, simd => write_normalized_exponentials(
            simd,
            &self.filter_log_weights,
            &mut self.filter_weights,
            maximum,
            total,
        ));
        let log_total = maximum + total.ln();
        for weight in &mut self.filter_log_weights {
            *weight -= log_total;
        }
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

fn history_coverage(grid: &CapacityGrid, concurrency_max: f64) -> Result<f64, CapacityModelError> {
    let coverage = (0..grid.service_times_seconds.len())
        .map(|index| effective_service_time(grid, index, concurrency_max))
        .fold(0.0_f64, f64::max);
    if !coverage.is_finite() {
        return Err(CapacityModelError::InvalidObservationContract);
    }
    Ok(coverage)
}

fn validate_capacity_allocation(
    grid: &CapacityGrid,
    artifact: &CapacityModelArtifact,
    allocation: CapacityAllocation,
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
        capacity_update_operation_count(allocation).ok_or(CapacityModelError::StorageBound)?;
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

fn capacity_update_operation_count(allocation: CapacityAllocation) -> Option<u64> {
    let states = u64::try_from(allocation.state_count).ok()?;
    let groups = u64::try_from(allocation.group_limit).ok()?;
    let cells = u64::try_from(allocation.cell_count).ok()?;
    // Each cell fills its rate vector and scans it for the raw path score.
    let path_cost = cells.checked_mul(states)?.checked_mul(2)?;
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
    // One merge builds the completion statistics. Each rate group then scores
    // six Erlang clocks. The deterministic clock uses indicators and one
    // closed-form marginal. Boundary ages stay bounded by the live-state limit.
    let duration_summary_cost = attempts.checked_mul(5)?;
    let rate_sort_cost = cells
        .checked_mul(u64::from(cells.max(1).ilog2() + 1))?
        .checked_mul(2)?;
    let duration_density_cost = cells
        .checked_mul((SERVICE_CLOCKS.len() - 1) as u64)?
        .checked_mul(12)?
        .checked_mul(WITHIN_CELL_NEWTON_STEPS + 1)?;
    let duration_boundary_cost = cells
        .checked_mul((SERVICE_CLOCKS.len() - 1) as u64)?
        .checked_mul(states)?
        .checked_mul(20)?
        .checked_mul(WITHIN_CELL_NEWTON_STEPS + 1)?;
    let duration_cost = duration_summary_cost
        .checked_add(rate_sort_cost)?
        .checked_add(duration_density_cost)?
        .checked_add(duration_boundary_cost)?
        .checked_add(cells.checked_mul(attempts.checked_add(states)?)?)?
        .checked_add(cells)?;
    let trace_cost = groups.checked_mul(5)?;
    // The owner-label contract scans each attempt for its key maximum,
    // duplicate state, distinct count, and stored columns.
    let owner_label_cost = attempts.checked_mul(8)?;
    let filter_cost = u64::try_from(allocation.filter_curve_count).ok()?;
    path_cost
        .checked_add(residual_grid_cost)?
        .checked_add(residual_ring_cost)?
        .checked_add(residual_sort_cost)?
        .checked_add(duration_cost)?
        .checked_add(trace_cost)?
        .checked_add(owner_label_cost)?
        .checked_add(filter_cost)
}

fn capacity_storage_bytes(allocation: CapacityAllocation) -> Result<usize, CapacityModelError> {
    let CapacityAllocation {
        filter_curve_count,
        cell_count,
        filter_count,
        state_count,
        transition_count,
        ..
    } = allocation;
    filter_curve_count
        .checked_add(
            cell_count
                .checked_mul(5 + SERVICE_CLOCKS.len())
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
                .checked_mul(size_of::<u64>() + 3 * size_of::<u32>())
                .and_then(|transition_bytes| bytes.checked_add(transition_bytes))
        })
        .and_then(|bytes| bytes.checked_add(size_of::<u32>()))
        .ok_or(CapacityModelError::StorageBound)
}

/// Derives the transition buffer size from the certified attempt bound.
fn attempt_buffer_count(attempt_count_max: u32) -> Result<usize, CapacityModelError> {
    let attempt_count =
        usize::try_from(attempt_count_max).map_err(|_| CapacityModelError::StorageBound)?;
    attempt_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(1))
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
    let distribution = Gamma::new(shape, shape / mean_per_second)
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
    let mut coverage = Vec::with_capacity(5);
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
        service_clock_assumption: ServiceClockAssumption::ErlangAndDeterministicRenewal,
        service_duration_evidence: ServiceDurationEvidence::ObservedAttemptDurationsAndBoundaryAges,
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
        || artifact.budget.path_time_error_seconds() != REPORT_CLOCK_ERROR_SECONDS
        || artifact.service_clock_assumption
            != ServiceClockAssumption::ErlangAndDeterministicRenewal
        || artifact.service_duration_evidence
            != ServiceDurationEvidence::ObservedAttemptDurationsAndBoundaryAges
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
        artifact.hazard_shape / artifact.hazard_mean_per_second,
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
            let index = u32::try_from(index).unwrap_or(u32::MAX);
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
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            (f64::from(index) + 0.5_f64) * width
        })
        .collect::<Vec<_>>();
    let mut weights = (0..count)
        .map(|index| {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
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

fn visit_completion_cdf(
    coefficients: &[f64],
    sample_count_reciprocal: f64,
    count_max: u32,
    visit: &mut impl FnMut(u32, f64) -> bool,
) {
    let mut cumulative = 0.0_f64;
    for count in 0..=count_max {
        cumulative += coefficients[count as usize] * sample_count_reciprocal;
        if !visit(count, cumulative.clamp(0.0_f64, 1.0_f64)) {
            break;
        }
    }
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

struct CompletionGeneration<'a> {
    stages: &'a mut Vec<f64>,
    owners: &'a mut Vec<OwnerGeneratedWindow>,
    owner_snapshot: &'a Vec<OwnerGeneratedWindow>,
    arrival_keys: &'a [usize],
    owner_counts: &'a mut Vec<OwnerCount>,
    owner_key_layout: OwnerKeyLayout,
    saturated_owner_window: bool,
    owner_keys: &'a [u64],
    owner_slots: &'a [u64],
}

fn generate_completion_count(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    slot_count: u32,
    random: &mut RandomStream,
    generation: CompletionGeneration<'_>,
) -> u32 {
    let CompletionGeneration {
        stages,
        owners,
        owner_snapshot,
        arrival_keys,
        owner_counts,
        owner_key_layout,
        saturated_owner_window,
        owner_keys,
        owner_slots,
    } = generation;
    if !evidence.owner_supplied_attempts().is_empty() {
        if saturated_owner_window
            && let Some(completed) = generate_saturated_owner_completion_count(
                evidence,
                walk,
                random,
                owner_keys,
                owner_slots,
            )
        {
            return completed;
        }
        if owner_key_layout != OwnerKeyLayout::General {
            let slots_per_owner = if owner_key_layout == OwnerKeyLayout::Serialized {
                1
            } else {
                evidence.slots_per_owner()
            };
            return generate_counted_owner_completion_count(
                evidence,
                walk,
                random,
                owner_counts,
                slots_per_owner,
            );
        }
        return generate_owner_completion_walk(
            evidence,
            walk,
            random,
            stages,
            owners,
            owner_snapshot,
            arrival_keys,
        )
        .0;
    }
    if walk.clock == ServiceClock::Deterministic {
        return generate_deterministic_completion_count(evidence, walk, slot_count, random, stages);
    }
    let ServiceClock::Erlang(shape) = walk.clock else {
        return 0;
    };
    let mut state = GeneratedWindow {
        now_seconds: 0.0_f64,
        busy: evidence.initial_busy_slots(),
        available: evidence.initial_available_attempts(),
        completed: 0,
    };
    stages.clear();
    stages.push(sample_erlang_work(shape, random));
    fill_available_slots(slot_count, &mut state.busy, &mut state.available);
    for (&offset_micros, &demand) in evidence
        .offsets_micros()
        .iter()
        .zip(evidence.demand_groups())
        .filter(|(_, demand)| **demand > 0)
    {
        let boundary_seconds = Duration::from_micros(offset_micros).as_secs_f64();
        generate_completions_until(walk, boundary_seconds, random, stages, &mut state);
        state.available = state.available.saturating_add(demand);
        let previous_busy = state.busy;
        fill_available_slots(slot_count, &mut state.busy, &mut state.available);
        debug_assert!(
            state.busy >= previous_busy,
            "new demand cannot remove a busy slot"
        );
    }
    generate_completions_until(
        walk,
        evidence.window().exposure_seconds(),
        random,
        stages,
        &mut state,
    );
    state.completed
}

/// Samples an exact completion count for one covered saturated owner draw.
///
/// For Erlang shape `k`, stage events form a Poisson process with rate
/// `k * T(s)`. Constant busy state makes `T(s)` constant. Therefore, if `S`
/// is the stage count, `(S + r) / k` is the completion count. The remainder
/// `(S + r) % k` is the next stage residual `r`. This window starts with
/// residual zero.
///
/// Each busy slot stays with its initial key while that key has queued work.
/// The walk attributes each completion uniformly across the busy slots. Thus,
/// conditional on total `N`, slot counts are `Multinomial(N, 1 / slot_count)`.
/// The conditional-binomial draw samples that identity in slot order. Coverage
/// proves that each initial key has enough initial work for its sampled count.
/// An uncovered draw returns to the general event walk.
fn generate_saturated_owner_completion_count(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    random: &mut RandomStream,
    owner_keys: &[u64],
    owner_slots: &[u64],
) -> Option<u32> {
    let ServiceClock::Erlang(shape) = walk.clock else {
        return None;
    };
    let point_rate = walk.grid.service_times_seconds[walk.cell].recip();
    let throughput = state_rate(walk.grid, walk.cell, evidence.initial_busy_slots() as usize)
        * walk.slot_rate
        / point_rate;
    let mean = PoissonMean::from_product(
        f64::from(shape) * throughput,
        evidence.window().exposure_seconds(),
    );
    let completed = saturated_erlang_completion_count(shape, mean, 0, random).0;
    saturated_owner_draw_is_covered(completed, owner_keys, owner_slots, random).then_some(completed)
}

fn saturated_owner_draw_is_covered(
    completed: u32,
    owner_keys: &[u64],
    owner_slots: &[u64],
    random: &mut RandomStream,
) -> bool {
    let mut remaining = u64::from(completed);
    let mut remaining_slots = owner_slots.len() as u64;
    for &key in owner_slots {
        let attributed = if remaining_slots == 1 {
            remaining
        } else {
            let probability =
                1.0_f64 / f64::from(u32::try_from(remaining_slots).unwrap_or(u32::MAX));
            let Ok(binomial) = Binomial::new(remaining, probability) else {
                return false;
            };
            binomial.sample(&mut *random)
        };
        let start = owner_keys.partition_point(|candidate| *candidate < key);
        let end = owner_keys.partition_point(|candidate| *candidate <= key);
        if attributed > end.saturating_sub(start) as u64 {
            return false;
        }
        remaining = remaining.saturating_sub(attributed);
        remaining_slots -= 1;
    }
    remaining == 0
}

/// Converts homogeneous Erlang stage events into completions and a residual.
fn saturated_erlang_completion_count(
    shape: u32,
    mean: PoissonMean,
    carried_stage_residual: u32,
    random: &mut RandomStream,
) -> (u32, u32) {
    let stages = sample_poisson(mean, random).saturating_add(u64::from(carried_stage_residual));
    let shape = u64::from(shape);
    (
        u32::try_from(stages / shape).unwrap_or(u32::MAX),
        u32::try_from(stages % shape).unwrap_or(u32::MAX),
    )
}

/// Prepares the fixed initial slots for per-draw saturation checks.
///
/// The owner set stays fixed. Every fleet slot must be in use at the start.
/// The per-draw check excludes arrivals because their offsets can follow a
/// sampled completion. This exclusion can only send a draw to the general walk.
fn saturated_owner_window(
    evidence: &OccupancyTraceEvidence<'_>,
    owner_key_layout: OwnerKeyLayout,
    keys: &mut Vec<u64>,
    slots: &mut Vec<u64>,
) -> bool {
    if owner_key_layout != OwnerKeyLayout::General
        || evidence.initial_busy_slots() != evidence.slot_count()
        || evidence.initial_busy_slots() == 0
    {
        return false;
    }
    let (active_counts, initial_keys) = evidence.owner_initial_work();
    keys.clear();
    slots.clear();
    let mut cursor = 0_usize;
    for (owner, supplied) in evidence
        .owner_supplied_attempts()
        .iter()
        .copied()
        .enumerate()
    {
        let end = cursor.saturating_add(supplied as usize);
        let Some(owner_keys) = initial_keys.get(cursor..end) else {
            return false;
        };
        cursor = end;
        let active = active_counts.get(owner).copied().unwrap_or(0) as usize;
        let Some(active_keys) = owner_keys.get(..active) else {
            return false;
        };
        let owner_prefix = (owner as u64) << 32_u32;
        keys.extend(owner_keys.iter().map(|key| owner_prefix | u64::from(*key)));
        slots.extend(active_keys.iter().map(|key| owner_prefix | u64::from(*key)));
    }
    if cursor != initial_keys.len() {
        return false;
    }
    keys.sort_unstable();
    slots.len() == evidence.initial_busy_slots() as usize
}

fn generate_owner_completion_walk(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    random: &mut RandomStream,
    stages: &mut Vec<f64>,
    owners: &mut Vec<OwnerGeneratedWindow>,
    owner_snapshot: &Vec<OwnerGeneratedWindow>,
    arrival_keys: &[usize],
) -> (u32, f64) {
    // The owner sampler projects one fleet walk onto owner queues. The fleet
    // rate stays constant between arrivals and sampled stage transitions.
    if walk.clock == ServiceClock::Deterministic {
        return (
            generate_deterministic_owner_completion_count(
                evidence,
                walk,
                random,
                stages,
                owners,
                owner_snapshot,
                arrival_keys,
            ),
            0.0_f64,
        );
    }
    let ServiceClock::Erlang(shape) = walk.clock else {
        return (0, 0.0_f64);
    };
    let (arrival_offsets, arrival_owners, _) = evidence.owner_arrivals();
    owners.clone_from(owner_snapshot);
    let mut now = 0.0_f64;
    let mut busy_slot_seconds = 0.0_f64;
    let mut fleet_stage = sample_erlang_work(shape, random);
    let mut arrivals = arrival_offsets
        .iter()
        .copied()
        .zip(arrival_owners.iter().copied())
        .zip(arrival_keys.iter().copied())
        .peekable();
    while let Some(offset) = arrivals.peek().map(|((offset, _), _)| *offset) {
        let boundary = Duration::from_micros(offset).as_secs_f64();
        generate_owner_completions_until(
            walk,
            evidence.slots_per_owner(),
            boundary,
            random,
            &mut OwnerCompletionState {
                now: &mut now,
                busy_slot_seconds: &mut busy_slot_seconds,
                fleet_stage: &mut fleet_stage,
                owners,
            },
        );
        while arrivals.peek().is_some_and(|((at, _), _)| *at == offset) {
            if let Some(((_, owner), key)) = arrivals.next()
                && let Some(state) = owners.get_mut(owner as usize)
            {
                open_owner_key(key, shape, evidence.slots_per_owner(), state);
            }
        }
    }
    generate_owner_completions_until(
        walk,
        evidence.slots_per_owner(),
        evidence.window().exposure_seconds(),
        random,
        &mut OwnerCompletionState {
            now: &mut now,
            busy_slot_seconds: &mut busy_slot_seconds,
            fleet_stage: &mut fleet_stage,
            owners,
        },
    );
    stages.clear();
    let completed = owners
        .iter()
        .fold(0_u32, |total, owner| total.saturating_add(owner.completed));
    (completed, busy_slot_seconds)
}

#[derive(Clone, Copy, Default)]
struct OwnerCount {
    active: u32,
    queued: u32,
}

fn owner_key_layout(evidence: &OccupancyTraceEvidence<'_>, keys: &mut Vec<u64>) -> OwnerKeyLayout {
    if evidence.owner_supplied_attempts().is_empty() {
        return OwnerKeyLayout::General;
    }
    keys.clear();
    let mut key_cursor = 0_usize;
    for (owner, supplied) in evidence
        .owner_supplied_attempts()
        .iter()
        .copied()
        .enumerate()
    {
        let key_end = key_cursor.saturating_add(supplied as usize);
        let owner_keys = evidence
            .owner_initial_work()
            .1
            .get(key_cursor..key_end)
            .unwrap_or(&[]);
        keys.extend(
            owner_keys
                .iter()
                .map(|key| (owner as u64) << 32_u32 | u64::from(*key)),
        );
        key_cursor = key_end;
    }
    keys.extend(
        evidence
            .owner_arrivals()
            .1
            .iter()
            .zip(evidence.owner_arrivals().2)
            .map(|(owner, key)| u64::from(*owner) << 32_u32 | u64::from(*key)),
    );
    keys.sort_unstable();
    if !keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return OwnerKeyLayout::Unique;
    }
    let serialized = keys
        .windows(2)
        .all(|pair| pair[0] == pair[1] || pair[0] >> 32_u32 != pair[1] >> 32_u32);
    if serialized {
        OwnerKeyLayout::Serialized
    } else {
        OwnerKeyLayout::General
    }
}

fn generate_counted_owner_completion_count(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    random: &mut RandomStream,
    owners: &mut Vec<OwnerCount>,
    slots_per_owner: u32,
) -> u32 {
    let supplied = evidence.owner_supplied_attempts();
    let active = evidence.owner_initial_work().0;
    owners.resize(supplied.len(), OwnerCount::default());
    for (owner, state) in owners.iter_mut().enumerate() {
        state.active = active.get(owner).copied().unwrap_or(0).min(slots_per_owner);
        state.queued = supplied[owner].saturating_sub(state.active);
    }
    let (arrival_offsets, arrival_owners, _) = evidence.owner_arrivals();
    let mut now = 0.0_f64;
    let mut work = match walk.clock {
        ServiceClock::Erlang(shape) => sample_erlang_work(shape, random),
        ServiceClock::Deterministic => 1.0_f64,
    };
    let mut completed = 0_u32;
    let mut arrivals = arrival_offsets
        .iter()
        .copied()
        .zip(arrival_owners.iter().copied())
        .peekable();
    while let Some(offset) = arrivals.peek().map(|(offset, _)| *offset) {
        generate_unique_owner_completions_until(
            walk,
            slots_per_owner,
            Duration::from_micros(offset).as_secs_f64(),
            random,
            &mut UniqueOwnerCompletionState {
                now: &mut now,
                work: &mut work,
                owners,
                completed: &mut completed,
            },
        );
        while arrivals.peek().is_some_and(|(at, _)| *at == offset) {
            if let Some((_, owner)) = arrivals.next()
                && let Some(state) = owners.get_mut(owner as usize)
            {
                if state.active < slots_per_owner {
                    state.active += 1;
                } else {
                    state.queued += 1;
                }
            }
        }
    }
    generate_unique_owner_completions_until(
        walk,
        slots_per_owner,
        evidence.window().exposure_seconds(),
        random,
        &mut UniqueOwnerCompletionState {
            now: &mut now,
            work: &mut work,
            owners,
            completed: &mut completed,
        },
    );
    completed
}

struct UniqueOwnerCompletionState<'a> {
    now: &'a mut f64,
    work: &'a mut f64,
    owners: &'a mut [OwnerCount],
    completed: &'a mut u32,
}

fn generate_unique_owner_completions_until(
    walk: &CompletionWalk<'_>,
    slots_per_owner: u32,
    boundary: f64,
    random: &mut RandomStream,
    state: &mut UniqueOwnerCompletionState<'_>,
) {
    loop {
        let concurrency = state.owners.iter().map(|owner| owner.active).sum::<u32>();
        if concurrency == 0 {
            break;
        }
        let fleet_rate = deterministic_fleet_rate(walk, concurrency);
        let rate = match walk.clock {
            ServiceClock::Erlang(shape) => f64::from(shape) * fleet_rate,
            ServiceClock::Deterministic => fleet_rate,
        };
        let event = *state.now + *state.work / rate;
        if event >= boundary {
            *state.work -= (boundary - *state.now) * rate;
            break;
        }
        *state.now = event;
        *state.work = match walk.clock {
            ServiceClock::Erlang(shape) => sample_erlang_work(shape, random),
            ServiceClock::Deterministic => 1.0_f64,
        };
        let mut slot = ((random.open_unit_f64() * f64::from(concurrency)) as u32)
            .min(concurrency.saturating_sub(1));
        for owner in state.owners.iter_mut() {
            if slot < owner.active {
                *state.completed = state.completed.saturating_add(1);
                if owner.queued > 0 {
                    owner.queued -= 1;
                } else {
                    owner.active -= 1;
                }
                break;
            }
            slot -= owner.active;
        }
        for owner in state.owners.iter_mut() {
            let started = slots_per_owner
                .saturating_sub(owner.active)
                .min(owner.queued);
            owner.active += started;
            owner.queued -= started;
        }
    }
    *state.now = boundary;
}

#[derive(Clone)]
struct OwnerGeneratedWindow {
    completed: u32,
    keys: Vec<OwnerKeyDepth>,
    active_keys: Vec<usize>,
    waiting_head: Option<usize>,
    waiting_tail: Option<usize>,
}

#[derive(Clone)]
struct OwnerKeyDepth {
    active: bool,
    queued_depth: u32,
    next_waiting: Option<usize>,
}

/// Builds the report-start key depths once for all predictive draws.
///
/// The count contract ignores live-attempt ages. Attempt labels within one key
/// are interchangeable for counts. A completion keeps its key while that key
/// has queued depth. Otherwise, it closes that key and opens the owner's next
/// waiting key. Thus active state and queued depth preserve every state change
/// that can affect a completion count. Per-attempt queue items add no state.
fn build_owner_depth_snapshot(
    evidence: &OccupancyTraceEvidence<'_>,
    owners: &mut Vec<OwnerGeneratedWindow>,
    arrival_key_indices: &mut Vec<usize>,
    key_indices: &mut HashMap<u64, usize>,
) {
    let supplied_attempts = evidence.owner_supplied_attempts();
    let (initial_active_counts, initial_keys) = evidence.owner_initial_work();
    owners.resize_with(supplied_attempts.len(), || OwnerGeneratedWindow {
        completed: 0,
        keys: Vec::new(),
        active_keys: Vec::new(),
        waiting_head: None,
        waiting_tail: None,
    });
    owners.truncate(supplied_attempts.len());
    for state in owners.iter_mut() {
        state.completed = 0;
        state.keys.clear();
        state.active_keys.clear();
        state.waiting_head = None;
        state.waiting_tail = None;
    }
    key_indices.clear();
    let mut key_cursor = 0_usize;
    for (owner, (&supplied, state)) in supplied_attempts.iter().zip(owners.iter_mut()).enumerate() {
        let key_end = key_cursor.saturating_add(supplied as usize);
        let owner_keys = initial_keys.get(key_cursor..key_end).unwrap_or(&[]);
        key_cursor = key_end;
        let active_count = initial_active_counts.get(owner).copied().unwrap_or(0) as usize;
        for (attempt, &key) in owner_keys.iter().enumerate() {
            let identity = (owner as u64) << 32_u32 | u64::from(key);
            if let Some(&key_index) = key_indices.get(&identity) {
                state.keys[key_index].queued_depth =
                    state.keys[key_index].queued_depth.saturating_add(1);
            } else {
                let key_index = state.keys.len();
                state.keys.push(OwnerKeyDepth {
                    active: attempt < active_count,
                    queued_depth: u32::from(attempt >= active_count),
                    next_waiting: None,
                });
                if attempt < active_count {
                    state.active_keys.push(key_index);
                } else {
                    enqueue_owner_key(key_index, state);
                }
                key_indices.insert(identity, key_index);
            }
        }
    }
    arrival_key_indices.clear();
    let (_, arrival_owners, arrival_keys) = evidence.owner_arrivals();
    for (&owner, &key) in arrival_owners.iter().zip(arrival_keys) {
        let identity = u64::from(owner) << 32_u32 | u64::from(key);
        let state = &mut owners[owner as usize];
        let key_index = *key_indices.entry(identity).or_insert_with(|| {
            let key_index = state.keys.len();
            state.keys.push(OwnerKeyDepth {
                active: false,
                queued_depth: 0,
                next_waiting: None,
            });
            key_index
        });
        arrival_key_indices.push(key_index);
    }
}

fn enqueue_owner_key(key: usize, state: &mut OwnerGeneratedWindow) {
    if let Some(tail) = state.waiting_tail {
        state.keys[tail].next_waiting = Some(key);
    } else {
        state.waiting_head = Some(key);
    }
    state.waiting_tail = Some(key);
}

fn open_owner_key(key: usize, _shape: u32, slot_limit: u32, state: &mut OwnerGeneratedWindow) {
    if state.keys[key].active || state.keys[key].queued_depth > 0 {
        state.keys[key].queued_depth = state.keys[key].queued_depth.saturating_add(1);
    } else if state.active_keys.len() < slot_limit as usize {
        state.keys[key].active = true;
        state.active_keys.push(key);
    } else {
        state.keys[key].queued_depth = 1;
        enqueue_owner_key(key, state);
    }
}

struct CompletionWalk<'a> {
    grid: &'a CapacityGrid,
    cell: usize,
    clock: ServiceClock,
    slot_rate: f64,
}

struct OwnerCompletionState<'a> {
    now: &'a mut f64,
    busy_slot_seconds: &'a mut f64,
    fleet_stage: &'a mut f64,
    owners: &'a mut [OwnerGeneratedWindow],
}

fn generate_owner_completions_until(
    walk: &CompletionWalk<'_>,
    slots_per_owner: u32,
    boundary: f64,
    random: &mut RandomStream,
    state: &mut OwnerCompletionState<'_>,
) {
    // The fleet walk samples one exact Erlang completion interval. See
    // `sample_erlang_work` for the boundary residual invariant.
    loop {
        let concurrency = state.owners.iter().fold(0_usize, |total, owner| {
            total.saturating_add(owner.active_keys.len())
        });
        if concurrency == 0 {
            break;
        }
        let ServiceClock::Erlang(shape) = walk.clock else {
            return;
        };
        let point_rate = 1.0_f64 / walk.grid.service_times_seconds[walk.cell];
        let aggregate_rate =
            state_rate(walk.grid, walk.cell, concurrency) * walk.slot_rate / point_rate;
        let rate = f64::from(shape) * aggregate_rate;
        let completion = *state.now + *state.fleet_stage / rate;
        if completion >= boundary {
            *state.fleet_stage -= (boundary - *state.now) * rate;
            let concurrency = u32::try_from(concurrency).unwrap_or(u32::MAX);
            *state.busy_slot_seconds += (boundary - *state.now) * f64::from(concurrency);
            break;
        }
        let concurrency = f64::from(u32::try_from(concurrency).unwrap_or(u32::MAX));
        *state.busy_slot_seconds += (completion - *state.now) * concurrency;
        *state.now = completion;
        *state.fleet_stage = sample_erlang_work(shape, random);
        let fleet_slot = (random.open_unit_f64() * concurrency) as usize;
        let Some((owner, slot)) = owner_slot(state.owners, fleet_slot) else {
            break;
        };
        let owner_state = &mut state.owners[owner];
        owner_state.completed = owner_state.completed.saturating_add(1);
        let key = owner_state.active_keys[slot];
        if owner_state.keys[key].queued_depth > 0 {
            owner_state.keys[key].queued_depth -= 1;
        } else {
            owner_state.keys[key].active = false;
            owner_state.active_keys.swap_remove(slot);
            dispatch_owner_keys(shape, slots_per_owner, owner_state);
        }
    }
    *state.now = boundary;
}

fn owner_slot(owners: &[OwnerGeneratedWindow], mut fleet_slot: usize) -> Option<(usize, usize)> {
    for (owner, state) in owners.iter().enumerate() {
        if fleet_slot < state.active_keys.len() {
            return Some((owner, fleet_slot));
        }
        fleet_slot = fleet_slot.saturating_sub(state.active_keys.len());
    }
    None
}

fn dispatch_owner_keys(shape: u32, slot_limit: u32, state: &mut OwnerGeneratedWindow) {
    while state.active_keys.len() < slot_limit as usize {
        let Some(key) = state.waiting_head else {
            break;
        };
        state.waiting_head = state.keys[key].next_waiting;
        if state.waiting_head.is_none() {
            state.waiting_tail = None;
        }
        state.keys[key].next_waiting = None;
        state.keys[key].active = true;
        state.keys[key].queued_depth -= 1;
        state.active_keys.push(key);
    }
    let _ = shape;
}

#[cfg(test)]
fn owner_open_key_count(state: &OwnerGeneratedWindow) -> usize {
    state
        .keys
        .iter()
        .filter(|key| key.active || key.queued_depth > 0)
        .count()
}

fn generate_completions_until(
    walk: &CompletionWalk<'_>,
    boundary_seconds: f64,
    random: &mut RandomStream,
    stages: &mut [f64],
    state: &mut GeneratedWindow,
) {
    while state.busy > 0 {
        let point_rate = 1.0_f64 / walk.grid.service_times_seconds[walk.cell];
        let aggregate_rate =
            state_rate(walk.grid, walk.cell, state.busy as usize) * walk.slot_rate / point_rate;
        let ServiceClock::Erlang(shape) = walk.clock else {
            return;
        };
        let rate = f64::from(shape) * aggregate_rate;
        let completion = state.now_seconds + stages[0] / rate;
        if completion >= boundary_seconds {
            stages[0] -= (boundary_seconds - state.now_seconds) * rate;
            break;
        }
        state.now_seconds = completion;
        stages[0] = sample_erlang_work(shape, random);
        if state.available > 0 {
            state.available -= 1;
        } else {
            state.busy -= 1;
        }
        state.completed = state.completed.saturating_add(1);
    }
    state.now_seconds = boundary_seconds;
}

/// Samples the operational work for one fleet completion.
///
/// For shape `k`, `sum(Exp(k * T(s)))` equals `Gamma(k, k * T(s))`.
/// Equivalently, one `Gamma(k, 1)` work draw completes after `work / rate`.
/// A boundary after `dt` consumes `rate * dt` work. The current exponential
/// stage is memoryless. Its residual plus the remaining whole stages has the
/// same conditional law. Thus subtraction preserves the exact residual stage
/// count and work across each rate change.
fn sample_erlang_work(remaining: u32, random: &mut RandomStream) -> f64 {
    match remaining {
        1 => -random.open_unit_f64().ln(),
        2 => -(random.open_unit_f64() * random.open_unit_f64()).ln(),
        _ => {
            let _decorrelation = random.open_unit_f64();
            sample_gamma(f64::from(remaining), random)
        }
    }
}

fn deterministic_fleet_rate(walk: &CompletionWalk<'_>, concurrency: u32) -> f64 {
    let point_rate = walk.grid.service_times_seconds[walk.cell].recip();
    state_rate(walk.grid, walk.cell, concurrency as usize) * walk.slot_rate / point_rate
}

fn generate_deterministic_completion_count(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    slot_count: u32,
    random: &mut RandomStream,
    operational_work: &mut Vec<f64>,
) -> u32 {
    let mut state = GeneratedWindow {
        now_seconds: 0.0_f64,
        busy: evidence.initial_busy_slots(),
        available: evidence.initial_available_attempts(),
        completed: 0,
    };
    operational_work.clear();
    operational_work.push(1.0_f64);
    fill_available_slots(slot_count, &mut state.busy, &mut state.available);
    for (&offset, &demand) in evidence
        .offsets_micros()
        .iter()
        .zip(evidence.demand_groups())
        .filter(|(_, demand)| **demand > 0)
    {
        let boundary = Duration::from_micros(offset).as_secs_f64();
        generate_deterministic_completions_until(
            walk,
            boundary,
            random,
            operational_work,
            &mut state,
        );
        state.available = state.available.saturating_add(demand);
        let previous_busy = state.busy;
        fill_available_slots(slot_count, &mut state.busy, &mut state.available);
        debug_assert!(
            state.busy >= previous_busy,
            "new demand cannot remove a busy slot"
        );
    }
    generate_deterministic_completions_until(
        walk,
        evidence.window().exposure_seconds(),
        random,
        operational_work,
        &mut state,
    );
    state.completed
}

fn generate_deterministic_completions_until(
    walk: &CompletionWalk<'_>,
    boundary: f64,
    random: &mut RandomStream,
    operational_work: &mut [f64],
    state: &mut GeneratedWindow,
) {
    while state.busy > 0 {
        let rate = deterministic_fleet_rate(walk, state.busy);
        let completion = state.now_seconds + operational_work[0] / rate;
        if completion >= boundary {
            operational_work[0] -= (boundary - state.now_seconds) * rate;
            break;
        }
        state.now_seconds = completion;
        operational_work[0] = 1.0_f64;
        state.completed = state.completed.saturating_add(1);
        let _slot = ((random.open_unit_f64() * f64::from(state.busy)) as usize)
            .min(state.busy.saturating_sub(1) as usize);
        if state.available > 0 {
            state.available -= 1;
        } else {
            state.busy -= 1;
        }
    }
    state.now_seconds = boundary;
}

fn generate_deterministic_owner_completion_count(
    evidence: &OccupancyTraceEvidence<'_>,
    walk: &CompletionWalk<'_>,
    random: &mut RandomStream,
    deadlines: &mut Vec<f64>,
    owners: &mut Vec<OwnerGeneratedWindow>,
    owner_snapshot: &Vec<OwnerGeneratedWindow>,
    arrival_keys: &[usize],
) -> u32 {
    let (arrival_offsets, arrival_owners, _) = evidence.owner_arrivals();
    owners.clone_from(owner_snapshot);
    let mut now = 0.0_f64;
    let mut fleet_work = 1.0_f64;
    let mut arrivals = arrival_offsets
        .iter()
        .copied()
        .zip(arrival_owners.iter().copied())
        .zip(arrival_keys.iter().copied())
        .peekable();
    while let Some(offset) = arrivals.peek().map(|((offset, _), _)| *offset) {
        let boundary = Duration::from_micros(offset).as_secs_f64();
        generate_deterministic_owner_completions_until(
            walk,
            evidence.slots_per_owner(),
            boundary,
            random,
            &mut now,
            &mut fleet_work,
            owners,
        );
        while arrivals.peek().is_some_and(|((at, _), _)| *at == offset) {
            if let Some(((_, owner), key)) = arrivals.next()
                && let Some(state) = owners.get_mut(owner as usize)
            {
                open_owner_key(key, 1, evidence.slots_per_owner(), state);
            }
        }
    }
    generate_deterministic_owner_completions_until(
        walk,
        evidence.slots_per_owner(),
        evidence.window().exposure_seconds(),
        random,
        &mut now,
        &mut fleet_work,
        owners,
    );
    deadlines.clear();
    owners
        .iter()
        .fold(0_u32, |total, owner| total.saturating_add(owner.completed))
}

fn generate_deterministic_owner_completions_until(
    walk: &CompletionWalk<'_>,
    slot_limit: u32,
    boundary: f64,
    random: &mut RandomStream,
    now: &mut f64,
    fleet_work: &mut f64,
    owners: &mut [OwnerGeneratedWindow],
) {
    // Shared operational work gives the exact deterministic fleet law.
    loop {
        let concurrency = fleet_concurrency(owners);
        if concurrency == 0 {
            break;
        }
        let rate = deterministic_fleet_rate(walk, concurrency as u32);
        let completion = *now + *fleet_work / rate;
        if completion >= boundary {
            *fleet_work -= (boundary - *now) * rate;
            break;
        }
        *fleet_work = 1.0_f64;
        *now = completion;
        let concurrency_float = f64::from(u32::try_from(concurrency).unwrap_or(u32::MAX));
        let fleet_slot = ((random.open_unit_f64() * concurrency_float) as usize)
            .min(concurrency.saturating_sub(1));
        let Some((owner, slot)) = owner_slot(owners, fleet_slot) else {
            break;
        };
        let state = &mut owners[owner];
        state.completed = state.completed.saturating_add(1);
        let key = state.active_keys[slot];
        if state.keys[key].queued_depth > 0 {
            state.keys[key].queued_depth -= 1;
        } else {
            state.keys[key].active = false;
            state.active_keys.swap_remove(slot);
            dispatch_owner_keys(1, slot_limit, state);
        }
    }
    *now = boundary;
}

fn fleet_concurrency(owners: &[OwnerGeneratedWindow]) -> usize {
    owners.iter().fold(0_usize, |total, owner| {
        total.saturating_add(owner.active_keys.len())
    })
}

fn fill_available_slots(slot_count: u32, busy: &mut u32, available: &mut u32) {
    let started = slot_count.saturating_sub(*busy).min(*available);
    *busy = busy.saturating_add(started);
    *available -= started;
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

/// Folds a boundary trace into per-state exposure and completion counts.
///
/// One trace group is a simultaneous batch: the state path is defined at
/// group boundaries only, and a group's completions are attributed to the
/// state that accrued the exposure they came from.
fn fold_trace(
    evidence: &OccupancyTraceEvidence<'_>,
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

fn aggregate_completion_log_score(
    grid: &CapacityGrid,
    index: usize,
    completion_counts: &[u32],
) -> f64 {
    let mut score = 0.0_f64;
    for (state, count) in completion_counts.iter().copied().enumerate().skip(1) {
        if count > 0 {
            score += f64::from(count) * state_rate(grid, index, state).ln();
        }
    }
    score
}

fn duration_statistics(durations_micros: &[u64]) -> DurationStatistics {
    let mut duration_sum_seconds = 0.0_f64;
    let mut log_duration_sum_seconds = 0.0_f64;
    for &duration in durations_micros {
        let duration_seconds = Duration::from_micros(duration).as_secs_f64();
        duration_sum_seconds += duration_seconds;
        log_duration_sum_seconds += duration_seconds.ln();
    }
    DurationStatistics {
        completion_count: u32::try_from(durations_micros.len()).unwrap_or(u32::MAX),
        duration_sum_seconds,
        log_duration_sum_seconds,
    }
}

fn repeated_service_logs(service_times: &[f64], map: impl Fn(f64) -> f64 + Copy) -> Vec<f64> {
    service_times
        .iter()
        .flat_map(|service| repeat_n(map(*service), SERVICE_CLOCKS.len()))
        .collect()
}

fn duration_log_rate_bounds(grid: &CapacityGrid) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    (
        repeated_service_logs(&grid.service_times_seconds, |service| service.recip().ln()),
        repeated_service_logs(&grid.service_time_highs, |service| -service.ln()),
        repeated_service_logs(&grid.service_time_lows, |service| -service.ln()),
    )
}

fn duration_log_likelihood(
    shape: u32,
    per_slot_rate: f64,
    evidence: &OccupancyTraceEvidence<'_>,
    statistics: DurationStatistics,
) -> f64 {
    let (offsets, durations, _, final_ages) = evidence.service_durations();
    let shape = f64::from(shape);
    let log_normalizer = shape * shape.ln() - ln_gamma(shape);
    let completion_count = f64::from(statistics.completion_count);
    if statistics.completion_count > 0
        && (per_slot_rate <= 0.0_f64 || !statistics.log_duration_sum_seconds.is_finite())
    {
        return f64::NEG_INFINITY;
    }
    let mut score = completion_count * log_normalizer
        + completion_count * REPORT_CLOCK_ERROR_SECONDS.ln()
        + (shape - 1.0_f64) * completion_count * per_slot_rate.ln()
        + (shape - 1.0_f64) * statistics.log_duration_sum_seconds
        - shape * per_slot_rate * statistics.duration_sum_seconds;
    for (&offset, &duration) in offsets.iter().zip(durations) {
        let entry_micros = duration.saturating_sub(offset);
        if entry_micros > 0 {
            let entry = Duration::from_micros(entry_micros).as_secs_f64() * per_slot_rate;
            score -= erlang_log_survival(shape, entry);
        }
    }
    let exposure_micros = evidence.window().exposure_micros();
    for elapsed_micros in final_ages.iter().copied() {
        let elapsed = Duration::from_micros(elapsed_micros).as_secs_f64() * per_slot_rate;
        score += erlang_log_survival(shape, elapsed);
        let entry_micros = elapsed_micros.saturating_sub(exposure_micros);
        if entry_micros > 0 {
            let entry = Duration::from_micros(entry_micros).as_secs_f64() * per_slot_rate;
            score -= erlang_log_survival(shape, entry);
        }
    }
    score
}

fn deterministic_feasible_interval(
    evidence: &OccupancyTraceEvidence<'_>,
    mut low: f64,
    mut high: f64,
) -> (f64, f64) {
    let (_, durations, _, final_ages) = evidence.service_durations();
    for duration in durations.iter().copied() {
        let duration_low = Duration::from_micros(duration).as_secs_f64();
        let duration_high = Duration::from_micros(duration.saturating_add(1)).as_secs_f64();
        low = low.max(-duration_high.ln());
        high = high.min(-duration_low.ln());
    }
    for age in final_ages.iter().copied().filter(|age| *age > 0) {
        high = high.min(-Duration::from_micros(age).as_secs_f64().ln());
    }
    (low, high)
}

fn deterministic_within_cell_log_evidence(
    posterior: &DeterministicWithinCellPosterior<'_, '_>,
) -> (f64, f64, f64) {
    if posterior.low.to_bits() == posterior.high.to_bits() {
        let period = (-posterior.low).exp();
        let (_, durations, _, final_ages) = posterior.evidence.service_durations();
        let feasible = durations.iter().copied().all(|duration| {
            Duration::from_micros(duration).as_secs_f64() <= period
                && period < Duration::from_micros(duration.saturating_add(1)).as_secs_f64()
        }) && final_ages
            .iter()
            .copied()
            .all(|age| Duration::from_micros(age).as_secs_f64() < period);
        let score = if feasible {
            f64::from(posterior.aggregate_count) * posterior.low
        } else {
            f64::NEG_INFINITY
        };
        return (posterior.low, f64::INFINITY, score);
    }
    let (low, high) =
        deterministic_feasible_interval(posterior.evidence, posterior.low, posterior.high);
    if low >= high {
        return (low, f64::INFINITY, f64::NEG_INFINITY);
    }
    let count = f64::from(posterior.aggregate_count);
    match posterior.prior {
        CapacityPrior::LogUniform => {
            let log_integral = if count == 0.0_f64 {
                (high - low).ln()
            } else {
                let upper = count * high;
                upper + (-(count * (high - low))).exp_m1().abs().ln() - count.ln()
            };
            (
                high,
                -count,
                log_integral - (posterior.high - posterior.low).ln(),
            )
        }
        CapacityPrior::LogNormal {
            service_time_median_seconds,
            log_standard_deviation,
            ..
        } => {
            let mean = -service_time_median_seconds.ln();
            let variance = log_standard_deviation.powi(2);
            let shifted_mean = mean + count * variance;
            let Ok(shifted) = Normal::new(shifted_mean, log_standard_deviation) else {
                return (shifted_mean, variance.recip(), f64::NEG_INFINITY);
            };
            let Ok(prior) = Normal::new(mean, log_standard_deviation) else {
                return (shifted_mean, variance.recip(), f64::NEG_INFINITY);
            };
            let feasible_mass = shifted.cdf(high) - shifted.cdf(low);
            let cell_mass = prior.cdf(posterior.high) - prior.cdf(posterior.low);
            let score = count.mul_add(mean, 0.5_f64 * count * count * variance)
                + feasible_mass.max(f64::MIN_POSITIVE).ln()
                - cell_mass.max(f64::MIN_POSITIVE).ln();
            (shifted_mean.clamp(low, high), variance.recip(), score)
        }
    }
}

fn within_cell_mode(initial: f64, posterior: &WithinCellPosterior<'_, '_>) -> (f64, f64) {
    let mut mode = initial;
    for _ in 0..WITHIN_CELL_NEWTON_STEPS {
        let (_, gradient, second) = within_cell_log_posterior(mode, posterior);
        if !second.is_finite() || second >= 0.0_f64 {
            break;
        }
        mode = (mode - gradient / second).clamp(posterior.low, posterior.high);
    }
    let (_, _, second) = within_cell_log_posterior(mode, posterior);
    (mode, (-second).max(f64::MIN_POSITIVE))
}

fn within_cell_laplace_log_evidence(
    mode: f64,
    curvature: f64,
    posterior: &WithinCellPosterior<'_, '_>,
) -> f64 {
    let (value, ..) = within_cell_log_posterior(mode, posterior);
    let Ok(normal) = Normal::new(mode, curvature.sqrt().recip()) else {
        return f64::NEG_INFINITY;
    };
    let mass = (normal.cdf(posterior.high) - normal.cdf(posterior.low)).max(f64::MIN_POSITIVE);
    value + 0.5_f64 * (2.0_f64 * PI / curvature).ln() + mass.ln()
}

fn within_cell_log_posterior(
    log_rate: f64,
    posterior: &WithinCellPosterior<'_, '_>,
) -> (f64, f64, f64) {
    let rate = log_rate.exp();
    let shape_float = f64::from(posterior.shape);
    let mut value = duration_log_likelihood(
        posterior.shape,
        rate,
        posterior.evidence,
        posterior.statistics,
    ) + f64::from(posterior.aggregate_count) * log_rate;
    let mut gradient = (shape_float - 1.0_f64) * f64::from(posterior.statistics.completion_count)
        - shape_float * rate * posterior.statistics.duration_sum_seconds
        + f64::from(posterior.aggregate_count);
    let mut second = -shape_float * rate * posterior.statistics.duration_sum_seconds;
    let (offsets, durations, _, final_ages) = posterior.evidence.service_durations();
    for (&offset, &duration) in offsets.iter().zip(durations) {
        let entry = duration.saturating_sub(offset);
        if entry > 0 {
            let (first, curvature) = erlang_log_survival_derivatives(posterior.shape, rate, entry);
            gradient -= first;
            second -= curvature;
        }
    }
    let exposure = posterior.evidence.window().exposure_micros();
    for elapsed in final_ages.iter().copied() {
        let (first, curvature) = erlang_log_survival_derivatives(posterior.shape, rate, elapsed);
        gradient += first;
        second += curvature;
        let entry = elapsed.saturating_sub(exposure);
        if entry > 0 {
            let (first, curvature) = erlang_log_survival_derivatives(posterior.shape, rate, entry);
            gradient -= first;
            second -= curvature;
        }
    }
    match posterior.prior {
        CapacityPrior::LogUniform => value -= (posterior.high - posterior.low).ln(),
        CapacityPrior::LogNormal {
            service_time_median_seconds,
            log_standard_deviation,
            ..
        } => {
            let mean = -service_time_median_seconds.ln();
            let standardized = (log_rate - mean) / log_standard_deviation;
            let normal = Normal::new(mean, log_standard_deviation);
            let mass = normal.map_or(1.0_f64, |distribution| {
                distribution.cdf(posterior.high) - distribution.cdf(posterior.low)
            });
            value += -0.5_f64 * standardized * standardized
                - log_standard_deviation.ln()
                - 0.5_f64 * (2.0_f64 * PI).ln()
                - mass.max(f64::MIN_POSITIVE).ln();
            gradient -= (log_rate - mean) / log_standard_deviation.powi(2);
            second -= log_standard_deviation.powi(2).recip();
        }
    }
    (value, gradient, second)
}

fn erlang_log_survival_derivatives(shape: u32, rate: f64, micros: u64) -> (f64, f64) {
    if micros == 0 {
        return (0.0_f64, 0.0_f64);
    }
    let z = f64::from(shape) * rate * Duration::from_micros(micros).as_secs_f64();
    let mut term = 1.0_f64;
    let mut sum = term;
    for order in 1..shape {
        term *= z / f64::from(order);
        sum += term;
    }
    let quotient = z * term / sum;
    (-quotient, -quotient * (f64::from(shape) - z + quotient))
}

fn truncated_log_rate_draw(
    mode: f64,
    curvature: f64,
    low: f64,
    high: f64,
    lower: f64,
    upper: f64,
    draw: f64,
) -> f64 {
    let Ok(normal) = Normal::new(mode, curvature.sqrt().recip()) else {
        return mode.exp();
    };
    normal
        .inverse_cdf(lower + draw * (upper - lower))
        .clamp(low, high)
        .exp()
}

/// Draws a deterministic rate from its feasible interval.
///
/// An infeasible member has zero conditional posterior mass. Selection cannot
/// call the clamp branch for that member. The clamp enforces the cell bound if
/// stored state becomes inconsistent.
#[derive(Clone, Copy)]
struct DeterministicRateDraw {
    mode: f64,
    curvature: f64,
    low: f64,
    high: f64,
    cell: (f64, f64),
    prior: CapacityPrior,
    lower_cdf: f64,
    upper_cdf: f64,
    draw: f64,
}

fn deterministic_log_rate_draw(parameters: DeterministicRateDraw) -> f64 {
    let DeterministicRateDraw {
        mode,
        curvature,
        low,
        high,
        cell,
        prior,
        lower_cdf,
        upper_cdf,
        draw,
    } = parameters;
    if low >= high {
        return mode.clamp(cell.0, cell.1).exp();
    }
    match prior {
        CapacityPrior::LogNormal { .. } => {
            truncated_log_rate_draw(mode, curvature, low, high, lower_cdf, upper_cdf, draw)
        }
        CapacityPrior::LogUniform => {
            let count = -curvature;
            if count == 0.0_f64 {
                low + draw * (high - low)
            } else {
                let lower = (count * (low - high)).exp();
                high + (lower + draw * (1.0_f64 - lower)).ln() / count
            }
            .exp()
        }
    }
}

fn erlang_log_survival(shape: f64, operational_time: f64) -> f64 {
    if operational_time <= 0.0_f64 {
        return 0.0_f64;
    }
    gamma_ur(shape, shape * operational_time)
        .max(f64::MIN_POSITIVE)
        .ln()
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
        let biased = u64::try_from(exponent + 1_023).unwrap_or(u64::MAX);
        f64::from_bits(biased << 52)
    } else {
        let shift = u32::try_from(exponent + 1_074).unwrap_or(0);
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

fn normalize_log_weights(weights: &[f64], log_values: &[f64], output: &mut [f64]) {
    let maximum = weights
        .iter()
        .zip(log_values)
        .filter(|(weight, value)| **weight > 0.0_f64 && value.is_finite())
        .map(|(weight, value)| weight.ln() + value)
        .fold(f64::NEG_INFINITY, f64::max);
    let normalizer = weights
        .iter()
        .zip(log_values)
        .filter(|(weight, value)| **weight > 0.0_f64 && value.is_finite())
        .map(|(weight, value)| (weight.ln() + value - maximum).exp())
        .sum::<f64>();
    for ((output, weight), value) in output.iter_mut().zip(weights).zip(log_values) {
        *output = if *weight > 0.0_f64 && value.is_finite() {
            (weight.ln() + value - maximum).exp() / normalizer
        } else {
            0.0_f64
        };
    }
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

#[cfg(test)]
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
