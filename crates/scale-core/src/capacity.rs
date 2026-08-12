use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use thiserror::Error;

use crate::arrival::ArrivalPrior;
use crate::change_point::ChangePointKernel;

const CAPACITY_CELL_COUNT_MAX: u32 = 4_096;
const DEFAULT_WINDOW_INFLUENCE_BOUND_PROBABILITY: f64 = 0.05_f64;
const NO_KNEE_PRIOR_PROBABILITY: f64 = 0.5_f64;
const NO_COLLAPSE_PRIOR_PROBABILITY: f64 = 0.5_f64;
const HAZARD_GRID_COUNT: usize = 9;

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
    LogUniform {
        /// Reciprocal bound on one window's pairwise likelihood ratio.
        window_influence_bound_probability: f64,
    },
    /// Independent normal priors on logarithmic service time and capacity.
    LogNormal {
        /// Median handler service time.
        service_time_median_seconds: f64,
        /// Median aggregate peak capacity.
        capacity_median_per_second: f64,
        /// Standard deviation on both natural-log axes.
        log_standard_deviation: f64,
        /// Reciprocal bound on one window's pairwise likelihood ratio.
        window_influence_bound_probability: f64,
    },
}

impl CapacityPrior {
    fn validate(self) -> Result<(), CapacityGridError> {
        match self {
            Self::LogUniform {
                window_influence_bound_probability,
            } if valid_influence_bound_probability(window_influence_bound_probability) => Ok(()),
            Self::LogNormal {
                service_time_median_seconds,
                capacity_median_per_second,
                log_standard_deviation,
                window_influence_bound_probability,
            } if service_time_median_seconds.is_finite()
                && service_time_median_seconds > 0.0_f64
                && capacity_median_per_second.is_finite()
                && capacity_median_per_second > 0.0_f64
                && log_standard_deviation.is_finite()
                && log_standard_deviation >= f64::EPSILON
                && valid_influence_bound_probability(window_influence_bound_probability) =>
            {
                Ok(())
            }
            Self::LogUniform { .. } | Self::LogNormal { .. } => {
                Err(CapacityGridError::InvalidPrior)
            }
        }
    }

    const fn window_influence_bound_probability(self) -> f64 {
        match self {
            Self::LogUniform {
                window_influence_bound_probability,
            }
            | Self::LogNormal {
                window_influence_bound_probability,
                ..
            } => window_influence_bound_probability,
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
            CapacityPrior::LogUniform {
                window_influence_bound_probability: DEFAULT_WINDOW_INFLUENCE_BOUND_PROBABILITY,
            },
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
        let cell_count_u32 = u32::try_from(cell_count).map_err(|_| CapacityGridError::TooLarge)?;
        if cell_count_u32 > CAPACITY_CELL_COUNT_MAX {
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
        debug_assert!(
            self.service_times_seconds.len() <= CAPACITY_CELL_COUNT_MAX as usize,
            "the constructor limits the capacity grid"
        );
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
#[derive(Debug, PartialEq)]
pub struct ResourceWindow {
    concurrency: f64,
    exposure_seconds: f64,
    completed_attempts: u32,
    started_attempts: Option<u32>,
}

impl ResourceWindow {
    /// Constructs one eligible resource window.
    ///
    /// # Errors
    ///
    /// Returns an error when concurrency or exposure is not positive and
    /// finite.
    pub fn new(
        concurrency: f64,
        exposure_seconds: f64,
        completed_attempts: u32,
    ) -> Result<Self, ResourceWindowError> {
        validate_positive(concurrency, "concurrency")?;
        validate_positive(exposure_seconds, "exposure_seconds")?;
        Ok(Self {
            concurrency,
            exposure_seconds,
            completed_attempts,
            started_attempts: None,
        })
    }

    /// Constructs one resource window with an observed start count.
    ///
    /// # Errors
    ///
    /// Returns an error when concurrency or exposure is not positive and
    /// finite.
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
}

#[derive(Clone, Copy)]
struct StartWindow {
    exposure_seconds: f64,
    started_attempts: u32,
}

pub(crate) struct CapacityFactor {
    grid: CapacityGrid,
    prior_weights: Vec<f64>,
    weights: Vec<f64>,
    likelihoods: Vec<f64>,
    hazard_rates_per_second: Vec<f64>,
    hazard_weights: Vec<f64>,
    hazard_curve_weights: Vec<f64>,
    start_history: Vec<StartWindow>,
    start_history_head: usize,
    start_history_len: usize,
    predictive_start_history: Vec<StartWindow>,
}

impl CapacityFactor {
    #[cfg(test)]
    pub(crate) fn new(grid: CapacityGrid, change_rate_per_second: f64) -> Self {
        Self::new_with_prior(grid, change_rate_per_second, ArrivalPrior::broad_fallback())
    }

    pub(crate) fn new_with_prior(
        grid: CapacityGrid,
        change_rate_per_second: f64,
        arrival_prior: ArrivalPrior,
    ) -> Self {
        let cell_count = grid.service_times_seconds.len();
        let minimum_service_time = grid
            .service_times_seconds
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let maximum_service_time = grid
            .service_times_seconds
            .iter()
            .copied()
            .fold(0.0_f64, f64::max);
        let start_history_capacity = ((maximum_service_time / minimum_service_time).ceil()
            as usize)
            .min(CAPACITY_CELL_COUNT_MAX as usize);
        let prior_weights = capacity_prior(&grid);
        let (hazard_rates_per_second, hazard_weights) =
            hazard_prior(change_rate_per_second, arrival_prior.shape());
        let mut hazard_curve_weights = Vec::with_capacity(cell_count * hazard_weights.len());
        for _ in &hazard_weights {
            hazard_curve_weights.extend_from_slice(&prior_weights);
        }
        Self {
            grid,
            weights: prior_weights.clone(),
            prior_weights,
            likelihoods: vec![0.0; cell_count],
            hazard_rates_per_second,
            hazard_weights,
            hazard_curve_weights,
            start_history: vec![
                StartWindow {
                    exposure_seconds: 0.0_f64,
                    started_attempts: 0,
                };
                start_history_capacity
            ],
            start_history_head: 0,
            start_history_len: 0,
            predictive_start_history: vec![
                StartWindow {
                    exposure_seconds: 0.0_f64,
                    started_attempts: 0,
                };
                start_history_capacity
            ],
        }
    }

    pub(crate) const fn posterior_value_count(&self) -> u32 {
        self.grid.capacity_value_count()
    }

    pub(crate) fn curve_posterior_value_count(&self) -> u32 {
        self.grid.cell_count()
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
        let had_start_history = self.start_history_len > 0;
        self.predictive_start_history
            .copy_from_slice(&self.start_history);
        let mut head = self.start_history_head;
        let mut length = self.start_history_len;
        if window.started_attempts.is_some() {
            record_start_window(
                &mut self.predictive_start_history,
                &mut head,
                &mut length,
                window,
            );
        }
        for (index, cell) in cells.iter_mut().enumerate() {
            cell.mean = completion_mean(
                &self.grid,
                index,
                &self.predictive_start_history,
                head,
                length,
                had_start_history,
                window,
            )
            .0;
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
        for (hazard, curve_weights) in self
            .hazard_rates_per_second
            .iter()
            .zip(self.hazard_curve_weights.chunks_exact_mut(cell_count))
        {
            let transition = ChangePointKernel::new(*hazard).probabilities(elapsed);
            for (weight, prior) in curve_weights.iter_mut().zip(&self.prior_weights) {
                *weight = transition.retained * *weight + transition.redrawn * prior;
            }
        }
        self.mix_hazard_filters();
    }

    pub(crate) fn update(&mut self, simd_level: Level, window: &ResourceWindow) {
        self.update_window(simd_level, window);
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

    #[cfg(test)]
    pub(crate) fn expected_throughput(&self, simd_level: Level, concurrency: f64) -> f64 {
        dispatch!(simd_level, simd => expected_throughput(
            simd,
            &self.weights,
            &self.grid.service_times_seconds,
            &self.grid.capacities_per_second,
            &self.grid.collapse_values,
            &self.grid.no_knee,
            concurrency,
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

    #[cfg(test)]
    pub(crate) fn expected_capacity_scalar(&self) -> f64 {
        if self.knee_probability() <= f64::EPSILON {
            return 0.0_f64;
        }
        self.weights
            .iter()
            .zip(&self.grid.capacities_per_second)
            .map(|(weight, capacity)| weight * capacity)
            .sum::<f64>()
            / self.knee_probability()
    }

    #[cfg(test)]
    pub(crate) fn expected_throughput_scalar(&self, concurrency: f64) -> f64 {
        self.weights
            .iter()
            .enumerate()
            .map(|(index, weight)| {
                weight
                    * throughput(
                        self.grid.service_times_seconds[index],
                        self.grid.capacities_per_second[index],
                        self.grid.collapse_values[index],
                        self.grid.no_knee[index] > 0.0_f64,
                        concurrency,
                    )
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn saturation_probability_scalar(&self, concurrency: f64) -> f64 {
        self.weights
            .iter()
            .enumerate()
            .filter(|&(index, _)| {
                self.grid.service_times_seconds[index] * self.grid.capacities_per_second[index]
                    <= concurrency
                    && self.grid.no_knee[index] == 0.0_f64
            })
            .map(|(_, weight)| weight)
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn no_collapse_probability(&self) -> f64 {
        self.weights
            .iter()
            .take(self.grid.knee_cell_count as usize)
            .zip(&self.grid.collapse_values)
            .filter(|(_, collapse)| **collapse == 0.0_f64)
            .map(|(weight, _)| weight)
            .sum::<f64>()
            / self.knee_probability()
    }

    pub(crate) fn declining_probability(&self) -> f64 {
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return 0.0_f64;
        }
        self.weights
            .iter()
            .take(self.grid.knee_cell_count as usize)
            .zip(&self.grid.collapse_values)
            .filter(|(_, collapse)| **collapse > 0.0_f64)
            .map(|(weight, _)| weight)
            .sum::<f64>()
            / knee_probability
    }

    /// Returns the replica cap from the posterior knee.
    ///
    /// When declining mass exceeds the error allowance, the cap does not
    /// admit slots above the knee quantile. The one-replica minimum can exceed
    /// a knee smaller than one replica.
    pub(crate) fn cap(&self, slots_per_replica: u32, replica_count_max: u32, epsilon: f64) -> u32 {
        if self.no_knee_probability() > epsilon {
            return replica_count_max;
        }
        let knee = self.knee_quantile(1.0_f64 - epsilon);
        let replicas = knee / f64::from(slots_per_replica);
        let cap = if self.declining_probability() > epsilon {
            replicas.floor()
        } else {
            replicas.ceil()
        };
        cap.clamp(1.0, f64::from(replica_count_max)) as u32
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

    fn knee_quantile(&self, probability: f64) -> f64 {
        let knee_probability = self.knee_probability();
        if knee_probability <= f64::EPSILON {
            return 0.0_f64;
        }
        let mut cumulative = 0.0_f64;
        for (&value, index) in self.grid.knee_values.iter().zip(0_u32..) {
            cumulative += self
                .grid
                .knee_indexes
                .iter()
                .zip(&self.weights)
                .filter(|(candidate, _)| **candidate == index)
                .map(|(_, weight)| *weight)
                .sum::<f64>()
                / knee_probability;
            if cumulative >= probability {
                return value;
            }
        }
        self.grid.knee_values[self.grid.knee_values.len() - 1]
    }

    fn update_window(&mut self, _simd_level: Level, window: &ResourceWindow) {
        let had_start_history = self.start_history_len > 0;
        if window.started_attempts.is_some() {
            self.record_start_window(window);
        }
        let influence_bound_probability = self.grid.prior.window_influence_bound_probability();
        for (index, likelihood) in self.likelihoods.iter_mut().enumerate() {
            let service_time = self.grid.service_times_seconds[index];
            let effective_service_time = window.concurrency
                / throughput(
                    service_time,
                    self.grid.capacities_per_second[index],
                    self.grid.collapse_values[index],
                    self.grid.no_knee[index] > 0.0_f64,
                    window.concurrency,
                );
            let (mean, occupancy_eligible) = completion_mean(
                &self.grid,
                index,
                &self.start_history,
                self.start_history_head,
                self.start_history_len,
                had_start_history,
                window,
            );
            *likelihood = poisson_log_kernel(f64::from(window.completed_attempts), mean);
            if occupancy_eligible && had_start_history {
                let occupancy_mean = in_flight_mean(
                    &self.start_history,
                    self.start_history_head,
                    self.start_history_len,
                    window.exposure_seconds,
                    effective_service_time,
                );
                *likelihood += poisson_log_kernel(window.concurrency, occupancy_mean);
            }
        }
        self.bound_likelihoods(influence_bound_probability);
        self.update_hazard_filters();
    }

    fn record_start_window(&mut self, window: &ResourceWindow) {
        record_start_window(
            &mut self.start_history,
            &mut self.start_history_head,
            &mut self.start_history_len,
            window,
        );
    }

    fn update_hazard_filters(&mut self) {
        let cell_count = self.weights.len();
        for (hazard_weight, curve_weights) in self
            .hazard_weights
            .iter_mut()
            .zip(self.hazard_curve_weights.chunks_exact_mut(cell_count))
        {
            let predictive = curve_weights
                .iter()
                .zip(&self.likelihoods)
                .map(|(weight, likelihood)| weight * likelihood)
                .sum::<f64>();
            *hazard_weight *= predictive;
            if predictive > 0.0_f64 {
                for (weight, likelihood) in curve_weights.iter_mut().zip(&self.likelihoods) {
                    *weight *= likelihood / predictive;
                }
            }
        }
        normalize(&mut self.hazard_weights);
        self.mix_hazard_filters();
    }

    fn mix_hazard_filters(&mut self) {
        self.weights.fill(0.0_f64);
        let cell_count = self.weights.len();
        for (hazard_weight, curve_weights) in self
            .hazard_weights
            .iter()
            .zip(self.hazard_curve_weights.chunks_exact(cell_count))
        {
            for (weight, curve_weight) in self.weights.iter_mut().zip(curve_weights) {
                *weight += hazard_weight * curve_weight;
            }
        }
    }

    fn bound_likelihoods(&mut self, influence_bound_probability: f64) {
        let maximum = self
            .likelihoods
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            maximum.is_finite(),
            "a capacity window must have a finite maximum likelihood"
        );
        let minimum = maximum + influence_bound_probability.ln();
        for likelihood in &mut self.likelihoods {
            *likelihood = (likelihood.max(minimum) - maximum).exp();
        }
    }

    #[cfg(test)]
    fn apply_likelihood(&mut self, simd_level: Level, influence_bound_probability: f64) {
        self.bound_likelihoods(influence_bound_probability);
        self.multiply_likelihood(simd_level);
    }

    #[cfg(test)]
    fn multiply_likelihood(&mut self, simd_level: Level) {
        dispatch!(simd_level, simd => multiply_weights(simd, &mut self.weights, &self.likelihoods));
        let total = self.weights.iter().sum::<f64>();
        if total > 0.0_f64 {
            for weight in &mut self.weights {
                *weight /= total;
            }
        }
    }
}

fn capacity_prior(grid: &CapacityGrid) -> Vec<f64> {
    let mut weights = match grid.prior {
        CapacityPrior::LogUniform { .. } => log_uniform_capacity_prior(grid),
        CapacityPrior::LogNormal {
            service_time_median_seconds,
            capacity_median_per_second,
            log_standard_deviation,
            ..
        } => log_normal_capacity_prior(
            grid,
            service_time_median_seconds,
            capacity_median_per_second,
            log_standard_deviation,
        ),
    };
    let service_stride = grid.capacity_count as usize * grid.collapse_count as usize;
    let mut no_knee_weights = Vec::with_capacity(grid.service_time_count as usize);
    for service in 0..grid.service_time_count as usize {
        let start = service * service_stride;
        let service_mass = weights[start..start + service_stride].iter().sum::<f64>();
        no_knee_weights.push(service_mass * NO_KNEE_PRIOR_PROBABILITY);
    }
    for weight in &mut weights {
        *weight *= 1.0_f64 - NO_KNEE_PRIOR_PROBABILITY;
    }
    weights.extend(no_knee_weights);
    weights
}

fn hazard_prior(mean_per_second: f64, shape: f64) -> (Vec<f64>, Vec<f64>) {
    if mean_per_second <= f64::EPSILON {
        return (vec![0.0_f64], vec![1.0_f64]);
    }
    let center = (HAZARD_GRID_COUNT / 2) as i32;
    let mut rates = Vec::with_capacity(HAZARD_GRID_COUNT);
    let mut weights = Vec::with_capacity(HAZARD_GRID_COUNT);
    for index in 0..HAZARD_GRID_COUNT {
        let rate = mean_per_second * 2.0_f64.powi(index as i32 - center);
        rates.push(rate);
        weights.push((shape * rate.ln() - shape * rate / mean_per_second).exp());
    }
    normalize(&mut weights);
    (rates, weights)
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
        exposure_seconds: window.exposure_seconds,
        started_attempts: window.started_attempts.unwrap_or_default(),
    };
    *head = (*head + 1) % history.len();
    *length = (*length + 1).min(history.len());
}

fn lagged_starts(
    history: &[StartWindow],
    head: usize,
    length: usize,
    exposure_seconds: f64,
    service_time_seconds: f64,
) -> Option<f64> {
    let target_start = service_time_seconds;
    let target_end = service_time_seconds + exposure_seconds;
    let mut age = 0.0_f64;
    let mut expected = 0.0_f64;
    for offset in 0..length {
        let index = (head + history.len() - 1 - offset) % history.len();
        let window = history[index];
        let window_end = age + window.exposure_seconds;
        let overlap = window_end.min(target_end) - age.max(target_start);
        if overlap > 0.0_f64 {
            expected += f64::from(window.started_attempts) * overlap / window.exposure_seconds;
        }
        age = window_end;
        if age >= target_end {
            return Some(expected);
        }
    }
    None
}

fn completion_mean(
    grid: &CapacityGrid,
    index: usize,
    history: &[StartWindow],
    head: usize,
    length: usize,
    had_start_history: bool,
    window: &ResourceWindow,
) -> (f64, bool) {
    let service_time = grid.service_times_seconds[index];
    let effective_service_time = window.concurrency
        / throughput(
            service_time,
            grid.capacities_per_second[index],
            grid.collapse_values[index],
            grid.no_knee[index] > 0.0_f64,
            window.concurrency,
        );
    let lagged = if window.started_attempts.is_none()
        || (!had_start_history && window.completed_attempts > 0)
    {
        None
    } else if !had_start_history {
        Some(0.0_f64)
    } else {
        lagged_starts(
            history,
            head,
            length,
            window.exposure_seconds,
            effective_service_time,
        )
    };
    let occupancy_eligible = lagged.is_some();
    let mean = lagged
        .filter(|starts| *starts > 0.0_f64 || window.completed_attempts == 0)
        .unwrap_or_else(|| {
            let (low, high) = grid.throughput_interval(index, window.concurrency);
            f64::from(window.completed_attempts).clamp(
                window.exposure_seconds * low,
                window.exposure_seconds * high,
            )
        });
    (mean, occupancy_eligible)
}

fn log_uniform_capacity_prior(grid: &CapacityGrid) -> Vec<f64> {
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
                let collapse_mass =
                    collapse_mass(&grid.collapse_values[..collapse_count], collapse_index);
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
) -> Vec<f64> {
    let service_count = grid.service_time_count as usize;
    let capacity_count = grid.capacity_count as usize;
    let collapse_count = grid.collapse_count as usize;
    let service_stride = capacity_count * collapse_count;
    let mut log_weights = Vec::with_capacity(grid.service_times_seconds.len());
    for service_index in 0..service_count {
        let service_log_mass = log_normal_cell_mass(
            service_index,
            service_count,
            service_median,
            log_standard_deviation,
            |index| grid.service_times_seconds[index * service_stride],
        );
        for capacity_index in 0..capacity_count {
            let capacity_log_mass = log_normal_cell_mass(
                capacity_index,
                capacity_count,
                capacity_median,
                log_standard_deviation,
                |index| grid.capacities_per_second[index * collapse_count],
            );
            for collapse_index in 0..collapse_count {
                let collapse_log_mass =
                    collapse_mass(&grid.collapse_values[..collapse_count], collapse_index).ln();
                log_weights.push(service_log_mass + capacity_log_mass + collapse_log_mass);
            }
        }
    }
    let maximum = log_weights
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let mut weights = log_weights
        .into_iter()
        .map(|weight| (weight - maximum).exp())
        .collect::<Vec<_>>();
    let total = weights.iter().sum::<f64>();
    for weight in &mut weights {
        *weight /= total;
    }
    weights
}

fn log_normal_cell_mass<Value>(
    index: usize,
    count: usize,
    median: f64,
    log_standard_deviation: f64,
    value: Value,
) -> f64
where
    Value: Fn(usize) -> f64,
{
    let standardized = (value(index).ln() - median.ln()) / log_standard_deviation;
    bounded_log_width(index, count, &value).ln() - 0.5_f64 * standardized * standardized
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

fn bounded_log_width<Value>(index: usize, count: usize, value: Value) -> f64
where
    Value: Fn(usize) -> f64,
{
    if count == 1 {
        return 1.0_f64;
    }
    let center = value(index).ln();
    let lower = if index == 0 {
        value(0).ln()
    } else {
        value(index - 1).ln().midpoint(center)
    };
    let upper = if index + 1 == count {
        value(count - 1).ln()
    } else {
        center.midpoint(value(index + 1).ln())
    };
    upper - lower
}

fn collapse_mass(values: &[f64], index: usize) -> f64 {
    if values.len() == 1 {
        return 1.0_f64;
    }
    if values[0] != 0.0_f64 {
        return bounded_linear_mass(values, index);
    }
    if index == 0 {
        return NO_COLLAPSE_PRIOR_PROBABILITY;
    }
    (1.0_f64 - NO_COLLAPSE_PRIOR_PROBABILITY) * bounded_linear_mass(&values[1..], index - 1)
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

#[cfg(test)]
fn multiply_weights<S: Simd>(simd: S, weights: &mut [f64], likelihoods: &[f64]) {
    let lane_count = S::f64s::N;
    let vector_count = weights.len() / lane_count;
    for vector_index in 0..vector_count {
        let start = vector_index * lane_count;
        let end = start + lane_count;
        let weight = S::f64s::from_slice(simd, &weights[start..end]);
        let likelihood = S::f64s::from_slice(simd, &likelihoods[start..end]);
        (weight * likelihood).store_slice(&mut weights[start..end]);
    }
    for index in vector_count * lane_count..weights.len() {
        weights[index] *= likelihoods[index];
    }
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

#[cfg(test)]
fn expected_throughput<S: Simd>(
    simd: S,
    weights: &[f64],
    service_times_seconds: &[f64],
    capacities_per_second: &[f64],
    collapse_values: &[f64],
    no_knee_values: &[f64],
    concurrency: f64,
) -> f64 {
    if concurrency <= 0.0_f64 {
        return 0.0_f64;
    }
    let lane_count = S::f64s::N;
    let vector_count = weights.len() / lane_count;
    let concurrency_vector = S::f64s::splat(simd, concurrency);
    let one = S::f64s::splat(simd, 1.0_f64);
    let mut sum = S::f64s::splat(simd, 0.0_f64);
    for vector_index in 0..vector_count {
        let start = vector_index * lane_count;
        let end = start + lane_count;
        let weight = S::f64s::from_slice(simd, &weights[start..end]);
        let service_time = S::f64s::from_slice(simd, &service_times_seconds[start..end]);
        let capacity = S::f64s::from_slice(simd, &capacities_per_second[start..end]);
        let collapse = S::f64s::from_slice(simd, &collapse_values[start..end]);
        let no_knee = S::f64s::from_slice(simd, &no_knee_values[start..end]);
        let knee = capacity * service_time;
        let excess = (concurrency_vector - knee) / knee;
        let linear = concurrency_vector / service_time;
        let saturated = capacity / (one + collapse * excess * excess);
        let rate = no_knee.simd_gt(S::f64s::splat(simd, 0.0_f64)).select(
            linear,
            concurrency_vector.simd_le(knee).select(linear, saturated),
        );
        sum += weight * rate;
    }
    let mut total = sum.as_slice().iter().sum::<f64>();
    for index in vector_count * lane_count..weights.len() {
        total += weights[index]
            * throughput(
                service_times_seconds[index],
                capacities_per_second[index],
                collapse_values[index],
                no_knee_values[index] > 0.0_f64,
                concurrency,
            );
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

fn poisson_log_kernel(count: f64, mean: f64) -> f64 {
    if mean > 0.0_f64 {
        count * mean.ln() - mean
    } else if count == 0.0_f64 {
        0.0
    } else {
        f64::NEG_INFINITY
    }
}

fn in_flight_mean(
    history: &[StartWindow],
    head: usize,
    length: usize,
    exposure_seconds: f64,
    service_time_seconds: f64,
) -> f64 {
    let mut age = 0.0_f64;
    let mut in_flight_seconds = 0.0_f64;
    for offset in 0..length {
        let index = (head + history.len() - 1 - offset) % history.len();
        let window = history[index];
        let window_end = age + window.exposure_seconds;
        let integral = in_flight_integral(window_end, exposure_seconds, service_time_seconds)
            - in_flight_integral(age, exposure_seconds, service_time_seconds);
        in_flight_seconds +=
            f64::from(window.started_attempts) * integral / window.exposure_seconds;
        age = window_end;
        if age >= service_time_seconds + exposure_seconds {
            break;
        }
    }
    in_flight_seconds / exposure_seconds
}

fn in_flight_integral(age: f64, exposure_seconds: f64, service_time_seconds: f64) -> f64 {
    let rising_end = exposure_seconds.min(service_time_seconds);
    let plateau_end = exposure_seconds.max(service_time_seconds);
    let falling_end = exposure_seconds + service_time_seconds;
    let rising = age.min(rising_end);
    let plateau = (age.min(plateau_end) - rising_end).max(0.0_f64);
    let falling = (age.min(falling_end) - plateau_end).max(0.0_f64);
    rising * rising * 0.5_f64 + plateau * rising_end + falling * rising_end
        - falling * falling * 0.5_f64
}

fn valid_influence_bound_probability(probability: f64) -> bool {
    probability.is_finite() && probability > 0.0_f64 && probability < 0.5_f64
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
    #[error("the capacity grid exceeds 4096 cells")]
    TooLarge,
    /// The grid could not map a cell to its knee value.
    #[error("a capacity-grid knee value has no axis index")]
    KneeIndex,
}

/// Invalid passive resource window.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ResourceWindowError {
    /// A required value is not positive and finite.
    #[error("{name} value {value} must be positive and finite")]
    InvalidValue {
        /// Name of the invalid field.
        name: &'static str,
        /// Invalid field value.
        value: f64,
    },
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
