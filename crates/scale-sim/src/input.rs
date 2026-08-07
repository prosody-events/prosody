use thiserror::Error;

use prosody_scale_core::RandomStream;

use crate::{EventSpec, Plant, PlantError};

/// Bounded piecewise-constant values indexed by virtual time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepSeries<T> {
    at_micros: Vec<u64>,
    values: Vec<T>,
}

/// Bounded inverse-CDF table for deterministic distribution draws.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuantileTable {
    values: Vec<u64>,
    value_count: u32,
}

/// Event releases paired with one handler latency distribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadSeries {
    release_micros: Vec<u64>,
    event_counts: Vec<u32>,
    handler_micros: QuantileTable,
    partition_count: u32,
    key_count: u32,
    dependency_operations: u32,
    seed: u64,
}

/// Added latency as a piecewise-constant function of concurrency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcurrencyLatencyCurve {
    concurrency: Vec<u32>,
    added_micros: Vec<u64>,
}

impl<T: Copy> StepSeries<T> {
    pub(crate) fn constant(value: T) -> Self {
        Self {
            at_micros: vec![0],
            values: vec![value],
        }
    }

    /// Copies one validated series into contiguous fixed-size columns.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, mismatched, or unordered input.
    pub fn new(at_micros: &[u64], values: &[T]) -> Result<Self, InputError> {
        validate_axes(at_micros, values.len())?;
        Ok(Self {
            at_micros: at_micros.to_vec(),
            values: values.to_vec(),
        })
    }

    /// Returns the latest value at or before one virtual time.
    ///
    /// Returns the first value when the time precedes the first point.
    #[must_use]
    pub fn at(&self, now_micros: u64) -> T {
        let index = self
            .at_micros
            .partition_point(|&at_micros| at_micros <= now_micros)
            .saturating_sub(1);
        self.values[index]
    }
}

impl WorkloadSeries {
    /// Copies one bounded workload series.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid axes or zero partition and key counts.
    pub fn new(
        release_micros: &[u64],
        event_counts: &[u32],
        handler_micros: QuantileTable,
        partition_count: u32,
        key_count: u32,
        dependency_operations: u32,
        seed: u64,
    ) -> Result<Self, InputError> {
        validate_axes(release_micros, event_counts.len())?;
        if partition_count == 0 || key_count == 0 {
            return Err(InputError::ZeroBound);
        }
        Ok(Self {
            release_micros: release_micros.to_vec(),
            event_counts: event_counts.to_vec(),
            handler_micros,
            partition_count,
            key_count,
            dependency_operations,
            seed,
        })
    }

    /// Adds every generated event to one preallocated plant.
    ///
    /// # Errors
    ///
    /// Returns an error when the plant cannot accept a generated event.
    pub fn add_to(&self, plant: &mut Plant) -> Result<(), PlantError> {
        let mut random = RandomStream::new(self.seed);
        let mut event_index = 0_u32;
        for (&release_micros, &event_count) in self.release_micros.iter().zip(&self.event_counts) {
            for _ in 0..event_count {
                plant.add_event(EventSpec {
                    release_micros,
                    partition: event_index % self.partition_count,
                    key: event_index % self.key_count,
                    handler_micros: self.handler_micros.sample(&mut random),
                    dependency_operations: self.dependency_operations,
                    outcome: crate::EventOutcome::Final(crate::FinalOutcome::Success),
                    source: crate::EventSource::Message,
                })?;
                event_index = event_index.saturating_add(1);
            }
        }
        Ok(())
    }
}

impl QuantileTable {
    pub(crate) fn constant(value: u64) -> Self {
        Self {
            values: vec![value],
            value_count: 1,
        }
    }

    /// Copies sorted quantiles into one fixed-size table.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is empty, too large, or unordered.
    pub fn new(values: &[u64]) -> Result<Self, InputError> {
        if values.is_empty() {
            return Err(InputError::Empty);
        }
        if !values.windows(2).all(|pair| pair[0] <= pair[1]) {
            return Err(InputError::UnorderedValues);
        }
        let value_count = u32::try_from(values.len()).map_err(|_| InputError::PlatformLimit)?;
        Ok(Self {
            values: values.to_vec(),
            value_count,
        })
    }

    /// Draws one quantile through the counter-based random stream.
    #[must_use]
    pub fn sample(&self, random: &mut RandomStream) -> u64 {
        let random_high = (random.next_u64() >> 32_u32) as u32;
        let product = u64::from(random_high) * u64::from(self.value_count);
        let index = (product >> 32_u32) as u32;
        self.values[index as usize]
    }
}

impl ConcurrencyLatencyCurve {
    /// Copies one validated handler response curve.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, mismatched, or unordered input.
    pub fn new(concurrency: &[u32], added_micros: &[u64]) -> Result<Self, InputError> {
        validate_axes(concurrency, added_micros.len())?;
        Ok(Self {
            concurrency: concurrency.to_vec(),
            added_micros: added_micros.to_vec(),
        })
    }

    pub(crate) fn zero() -> Self {
        Self {
            concurrency: vec![0],
            added_micros: vec![0],
        }
    }

    pub(crate) fn added_micros(&self, concurrency: u32) -> u64 {
        let index = self
            .concurrency
            .partition_point(|&threshold| threshold <= concurrency)
            .saturating_sub(1);
        self.added_micros[index]
    }
}

fn validate_axes<Axis: Ord>(axis: &[Axis], value_count: usize) -> Result<(), InputError> {
    if axis.is_empty() {
        return Err(InputError::Empty);
    }
    if axis.len() != value_count {
        return Err(InputError::LengthMismatch);
    }
    if !axis.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(InputError::UnorderedAxis);
    }
    Ok(())
}

/// Invalid simulator input table.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InputError {
    /// An input table has no values.
    #[error("an input table must not be empty")]
    Empty,
    /// Input columns have different lengths.
    #[error("input table columns must have equal lengths")]
    LengthMismatch,
    /// An input axis does not increase strictly.
    #[error("an input axis must increase strictly")]
    UnorderedAxis,
    /// Quantile values do not increase.
    #[error("quantile values must not decrease")]
    UnorderedValues,
    /// An input count does not fit this platform.
    #[error("an input count exceeds this platform's address space")]
    PlatformLimit,
    /// A required workload bound is zero.
    #[error("workload partition and key counts must be positive")]
    ZeroBound,
}

#[cfg(test)]
mod tests {
    use prosody_scale_core::RandomStream;

    use super::{ConcurrencyLatencyCurve, QuantileTable, StepSeries};

    #[test]
    fn input_tables_replay_boundaries_and_distributions() -> Result<(), super::InputError> {
        let demand = StepSeries::new(&[0_u64, 10, 20], &[2_u32, 5, 3])?;
        assert_eq!(demand.at(0), 2);
        assert_eq!(demand.at(19), 5);
        assert_eq!(demand.at(20), 3);

        let curve = ConcurrencyLatencyCurve::new(&[0_u32, 16, 32], &[0_u64, 1_000, 8_000])?;
        assert_eq!(curve.added_micros(31), 1_000);

        let quantiles = QuantileTable::new(&[30_u64, 45, 60, 90])?;
        let mut first = RandomStream::new(19);
        let mut replay = RandomStream::new(19);
        for _ in 0_u32..128 {
            assert_eq!(quantiles.sample(&mut first), quantiles.sample(&mut replay));
        }
        Ok(())
    }
}
