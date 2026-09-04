//! Metric instruments for committed keyed-state cell loads.

use crate::error::{ClassifyError, ErrorCategory};
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use quanta::Instant;

const OPERATION: &str = "prosody.state.cell.operation.name";
const SOURCE: &str = "prosody.state.cell.load.source";
const CACHE_RESULT: &str = "prosody.state.cell.cache.result";
const ERROR_CATEGORY: &str = "prosody.error.category";

/// Records where committed cell loads got their answers.
///
/// Attributes use fixed values. They never contain collection, key, or cell
/// identities.
#[derive(Clone)]
pub(crate) struct CellMetrics {
    loads: Counter<u64>,
    load_duration: Histogram<f64>,
    cache_errors: Counter<u64>,
}

impl CellMetrics {
    /// Builds the cell instruments on `meter`.
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            loads: meter
                .u64_counter("prosody.state.cell.loads")
                .with_description("Committed keyed-state cells loaded by answer source")
                .with_unit("{cell}")
                .build(),
            load_duration: meter
                .f64_histogram("prosody.state.cell.load.duration")
                .with_description("Duration of committed keyed-state cell load operations")
                .with_unit("s")
                .build(),
            cache_errors: meter
                .u64_counter("prosody.state.cell.cache.errors")
                .with_description("Keyed-state cell cache operation errors")
                .with_unit("{error}")
                .build(),
        }
    }

    /// Records one point load.
    pub(super) fn point<T, E: ClassifyError>(
        &self,
        started: Instant,
        source: Source,
        cache_result: CacheResult,
        outcome: &Result<T, E>,
    ) {
        self.record(1, "get", started, source, cache_result, outcome);
    }

    /// Records all cells in one batch load.
    pub(super) fn batch<T, E: ClassifyError>(
        &self,
        cells: usize,
        started: Instant,
        source: Source,
        cache_result: CacheResult,
        outcome: &Result<T, E>,
    ) {
        self.record(cells, "get_many", started, source, cache_result, outcome);
    }

    fn record<T, E: ClassifyError>(
        &self,
        cells: usize,
        operation: &'static str,
        started: Instant,
        source: Source,
        result: CacheResult,
        outcome: &Result<T, E>,
    ) {
        let cells = cells as u64;
        let attributes = [
            KeyValue::new(OPERATION, operation),
            KeyValue::new(SOURCE, source.as_str()),
            KeyValue::new(CACHE_RESULT, result.as_str()),
        ];
        let duration = started.elapsed().as_secs_f64();
        match outcome {
            Ok(_) => {
                self.loads.add(cells, &attributes);
                self.load_duration.record(duration, &attributes);
            }
            Err(error) => self.load_duration.record(
                duration,
                &[
                    attributes[0].clone(),
                    attributes[1].clone(),
                    attributes[2].clone(),
                    KeyValue::new(ERROR_CATEGORY, error_category(error.classify_error())),
                ],
            ),
        }
    }

    /// Records one cache operation failure.
    pub(super) fn cache_error(&self, operation: &'static str, cache_phase: &'static str) {
        self.cache_errors.add(
            1,
            &[
                KeyValue::new(OPERATION, operation),
                KeyValue::new("prosody.state.cell.cache.phase", cache_phase),
            ],
        );
    }
}

const fn error_category(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Transient => "transient",
        ErrorCategory::Permanent => "permanent",
        ErrorCategory::Terminal => "terminal",
    }
}

impl Default for CellMetrics {
    fn default() -> Self {
        Self::new(&meter("prosody"))
    }
}

#[derive(Clone, Copy)]
pub(super) enum Source {
    Cache,
    Store,
}

impl Source {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Store => "store",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum CacheResult {
    Hit,
    Miss,
    Expired,
    Error,
    Disabled,
    NotAllHit,
}

impl CacheResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Expired => "expired",
            Self::Error => "error",
            Self::Disabled => "disabled",
            Self::NotAllHit => "not_all_hit",
        }
    }
}
