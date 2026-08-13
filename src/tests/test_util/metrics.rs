//! Reading back what the code under test recorded on a meter, and holding its
//! labels to the rule that no identity is ever one.

use color_eyre::Result;
use color_eyre::eyre::{bail, ensure};
use opentelemetry::KeyValue;
use opentelemetry::global::set_meter_provider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::collections::BTreeMap;

/// Captures every metric this process records, for a test whose claim is the
/// attribute set a counter carries rather than an in-process oracle.
///
/// The peer instruments are `LazyLock` statics bound to whatever meter provider
/// is global when they are first touched, so install this **before** the code
/// under test records anything. One process installs one meter provider, so
/// this belongs to tests that own their process — nextest gives each test one.
///
/// This is a deliberate process-global install, for the same reason
/// [`GlobalSpans`](super::GlobalSpans) is one: the recording sites are free
/// functions, spawned workers, and a `Default`-constructed codec, so there is
/// no owner to inject a `Meter` into.
pub(crate) struct GlobalMetrics {
    exporter: InMemoryMetricExporter,
    /// Read directly for its `force_flush`, which is what moves a recorded
    /// value into the exporter before a test reads it.
    provider: SdkMeterProvider,
}

impl GlobalMetrics {
    /// Installs the pipeline as this process's global meter provider.
    pub(crate) fn install() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        set_meter_provider(provider.clone());
        Self { exporter, provider }
    }

    /// Every series the instrument named `name` carries, as its exact attribute
    /// map and what it stands at: a sum's total, a gauge's latest value, or the
    /// number of values recorded on a histogram.
    ///
    /// The attributes are returned verbatim, so a test can assert on the whole
    /// set and catch an identity that leaked into a label.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot flush, when the exporter
    /// cannot be read, or for an instrument this harness cannot read.
    pub(crate) fn points(&self, name: &str) -> Result<Vec<(BTreeMap<String, String>, i64)>> {
        self.provider.force_flush()?;
        let mut points: BTreeMap<BTreeMap<String, String>, i64> = BTreeMap::new();
        for resource in self.exporter.get_finished_metrics()? {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics().filter(|metric| metric.name() == name) {
                    collect_points(name, metric.data(), &mut points)?;
                }
            }
        }
        Ok(points.into_iter().collect())
    }
}

/// One data point's whole attribute set, for a comparison that catches an extra
/// attribute as well as a wrong one.
pub(crate) fn label(key: &str, value: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(key.to_owned(), value.to_owned())])
}

/// Asserts that every label is a plain lowercase token and that no two are
/// equal, so one outcome can never be read as another in a dashboard.
///
/// Check one enum's labels per call. Two enums under different attribute keys
/// share no namespace, so merging them can red on a legitimate future label.
///
/// # Errors
///
/// Returns an error naming the first label that is not lowercase, or the first
/// one that repeats.
pub(crate) fn assert_distinct_labels<'a>(labels: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut seen: Vec<&str> = Vec::new();
    for label in labels {
        ensure!(
            !label.is_empty() && label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{label} is not a plain lowercase label"
        );
        ensure!(
            !seen.contains(&label),
            "{label} labels more than one outcome"
        );
        seen.push(label);
    }
    Ok(())
}

/// Reads one metric's points into `points`, keeping the last value seen for
/// each attribute set. The exporter is cumulative, so a later export of one
/// series supersedes an earlier one.
///
/// A floating-point sum or gauge is refused rather than rounded. Reporting it
/// is the point: a silent empty read would let
/// `assert!(points(..)?.is_empty())` pass while proving nothing.
fn collect_points(
    name: &str,
    data: &AggregatedMetrics,
    points: &mut BTreeMap<BTreeMap<String, String>, i64>,
) -> Result<()> {
    match data {
        AggregatedMetrics::U64(data) => read(name, data, points, |value| Ok(i64::try_from(value)?)),
        AggregatedMetrics::I64(data) => read(name, data, points, Ok),
        AggregatedMetrics::F64(data) => read(name, data, points, |_| {
            bail!("{name} carries floating-point values, which this harness cannot read")
        }),
    }
}

/// Reads one instrument's data points, turning each recorded value into the
/// number a test compares against through `value`.
///
/// A histogram never consults `value`: what a test asserts on there is how many
/// values were recorded under each attribute set, not what they summed to. So a
/// floating-point histogram is readable where a floating-point sum is not.
fn read<T: Copy>(
    name: &str,
    data: &MetricData<T>,
    points: &mut BTreeMap<BTreeMap<String, String>, i64>,
    value: impl Fn(T) -> Result<i64>,
) -> Result<()> {
    match data {
        MetricData::Sum(sum) => {
            for point in sum.data_points() {
                let _ = points.insert(attribute_map(point.attributes()), value(point.value())?);
            }
        }
        MetricData::Gauge(gauge) => {
            for point in gauge.data_points() {
                let _ = points.insert(attribute_map(point.attributes()), value(point.value())?);
            }
        }
        MetricData::Histogram(histogram) => {
            for point in histogram.data_points() {
                let _ = points.insert(
                    attribute_map(point.attributes()),
                    i64::try_from(point.count())?,
                );
            }
        }
        MetricData::ExponentialHistogram(_) => {
            bail!("{name} is an exponential histogram, which this harness cannot read");
        }
    }
    Ok(())
}

/// One data point's attributes, rendered as plain strings so a test can compare
/// the whole set.
fn attribute_map<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
    attributes
        .map(|attribute| {
            (
                attribute.key.as_str().to_owned(),
                attribute.value.as_str().into_owned(),
            )
        })
        .collect()
}
