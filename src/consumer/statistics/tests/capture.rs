//! In-memory metric capture: a meter provider the tests own, gauges that
//! record into it, and readback of the exported gauge series.

use super::super::KafkaMetrics;
use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

/// The meter scope every test provider registers its instruments under.
const SCOPE: &str = "statistics-tests";

pub(super) const FETCH_MESSAGES: &str = "prosody.kafka.consumer.fetch_queue.messages";
pub(super) const FETCH_BYTES: &str = "prosody.kafka.consumer.fetch_queue.size";
pub(super) const METADATA_AGE: &str = "prosody.kafka.consumer.metadata.age";

/// A meter provider that exports into memory. Never installed globally, so
/// parallel tests stay isolated.
pub(super) fn test_meter() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    (provider, exporter)
}

/// Gauges recording into `provider`'s meter, so their exported series are
/// readable back.
pub(super) fn metrics_with(group_id: &str, provider: &SdkMeterProvider) -> KafkaMetrics {
    KafkaMetrics::new(&provider.meter(SCOPE), group_id)
}

/// The last exported value of `name` on the data point carrying every attribute
/// in `wanted`, or `None` when no such series exists.
///
/// A second matching data point is a test bug: the assertion would then depend
/// on export order, so this fails instead.
pub(super) fn gauge_value(
    exporter: &InMemoryMetricExporter,
    name: &str,
    wanted: &[(&str, String)],
) -> Result<Option<u64>> {
    let batches = exporter.get_finished_metrics()?;
    let last = batches
        .last()
        .ok_or_else(|| eyre!("no metric batch was exported"))?;
    let mut found = None;
    for scope in last.scope_metrics() {
        for metric in scope.metrics().filter(|metric| metric.name() == name) {
            let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                bail!("{name} was not exported as a u64 gauge");
            };
            for point in gauge.data_points() {
                let matched = wanted.iter().all(|(key, value)| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == *key && kv.value.to_string() == *value)
                });
                if matched {
                    if found.is_some() {
                        bail!("{name} exported more than one data point matching {wanted:?}");
                    }
                    found = Some(point.value());
                }
            }
        }
    }
    Ok(found)
}

/// The value of a per-partition gauge for one `(topic, id)` series.
pub(super) fn partition_gauge(
    exporter: &InMemoryMetricExporter,
    name: &str,
    topic: &str,
    id: i32,
) -> Result<Option<u64>> {
    gauge_value(
        exporter,
        name,
        &[
            ("messaging.destination.name", topic.to_owned()),
            ("messaging.destination.partition.id", id.to_string()),
        ],
    )
}
