use crate::cassandra::CassandraConfiguration;
use crate::otel::SpanRelation;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use opentelemetry::global::set_meter_provider;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, TraceContextExt as _, TraceFlags, TraceId, TraceState,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::env;
use std::fmt::{Debug, Write as _};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tracing::field::{Field, Visit};
use tracing::subscriber::{DefaultGuard, set_default, set_global_default, with_default};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;

/// The shared, pre-migrated keyspace every Cassandra-backed test runs against.
///
/// Tests never create per-test keyspaces — minting one per test leaks schema
/// (orphaned keyspaces bloat the cluster and eventually time out migration
/// tests). Isolation comes from fresh per-test identifiers (segment ids,
/// group ids, topics) instead.
pub const TEST_KEYSPACE: &str = "prosody_test";

/// The trace id [`sampled_remote_context`] carries.
const SAMPLED_REMOTE_TRACE: TraceId =
    TraceId::from_bytes(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128.to_be_bytes());

/// Shared multi-threaded runtime for all unit tests in the crate.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
)]
pub static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Depth-bounded `serde_json::Value` generator shared by the state-codec
/// and descriptor round-trip properties.
///
/// Floats are deliberately excluded: JSON has no NaN and float identity
/// is not the invariant under test — structural round-tripping is.
#[derive(Clone, Debug)]
pub struct ArbJson(pub Value);

impl Arbitrary for ArbJson {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(arbitrary_json(g, 3))
    }
}

/// Property-test iteration count for live-backend suites: `INTEGRATION_TESTS`
/// if set, else `default`. CI cranks it up; dev loops stay fast.
pub(crate) fn integration_test_count(default: u64) -> u64 {
    env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Configuration for the local test cluster (`localhost:9042`) over the
/// shared, pre-migrated [`TEST_KEYSPACE`].
pub(crate) fn test_cassandra_config() -> CassandraConfiguration {
    CassandraConfiguration {
        datacenter: None,
        rack: None,
        nodes: vec!["localhost:9042".to_owned()],
        keyspace: TEST_KEYSPACE.to_owned(),
        user: None,
        password: None,
        retention: Duration::from_mins(10),
    }
}

/// `SpanExporter` that accumulates every exported span into a shared buffer so
/// a test can assert on span relationships after the fact. Cloning shares the
/// buffer; [`captured_spans`] hands one clone to the provider and reads the
/// other once the provider flushes.
#[derive(Clone, Debug, Default)]
struct TestExporter(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for TestExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().extend(batch);
        Ok(())
    }
}

/// Runs `f` under a tracing subscriber wired to an OpenTelemetry pipeline and
/// returns every span that ended during the call.
///
/// Pure in-process: the simple span processor drives each export synchronously
/// on span end, so no tokio runtime is required. Dropping the provider flushes
/// any span still buffered before the captured vector is read back.
pub(crate) fn captured_spans(f: impl FnOnce()) -> Vec<SpanData> {
    captured_spans_filtered(LevelFilter::TRACE, f)
}

/// [`captured_spans`] with a maximum-level filter on the exporting layer, for
/// asserting which spans a level-filtered subscriber exports at all — the
/// span-level invariant: application-facing spans at INFO, framework-internal
/// spans at DEBUG.
pub(crate) fn captured_spans_filtered(max_level: LevelFilter, f: impl FnOnce()) -> Vec<SpanData> {
    let (exporter, provider, subscriber) = span_pipeline(max_level);

    with_default(subscriber, f);
    drop(provider);

    let spans = exporter.0.lock();
    spans.clone()
}

/// One `OpenTelemetry` capture pipeline: the exporter to read spans from, the
/// provider that must outlive the capture, and the subscriber to install.
///
/// Both capture surfaces build it here, so the pipeline cannot change for the
/// thread-local one and not for the process-wide one.
fn span_pipeline(
    max_level: LevelFilter,
) -> (
    TestExporter,
    SdkTracerProvider,
    impl Subscriber + Send + Sync + 'static,
) {
    let exporter = TestExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("test"))
            .with_filter(max_level),
    );
    (exporter, provider, subscriber)
}

/// Captures spans opened on **any** thread, for a test whose subject runs in a
/// spawned task.
///
/// [`captured_spans`] installs its subscriber with `with_default`, which is
/// thread-local, so it cannot see a span a server task opened. This installs
/// the same pipeline as the process default instead — which one test process
/// may do exactly once, so it belongs to tests that own their process.
pub(crate) struct GlobalSpans {
    exporter: TestExporter,
    /// Held so the pipeline outlives the capture. The simple processor exports
    /// on span end, so nothing has to be flushed before a read.
    _provider: SdkTracerProvider,
}

impl GlobalSpans {
    /// Installs the pipeline as this process's default subscriber.
    ///
    /// # Errors
    ///
    /// Returns an error when a default subscriber is already installed.
    pub(crate) fn install() -> Result<Self> {
        let (exporter, provider, subscriber) = span_pipeline(LevelFilter::TRACE);
        set_global_default(subscriber)?;
        Ok(Self {
            exporter,
            _provider: provider,
        })
    }

    /// Every span that has ended so far, on every thread.
    pub(crate) fn ended(&self) -> Vec<SpanData> {
        self.exporter.0.lock().clone()
    }
}

/// Captures every metric this process records, for a test whose claim is the
/// attribute set a counter carries rather than an in-process oracle.
///
/// The peer instruments are `LazyLock` statics bound to whatever meter provider
/// is global when they are first touched, so install this **before** the code
/// under test records anything. One process installs one meter provider, so
/// this belongs to tests that own their process — nextest gives each test one.
///
/// This is a deliberate process-global install, for the same reason
/// [`GlobalSpans`] is one: the recording sites are free functions, spawned
/// workers, and a `Default`-constructed codec, so there is no owner to inject a
/// `Meter` into.
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

    /// Every point recorded on the counter or gauge named `name`, as its exact
    /// attribute map and its latest value.
    ///
    /// The attributes are returned verbatim, so a test can assert on the whole
    /// set and catch an identity that leaked into a label.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot flush or the exporter cannot
    /// be read.
    pub(crate) fn points(&self, name: &str) -> Result<Vec<(BTreeMap<String, String>, u64)>> {
        self.provider.force_flush()?;
        let mut points: BTreeMap<BTreeMap<String, String>, u64> = BTreeMap::new();
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

/// Reads one metric's `u64` sum or gauge points into `points`, keeping the last
/// value seen for each attribute set. The exporter is cumulative, so a later
/// export of one series supersedes an earlier one.
///
/// # Errors
///
/// Returns an error for an instrument this harness cannot read — a signed or
/// floating-point one, or a histogram. Reporting it is the point: a silent
/// empty read would let `assert!(points(..)?.is_empty())` pass while proving
/// nothing.
fn collect_points(
    name: &str,
    data: &AggregatedMetrics,
    points: &mut BTreeMap<BTreeMap<String, String>, u64>,
) -> Result<()> {
    let AggregatedMetrics::U64(data) = data else {
        bail!("{name} is not a u64 instrument, so this harness cannot read its points");
    };
    match data {
        MetricData::Sum(sum) => {
            for point in sum.data_points() {
                let _ = points.insert(attribute_map(point.attributes()), point.value());
            }
        }
        MetricData::Gauge(gauge) => {
            for point in gauge.data_points() {
                let _ = points.insert(attribute_map(point.attributes()), point.value());
            }
        }
        MetricData::Histogram(_) | MetricData::ExponentialHistogram(_) => {
            bail!("{name} is a histogram, so this harness cannot read its points");
        }
    }
    Ok(())
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

/// Accumulates every captured tracing event, rendered field-by-field, into a
/// shared buffer. Cloning shares the buffer.
#[derive(Clone, Default)]
pub(crate) struct CapturedEvents(Arc<Mutex<Vec<String>>>);

impl CapturedEvents {
    /// Whether any captured event's rendering contains `needle` (matches its
    /// message or any field value).
    pub(crate) fn contains(&self, needle: &str) -> bool {
        self.0.lock().iter().any(|event| event.contains(needle))
    }
}

/// Tracing layer that captures every event at or above `max_level` in
/// severity. Setting `max_level` to WARN also captures ERROR events. It
/// renders every field, including `message`, into [`CapturedEvents`].
struct EventCaptureLayer {
    max_level: Level,
    events: CapturedEvents,
}

impl<S> Layer<S> for EventCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if *event.metadata().level() > self.max_level {
            return;
        }
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        self.events.0.lock().push(visitor.output);
    }
}

/// Renders every event field — including the format-string `message` — into one
/// string. Only [`Visit::record_debug`] is implemented; every typed
/// `record_*` forwards to it by default, so this captures all field kinds.
#[derive(Default)]
struct EventVisitor {
    output: String,
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        let _ = write!(&mut self.output, "{}={value:?};", field.name());
    }
}

/// Installs a subscriber that captures every tracing event at or above
/// `max_level` in severity for the current thread. Returns the shared
/// [`CapturedEvents`] buffer and the guard that keeps the subscriber active.
/// Drop the guard to uninstall it.
///
/// Use this instead of [`captured_spans`] when the test body is `async`.
/// [`captured_spans`] only scopes a synchronous closure. A `#[tokio::test]`
/// runs its awaits on the calling thread, so the thread-local subscriber
/// installed here stays active across them.
///
/// This is the event analog of [`captured_spans`]: use it to assert that a
/// specific diagnostic log fired. The `captured_spans` `OTel` pipeline
/// records only span data, not free-standing events.
#[must_use]
pub(crate) fn capture_events(max_level: Level) -> (CapturedEvents, DefaultGuard) {
    let events = CapturedEvents::default();
    let subscriber = tracing_subscriber::registry().with(EventCaptureLayer {
        max_level,
        events: events.clone(),
    });
    let guard = set_default(subscriber);
    (events, guard)
}

/// A fixed, valid, sampled remote span context wrapped in a [`Context`], for
/// tests that need a known scheduling or parent context to assert span
/// topology against.
///
/// Both ids are non-zero so [`SpanContext::is_valid`] holds (a follows-from
/// link is only emitted for a valid context), and the sampled flag makes a
/// span derived from it exportable through [`captured_spans`].
pub(crate) fn sampled_remote_context() -> Context {
    let span_context = SpanContext::new(
        SAMPLED_REMOTE_TRACE,
        SpanId::from(0x1122_3344_5566_7788),
        TraceFlags::SAMPLED,
        true,
        TraceState::NONE,
    );
    Context::current().with_remote_span_context(span_context)
}

/// The one exported span named `name`.
///
/// Exactly one must exist: a test that reads "the" span of a name is asserting
/// on a second one it never saw as soon as two are exported.
///
/// # Errors
///
/// Returns an error when no span of that name was exported, or when more than
/// one was.
pub(crate) fn named<'a>(spans: &'a [SpanData], name: &str) -> Result<&'a SpanData> {
    let mut found = spans.iter().filter(|span| span.name == name);
    let span = found
        .next()
        .ok_or_else(|| eyre!("span {name} was not exported"))?;
    ensure!(
        found.next().is_none(),
        "more than one {name} span was exported"
    );
    Ok(span)
}

/// Asserts, by id equality rather than any `is_some()`/validity proxy, that the
/// exported span named `name` connects to `target` per `relation`.
///
/// - [`SpanRelation::Child`]: `target` is the span's `OTel` parent — its span
///   id is the parent id, its trace id is shared, and no link is added.
/// - [`SpanRelation::FollowsFrom`]: the span is a fresh trace root that carries
///   `target` as its sole link, tagged `opentracing.ref_type = follows_from`.
pub(crate) fn assert_span_relation(
    spans: &[SpanData],
    name: &str,
    relation: SpanRelation,
    target: &SpanContext,
) -> Result<()> {
    let span = spans
        .iter()
        .find(|s| s.name.as_ref() == name)
        .ok_or_else(|| eyre!("span {name:?} was not exported"))?;
    assert_eq!(
        span.span_kind,
        SpanKind::Consumer,
        "related span {name:?} is consumer-kind under every relation"
    );
    match relation {
        SpanRelation::Child => {
            assert_eq!(
                span.parent_span_id,
                target.span_id(),
                "child {name:?} must be parented on the target span"
            );
            assert_eq!(
                span.span_context.trace_id(),
                target.trace_id(),
                "child {name:?} shares the target trace"
            );
            assert!(
                span.links.links.is_empty(),
                "child {name:?} establishes a parent, not a link"
            );
        }
        SpanRelation::FollowsFrom => {
            assert_eq!(
                span.parent_span_id,
                SpanId::INVALID,
                "follows-from {name:?} is a trace root"
            );
            assert_ne!(
                span.span_context.trace_id(),
                target.trace_id(),
                "follows-from {name:?} starts a fresh trace"
            );
            assert_eq!(
                span.links.links.len(),
                1,
                "follows-from {name:?} adds exactly one link"
            );
            let link = span
                .links
                .links
                .first()
                .ok_or_else(|| eyre!("follows-from {name:?} is missing its link"))?;
            assert_eq!(
                &link.span_context, target,
                "the follows-from link points at the target context"
            );
            let ref_type = link
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == "opentracing.ref_type")
                .ok_or_else(|| {
                    eyre!(
                        "follows-from {name:?} link is missing the opentracing.ref_type attribute"
                    )
                })?;
            assert_eq!(
                ref_type.value.as_str().as_ref(),
                "follows_from",
                "the follows-from {name:?} link is a follows-from reference"
            );
        }
    }
    Ok(())
}

fn arbitrary_json(g: &mut Gen, depth: u8) -> Value {
    let variants = if depth == 0 { 4 } else { 6 };
    match u8::arbitrary(g) % variants {
        0 => Value::Null,
        1 => Value::Bool(bool::arbitrary(g)),
        2 => Value::from(i64::arbitrary(g)),
        3 => Value::String(String::arbitrary(g)),
        4 => Value::Array(
            (0..u8::arbitrary(g) % 4)
                .map(|_| arbitrary_json(g, depth - 1))
                .collect(),
        ),
        _ => Value::Object(
            (0..u8::arbitrary(g) % 4)
                .map(|_| (String::arbitrary(g), arbitrary_json(g, depth - 1)))
                .collect::<Map<_, _>>(),
        ),
    }
}
