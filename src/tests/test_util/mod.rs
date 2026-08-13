mod metrics;

pub(crate) use self::metrics::{GlobalMetrics, assert_distinct_labels, label};
use crate::cassandra::CassandraConfiguration;
use crate::otel::SpanRelation;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::trace::{
    SpanContext, SpanId, SpanKind, TraceContextExt as _, TraceFlags, TraceId, TraceState,
};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use serde_json::{Map, Value};
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

/// Accumulates every captured tracing event, rendered field-by-field, into a
/// shared buffer. Cloning shares the buffer.
#[derive(Clone, Default)]
pub(crate) struct CapturedEvents(Arc<Mutex<Vec<String>>>);

impl CapturedEvents {
    /// Whether any captured event's rendering contains `needle` (matches its
    /// message or any field value).
    pub(crate) fn contains(&self, needle: &str) -> bool {
        self.count(needle) > 0
    }

    /// How many captured events' renderings contain `needle`.
    pub(crate) fn count(&self, needle: &str) -> usize {
        self.0
            .lock()
            .iter()
            .filter(|event| event.contains(needle))
            .count()
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

/// One attribute from one exported span.
pub(crate) fn span_attribute<'a>(
    span: &'a SpanData,
    key: &str,
) -> Result<&'a opentelemetry::Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
        .ok_or_else(|| eyre!("the {} span carries no {key}", span.name))
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
