use crate::cassandra::CassandraConfiguration;
use crate::otel::SpanRelation;
use color_eyre::Result;
use color_eyre::eyre::eyre;
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
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tracing::subscriber::with_default;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;

/// The shared, pre-migrated keyspace every Cassandra-backed test runs against.
///
/// Tests never create per-test keyspaces — minting one per test leaks schema
/// (orphaned keyspaces bloat the cluster and eventually time out migration
/// tests). Isolation comes from fresh per-test identifiers (segment ids,
/// group ids, topics) instead.
pub const TEST_KEYSPACE: &str = "prosody_test";

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
    let exporter = TestExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("test"))
            .with_filter(max_level),
    );

    with_default(subscriber, f);
    drop(provider);

    let spans = exporter.0.lock();
    spans.clone()
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
        TraceId::from(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
        SpanId::from(0x1122_3344_5566_7788),
        TraceFlags::SAMPLED,
        true,
        TraceState::NONE,
    );
    Context::current().with_remote_span_context(span_context)
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
