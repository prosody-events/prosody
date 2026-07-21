use opentelemetry::propagation::Injector;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::subscriber::with_default;
use tracing_subscriber::layer::SubscriberExt as _;

use super::TelemetryInjector;
use crate::propagator::new_propagator;

#[test]
fn set_stores_traceparent() {
    let mut injector = TelemetryInjector::default();
    injector.set("traceparent", "00-abc-def-01".to_owned());
    let (trace_parent, trace_state) = injector.into_parts();
    assert_eq!(trace_parent.as_deref(), Some("00-abc-def-01"));
    assert_eq!(trace_state, None);
}

#[test]
fn set_stores_tracestate() {
    let mut injector = TelemetryInjector::default();
    injector.set("tracestate", "vendor=value".to_owned());
    let (trace_parent, trace_state) = injector.into_parts();
    assert_eq!(trace_parent, None);
    assert_eq!(trace_state.as_deref(), Some("vendor=value"));
}

#[test]
fn set_ignores_unknown_keys() {
    let mut injector = TelemetryInjector::default();
    injector.set("x-unknown", "should-be-ignored".to_owned());
    let (trace_parent, trace_state) = injector.into_parts();
    assert_eq!(trace_parent, None);
    assert_eq!(trace_state, None);
}

#[test]
fn extract_without_active_span_returns_none() {
    let propagator = new_propagator();
    let injector = TelemetryInjector::extract(&propagator);
    let (trace_parent, trace_state) = injector.into_parts();
    assert_eq!(trace_parent, None);
    assert_eq!(trace_state, None);
}

#[test]
fn extract_with_active_span_returns_traceparent() {
    let tracer = SdkTracerProvider::builder().build().tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

    with_default(subscriber, || {
        let span = tracing::info_span!("test-span");
        let _guard = span.enter();

        let propagator = new_propagator();
        let injector = TelemetryInjector::extract(&propagator);
        let (trace_parent, _trace_state) = injector.into_parts();
        assert!(trace_parent.is_some());
    });
}
