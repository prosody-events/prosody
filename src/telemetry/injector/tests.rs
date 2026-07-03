use opentelemetry::propagation::Injector;

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
