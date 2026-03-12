//! Zero-allocation trace context extractor for telemetry events.
//!
//! Extracts W3C `traceparent` and `tracestate` headers from the current span
//! context using the OpenTelemetry propagator. The extracted values are stored
//! as owned `Box<str>` fields for inclusion in telemetry event payloads.

use opentelemetry::propagation::{Injector, TextMapCompositePropagator, TextMapPropagator};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Extracts W3C trace context from the current OpenTelemetry span.
///
/// Implements the [`Injector`] trait so the propagator can write `traceparent`
/// and `tracestate` values into the struct's fields.
#[derive(Clone, Debug, Default)]
pub(crate) struct TelemetryInjector {
    trace_parent: Option<Box<str>>,
    trace_state: Option<Box<str>>,
}

impl TelemetryInjector {
    /// Creates a new empty injector.
    fn new() -> Self {
        Self::default()
    }

    /// Extracts trace context from the current span using the given propagator.
    ///
    /// Captures `Span::current()` context and injects `traceparent` /
    /// `tracestate` into this struct via the propagator.
    pub(crate) fn extract(propagator: &TextMapCompositePropagator) -> Self {
        let mut injector = Self::new();
        let context = Span::current().context();
        propagator.inject_context(&context, &mut injector);
        injector
    }

    /// Consumes self and returns owned `trace_parent` and `trace_state`.
    pub(crate) fn into_parts(self) -> (Option<Box<str>>, Option<Box<str>>) {
        (self.trace_parent, self.trace_state)
    }
}

impl Injector for TelemetryInjector {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.trace_parent = Some(value.into_boxed_str()),
            "tracestate" => self.trace_state = Some(value.into_boxed_str()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
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
}
