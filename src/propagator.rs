//! Provides a composite OpenTelemetry propagator for distributed tracing.
//!
//! This module creates a `TextMapCompositePropagator` that combines
//! `BaggagePropagator` and `TraceContextPropagator`. This composite propagator
//! enables the transmission of both baggage (key-value pairs) and trace context
//! across service boundaries in distributed systems.

use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};

/// Creates a new composite OpenTelemetry propagator.
///
/// This function constructs a `TextMapCompositePropagator` that includes both
/// `BaggagePropagator` and `TraceContextPropagator`. The resulting propagator
/// can be used to inject and extract both baggage and trace context in
/// distributed tracing scenarios.
///
/// # Returns
///
/// A `TextMapCompositePropagator` configured with baggage and trace context
/// propagators.
#[must_use]
pub fn new_propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        // Include BaggagePropagator for propagating key-value pairs
        Box::new(BaggagePropagator::new()),
        // Include TraceContextPropagator for propagating trace context
        Box::new(TraceContextPropagator::new()),
    ])
}
