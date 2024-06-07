use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};

pub fn new_propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(BaggagePropagator::new()),
        Box::new(TraceContextPropagator::new()),
    ])
}
