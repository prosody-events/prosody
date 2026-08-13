//! Trace context across a peer call, carried in gRPC metadata.
//!
//! One pair, mirroring the Kafka pair the message path already uses: the client
//! injects the context of the span it calls from, and the service extracts it
//! and opens its own span as a child. Both go through the crate's composite
//! propagator, so a peer hop carries W3C `traceparent`, `tracestate` and
//! `baggage` and invents no header of its own.

use opentelemetry::propagation::{Extractor, Injector};
use tonic::metadata::{Ascii, KeyRef, MetadataKey, MetadataMap, MetadataValue};
use tracing::debug;

/// Writes propagation headers into an outbound call's metadata.
pub(super) struct MetadataInjector<'a>(&'a mut MetadataMap);

/// Reads propagation headers from an inbound call's metadata.
pub(super) struct MetadataExtractor<'a>(&'a MetadataMap);

impl<'a> MetadataInjector<'a> {
    /// Injects into `metadata`.
    pub(super) const fn new(metadata: &'a mut MetadataMap) -> Self {
        Self(metadata)
    }
}

impl Injector for MetadataInjector<'_> {
    /// A propagator's key and value are always printable ASCII, so neither
    /// conversion fails for a well-formed context. A broken trace must never
    /// fail a delivery, so a rejected pair is logged and left out.
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = MetadataKey::<Ascii>::from_bytes(key.as_bytes()) else {
            debug!(key, "a propagation key is not valid gRPC metadata");
            return;
        };
        let Ok(value) = MetadataValue::try_from(value) else {
            debug!(key, "a propagation value is not valid gRPC metadata");
            return;
        };
        drop(self.0.insert(name, value));
    }
}

impl<'a> MetadataExtractor<'a> {
    /// Extracts from `metadata`.
    pub(super) const fn new(metadata: &'a MetadataMap) -> Self {
        Self(metadata)
    }
}

impl Extractor for MetadataExtractor<'_> {
    /// A value that is not printable ASCII carries no context this build can
    /// read. Dropping it silently would lose a peer's trace with no sign of
    /// why, so the rejection is logged, mirroring [`MetadataInjector::set`].
    fn get(&self, key: &str) -> Option<&str> {
        let Ok(value) = self.0.get(key)?.to_str() else {
            debug!(key, "a propagation value is not readable gRPC metadata");
            return None;
        };
        Some(value)
    }

    /// Binary metadata carries no propagation, so only the ASCII keys are
    /// offered.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(name) => Some(name.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}
