//! Provides functionality to inject OpenTelemetry context into Kafka record
//! headers.
//!
//! This module implements the `Injector` trait from OpenTelemetry, allowing
//! trace context to be injected into Kafka record headers. This is essential
//! for maintaining distributed tracing context across services that communicate
//! via Kafka.
use std::mem::take;

use opentelemetry::propagation::Injector;
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureRecord;

/// Wraps a mutable reference to a `FutureRecord` to implement the `Injector`
/// trait.
///
/// This struct allows OpenTelemetry context to be injected into Kafka record
/// headers.
pub struct RecordInjector<'a, 'b, K, P>(&'a mut FutureRecord<'b, K, P>)
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized;

impl<'a, 'b, K, P> RecordInjector<'a, 'b, K, P>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    /// Creates a new `RecordInjector` from a mutable reference to a
    /// `FutureRecord`.
    ///
    /// # Arguments
    ///
    /// * `record` - A mutable reference to the `FutureRecord` to inject headers
    ///   into.
    ///
    /// # Returns
    ///
    /// A new `RecordInjector` instance.
    pub fn new(record: &'a mut FutureRecord<'b, K, P>) -> Self {
        Self(record)
    }
}

impl<'a, 'b, K, P> Injector for RecordInjector<'a, 'b, K, P>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    /// Injects a key-value pair into the Kafka record headers.
    ///
    /// This method is called by OpenTelemetry to inject context information
    /// into the Kafka record.
    ///
    /// # Arguments
    ///
    /// * `key` - The header key to inject.
    /// * `value` - The header value to inject.
    fn set(&mut self, key: &str, value: String) {
        // Get or create the headers for the record
        let headers = self
            .0
            .headers
            .get_or_insert_with(|| OwnedHeaders::new_with_capacity(1));

        // Insert the new header, replacing the existing headers
        *headers = take(headers).insert(Header {
            key,
            value: Some(value.as_bytes()),
        });
    }
}
