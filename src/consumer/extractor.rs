//! Provides functionality for extracting metadata from Kafka messages.
//! This module includes a custom extractor that implements the `Extractor`
//! trait from the `opentelemetry` crate, facilitating the extraction of tracing
//! headers from Kafka message headers for distributed tracing purposes.

use std::str;

use opentelemetry::propagation::Extractor;
use rdkafka::Message;
use rdkafka::message::{BorrowedMessage, Headers};

/// A wrapper around a Kafka `BorrowedMessage` that implements the `Extractor`
/// trait, enabling the extraction of trace context information from the message
/// headers.
pub struct MessageExtractor<'a>(&'a BorrowedMessage<'a>);

impl<'a> MessageExtractor<'a> {
    /// Creates a new `MessageExtractor` for the specified Kafka message.
    ///
    /// # Arguments
    /// * `message` - A reference to the borrowed Kafka message from which
    ///   headers are extracted.
    ///
    /// # Returns
    /// A new instance of `MessageExtractor`.
    pub fn new(message: &'a BorrowedMessage<'a>) -> Self {
        Self(message)
    }
}

impl Extractor for MessageExtractor<'_> {
    /// Retrieves the value of a header associated with the given key.
    ///
    /// # Arguments
    /// * `key` - The key of the header to retrieve.
    ///
    /// # Returns
    /// An option containing the header value if found, otherwise `None`.
    fn get(&self, key: &str) -> Option<&str> {
        let value = self
            .0
            .headers()?
            .iter()
            .find(|header| header.key == key)?
            .value?;

        str::from_utf8(value).ok()
    }

    /// Returns a collection of all header keys present in the Kafka message.
    ///
    /// # Returns
    /// A vector of header keys if headers are present; otherwise, an empty
    /// vector.
    fn keys(&self) -> Vec<&str> {
        let Some(headers) = self.0.headers() else {
            return vec![];
        };

        headers.iter().map(|header| header.key).collect()
    }
}
