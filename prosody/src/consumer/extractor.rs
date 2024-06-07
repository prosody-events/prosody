use std::str;

use opentelemetry::propagation::Extractor;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::Message;

pub struct MessageExtractor<'a>(&'a BorrowedMessage<'a>);

impl<'a> MessageExtractor<'a> {
    pub fn new(message: &'a BorrowedMessage<'a>) -> Self {
        Self(message)
    }
}

impl<'a> Extractor for MessageExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .headers()?
            .iter()
            .find_map(|header| (header.key == key).then_some(header.key))
    }

    fn keys(&self) -> Vec<&str> {
        let Some(headers) = self.0.headers() else {
            return vec![];
        };

        headers.iter().map(|header| header.key).collect()
    }
}
