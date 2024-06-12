use std::mem::take;

use opentelemetry::propagation::Injector;
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::FutureRecord;

pub struct RecordInjector<'a, 'b, K, P>(&'a mut FutureRecord<'b, K, P>)
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized;

impl<'a, 'b, K, P> RecordInjector<'a, 'b, K, P>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    pub fn new(record: &'a mut FutureRecord<'b, K, P>) -> Self {
        Self(record)
    }
}

impl<'a, 'b, K, P> Injector for RecordInjector<'a, 'b, K, P>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    fn set(&mut self, key: &str, value: String) {
        let headers = self
            .0
            .headers
            .get_or_insert_with(|| OwnedHeaders::new_with_capacity(1));

        *headers = take(headers).insert(Header {
            key,
            value: Some(value.as_bytes()),
        });
    }
}
