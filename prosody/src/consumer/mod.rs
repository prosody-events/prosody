use std::error::Error;
use std::future::Future;

use crate::consumer::message::ConsumerMessage;

mod context;
pub mod message;
mod partition;
mod poll;

pub trait Keyed {
    type Key;

    fn key(&self) -> &Self::Key;
}

pub trait MessageHandler {
    type Error: Error;

    fn handle(
        &self,
        context: &MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug)]
pub struct MessageContext;

pub struct KafkaConsumer {}
