use std::error::Error;
use std::future::Future;

use crate::consumer::message::ConsumerMessage;

pub mod message;
mod offsets;
mod partition;

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
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

#[derive(Debug)]
pub struct MessageContext;
