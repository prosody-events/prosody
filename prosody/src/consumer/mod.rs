use std::error::Error;
use std::future::Future;

use crate::consumer::message::ConsumerMessage;

pub mod message;
mod offsets;

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
