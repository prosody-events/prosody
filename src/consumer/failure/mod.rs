use std::fmt::Display;
use std::future::Future;

use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::HandlerProvider;

pub mod log;
pub mod retry;

pub trait FailureStrategy<T> {
    fn with_handler(&self, handler: T) -> impl HandlerProvider
    where
        T: FallibleHandler;
}

pub trait FallibleHandler {
    type Error: Display + Send;

    fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
