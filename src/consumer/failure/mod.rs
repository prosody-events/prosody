use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::HandlerProvider;

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
        context: Arc<MessageContext>,
        message: Arc<ConsumerMessage>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
