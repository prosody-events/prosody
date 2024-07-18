use std::fmt::Display;
use std::future::Future;

use crate::consumer::message::{ConsumerMessage, MessageContext};
use crate::consumer::HandlerProvider;

pub mod log;
pub mod retry;
pub mod topic;

pub trait FailureStrategy {
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler
    where
        T: FallibleHandler;

    fn and_then<T>(self, next_handler: T) -> ComposedStrategy<Self, T>
    where
        Self: Sized,
    {
        ComposedStrategy(self, next_handler)
    }
}

pub trait FallibleHandler: Clone + Send + Sync + 'static {
    type Error: Display + Send;

    fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Clone, Debug)]
pub struct ComposedStrategy<S1, S2>(S1, S2);

impl<S1, S2> FailureStrategy for ComposedStrategy<S1, S2>
where
    S1: FailureStrategy,
    S2: FailureStrategy,
{
    fn with_handler<T>(&self, handler: T) -> impl HandlerProvider + FallibleHandler + Clone
    where
        T: FallibleHandler,
    {
        self.1.with_handler(self.0.with_handler(handler))
    }
}
