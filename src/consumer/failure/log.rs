use tracing::error;

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};

#[derive(Copy, Clone, Debug)]
pub struct LogStrategy;

#[derive(Clone, Debug)]
struct LogHandler<T>(T);

impl<T> FailureStrategy<T> for LogStrategy
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    fn with_handler(&self, handler: T) -> impl HandlerProvider
    where
        T: FallibleHandler,
    {
        LogHandler(handler)
    }
}

impl<T> MessageHandler for LogHandler<T>
where
    T: FallibleHandler + Send + Sync,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();
        if let Err(error) = self.0.handle(context, message).await {
            error!("failed to handle message: {error:#}");
        }
        uncommitted_offset.commit();
    }

    async fn shutdown(self) {}
}
