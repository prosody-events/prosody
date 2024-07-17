use std::cmp::min;
use std::sync::Arc;
use std::time::Duration;

use humantime::format_duration;
use rand::rngs::SmallRng;
use rand::{thread_rng, Rng, SeedableRng};
use tokio::time::sleep;
use tracing::error;

use crate::consumer::failure::{FailureStrategy, FallibleHandler};
use crate::consumer::message::{MessageContext, UncommittedMessage};
use crate::consumer::{HandlerProvider, MessageHandler};

#[derive(Clone, Debug)]
pub struct RetryStrategy(RetryConfiguration);

#[derive(Clone, Debug)]
pub struct RetryConfiguration {
    base: u32,
    base_delay: Duration,
    max_delay: Duration,
}

#[derive(Clone, Debug)]
struct RetryHandler<T> {
    config: RetryConfiguration,
    handler: T,
}

impl<T> FailureStrategy<T> for RetryStrategy
where
    T: FallibleHandler + Clone + Send + Sync + 'static,
{
    fn with_handler(&self, handler: T) -> impl HandlerProvider
    where
        T: FallibleHandler,
    {
        RetryHandler {
            config: self.0.clone(),
            handler,
        }
    }
}

impl<T> MessageHandler for RetryHandler<T>
where
    T: FallibleHandler + Send + Sync,
{
    async fn handle(&self, context: MessageContext, message: UncommittedMessage) {
        let (message, uncommitted_offset) = message.into_inner();
        let context = Arc::new(context);
        let message = Arc::new(message);
        let mut rng = SmallRng::from_rng(thread_rng()).unwrap_or_else(|_| SmallRng::from_entropy());
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);

            match self.handler.handle(context.clone(), message.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    let exp_backoff = self.config.base.saturating_pow(attempt);
                    let jitter = Duration::from_millis(u64::from(rng.gen_range(0..exp_backoff)));
                    let sleep_time = min(jitter, self.config.max_delay);

                    error!(
                        %attempt, "failed to handle message: {error:#}; retrying after {}",
                        format_duration(sleep_time)
                    );

                    sleep(sleep_time).await;
                }
            }
        }

        uncommitted_offset.commit();
    }

    async fn shutdown(self) {}
}
