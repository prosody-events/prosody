use std::future::Future;

use super::{
    ConsumerMessage, DemandType, EventContext, EventHandler, Keyed, Resolution, RetryHandler,
    SettlementHandler, UncommittedMessage, UncommittedTimer, abandon, debug, log_message_failure,
    log_timer_failure, settle,
};
use crate::consumer::Uncommitted;

async fn process_record<T, C, Q, F, Fut>(
    handler: &RetryHandler<T>,
    context: C,
    message: UncommittedMessage<Q>,
    demand_type: DemandType,
    dispatch: F,
) where
    C: EventContext<Payload = T::Payload>,
    F: Fn(C, ConsumerMessage<Q>, DemandType) -> Fut,
    Fut: Future<Output = Result<T::Output, T::Error>>,
    T: SettlementHandler,
{
    let topic = message.topic();
    let partition = message.partition();
    let key = message.key().to_owned();
    let offset = message.offset();
    let (message, uncommitted) = message.into_inner();
    let (resolution, final_ctx) = handler
        .run(
            context,
            demand_type,
            None,
            |ctx, dt| dispatch(ctx, message.clone(), dt),
            |ctx, error| handler.handler.after_abort(ctx, Err(error)),
            |reason| {
                log_message_failure(
                    topic.as_ref(),
                    partition,
                    key.as_ref(),
                    offset,
                    &reason,
                    "; discarding message",
                );
            },
        )
        .await;
    finish(handler, final_ctx, uncommitted, resolution).await;
}

async fn finish<T, C, G>(
    handler: &RetryHandler<T>,
    final_ctx: C,
    uncommitted: G,
    resolution: Resolution<T::Output, T::Error>,
) where
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
    T: SettlementHandler,
{
    // Settle on the FINAL dispatch context (a fresh re-pinned Arc for a
    // retried event), not the original `context`: a leaked attempt-1 clone
    // could have invalidated the original's shared Arc, which would strand
    // the real attempt's dirty overlay at settle.
    match resolution {
        Resolution::Commit(result) => settle(handler, final_ctx, uncommitted, result).await,
        Resolution::Abort(error) => {
            // Terminal abort: nothing staged (the receipt never minted),
            // and abandon touches no state.
            abandon(handler, final_ctx, uncommitted, Err(error)).await;
        }
    }
}

impl<T> EventHandler for RetryHandler<T>
where
    T: SettlementHandler,
{
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        process_record(self, context, message, demand_type, |ctx, message, dt| {
            self.handler.on_message(ctx, message, dt)
        })
        .await;
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: UncommittedMessage<()>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        process_record(self, context, message, demand_type, |ctx, message, dt| {
            self.handler.on_excise(ctx, message, dt)
        })
        .await;
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext<Payload = T::Payload>,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted) = timer.into_inner();

        let (resolution, final_ctx) = self
            .run(
                context,
                demand_type,
                None,
                |ctx, dt| self.handler.on_timer(ctx, trigger.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
                |reason| log_timer_failure(&reason, "; discarding timer"),
            )
            .await;

        finish(self, final_ctx, uncommitted, resolution).await;
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");
        self.handler.shutdown().await;
    }
}
