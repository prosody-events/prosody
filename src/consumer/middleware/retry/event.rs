use super::{
    DemandType, EventContext, EventHandler, Keyed, Resolution, RetryHandler, SettlementHandler,
    UncommittedMessage, UncommittedTimer, abandon, debug, log_message_failure, log_timer_failure,
    settle,
};

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
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();

        let (resolution, final_ctx) = self
            .run(
                context,
                demand_type,
                None,
                |ctx, dt| self.handler.on_message(ctx, message.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
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

        // Settle on the FINAL dispatch context (a fresh re-pinned Arc for a
        // retried event), not the original `context`: a leaked attempt-1 clone
        // could have invalidated the original's shared Arc, which would strand
        // the real attempt's dirty overlay at settle.
        match resolution {
            Resolution::Commit(result) => {
                settle(self, final_ctx, uncommitted_offset, result).await;
            }
            Resolution::Abort(error) => {
                // Terminal abort: nothing staged (the receipt never minted),
                // and abandon touches no state.
                abandon(self, final_ctx, uncommitted_offset, Err(error)).await;
            }
        }
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: UncommittedMessage<()>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        let topic = message.topic();
        let partition = message.partition();
        let key = message.key().to_owned();
        let offset = message.offset();
        let (message, uncommitted_offset) = message.into_inner();
        let (resolution, final_ctx) = self
            .run(
                context,
                demand_type,
                None,
                |ctx, dt| self.handler.on_excise(ctx, message.clone(), dt),
                |ctx, error| self.handler.after_abort(ctx, Err(error)),
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

        match resolution {
            Resolution::Commit(result) => {
                settle(self, final_ctx, uncommitted_offset, result).await;
            }
            Resolution::Abort(error) => {
                abandon(self, final_ctx, uncommitted_offset, Err(error)).await;
            }
        }
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

        // Settle on the FINAL dispatch context — see the message arm above.
        match resolution {
            Resolution::Commit(result) => settle(self, final_ctx, uncommitted, result).await,
            Resolution::Abort(error) => {
                // Terminal abort: nothing staged (the receipt never minted),
                // and abandon touches no state.
                abandon(self, final_ctx, uncommitted, Err(error)).await;
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down retry handler");
        self.handler.shutdown().await;
    }
}
