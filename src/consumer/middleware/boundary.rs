use std::convert::Infallible;
use std::io::Error as IoError;

use super::{ClassifyError, ErrorCategory, FallibleHandler, SettlementHandler, settle};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::UncommittedMessage;
use crate::consumer::{DemandType, EventHandler};
use crate::timers::UncommittedTimer;

/// Runs a fallible handler through the shared settlement boundary.
/// Each invocation produces one result and one apply hook.
/// `Final` stages cells, promotes them, records the marker, and commits the
/// source. `Rejected` discards state, records the marker best effort, and
/// commits the source. `Bypassed` discards state and commits the source without
/// a marker. `Abandoned` discards state and aborts the source without a marker.
/// [`RetryHandler`](crate::consumer::middleware::retry::RetryHandler) uses the
/// same boundary.
pub trait FallibleEventHandler: FallibleHandler {
    /// Called when message processing fails.
    fn on_message_error(&self, _error: &Self::Error) {}

    /// Called when timer processing fails.
    fn on_timer_error(&self, _error: &Self::Error) {}
}

impl<T> EventHandler for T
where
    T: FallibleEventHandler + SettlementHandler,
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
        // Invoke the inner FallibleHandler EXACTLY ONCE, then hand its single
        // result to the shared durability sequence. `settle` fires EXACTLY
        // ONE apply hook, so the per-invocation invariant holds.
        let (inner_message, uncommitted_offset) = message.into_inner();
        let result =
            FallibleHandler::on_message(self, context.clone(), inner_message, demand_type).await;
        if let Err(error) = &result {
            self.on_message_error(error);
        }
        settle(self, context, uncommitted_offset, result).await;
    }

    async fn on_excise<C>(
        &self,
        context: C,
        message: UncommittedMessage<()>,
        demand_type: DemandType,
    ) where
        C: EventContext<Payload = T::Payload>,
    {
        let (message, uncommitted_offset) = message.into_inner();
        let result = FallibleHandler::on_excise(self, context.clone(), message, demand_type).await;
        if let Err(error) = &result {
            self.on_message_error(error);
        }
        settle(self, context, uncommitted_offset, result).await;
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext<Payload = T::Payload>,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();
        let result = FallibleHandler::on_timer(self, context.clone(), trigger, demand_type).await;
        if let Err(error) = &result {
            self.on_timer_error(error);
        }
        settle(self, context, uncommitted_timer, result).await;
    }

    async fn shutdown(self) {
        FallibleHandler::shutdown(self).await;
    }
}

impl ClassifyError for Infallible {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

impl ClassifyError for IoError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Transient
    }
}
