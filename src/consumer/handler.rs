//! The contract an application implements to receive events, and the types
//! handed to it.

use crate::consumer::event_context::EventContext;
use crate::consumer::message::UncommittedMessage;
use crate::timers::UncommittedTimer;
use crate::{Partition, Topic};
#[cfg(test)]
use quickcheck::{Arbitrary, Gen};
use serde::{Serialize, Serializer};
use std::future::Future;

/// The demand a dispatch serves: the first attempt at an event, or a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemandType {
    /// The first attempt at an event.
    Normal,
    /// An attempt after one or more failures.
    Failure {
        /// The estimated retry ordinal: 1 on the first retry.
        ///
        /// Each retry or defer middleware adds its retries to the outer
        /// demand's count. The ordinal restarts when an event moves
        /// from retry middleware to defer middleware, so it can fall.
        /// It is monotone only within one layer.
        /// A message queued behind a deferred head reports 1 on its first
        /// handler call. Keep an exact count in keyed state if
        /// necessary.
        retry: u32,
    },
}

impl DemandType {
    /// The retry ordinal: 0 for [`Normal`](Self::Normal).
    #[must_use]
    pub fn retry(self) -> u32 {
        match self {
            Self::Normal => 0,
            Self::Failure { retry } => retry,
        }
    }

    /// The demand after additional failures; zero failures leave this demand
    /// unchanged.
    #[must_use]
    pub(crate) fn retried(self, retries: u32) -> Self {
        match retries {
            0 => self,
            _ => Self::Failure {
                retry: self.retry().saturating_add(retries),
            },
        }
    }
}

#[cfg(test)]
impl Arbitrary for DemandType {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 10 {
            0 => Self::Normal,
            9 => Self::Failure { retry: u32::MAX },
            retry => Self::Failure {
                retry: u32::from(retry),
            },
        }
    }
}

impl Serialize for DemandType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Self::Normal => "normal",
            Self::Failure { .. } => "failure",
        })
    }
}

/// This trait is implemented by message types that have a key field,
/// allowing key-based message routing and processing.
pub trait Keyed {
    /// The type of the key.
    type Key;

    /// Retrieves the key of the item.
    fn key(&self) -> &Self::Key;
}

/// Provides transaction-like semantics for event processing acknowledgment.
///
/// The [`Uncommitted`] trait enables reliable event processing by requiring
/// explicit acknowledgment after processing. Events that implement this trait
/// must be either committed (successfully processed) or aborted (failed
/// processing) to ensure proper resource cleanup and delivery guarantees.
///
/// ## Transaction Semantics
///
/// The trait provides a simple two-phase commit protocol:
/// 1. **Processing**: Application processes the delivered event
/// 2. **Acknowledgment**: Application calls [`Uncommitted::commit()`] or
///    [`Uncommitted::abort()`]
///
/// ## Reliability Guarantees
///
/// - **At-least-once delivery**: Events are delivered at least once until
///   committed
/// - **Resource cleanup**: Proper acknowledgment ensures resources are cleaned
///   up
/// - **Fault tolerance**: Uncommitted events survive application crashes
/// - **Graceful shutdown**: Uncommitted events are handled during shutdown
pub trait Uncommitted {
    /// Acknowledges successful processing of the event.
    ///
    /// This method should be called when the event has been successfully
    /// processed and should be permanently removed from the system. Committing
    /// an event typically triggers cleanup operations and prevents redelivery.
    fn commit(self) -> impl Future<Output = ()> + Send;

    /// Acknowledges failed processing of the event.
    ///
    /// This method should be called when event processing is shutting down and
    /// cannot continue. Abort should only be called when the partition is being
    /// revoked.
    fn abort(self) -> impl Future<Output = ()> + Send;
}

/// Provides handlers for processing messages from specific partitions.
///
/// This trait allows creating custom message handlers for each partition,
/// enabling partition-specific processing logic if needed.
pub trait HandlerProvider: Send + Sync + 'static {
    /// The type of message handler provided.
    type Handler: EventHandler + Send + Sync + 'static;

    /// Creates a handler for a specific topic and partition.
    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler;
}

/// Defines the behavior for handling consumed Kafka messages.
///
/// This is the primary trait to implement for message processing logic.
/// It provides methods for processing messages and handling shutdown.
pub trait EventHandler {
    /// The payload type carried by messages delivered to this handler.
    type Payload: Send + Sync + 'static;

    /// Processes a consumed message.
    ///
    /// This method should contain the business logic for message processing.
    /// It should commit or abort the message when processing is complete.
    fn on_message<C>(
        &self,
        context: C,
        message: UncommittedMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>;

    /// Processes an excise record.
    fn on_excise<C>(
        &self,
        context: C,
        message: UncommittedMessage<()>,
        demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>;

    /// Handles timer events when they fire.
    ///
    /// This method is called when a scheduled timer reaches its execution time
    /// and is delivered to the application for processing. The timer must be
    /// explicitly committed or aborted after processing to ensure proper
    /// resource cleanup. The returned future completing does not itself
    /// commit the timer.
    ///
    /// # Processing Requirements
    ///
    /// Implementations must ensure that the timer is properly acknowledged:
    /// - Call `timer.commit()` after successful processing
    /// - Call `timer.abort()` if processing fails or should be retried
    fn on_timer<C, T>(
        &self,
        context: C,
        timer: T,
        demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer;

    /// Shuts down the message handler.
    ///
    /// This method is called when the consumer is shutting down.
    /// It should clean up any resources used by the handler.
    fn shutdown(self) -> impl Future<Output = ()> + Send;
}
