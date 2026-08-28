//! The contract an application implements to receive events, and the types
//! handed to it.

use crate::consumer::event_context::EventContext;
use crate::consumer::message::UncommittedMessage;
use crate::timers::UncommittedTimer;
use crate::{Partition, Topic};
use serde::Serialize;
use std::future::Future;

/// Represents the type of demand being processed.
///
/// Demand types allow the system to distinguish between normal processing
/// and failure handling scenarios, enabling different processing behaviors
/// for the same event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DemandType {
    /// Normal demand represents the initial processing attempt of an event.
    Normal,

    /// Failure demand represents retry processing after a previous failure.
    /// This is typically created by retry middleware when an event fails
    /// and needs to be reprocessed.
    Failure,
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

/// Selects how the settle boundary protects state after it records a receipt.
///
/// A message receipt is a deduplication row. A timer receipt deletes its
/// key-index row while its slab row remains as the redelivery source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Redelivery {
    /// Promote state before retirement. A committed redelivery sweeps state.
    Sweeps,
    /// Arm a safety timer before commit. A redelivery runs the handler again.
    Reruns,
}

/// Splits an event commit into a receipt and redelivery-source retirement.
///
/// The receipt makes the commit oracle report that the event committed.
/// The boundary records a message deduplication row before it calls
/// `receipt()`. The message guard does nothing in `receipt()`. A timer guard
/// deletes its key-index row.
///
/// [`Redelivery::Sweeps`] lets the boundary promote state before retirement.
/// A committed redelivery then sweeps the key and retires its source.
/// [`Redelivery::Reruns`] arms a safety timer before the combined commit.
/// This posture lets defer refires reload work and rescheduled timers run
/// again.
pub trait Receipted: Uncommitted + sealed::Sealed {
    /// Returns the redelivery posture for this event.
    fn redelivery(&self) -> impl Future<Output = Redelivery> + Send;

    /// Records the receipt. Retries every failure until the write succeeds.
    fn receipt(&mut self) -> impl Future<Output = ()> + Send;
}

pub(crate) mod sealed {
    pub trait Sealed {}
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
