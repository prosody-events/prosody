//! The cart scenario: the collections both tests register, the handler that
//! drives them, and the observations it streams back for verification.
//!
//! [`CartHandler`] stays generic over `C: EventContext` and names no concrete
//! context type. It reaches `last_seen` and `receipt` through the erased
//! `message_value_state(name)` vend method rather than a typed
//! `MessageDescriptor<L>` handle: that handle's resolver names a concrete
//! loader `L`, so it needs `C::State` pinned to that exact `L`, which `C:
//! EventContext` alone cannot supply.

use color_eyre::eyre::{Result, ensure, eyre};
use prosody::JsonCodec;
use prosody::Offset;
use prosody::codec::JsonCodecError;
use prosody::consumer::event_context::{ErasedStateError, EventContext, StateAccessError};
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{DemandType, MessageDescriptor, message_state};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::loader::KafkaLoader;
use prosody::state::descriptor::{CellStateError, Registered, ValueDescriptor, value_state};
use prosody::timers::datetime::{CompactDateTime, CompactDateTimeError};
use prosody::timers::duration::CompactDuration;
use prosody::timers::{TimerType, Trigger};
use serde_json::{Value, json};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use tracing::error;

/// Number of messages the tests produce. The handler schedules the read-back
/// timer once the cart holds this many items.
const MESSAGE_COUNT: usize = 2;

/// `last_seen`'s registered name. The handler reaches this collection through
/// the erased vend method by name, so it needs the name as well as the
/// descriptor.
pub(crate) const LAST_SEEN: &str = "last_seen";

/// A second Kafka-message collection, recorded alongside `last_seen`. The
/// publication test registers it published so a standalone reader can resolve
/// it through `ReaderLoader::Kafka`, which re-fetches a message-ref cell's body
/// from Kafka. `last_seen` stays private, so it cannot demonstrate that path.
pub(crate) const RECEIPT: &str = "receipt";

/// Hang-guard for a single observation. Never the assertion — content decides.
const OBSERVATION_GUARD: Duration = Duration::from_mins(1);

/// The value collection the handler accumulates cart items into.
pub(crate) fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// The Kafka-message collection recording the last message seen.
pub(crate) fn last_seen() -> MessageDescriptor<KafkaLoader<JsonCodec>> {
    message_state(LAST_SEEN)
}

/// What the handler saw, streamed to the test for content assertions.
#[derive(Debug)]
pub(crate) enum Observation {
    /// `on_message`: the cart value after this message's read-modify-write.
    Message { cart: Value },

    /// `on_timer`: the accumulated cart plus the re-fetched last-seen message.
    Timer {
        cart: Option<Value>,
        last_seen: Option<(Offset, Value)>,
    },
}

/// Accumulates each message's `"item"` field into the `cart` cell, records the
/// message in `last_seen` and `receipt`, and schedules an `Application` timer
/// once the cart is full. The timer reads both cells back.
#[derive(Clone)]
pub(crate) struct CartHandler {
    pub(crate) observations_tx: Sender<Observation>,

    /// The registration handle for the `cart` value collection — the handler
    /// can bind only collections it was handed a token for.
    pub(crate) cart: Registered<ValueDescriptor>,
}

impl CartHandler {
    async fn handle_message<C>(
        &self,
        ctx: C,
        message: ConsumerMessage<Value>,
    ) -> Result<(), CartHandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        // Read-modify-write on the value cell: each message appends its
        // item to the array committed by the previous event.
        let cart = ctx.state(self.cart)?;
        let mut items = match cart.get().await? {
            Some(Value::Array(items)) => items,
            Some(other) => return Err(CartHandlerError::UnexpectedCell(other)),
            None => Vec::new(),
        };
        items.push(
            message
                .payload()
                .get("item")
                .cloned()
                .unwrap_or(Value::Null),
        );
        let full = items.len() == MESSAGE_COUNT;
        let updated = Value::Array(items);
        cart.set(updated.clone()).await?;

        ctx.clone()
            .boxed()
            .message_value_state(LAST_SEEN)?
            .set(message.clone())
            .await?;

        // The same message into the receipt collection, which the publication
        // test publishes so a reader in another consumer group can resolve it.
        ctx.clone()
            .boxed()
            .message_value_state(RECEIPT)?
            .set(message)
            .await?;

        // The final message completes the cart; schedule the timer that
        // reads the accumulated state back. Per-key serialization
        // guarantees the fire dispatches only after this event commits.
        if full {
            let fire =
                CompactDateTime::now().and_then(|now| now.add_duration(CompactDuration::new(2)))?;
            ctx.schedule(fire, TimerType::Application)
                .await
                .map_err(|e| CartHandlerError::Schedule(e.to_string()))?;
        }

        self.observations_tx
            .send(Observation::Message { cart: updated })
            .await
            .map_err(|_| CartHandlerError::ChannelClosed)?;
        Ok(())
    }

    async fn handle_timer<C>(&self, ctx: C) -> Result<(), CartHandlerError>
    where
        C: EventContext<Payload = Value>,
    {
        let cart = ctx.state(self.cart)?.get().await?;
        // Re-fetches the original message body from Kafka through the
        // consumer's loader, decoded by the consumer's own codec.
        let last_seen = ctx
            .clone()
            .boxed()
            .message_value_state(LAST_SEEN)?
            .get()
            .await?
            .map(|message| (message.offset(), message.payload().clone()));

        self.observations_tx
            .send(Observation::Timer { cart, last_seen })
            .await
            .map_err(|_| CartHandlerError::ChannelClosed)?;
        Ok(())
    }
}

impl FallibleHandler for CartHandler {
    type Error = CartHandlerError;
    type Output = ();
    type Payload = Value;

    async fn on_message<C>(
        &self,
        ctx: C,
        message: ConsumerMessage<Value>,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let result = self.handle_message(ctx, message).await;
        if let Err(error) = &result {
            // Surface the full error chain in the test log; the pipeline's
            // own logging shows only the outer middleware display.
            error!(?error, "cart handler failed on message");
        }
        result
    }

    async fn on_timer<C>(
        &self,
        ctx: C,
        _trigger: Trigger,
        _demand: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let result = self.handle_timer(ctx).await;
        if let Err(error) = &result {
            error!(?error, "cart handler failed on timer");
        }
        result
    }

    async fn shutdown(self) {}
}

/// The content assertions, in the deterministic per-key order: message 1,
/// message 2, then the timer the second message scheduled.
pub(crate) async fn verify_observations(
    rx: &mut Receiver<Observation>,
    second: &Value,
) -> Result<()> {
    for (label, expected) in [
        ("first", json!(["apple"])),
        ("second", json!(["apple", "banana"])),
    ] {
        match next_observation(rx).await? {
            Observation::Message { cart } => ensure!(
                cart == expected,
                "{label} message cart: expected {expected}, got {cart}"
            ),
            other @ Observation::Timer { .. } => {
                return Err(eyre!("expected {label} message observation, got {other:?}"));
            }
        }
    }

    match next_observation(rx).await? {
        Observation::Timer { cart, last_seen } => {
            ensure!(
                cart == Some(json!(["apple", "banana"])),
                "timer must observe the accumulated cart, got {cart:?}"
            );
            let (offset, payload) =
                last_seen.ok_or_else(|| eyre!("timer observed no last-seen message"))?;
            ensure!(
                offset == 1,
                "last-seen must reference the second message's offset, got {offset}"
            );
            ensure!(
                payload == *second,
                "last-seen must re-fetch the second message's payload, got {payload}"
            );
            Ok(())
        }
        other @ Observation::Message { .. } => {
            Err(eyre!("expected timer observation, got {other:?}"))
        }
    }
}

async fn next_observation(rx: &mut Receiver<Observation>) -> Result<Observation> {
    timeout(OBSERVATION_GUARD, rx.recv())
        .await
        .map_err(|_| eyre!("timed out waiting for an observation"))?
        .ok_or_else(|| eyre!("observation channel closed"))
}

/// Errors the handler can surface. Everything classifies Permanent so a failure
/// fails the test fast instead of retrying into a timeout.
#[derive(Debug, Error)]
pub(crate) enum CartHandlerError {
    #[error(transparent)]
    Access(#[from] StateAccessError),

    #[error(transparent)]
    Value(#[from] CellStateError<JsonCodecError>),

    #[error(transparent)]
    Kafka(#[from] ErasedStateError),

    #[error("unexpected cart cell: {0}")]
    UnexpectedCell(Value),

    #[error(transparent)]
    FireTime(#[from] CompactDateTimeError),

    #[error("failed to schedule the read-back timer: {0}")]
    Schedule(String),

    #[error("observation channel closed")]
    ChannelClosed,
}

impl ClassifyError for CartHandlerError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
