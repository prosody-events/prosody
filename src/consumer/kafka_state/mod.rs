//! Kafka-message keyed state: cells that hold a durable pointer to a Kafka
//! message body, resolved back to the full message through the consumer's
//! [`MessageLoader`].
//!
//! This is the one place the otherwise Kafka-agnostic keyed-state machinery
//! meets Kafka. It supplies a single
//! [`CellType`](crate::state::descriptor::CellType), [`MessageCell`], composed
//! from two strategies that live here rather than in `src/state`:
//!
//! * [`MessageRefCodec`] — the codec: `bytes ↔ MessageRef`, `MsgPack`.
//! * [`MessageResolver`] — the resolver: `MessageRef → message`, loading
//!   through a [`MessageLoader`].
//!
//! Because [`MessageCell`] is an ordinary cell type, it works in **any**
//! collection kind: [`message_state`] declares a single-value collection,
//! [`message_map_state`] an ordered map of message refs, and
//! [`message_deque_state`] a deque of them. In every kind a handle's `get`
//! returns the full [`ConsumerMessage`] and `set` takes the message in hand.
//!
//! The loader type rides on [`MessageResolver`], so it is named in the cell
//! type (`MessageCell<L>`): a declaration states its true dependency — the
//! loader it will resolve through — rather than being loader-agnostic until
//! bind. Handlers pin it by annotating the descriptor or aliasing
//! `MessageCell<MyLoader>` once.

use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::loader::MessageLoader;
use crate::state::descriptor::{
    CellResolver, CellStateError, DequeDescriptor, MapDescriptor, ValueDescriptor, WithResolver,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::{Offset, Partition, Topic};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod codec;

pub use self::codec::{MessageRefCodec, MessageRefCodecError};

/// Durable pointer to a Kafka message body.
///
/// Persisted as the `MsgPack`-encoded cell of a Kafka-message collection and
/// resolved back to the full message via a [`MessageLoader`]. Derived from the
/// [`ConsumerMessage`] in hand at write time — the only production source of a
/// ref.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRef {
    /// Kafka topic.
    #[serde(with = "codec::topic_serde")]
    pub topic: Topic,

    /// Kafka partition.
    pub partition: Partition,

    /// Kafka offset within the partition.
    pub offset: Offset,
}

impl<P> From<&ConsumerMessage<P>> for MessageRef {
    fn from(message: &ConsumerMessage<P>) -> Self {
        Self {
            topic: message.topic(),
            partition: message.partition(),
            offset: message.offset(),
        }
    }
}

/// Resolution strategy that loads the full message a [`MessageRef`] points at
/// through a [`MessageLoader`] `L`, and lowers a message in hand back to its
/// ref.
///
/// The loader rides on the resolver's [`CellResolver::Context`] (`&'s L`), so
/// the resolver is a zero-sized, session-free strategy: the framework borrows
/// the loader from the session and hands it to [`CellResolver::resolve`].
pub struct MessageResolver<L>(PhantomData<fn() -> L>);

// `L: 'static` — a resolver only ever borrows a session's loader, and
// `StateSession::Loader` is always `'static`; the bound lets the `&'s L`
// context GAT hold for any `'s`.
impl<L: MessageLoader + 'static> CellResolver for MessageResolver<L> {
    type Context<'s> = &'s L;
    type Resolved = ConsumerMessage<L::Payload>;
    type Stored = MessageRef;
    type Write<'a> = &'a ConsumerMessage<L::Payload>;

    /// Frozen into the durable structural identity; never change it once cells
    /// exist. Shares the spelling of [`MessageRefCodec`]'s codec id by
    /// coincidence — the two tokens are independent identity columns.
    const RESOLVER_ID: Option<&'static str> = Some("message-ref");

    // Desugared `-> impl Future + Send` rather than `async fn`: the returned
    // future borrows the loader through the `Context<'s>` GAT, and rustc
    // #100013 fails `Send` inference for `async fn` futures that hold GAT
    // projections. Resolution never waits for capacity because its caller may
    // retain permits from earlier resolved messages.
    fn resolve(
        loader: Self::Context<'_>,
        stored: MessageRef,
    ) -> impl Future<Output = Result<Self::Resolved, StateAccessError>> + Send {
        let MessageRef {
            topic,
            partition,
            offset,
        } = stored;
        async move {
            loader
                .try_load_message(topic, partition, offset)
                .await
                .map_err(|error| StateAccessError::load(&error))
        }
    }

    fn stored_from(write: Self::Write<'_>) -> MessageRef {
        MessageRef::from(write)
    }
}

/// The Kafka-message [`CellType`](crate::state::descriptor::CellType):
/// [`MessageRefCodec`] paired with
/// [`MessageResolver`] over loader `L`. Usable in any collection kind — see the
/// module docs and the `message_*_state` sugar.
pub type MessageCell<L> = WithResolver<MessageRefCodec, MessageResolver<L>>;

/// Descriptor for a single-value collection whose cell references a Kafka
/// message body; declare via [`message_state`].
pub type MessageDescriptor<L> = ValueDescriptor<MessageCell<L>>;

/// Error returned by Kafka-message handle operations.
pub type MessageStateError = CellStateError<MessageRefCodecError>;

/// Declares a single-value Kafka-message collection named `name`.
///
/// `name` may be any runtime string and is interned (see
/// [`value_state`](crate::state::descriptor::value_state)); it is not
/// validated here — an empty name fails loudly at registration, the
/// fallible boundary.
#[must_use]
pub fn message_state<L: MessageLoader>(name: &str) -> MessageDescriptor<L> {
    MessageDescriptor::new(name)
}

/// Declares an ordered-map Kafka-message collection named `name`, keyed by
/// `KC` with [`MessageCell`] values (see [`message_state`] for the `name`
/// contract).
#[must_use]
pub fn message_map_state<KC, L>(name: &str) -> MapDescriptor<KC, MessageCell<L>>
where
    KC: OrderedKeyCodec,
    L: MessageLoader,
{
    MapDescriptor::new(name)
}

/// Declares a deque Kafka-message collection named `name` with [`MessageCell`]
/// entries (see [`message_state`] for the `name` contract).
#[must_use]
pub fn message_deque_state<L: MessageLoader>(name: &str) -> DequeDescriptor<MessageCell<L>> {
    DequeDescriptor::new(name)
}

#[cfg(test)]
mod tests;
