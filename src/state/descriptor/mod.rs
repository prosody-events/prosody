//! Typed descriptors for keyed-state collections.
//!
//! Stores speak raw [`Bytes`]; this layer owns the typing. A
//! handler declares a descriptor once — usually as a `const` — registers it
//! with the consumer, and binds it to *any*
//! [`EventContext`](crate::consumer::event_context::EventContext) to obtain a
//! typed, owned handle:
//!
//! ```
//! use prosody::codec::JsonCodecError;
//! use prosody::consumer::DemandType;
//! use prosody::consumer::event_context::EventContext;
//! use prosody::consumer::message::ConsumerMessage;
//! use prosody::consumer::middleware::FallibleHandler;
//! use prosody::state::descriptor::{ValueDescriptor, ValueStateError, value_state};
//! use prosody::timers::Trigger;
//! use serde_json::Value;
//!
//! const CART: ValueDescriptor = value_state("cart");
//!
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl FallibleHandler for MyHandler {
//!     type Error = ValueStateError<JsonCodecError>;
//!     type Output = ();
//!     type Payload = Value;
//!
//!     async fn on_message<C>(
//!         &self,
//!         ctx: C,
//!         message: ConsumerMessage<Value>,
//!         _demand: DemandType,
//!     ) -> Result<(), Self::Error>
//!     where
//!         C: EventContext<Payload = Value>,
//!     {
//!         // Read-modify-write: each message appends to the cell
//!         // committed by the previous event on this key.
//!         let cart = ctx.state(CART)?;
//!         let mut items = match cart.get().await? {
//!             Some(Value::Array(items)) => items,
//!             _ => Vec::new(),
//!         };
//!         items.push(message.payload().clone());
//!         cart.set(Value::Array(items)).await?;
//!         Ok(())
//!     }
//!
//!     async fn on_timer<C>(
//!         &self,
//!         ctx: C,
//!         _trigger: Trigger,
//!         _demand: DemandType,
//!     ) -> Result<(), Self::Error>
//!     where
//!         C: EventContext<Payload = Value>,
//!     {
//!         // Timer handlers bind the same way; state persists across
//!         // event kinds for the key.
//!         let _cart = ctx.state(CART)?.get().await?;
//!         Ok(())
//!     }
//!
//!     async fn shutdown(self) {}
//! }
//! ```
//!
//! Two descriptor kinds ship today:
//!
//! * [`ValueDescriptor`] — a codec-backed single value. The codec **is** the
//!   typing: the cell type is `C::Payload`, and the default codec is
//!   [`JsonCodec`] (cells are [`serde_json::Value`]s, exactly like the default
//!   message payload). A typed cell means writing a `CartCodec: Codec<Payload =
//!   Cart>` — one codec, one layer of encoding.
//! * [`KafkaMessageDescriptor`] — codec-free; cells hold a [`KafkaMessageRef`]
//!   and `get()` resolves the full consumer message (decoded by the consumer's
//!   own codec) through the message loader.
//!
//! Every descriptor asserts a [`StructuralIdentity`] — the frozen
//! `(kind, cell kind, codec id, schema label)` tuple. The identity is
//! checked at registration (same name ⇒ same identity), at bind, and
//! against the durable per-segment identity table on first use, so a
//! process carrying an incompatible descriptor fails loudly instead of
//! silently misreading cells.

use crate::codec::{Codec, JsonCodec};
use crate::consumer::event_context::StateAccessError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::CollectionKindId;
use crate::state::session::StateSession;
use crate::state::{StateName, StoreOutcome};
use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

pub mod kafka;

pub use kafka::{
    KafkaMessageDescriptor, KafkaMessageHandle, KafkaMessageRef, KafkaStateError,
    kafka_message_state,
};

/// Cell-format discriminator persisted in a collection's structural
/// identity.
///
/// Values are frozen: new cell kinds get new discriminants, never
/// repurposed ones.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CellKind {
    /// Raw bytes produced by a user-facing [`Codec`].
    Codec = 1,

    /// A `MsgPack`-encoded [`KafkaMessageRef`].
    KafkaMessageRef = 2,
}

impl From<CellKind> for i16 {
    fn from(cell_kind: CellKind) -> Self {
        cell_kind as i16
    }
}

/// Opt-in user-supplied schema version label, part of the frozen identity.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SchemaLabel(Arc<str>);

impl SchemaLabel {
    /// Returns the label text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SchemaLabel {
    fn from(label: &str) -> Self {
        Self(Arc::from(label))
    }
}

/// The context-independent fields every descriptor carries: the collection
/// name and the opt-in schema label.
///
/// Embedded in each descriptor so the shared `name()`/`with_schema_label`
/// plumbing and the identity-label conversion live in one place rather than
/// being copy-pasted per descriptor kind.
#[derive(Clone, Copy, Debug)]
struct DescriptorMeta {
    name: &'static str,
    schema_label: Option<&'static str>,
}

impl DescriptorMeta {
    /// Metadata for a collection named `name` with no schema label.
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            schema_label: None,
        }
    }

    /// Attaches an opt-in schema version label.
    const fn with_schema_label(mut self, label: &'static str) -> Self {
        self.schema_label = Some(label);
        self
    }

    /// The collection name.
    const fn name(&self) -> &'static str {
        self.name
    }

    /// The schema label resolved into its frozen-identity form.
    fn schema_label(&self) -> Option<SchemaLabel> {
        self.schema_label.map(SchemaLabel::from)
    }
}

/// The frozen structural identity a descriptor asserts for its collection:
/// collection kind, cell format, codec token, and optional schema label.
///
/// Operational settings (TTL, commit mode) are deliberately *not* part of
/// the identity — they may change between deploys; the identity may not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralIdentity {
    /// Collection kind discriminator.
    pub kind: CollectionKindId,

    /// Cell format discriminator.
    pub cell_kind: CellKind,

    /// Codec token ([`Codec::CODEC_ID`]; `None` for framework-defined
    /// cells).
    pub codec_id: Option<&'static str>,

    /// Optional user-supplied schema version label.
    pub schema_label: Option<SchemaLabel>,
}

/// Context-independent descriptor metadata: the name and frozen identity
/// that get registered and durably validated.
///
/// Split from [`StateDescriptor`] so registration can consume a
/// descriptor without binding it to a context.
pub trait DescriptorIdentity {
    /// The collection name this descriptor binds to.
    fn name(&self) -> &'static str;

    /// The structural identity this descriptor asserts.
    fn structural_identity(&self) -> StructuralIdentity;
}

/// A typed view over one keyed-state collection, bindable to any
/// [`StateSession`].
///
/// Handlers reach this through
/// [`EventContext::state`](crate::consumer::event_context::EventContext::state),
/// which binds against the context's per-event session. Binding validates
/// registration + structural identity through the session's
/// [`verify_state_registration`] and returns an owned, `Clone` handle that
/// wraps the session's byte cells with the descriptor's typing.
///
/// [`verify_state_registration`]: StateSession::verify_state_registration
pub trait StateDescriptor: DescriptorIdentity + Copy {
    /// Typed handle returned by [`Self::bind`]; owns a clone of the
    /// binding session.
    type Handle<S: StateSession>;

    /// Validates registration + structural identity and returns the typed
    /// handle.
    ///
    /// Consumes the descriptor — descriptors are cheap `Copy` declarations,
    /// so `ctx.state(DESC)` reads naturally at the call site.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] when the session provides
    /// no keyed state, [`StateAccessError::Unregistered`] when the
    /// collection is unregistered, or
    /// [`StateAccessError::IdentityMismatch`] when it is registered with a
    /// different identity.
    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError>;
}

/// Descriptor for a codec-backed single value collection.
///
/// The codec carries the typing: the cell type **is** `C::Payload`. The
/// default [`JsonCodec`] stores [`serde_json::Value`] cells — the same
/// default as the consumer's message payload. Declare as a `const` via
/// [`value_state`]; for a typed cell, declare a codec
/// (`CartCodec: Codec<Payload = Cart>`) and write
/// `const CART: ValueDescriptor<CartCodec> = value_state("cart");`.
pub struct ValueDescriptor<C = JsonCodec> {
    meta: DescriptorMeta,
    _marker: PhantomData<fn() -> C>,
}

impl<C> Clone for ValueDescriptor<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C> Copy for ValueDescriptor<C> {}

impl<C> fmt::Debug for ValueDescriptor<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueDescriptor")
            .field("meta", &self.meta)
            .finish()
    }
}

/// Declares a codec-backed value collection named `name` (JSON by
/// default — annotate the `const` with `ValueDescriptor<MyCodec>` to pick
/// another codec).
///
/// `name` is not validated here (const contexts cannot fail); an empty
/// name fails loudly at registration, the fallible boundary.
#[must_use]
pub const fn value_state<C>(name: &'static str) -> ValueDescriptor<C> {
    ValueDescriptor {
        meta: DescriptorMeta::new(name),
        _marker: PhantomData,
    }
}

impl<C> ValueDescriptor<C> {
    /// Attaches an opt-in schema version label to the frozen identity.
    #[must_use]
    pub const fn with_schema_label(mut self, label: &'static str) -> Self {
        self.meta = self.meta.with_schema_label(label);
        self
    }
}

impl<C> DescriptorIdentity for ValueDescriptor<C>
where
    C: Codec,
{
    fn name(&self) -> &'static str {
        self.meta.name()
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            cell_kind: CellKind::Codec,
            codec_id: Some(C::CODEC_ID),
            schema_label: self.meta.schema_label(),
        }
    }
}

impl<C> StateDescriptor for ValueDescriptor<C>
where
    C: Codec,
{
    type Handle<S: StateSession> = TypedValueHandle<S, C>;

    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        let name =
            session.verify_state_registration(self.meta.name(), &self.structural_identity())?;
        Ok(TypedValueHandle {
            session: session.clone(),
            name,
            _marker: PhantomData,
        })
    }
}

/// Typed, owned handle over a codec-backed value collection.
///
/// Owns a clone of the binding session (`Clone + Send + Sync + 'static` —
/// an FFI requirement); the codec runs only at the edges (`get` decodes,
/// `set` encodes) over the session's byte cells. Every operation first
/// guards on session termination ([`StateAccessError::Terminated`]); stale
/// post-dispatch use additionally fails through the per-event transaction
/// state machine.
pub struct TypedValueHandle<S, C> {
    session: S,
    name: StateName,
    _marker: PhantomData<fn() -> C>,
}

impl<S: Clone, C> Clone for TypedValueHandle<S, C> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, C> TypedValueHandle<S, C>
where
    S: StateSession,
    C: Codec,
{
    /// Reads and decodes the current visible value.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, or a codec error
    /// (Permanent) when the cell bytes do not decode as `C::Payload`.
    pub async fn get(&self) -> Result<Option<C::Payload>, ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        let Some(cell) = self.session.state_cell(&self.name).await? else {
            return Ok(None);
        };
        // `Codec::deserialize` parses in place (destructive); cells live in
        // shared `Bytes`, so copy first.
        let mut buf = cell.to_vec();
        let payload = C::with_cached_local(|codec| codec.deserialize(&mut buf))
            .map_err(ValueStateError::Codec)?;
        Ok(Some(payload))
    }

    /// Encodes `value` and buffers a set operation.
    ///
    /// Takes the value by value — [`Codec::serialize`] consumes its
    /// payload.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when `value` fails to encode, or
    /// an access error from the session.
    pub async fn set(&self, value: C::Payload) -> Result<(), ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        let mut buf = Vec::new();
        C::with_cached_local(|codec| codec.serialize(value, &mut buf))
            .map_err(ValueStateError::Codec)?;
        Ok(self
            .session
            .set_state_cell(&self.name, Bytes::from(buf))
            .await?)
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn clear(&self) -> Result<(), ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        Ok(self.session.clear_state_cell(&self.name).await?)
    }

    /// Drains buffered ops directly to authoritative state and returns the
    /// transaction to `Clean`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn flush(&self) -> Result<StoreOutcome, ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        Ok(self.session.flush_state_cell(&self.name).await?)
    }
}

/// Guards every handle operation: a session whose partition is shutting
/// down or whose event is cancelled refuses state access with
/// [`StateAccessError::Terminated`]. Shared by the value and Kafka-message
/// handles.
pub(crate) fn ensure_live<S>(session: &S) -> Result<(), StateAccessError>
where
    S: StateSession,
{
    if session.is_terminated() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}

/// Error returned by [`TypedValueHandle`] operations.
#[derive(Debug, Error)]
pub enum ValueStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// The context refused or failed the state access.
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// The codec failed to encode or decode the cell.
    #[error("state codec failed")]
    Codec(#[source] E),
}

impl<E> ClassifyError for ValueStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Access(e) => e.classify_error(),
            // Unconditionally Permanent: `Codec` promises no
            // classification, and a cell that does not round-trip will not
            // start doing so on retry.
            Self::Codec(_) => ErrorCategory::Permanent,
        }
    }
}

#[cfg(test)]
mod tests;
