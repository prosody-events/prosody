//! Typed descriptors for keyed-state collections.
//!
//! Stores speak raw [`Bytes`]; this layer owns the typing. A handler
//! declares a descriptor once — usually as a `const` — registers it with
//! the consumer, and binds it to the handler context to obtain a typed
//! handle:
//!
//! ```
//! use prosody::state::descriptor::{ValueDescriptor, value_state};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Cart {
//!     items: Vec<String>,
//! }
//!
//! const CART: ValueDescriptor<Cart> = value_state("cart");
//! // In a handler: let cart = ctx.state(CART)?;
//! //               cart.set(Cart { items: vec![] }).await?;
//! ```
//!
//! Two descriptor kinds ship today:
//!
//! * [`ValueDescriptor`] — a codec-backed single value (JSON by default via
//!   [`JsonStateCodec`]); cells hold the codec's bytes verbatim.
//! * [`KafkaMessageDescriptor`](kafka::KafkaMessageDescriptor) — cells hold a
//!   [`KafkaMessageRef`] and `get()` resolves the full consumer message through
//!   the defer message loader.
//!
//! Every descriptor asserts a [`StructuralIdentity`] — the frozen
//! `(kind, cell kind, codec id, schema label)` tuple. The identity is
//! checked at registration (same name ⇒ same identity), at bind, and
//! against the durable per-segment identity table on first use, so a
//! process carrying an incompatible descriptor fails loudly instead of
//! silently misreading cells.

use crate::codec::{CodecId, JsonStateCodec, StateCodec};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::CollectionKindId;
use crate::state::middleware::{
    ByteValueHandle, CollectionDefRegistry, DirtyValueBundle, DurableValueBundle, KeyedStateContext,
};
use crate::state::value::{DurableWalStore, TransactionValueStoreError, ValueKind, ValueStore};
use crate::state::{StateName, StoreOutcome};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

pub mod kafka;

pub use kafka::{
    KafkaMessageDescriptor, KafkaMessageRef, KafkaValueHandle, NoLoader, kafka_message_state,
};

type DirtyErr<S> = <S as ValueStore>::Error;
type DurableErr<D> = <D as DurableWalStore<ValueKind>>::Error;

/// Cell-format discriminator persisted in a collection's structural
/// identity.
///
/// Values are frozen: new cell kinds get new discriminants, never
/// repurposed ones.
#[repr(i16)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CellKind {
    /// Raw bytes produced by a user-facing [`StateCodec`].
    Codec = 1,

    /// A `MsgPack`-encoded [`KafkaMessageRef`].
    KafkaMessageRef = 2,
}

impl CellKind {
    /// Wire discriminator persisted beside durable identity.
    ///
    /// Paired with [`Self::from_i16`]; the two are inverses by construction.
    #[must_use]
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    /// Recovers a cell kind from its wire discriminator, or `None` for an
    /// unknown value. Inverse of [`Self::as_i16`].
    #[must_use]
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            1 => Some(Self::Codec),
            2 => Some(Self::KafkaMessageRef),
            _ => None,
        }
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

/// The frozen structural identity a descriptor asserts for its collection:
/// collection kind, cell format, codec, and optional schema label.
///
/// Operational settings (TTL, commit mode) are deliberately *not* part of
/// the identity — they may change between deploys; the identity may not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralIdentity {
    /// Collection kind discriminator.
    pub kind: CollectionKindId,

    /// Cell format discriminator.
    pub cell_kind: CellKind,

    /// Codec discriminator ([`CodecId::None`] for framework-defined cells).
    pub codec_id: CodecId,

    /// Optional user-supplied schema version label.
    pub schema_label: Option<SchemaLabel>,
}

/// Context-independent descriptor metadata: the name and frozen identity
/// that get registered and durably validated.
///
/// Split from [`StateDescriptor`] (which is generic over the binding
/// context) so registration can consume a descriptor without naming a
/// context type.
pub trait DescriptorIdentity {
    /// The collection name this descriptor binds to.
    fn name(&self) -> &'static str;

    /// The structural identity this descriptor asserts.
    fn structural_identity(&self) -> StructuralIdentity;
}

/// A typed view over one keyed-state collection, bindable to a handler
/// context.
///
/// `Ctx` is the binding context (the middleware's wrapped handler context);
/// the handle type depends on the stores and loader the context carries,
/// which is why the trait is parameterized by it.
pub trait StateDescriptor<Ctx>: DescriptorIdentity {
    /// Typed handle returned by [`Self::bind`].
    type Handle;

    /// Error returned when the descriptor cannot bind.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Validates registration + structural identity and returns the typed
    /// handle for the current event scope.
    ///
    /// Consumes the descriptor — descriptors are cheap `Copy` declarations,
    /// so `ctx.state(DESC)` reads naturally at the call site.
    ///
    /// # Errors
    ///
    /// Returns an error when the collection is unregistered or registered
    /// with a different identity.
    fn bind(self, ctx: &Ctx) -> Result<Self::Handle, Self::Error>;
}

/// Descriptor for a codec-backed single value collection.
///
/// `T` is the user's cell type; `C` the [`StateCodec`] that maps it to
/// cell bytes (JSON by default). Declare as a `const` via [`value_state`].
pub struct ValueDescriptor<T, C = JsonStateCodec> {
    name: &'static str,
    schema_label: Option<&'static str>,
    _marker: PhantomData<fn() -> (T, C)>,
}

impl<T, C> Clone for ValueDescriptor<T, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, C> Copy for ValueDescriptor<T, C> {}

impl<T, C> fmt::Debug for ValueDescriptor<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueDescriptor")
            .field("name", &self.name)
            .field("schema_label", &self.schema_label)
            .finish()
    }
}

/// Declares a JSON-coded value collection named `name`.
///
/// `name` is not validated here (const contexts cannot fail); an empty
/// name fails loudly at registration, the fallible boundary.
#[must_use]
pub const fn value_state<T>(name: &'static str) -> ValueDescriptor<T> {
    ValueDescriptor {
        name,
        schema_label: None,
        _marker: PhantomData,
    }
}

impl<T, C> ValueDescriptor<T, C> {
    /// Attaches an opt-in schema version label to the frozen identity.
    #[must_use]
    pub const fn with_schema_label(mut self, label: &'static str) -> Self {
        self.schema_label = Some(label);
        self
    }
}

impl<T, C> DescriptorIdentity for ValueDescriptor<T, C>
where
    C: StateCodec,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            cell_kind: CellKind::Codec,
            codec_id: C::CODEC_ID,
            schema_label: self.schema_label.map(SchemaLabel::from),
        }
    }
}

/// Typed handle over a codec-backed value collection.
///
/// Wraps the shared byte-transaction substrate; the codec runs only at the
/// edges (`get` decodes, `set` encodes).
pub struct TypedValueHandle<T, C, D, S> {
    inner: ByteValueHandle<D, S>,
    _marker: PhantomData<fn() -> (T, C)>,
}

impl<T, C, D, S> Clone for TypedValueHandle<T, C, D, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, C, D, S> TypedValueHandle<T, C, D, S>
where
    T: Serialize + DeserializeOwned,
    C: StateCodec,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    pub(crate) fn new(inner: ByteValueHandle<D, S>) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Reads and decodes the current visible value.
    ///
    /// # Errors
    ///
    /// Returns a transaction error or a codec error when the cell bytes do
    /// not decode as `T`.
    pub async fn get(
        &self,
    ) -> Result<Option<T>, TypedValueError<DirtyErr<S>, DurableErr<D>, C::Error>> {
        match self.inner.get().await? {
            Some(cell) => Ok(Some(C::decode(&cell).map_err(TypedValueError::Codec)?)),
            None => Ok(None),
        }
    }

    /// Encodes `value` and buffers a set operation.
    ///
    /// # Errors
    ///
    /// Returns a codec error when `value` fails to encode, or a transaction
    /// error from the underlying store.
    pub async fn set(
        &self,
        value: T,
    ) -> Result<(), TypedValueError<DirtyErr<S>, DurableErr<D>, C::Error>> {
        let cell = C::encode(&value).map_err(TypedValueError::Codec)?;
        Ok(self.inner.set(cell).await?)
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns a transaction error from the underlying store.
    pub async fn clear(&self) -> Result<(), TypedValueError<DirtyErr<S>, DurableErr<D>, C::Error>> {
        Ok(self.inner.clear().await?)
    }

    /// Drains buffered ops directly to authoritative state and returns the
    /// transaction to `Clean`.
    ///
    /// # Errors
    ///
    /// Returns a transaction error from the underlying store.
    pub async fn flush(
        &self,
    ) -> Result<StoreOutcome, TypedValueError<DirtyErr<S>, DurableErr<D>, C::Error>> {
        Ok(self.inner.flush().await?)
    }
}

/// Error returned by [`TypedValueHandle`] operations.
#[derive(Debug, Error)]
pub enum TypedValueError<DirtyE, DurableE, CodecE>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    CodecE: ClassifyError + Error + Send + Sync + 'static,
{
    /// The underlying value transaction failed.
    #[error(transparent)]
    Tx(#[from] TransactionValueStoreError<DirtyE, DurableE>),

    /// The state codec failed to encode or decode the cell.
    #[error("state codec failed")]
    Codec(#[source] CodecE),
}

impl<DirtyE, DurableE, CodecE> ClassifyError for TypedValueError<DirtyE, DurableE, CodecE>
where
    DirtyE: ClassifyError + Error + Send + Sync + 'static,
    DurableE: ClassifyError + Error + Send + Sync + 'static,
    CodecE: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Tx(e) => e.classify_error(),
            Self::Codec(e) => e.classify_error(),
        }
    }
}

/// Error returned when a descriptor cannot bind to a context.
#[derive(Debug, Error)]
pub enum BindError {
    /// The collection name was never registered with the consumer.
    #[error("state collection {name:?} is not registered")]
    Unregistered {
        /// The descriptor's collection name.
        name: &'static str,
    },

    /// The registry holds a different identity for this name.
    #[error(
        "state collection {name:?} identity mismatch: registered {registered:?}, descriptor \
         asserts {requested:?}"
    )]
    IdentityMismatch {
        /// The descriptor's collection name.
        name: &'static str,

        /// Identity held by the registry.
        registered: StructuralIdentity,

        /// Identity the binding descriptor asserts.
        requested: StructuralIdentity,
    },
}

impl ClassifyError for BindError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// Binds a value descriptor against any event scope and any loader: the
/// codec needs neither Kafka coordinates nor a message loader.
impl<C, D, S, L, Scope, T, Cdc> StateDescriptor<KeyedStateContext<C, D, S, L, Scope>>
    for ValueDescriptor<T, Cdc>
where
    T: Serialize + DeserializeOwned,
    Cdc: StateCodec,
    D: DurableValueBundle,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    type Error = BindError;
    type Handle = TypedValueHandle<T, Cdc, D, S>;

    fn bind(self, ctx: &KeyedStateContext<C, D, S, L, Scope>) -> Result<Self::Handle, BindError> {
        let name = require_registered(ctx.registry(), &self)?;
        Ok(TypedValueHandle::new(ctx.byte_handle(&name)))
    }
}

/// Validates `descriptor` against the registered collections and returns
/// the canonical [`StateName`] on success. Shared by every `bind` impl.
pub(crate) fn require_registered<D>(
    registry: &CollectionDefRegistry,
    descriptor: &D,
) -> Result<StateName, BindError>
where
    D: DescriptorIdentity,
{
    let Some((name, registered)) = registry.lookup(descriptor.name()) else {
        return Err(BindError::Unregistered {
            name: descriptor.name(),
        });
    };
    let requested = descriptor.structural_identity();
    if registered.identity != requested {
        return Err(BindError::IdentityMismatch {
            name: descriptor.name(),
            registered: registered.identity.clone(),
            requested,
        });
    }
    Ok(name.clone())
}

#[cfg(test)]
mod tests;
