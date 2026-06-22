//! Typed descriptors for keyed-state collections.
//!
//! A descriptor names a typed keyed-state collection — a plain `Copy` value
//! (names are interned). Build it with [`value_state`], registering it with
//! the consumer to mint a [`Registered`] capability handle. A handler binds
//! that handle via
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state)
//! to get a typed handle whose `get`/`set` read and write the cell committed
//! by the previous event on the key. `state` takes the handle, never a raw
//! descriptor, so a handler can reach only collections it registered.
//!
//! # Codec and resolver
//!
//! A [`ValueDescriptor`] is generic over two orthogonal strategies:
//!
//! * a [`Codec`] (`bytes ↔ Stored`, synchronous) — the codec **is** the typing
//!   of the stored cell. The default is [`JsonCodec`] (cells are
//!   [`serde_json::Value`]s, exactly like the default message payload). A typed
//!   cell means writing a `CartCodec: Codec<Payload = Cart>` — one codec, one
//!   layer of encoding.
//! * a [`CellResolver`] (`Stored → value`, asynchronous) — maps the decoded
//!   cell into what `get()` returns and what `set()` takes. The default is
//!   [`Passthrough`]: the resolved value *is* the stored value.
//!
//! Both are Kafka-agnostic. The consumer layer composes a non-trivial codec +
//! resolver pair to model a *reference* cell — bytes that decode to a durable
//! pointer which the resolver then loads into a full value — but `src/state`
//! never names that machinery; it only sees the two generic strategies.
//!
//! Every descriptor asserts a [`StructuralIdentity`] — the frozen
//! `(kind, codec id, resolver id)` tuple. Both the codec and the resolver are
//! part of the durable contract: the codec types the stored cell, and the
//! resolver maps it to and from the exposed value, so swapping either silently
//! would change what a cell means. The identity is checked at registration
//! (same `(state_type, name)` ⇒ same identity), at bind, and against the
//! group-global durable identity table on first use, so a process carrying an
//! incompatible descriptor fails loudly instead of silently misreading cells.

use crate::codec::{Codec, JsonCodec, SerializeBufGuard};
use crate::consumer::event_context::StateAccessError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::registry::CollectionDef;
use crate::state::session::{CellAccess, StateSession};
use crate::state::value::ValueKind;
use crate::state::{CollectionKindId, CommitMode, StateName, StateType, StoreOutcome};
use crate::timers::duration::CompactDuration;
use internment::Intern;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use thiserror::Error;

/// A resolver's durable token, the resolver half of a collection's
/// [`StructuralIdentity`].
///
/// Session-agnostic by design — the token is a property of the resolver
/// *strategy*, not of any session it runs against, so it is a plain associated
/// const rather than a method on [`CellResolver`]. `None` is the passthrough
/// resolver (the stored value *is* the exposed value); a `Some(token)` names a
/// non-trivial mapping (for example the consumer's Kafka-message reference
/// resolver). The token is frozen into the durable identity, so changing it
/// once cells exist is an incompatible identity change.
pub trait ResolverId {
    /// The resolver's durable token, or `None` for passthrough.
    const RESOLVER_ID: Option<&'static str>;
}

/// The frozen structural identity a descriptor asserts for its collection:
/// collection kind, codec token, and resolver token.
///
/// Operational settings (TTL, commit mode) are deliberately *not* part of
/// the identity — they may change between deploys; the identity may not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralIdentity {
    /// Collection kind discriminator.
    pub kind: CollectionKindId,

    /// Codec token ([`Codec::CODEC_ID`]). Always present — every cell is
    /// codec-produced.
    pub codec_id: &'static str,

    /// Resolver token ([`ResolverId::RESOLVER_ID`]); `None` for passthrough.
    pub resolver_id: Option<&'static str>,

    /// Key-codec token for keyed kinds (Map); `None` for kinds without a key
    /// codec (Value). Frozen into the durable identity the same way `codec_id`
    /// is, so a keyed collection's key encoding can never silently change.
    pub key_codec_id: Option<&'static str>,
}

/// Context-independent descriptor metadata: the name and frozen identity
/// that get registered and durably validated.
///
/// Split from [`StateDescriptor`] so registration can consume a
/// descriptor without binding it to a context.
pub trait DescriptorIdentity {
    /// The collection name this descriptor binds to.
    fn name(&self) -> &'static str;

    /// The state namespace this descriptor's collection lives in. Defaults to
    /// [`StateType::Application`]; the name is unique only *within* a
    /// `state_type`, so a framework collection can share a name with an
    /// application one without colliding.
    fn state_type(&self) -> StateType {
        StateType::Application
    }

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
    /// Consumes the descriptor — descriptors are cheap `Copy` declarations.
    /// Handlers never call this directly; they pass the [`Registered`] handle
    /// to `ctx.state(...)`, which unwraps and binds it.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] when the session provides
    /// no keyed state, [`StateAccessError::Unregistered`] when the
    /// collection is unregistered, or
    /// [`StateAccessError::IdentityMismatch`] when it is registered with a
    /// different identity.
    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError>;

    /// The operational settings (TTL, commit mode) this descriptor carries
    /// into registration, set via its fluent methods (see
    /// [`ValueDescriptor::ttl`]).
    ///
    /// Defaults to [`CollectionDef::new`] with `None` (indefinite retention,
    /// read-committed) so framework-internal descriptors need not carry one.
    fn collection_def(&self) -> CollectionDef {
        CollectionDef::new(None)
    }
}

/// Proof that a descriptor was registered with a consumer: the capability
/// handle [`EventContext::state`] requires.
///
/// # Invariant: unforgeability
///
/// A live `Registered<D>` implies `D` was registered: the field is private
/// and the only mint is the `pub(crate)` `new`, called solely from the
/// registration mechanism (`KeyedStateConfiguration::register` and the
/// high-level `client.register`). Downstream crates can neither construct nor
/// unwrap it, so "use a descriptor you never registered" cannot be expressed.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct Registered<D>(D);

impl<D> Registered<D>
where
    D: StateDescriptor,
{
    /// Mints the capability handle for a registered descriptor. The sole
    /// constructor, and crate-private, so a live `Registered<D>` always
    /// witnesses a registration.
    pub(crate) fn new(descriptor: D) -> Self {
        Self(descriptor)
    }

    /// Recovers the wrapped descriptor — an infallible move, not an unwrap.
    pub(crate) fn descriptor(self) -> D {
        self.0
    }
}

/// Strategy that maps a decoded cell value into the value a handle exposes,
/// and back.
///
/// The resolver is a zero-sized *strategy*, never an instance: every method
/// is static (no `&self`). Any runtime dependency it needs (for example a
/// message loader) is read from the [`StateSession`] handed to
/// [`Self::resolve`] — see the consumer layer's reference resolver. A
/// descriptor carries no resolver *state*; the resolver's durable *token*
/// ([`ResolverId`]) is a separate, session-agnostic const that rides the
/// [`StructuralIdentity`].
pub trait CellResolver<S: StateSession> {
    /// The decoded cell type — pinned to the codec's payload by the handle.
    type Stored;

    /// What [`StateHandle::get`] returns.
    type Resolved;

    /// What [`StateHandle::set`] takes. A GAT so a borrowing resolver (e.g.
    /// "store a reference to the message in hand") can take `&'a T` while a
    /// passthrough takes an owned value.
    type Write<'a>;

    /// Resolves a decoded cell into the exposed value, optionally using the
    /// session (for example its loader).
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError`] when resolution fails (for example a
    /// loader miss).
    fn resolve(
        session: &S,
        stored: Self::Stored,
    ) -> impl Future<Output = Result<Self::Resolved, StateAccessError>> + Send;

    /// Lowers a written value into the cell value the codec serializes.
    fn stored_from(write: Self::Write<'_>) -> Self::Stored;
}

/// Identity [`CellResolver`]: the resolved value *is* the stored value.
///
/// A zero-sized strategy; [`value_state`] pairs it with the codec so a plain
/// value collection round-trips `C::Payload` with no extra layer.
pub struct Passthrough<T>(PhantomData<fn() -> T>);

impl<T> ResolverId for Passthrough<T> {
    /// Passthrough adds no mapping, so it carries no resolver token.
    const RESOLVER_ID: Option<&'static str> = None;
}

impl<S, T> CellResolver<S> for Passthrough<T>
where
    S: StateSession,
    T: Send + 'static,
{
    type Resolved = T;
    type Stored = T;
    type Write<'a> = T;

    async fn resolve(_session: &S, stored: T) -> Result<T, StateAccessError> {
        Ok(stored)
    }

    fn stored_from(write: T) -> T {
        write
    }
}

/// Descriptor for a codec-backed single value collection.
///
/// Generic over a [`Codec`] (the cell typing) and a [`CellResolver`] (how a
/// decoded cell becomes the exposed value). The default [`JsonCodec`] stores
/// [`serde_json::Value`] cells — the same default as the consumer's message
/// payload — and the default [`Passthrough`] resolver exposes the stored
/// value directly. Declare via [`value_state`] — the name may be any
/// runtime string (it is interned, so descriptors stay `Copy`); for a
/// typed cell, declare a codec (`CartCodec: Codec<Payload = Cart>`) and
/// annotate the binding `ValueDescriptor<CartCodec>`.
pub struct ValueDescriptor<C = JsonCodec, R = Passthrough<<C as Codec>::Payload>> {
    name: &'static str,
    def: CollectionDef,
    _marker: PhantomData<fn() -> (C, R)>,
}

impl<C, R> Clone for ValueDescriptor<C, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C, R> Copy for ValueDescriptor<C, R> {}

impl<C, R> fmt::Debug for ValueDescriptor<C, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueDescriptor")
            .field("name", &self.name)
            .field("def", &self.def)
            .finish()
    }
}

/// Declares a codec-backed value collection named `name` (JSON by
/// default — annotate the binding with `ValueDescriptor<MyCodec>` to pick
/// another codec).
///
/// The resolver defaults to [`Passthrough`]: the value is stored and
/// returned verbatim.
///
/// `name` may be any runtime string — FFI clients register collections at
/// client startup from host-language names — and is interned, so the
/// descriptor stays `Copy`. It is not validated here; an empty name fails
/// loudly at registration, the fallible boundary.
#[must_use]
pub fn value_state<C>(name: &str) -> ValueDescriptor<C, Passthrough<C::Payload>>
where
    C: Codec,
{
    ValueDescriptor::new(name)
}

impl<C, R> ValueDescriptor<C, R> {
    /// Declares a value collection named `name` over an explicit codec and
    /// resolver pair.
    ///
    /// Bound-free by design: construction needs no `Codec`/`CellResolver`
    /// bounds (those surface only on [`StateHandle::get`]/`set`), so a
    /// consumer-layer alias can build a non-[`Passthrough`] descriptor — e.g.
    /// a reference cell whose resolver loads through a message loader.
    /// [`value_state`] is the convenience constructor for the common
    /// [`Passthrough`] case.
    ///
    /// `name` may be any runtime string and is interned (see
    /// [`value_state`]); it is not validated here — an empty name fails
    /// loudly at registration, the fallible boundary.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: intern_descriptor_str(name),
            def: CollectionDef::new(None),
            _marker: PhantomData,
        }
    }

    /// Sets the collection's TTL (the per-write Cassandra `USING TTL`),
    /// validated against the ceiling and the recovery delay at registration.
    #[must_use]
    pub fn ttl(mut self, ttl: CompactDuration) -> Self {
        self.def.ttl = Some(ttl);
        self
    }

    /// Clears the collection's TTL, selecting indefinite retention (the
    /// default).
    #[must_use]
    pub fn no_ttl(mut self) -> Self {
        self.def.ttl = None;
        self
    }

    /// Selects [`CommitMode::ReadCommitted`] (the default): writes stage
    /// provisionally and promote after the event commit.
    #[must_use]
    pub fn read_committed(mut self) -> Self {
        self.def.commit_mode = CommitMode::ReadCommitted;
        self
    }

    /// Selects [`CommitMode::ReadUncommitted`]: writes apply to committed
    /// state on handler success, with at-least-once semantics.
    #[must_use]
    pub fn read_uncommitted(mut self) -> Self {
        self.def.commit_mode = CommitMode::ReadUncommitted;
        self
    }
}

impl<C, R> DescriptorIdentity for ValueDescriptor<C, R>
where
    C: Codec,
    R: ResolverId,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            codec_id: C::CODEC_ID,
            resolver_id: R::RESOLVER_ID,
            // Value is single-cell: no key codec. Map emits `Some(KC::KEY_CODEC_ID)`.
            key_codec_id: None,
        }
    }
}

impl<C, R> StateDescriptor for ValueDescriptor<C, R>
where
    C: Codec,
    R: ResolverId,
{
    type Handle<S: StateSession> = StateHandle<S, C, R>;

    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        // The resolver token rides the identity (via `R: ResolverId`), but the
        // operational `R: CellResolver<S>` requirement surfaces on `get`/`set`,
        // not here, so this compiles through the shared `StateDescriptor` trait.
        // The handle carries the binding descriptor's `state_type` so its cell
        // ops address the right namespace.
        let name = session.verify_state_registration(
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )?;
        Ok(StateHandle::new(session.clone(), self.state_type(), name))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }
}

/// Typed, owned handle over a codec-backed value collection.
///
/// Owns a clone of the binding session (`Clone + Send + Sync + 'static` —
/// an FFI requirement); the codec runs only at the edges (`get` decodes,
/// `set` encodes) over the session's byte cells, and the resolver maps the
/// decoded cell to/from the exposed value. Every operation first guards on
/// session termination ([`StateAccessError::Terminated`]); stale
/// post-dispatch use additionally fails through the per-event transaction
/// state machine.
pub struct StateHandle<S, C, R> {
    session: S,
    state_type: StateType,
    name: StateName,
    _marker: PhantomData<fn() -> (C, R)>,
}

impl<S, C, R> StateHandle<S, C, R> {
    /// Wraps a verified session, the binding descriptor's `state_type`, and the
    /// canonical name. Resolver-agnostic so [`StateDescriptor::bind`] can mint
    /// it without a [`CellResolver`] bound.
    fn new(session: S, state_type: StateType, name: StateName) -> Self {
        Self {
            session,
            state_type,
            name,
            _marker: PhantomData,
        }
    }
}

impl<S: Clone, C, R> Clone for StateHandle<S, C, R> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            state_type: self.state_type,
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, C, R> StateHandle<S, C, R>
where
    S: CellAccess<ValueKind>,
    C: Codec,
    R: CellResolver<S, Stored = C::Payload>,
{
    /// Reads, decodes, and resolves the current visible value.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent)
    /// when the cell bytes do not decode as `C::Payload`, or a resolution
    /// error from the resolver.
    pub async fn get(&self) -> Result<Option<R::Resolved>, ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        // Value is the single-cell kind, so the address is the unit `()`.
        let Some(cell) = self
            .session
            .read_cell(self.state_type, &self.name, &())
            .await?
        else {
            return Ok(None);
        };
        // `Codec::deserialize` parses in place (destructive). In production the
        // cell `Bytes` is uniquely owned (every backend decode mints a fresh
        // `Bytes`), so reclaim it as a mutable buffer with zero copy and parse
        // in place. Only the shared-clone case (the in-memory test backend)
        // copies, exactly as before — no worse than the status quo.
        let stored = match cell.try_into_mut() {
            Ok(mut buf) => C::with_cached_local(|codec| codec.deserialize(&mut buf)),
            Err(cell) => {
                let mut buf = cell.to_vec();
                C::with_cached_local(|codec| codec.deserialize(&mut buf))
            }
        }
        .map_err(ValueStateError::Codec)?;
        Ok(Some(R::resolve(&self.session, stored).await?))
    }

    /// Lowers `value` through the resolver, encodes it, and buffers a set.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the cell fails to encode, or
    /// an access error from the session.
    pub async fn set(&self, value: R::Write<'_>) -> Result<(), ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        let stored = R::stored_from(value);
        // Serialize into a pooled, reusable buffer. The guard owns its buffer
        // (moved out of thread-local storage), so it is `Send` and rides the
        // cell write across the await; on drop it returns the buffer to the
        // pool for the next `set`.
        let mut buf = SerializeBufGuard::acquire();
        C::with_cached_local(|codec| codec.serialize(stored, &mut buf))
            .map_err(ValueStateError::Codec)?;
        Ok(self
            .session
            .set_cell(self.state_type, &self.name, &(), &buf)
            .await?)
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn clear(&self) -> Result<(), ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        Ok(self
            .session
            .clear_cell(self.state_type, &self.name, &())
            .await?)
    }

    /// Drains buffered ops directly to authoritative state and returns the
    /// transaction to `Clean` — a mid-handler write-through, valid in
    /// **either** commit mode.
    ///
    /// The contract is **at-least-once**: a flushed write is durable
    /// immediately, *not* atomically with the event's commit marker. A
    /// handler that fails after flushing re-runs against the
    /// already-applied state on retry or redelivery, so flushed writes
    /// must be idempotent (or the handler must tolerate re-execution).
    /// Ops buffered *after* the flush still ride the collection's normal
    /// commit path. Reads already see buffered writes without flushing —
    /// flush is for making them durable early, not for read-your-writes.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub async fn flush(&self) -> Result<StoreOutcome, ValueStateError<C::Error>> {
        ensure_live(&self.session)?;
        Ok(self
            .session
            .flush_cell(self.state_type, &self.name, &())
            .await?)
    }
}

/// Interns a descriptor string, returning the pool's canonical
/// `&'static str` (the [`Topic`](crate::Topic) idiom). Descriptor names
/// are a bounded set fixed at consumer build, so pool entries living for
/// the process is the intended retention; interning is what keeps
/// descriptors `Copy`.
fn intern_descriptor_str(s: &str) -> &'static str {
    Intern::<str>::from(s).as_ref()
}

/// Guards every handle operation: a session whose partition is shutting
/// down or whose event is cancelled refuses state access with
/// [`StateAccessError::Terminated`].
fn ensure_live<S>(session: &S) -> Result<(), StateAccessError>
where
    S: StateSession,
{
    if session.is_terminated() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}

/// Error returned by [`StateHandle`] operations.
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
pub(crate) mod tests;
