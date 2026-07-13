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
//! # Composed cell types
//!
//! A cell's typing is a [`CellType`] — the complete typed contract of a cell,
//! composed on three axes:
//!
//! - **Address:** an [`OrderedKeyCodec`] (`logical key ↔ order-preserving
//!   bytes`) — [`UnitKey`] for a single-cell collection, a real key codec for a
//!   keyed one.
//! - **Payload:** a [`Codec`] (`bytes ↔ stored`, synchronous — the codec **is**
//!   the stored typing).
//! - **Resolution:** a [`CellResolver`] (`stored ↔ exposed`, asynchronous).
//!
//! Every plain [`Codec`] is *already* a complete cell type: the blanket impls
//! address it with [`UnitKey`] and make it its own passthrough resolver, so a
//! value collection over `CartCodec: Codec<Payload = Cart>` needs no other
//! layer. To model a *reference* cell — bytes that decode to a durable pointer
//! a resolver then loads into a full value — pair a codec with a resolver via
//! [`WithResolver`]; the consumer layer's Kafka message cell is exactly that
//! pairing. To address a family of cells by a key, lift a single-cell type
//! through [`Keyed`]. `src/state` never speaks a cell's bytes — key or
//! value — directly; only its codecs do.
//!
//! A [`CellResolver`] is **session-free**: it declares the capability it needs
//! as [`CellResolver::Context`] and the framework extracts that context from
//! the session via [`FromSession`]. Passing the whole session to a resolver was
//! the complection that once forced its durable token onto a separate trait;
//! with the context split out, [`CellResolver::RESOLVER_ID`] sits on the one
//! resolver trait as a plain const, symmetric with [`Codec::FORMAT_ID`].
//!
//! Every descriptor asserts a [`StructuralIdentity`] — the frozen
//! `(kind, codec id, resolver id, key codec id)` tuple. Codec, resolver, and
//! key codec are all part of the durable contract: the codec types the stored
//! cell, the resolver maps it to and from the exposed value, and the key codec
//! orders keyed kinds, so swapping any silently would change what a cell means.
//! The identity is checked at registration (same `(state_type, name)` ⇒ same
//! identity), at bind, and against the group-global durable identity table on
//! first use, so a process carrying an incompatible descriptor fails loudly
//! instead of silently misreading cells.
//!
//! # Exposure
//!
//! Users define codecs, resolvers, and cell types (all public). Defining
//! collection *kinds* is deliberately unexposed for now: `CellView` is
//! crate-internal, and while [`CellScope`] and [`CollectionSpec`] are
//! nameable downstream (they ride public signatures), the scope's
//! view-minting surface is not — the structure is authoring-ready and opening
//! it later is purely additive visibility.
//!
//! [`StateDescriptor`] itself is **sealed** (by the crate-private
//! `SealedDescriptor` supertrait): a downstream crate can register and bind the
//! framework's descriptors but cannot add its own impl, so no custom descriptor
//! can receive the raw, gate-free [`CellSession`] from `bind` and reach cells
//! outside the KV4 session gate. This closes the one hole `CollectionSpec`'s
//! Exposure note (on [`CollectionSpec`]) does not — that seals cell *reach* for
//! kinds, this seals descriptor *authorship*.

use crate::codec::{Codec, JsonCodec, SerializeBufGuard};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::registry::CollectionDef;
use crate::state::session::{CellSession, MutatePermit, OpPermit};
use crate::state::{
    CollectionKindId, CommitMode, SHARD_FANOUT_CONCURRENCY, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use internment::Intern;
use std::error::Error;
use std::future::{Future, ready};
use std::marker::PhantomData;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::instrument;

pub mod deque;
pub mod map;

pub use deque::{DequeDescriptor, DequeHandle, DequeStateError, deque_state};
pub use map::{MapDescriptor, MapHandle, MapStateError, map_state};

/// Value's own section enum, lowered to the opaque [`Section`]. Value is a
/// one-cell collection, so it has exactly one section and addresses its single
/// cell at the empty coordinate.
#[repr(i8)]
enum ValueNs {
    Entries = 0,
}

/// The section holding a Value collection's single [`UnitKey`]-addressed cell.
const VALUE_SECTION: Section = Section::new(ValueNs::Entries as i8);

/// A resolver: how a decoded cell (`Stored`) maps to and from the value a
/// handle exposes (`Resolved`/`Write`).
///
/// A resolver is a zero-sized *strategy*, never an instance: every method is
/// static. It is **session-free** — it never sees the session. Instead it
/// declares the capability [`Self::resolve`] borrows as [`Self::Context`]
/// (`()` for none, `&'s L` for a loader); the framework extracts that context
/// from the session through [`FromSession`]. This keeps the resolver's token
/// ([`Self::RESOLVER_ID`]) a plain const on the one trait, symmetric with
/// [`Codec::FORMAT_ID`].
///
/// A resolver is *behavior over* decoded payloads — it must never change what
/// stored bytes mean ([`Codec::FORMAT_ID`]'s completeness law). Storage whose
/// payload denotes something the format doesn't imply (a reference, a
/// pointer) belongs in a dedicated codec, the way the message cell's
/// `"message-ref"` format is its own codec and its resolver merely fetches.
pub trait CellResolver {
    /// The decoded cell type this resolver maps from — pinned to the codec's
    /// payload by [`CellType`].
    type Stored;

    /// What a handle's `get` returns. `Send` so a resolved item survives a
    /// `buffered` scan window in a `Send` stream.
    type Resolved: Send;

    /// What a handle's `set` takes. A GAT so a borrowing resolver (e.g. "store
    /// a reference to the message in hand") can take `&'a T` while a
    /// passthrough takes an owned value.
    type Write<'a>;

    /// What [`Self::resolve`] borrows from the session: `()` for a passthrough,
    /// `&'s L` for a resolver that needs a loader. Extracted from the session
    /// by [`FromSession`], so the resolver itself stays session-free.
    type Context<'s>: Send;

    /// The resolver's token, or `None` for a passthrough (the stored value
    /// *is* the exposed value). Rides [`StructuralIdentity`] for the
    /// **in-process** bind-time check (`verify_state_registration`), catching
    /// two same-named descriptors with different resolvers in one binary. It
    /// is deliberately not part of the durable identity — resolvers are
    /// behavior, not data (see the trait doc).
    const RESOLVER_ID: Option<&'static str>;

    /// Resolves a decoded cell into the exposed value, using only the borrowed
    /// context.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError`] when resolution fails (for example a
    /// loader miss).
    fn resolve(
        ctx: Self::Context<'_>,
        stored: Self::Stored,
    ) -> impl Future<Output = Result<Self::Resolved, StateAccessError>> + Send;

    /// Lowers a written value into the cell value the codec serializes.
    fn stored_from(write: Self::Write<'_>) -> Self::Stored;
}

/// Framework adapter: how a [`CellResolver::Context`] is borrowed from a
/// session. Two impls, coherence-disjoint by type shape — `()` borrows
/// nothing, `&'s S::Loader` borrows the loader. A custom context (a local type)
/// is the public extension point.
pub trait FromSession<'s, S>: Sized {
    /// Extracts the resolver's context from the session.
    fn from_session(session: &'s S) -> Self;
}

impl<'s, S> FromSession<'s, S> for () {
    fn from_session(_session: &'s S) -> Self {}
}

impl<'s, S: CellSession> FromSession<'s, S> for &'s S::Loader {
    fn from_session(session: &'s S) -> Self {
        session.loader()
    }
}

/// Every [`Codec`] is its own passthrough [`CellResolver`] — the unit of
/// composition, so a plain codec is a complete [`CellType`] with no resolver
/// slot to fill.
impl<C: Codec> CellResolver for C {
    type Context<'s> = ();
    type Resolved = C::Payload;
    type Stored = C::Payload;
    type Write<'a> = C::Payload;

    const RESOLVER_ID: Option<&'static str> = None;

    fn resolve(
        _ctx: Self::Context<'_>,
        stored: C::Payload,
    ) -> impl Future<Output = Result<C::Payload, StateAccessError>> + Send {
        ready(Ok(stored))
    }

    fn stored_from(write: C::Payload) -> C::Payload {
        write
    }
}

/// The complete typed contract of a cell, composed on three axes: an
/// [`OrderedKeyCodec`] address, a [`Codec`] payload (`bytes ↔ stored`), and a
/// [`CellResolver`] (`stored ↔ exposed`). Codec/resolver compatibility
/// (`Resolver::Stored = Codec::Payload`) is enforced here, once. Users never
/// write a `CellType` impl: a plain codec satisfies it directly (unit-addressed
/// passthrough), [`WithResolver`] pairs a codec with a resolver ad hoc, and
/// [`Keyed`] lifts either into a key-addressed family.
pub trait CellType {
    /// The address codec — [`UnitKey`] for a single-cell type, a real key
    /// codec once lifted through [`Keyed`].
    type Key: OrderedKeyCodec;

    /// The codec typing the stored cell.
    type Codec: Codec;

    /// The resolver mapping the stored cell to and from the exposed value,
    /// pinned to store the codec's payload.
    type Resolver: CellResolver<Stored = <Self::Codec as Codec>::Payload>;
}

/// A plain codec is a complete single-cell type: unit address + codec + itself
/// as passthrough resolver.
impl<C: Codec> CellType for C {
    type Codec = C;
    type Key = UnitKey;
    type Resolver = C;
}

/// Resolver-axis composer: pairs a codec with a non-trivial resolver — the way
/// to compose a reference cell without writing a [`CellType`] impl. Single-cell
/// (`Key = UnitKey`); lift it through [`Keyed`] to address a family.
pub struct WithResolver<C, R>(PhantomData<fn() -> (C, R)>);

impl<C: Codec, R: CellResolver<Stored = C::Payload>> CellType for WithResolver<C, R> {
    type Codec = C;
    type Key = UnitKey;
    type Resolver = R;
}

/// Key-axis composer: lifts a single-cell [`CellType`] into a family addressed
/// by key codec `K`, keeping its payload and resolver. This plus the
/// [`UnitKey`] blanket is the stable-Rust encoding of an optional key axis
/// (associated-type defaults are unstable). Only single-cell types
/// (`Key = UnitKey`) can be lifted, so a double-keyed composition — which
/// would silently discard the inner key axis — is unrepresentable.
pub struct Keyed<K, T>(PhantomData<fn() -> (K, T)>);

impl<K: OrderedKeyCodec, T: CellType<Key = UnitKey>> CellType for Keyed<K, T> {
    type Codec = T::Codec;
    type Key = K;
    type Resolver = T::Resolver;
}

/// The logical key a cell type's ops address by — `()` for a single-cell type.
pub type KeyOf<T> = <<T as CellType>::Key as OrderedKeyCodec>::Key;

/// The codec error a cell type's `get`/`set` surface — the codec half of
/// [`CellStateError`].
pub type CellCodecError<T> = <<T as CellType>::Codec as Codec>::Error;

/// The value a cell type's `get` returns and its scan yields.
pub type ResolvedOf<T> = <<T as CellType>::Resolver as CellResolver>::Resolved;

/// The value a cell type's `set` takes.
pub type WriteOf<'a, T> = <<T as CellType>::Resolver as CellResolver>::Write<'a>;

/// The session capability a cell type's resolver borrows at resolve time.
pub type ContextOf<'s, T> = <<T as CellType>::Resolver as CellResolver>::Context<'s>;

/// One item [`CellView::scan`] yields: a decoded key paired with its resolved
/// value, or the error that ended the stream.
pub(crate) type ScanItem<T> = Result<(KeyOf<T>, ResolvedOf<T>), CellStateError<CellCodecError<T>>>;

/// The structural identity a descriptor asserts for its collection:
/// collection kind plus the cell's key-format, payload-format, and resolver
/// tokens.
///
/// The kind and format tokens are frozen durably (the
/// [`DurableDescriptorIdentity`](crate::state::descriptor_identity::DurableDescriptorIdentity)
/// row); the resolver token is checked in-process only, at bind time —
/// behavior, not data. Operational settings (TTL, commit mode) are
/// deliberately not part of the identity at all — they may change between
/// deploys; the identity may not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralIdentity {
    /// Collection kind discriminator.
    pub kind: CollectionKindId,

    /// Payload-format token ([`Codec::FORMAT_ID`]). Always present — every
    /// cell is codec-produced.
    pub format_id: &'static str,

    /// Resolver token ([`CellResolver::RESOLVER_ID`]); `None` for passthrough.
    /// In-process only — never persisted.
    pub resolver_id: Option<&'static str>,

    /// Key-format token ([`Codec::FORMAT_ID`] of the cell's key axis) —
    /// [`UnitKey`]'s for single-cell kinds (Value), the kind's pinned index
    /// codec for Deque, the user's chosen key codec for Map. Frozen into the
    /// durable identity the same way `format_id` is, so a collection's key
    /// encoding can never silently change.
    pub key_format_id: &'static str,
}

impl StructuralIdentity {
    /// Derives the identity a `kind` asserts for cell type `T`: every token is
    /// read straight off `T`'s axes, so a kind cannot lie about the cell it
    /// stores.
    pub(crate) fn of<T: CellType>(kind: CollectionKindId) -> Self {
        Self {
            kind,
            format_id: <T::Codec as Codec>::FORMAT_ID,
            resolver_id: <T::Resolver as CellResolver>::RESOLVER_ID,
            key_format_id: <T::Key as Codec>::FORMAT_ID,
        }
    }
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

/// Seals [`StateDescriptor`]. The trait is crate-internal (declared `pub`
/// inside a `pub(crate)` module so it caps at crate visibility yet reads as the
/// supertrait of the `pub` [`StateDescriptor`] without the `private_bounds`
/// lint), so only the framework's two descriptor carriers implement it —
/// [`Descriptor<K>`] (with its public
/// [`ValueDescriptor`]/[`MapDescriptor`]/[`DequeDescriptor`] aliases) and the
/// crate-internal lifecycle tunnel (`LifecycleAccess` in
/// [`crate::state::session`]). A downstream crate can name [`StateDescriptor`]
/// in bounds and call `bind`, but cannot add an impl — so it can never hand its
/// own `bind` the raw, gate-free [`CellSession`] and reach cells outside the
/// KV4 session gate.
pub(crate) mod sealed {
    use super::Descriptor;

    /// The seal marker; see the module-level item's doc.
    pub trait SealedDescriptor {}

    impl<K> SealedDescriptor for Descriptor<K> {}
}

pub(crate) use sealed::SealedDescriptor;

/// A typed view over one keyed-state collection, bindable to any
/// [`CellSession`].
///
/// Handlers reach this through
/// [`EventContext::state`](crate::consumer::event_context::EventContext::state),
/// which binds against the context's per-event session. Binding validates
/// registration + structural identity through the session's
/// [`verify_state_registration`] and returns an owned, `Clone` handle that
/// wraps the session's byte cells with the descriptor's typing.
///
/// Sealed by the crate-private `SealedDescriptor` supertrait: the two impls are
/// the framework's own [`Descriptor<K>`] and the lifecycle tunnel, so `bind`'s
/// raw-session access stays framework-only (see the module's Exposure note).
///
/// [`verify_state_registration`]: CellSession::verify_state_registration
pub trait StateDescriptor: DescriptorIdentity + Copy + SealedDescriptor {
    /// Typed handle returned by [`Self::bind`]; owns a clone of the binding
    /// [`CellSession`].
    type Handle<S: CellSession>;

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
    fn bind<S: CellSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError>;

    /// The operational settings (TTL, commit mode) this descriptor carries
    /// into registration, set via its fluent methods (see [`Self::ttl`]).
    ///
    /// Defaults to [`CollectionDef::new`] with `None` (indefinite retention,
    /// read-committed) so framework-internal descriptors need not carry one.
    fn collection_def(&self) -> CollectionDef {
        CollectionDef::new(None)
    }

    /// Returns a copy of this descriptor with `def` replacing its operational
    /// settings — the single hook the fluent config defaults build on.
    #[must_use]
    fn with_collection_def(self, def: CollectionDef) -> Self;

    /// Sets the collection's TTL (the per-write Cassandra `USING TTL`),
    /// validated against the ceiling and the recovery delay at registration.
    #[must_use]
    fn ttl(self, ttl: CompactDuration) -> Self {
        let mut def = self.collection_def();
        def.ttl = Some(ttl);
        self.with_collection_def(def)
    }

    /// Sets the collection's recovery-convergence bound: guarantee its
    /// provisional cells are swept back to committed within `d` of the commit,
    /// tightening how long an external (non-owner) reader can observe the
    /// prior committed value. Only ever *tightens* the per-key backstop; a
    /// value above the always-on `recovery_delay` floor is clamped by it.
    /// See [`CollectionDef`].
    #[must_use]
    fn recovery_within(self, d: CompactDuration) -> Self {
        let mut def = self.collection_def();
        def.recovery_within = Some(d);
        self.with_collection_def(def)
    }

    /// Selects [`CommitMode::ReadUncommitted`]: writes apply to committed
    /// state on handler success, with at-least-once semantics.
    #[must_use]
    fn read_uncommitted(self) -> Self {
        let mut def = self.collection_def();
        def.commit_mode = CommitMode::ReadUncommitted;
        self.with_collection_def(def)
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

impl<D> Registered<D> {
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

/// Per-kind specialization for the shared [`Descriptor`] skeleton: the cell
/// type a kind stores, its kind discriminator and optional key codec, and the
/// typed handle a bind mints from a [`CellScope`]. One zero-sized impl per
/// collection kind ([`ValueKind`], [`map::MapKind`], [`deque::DequeKind`]); the
/// public [`ValueDescriptor`]/[`MapDescriptor`]/[`DequeDescriptor`] aliases
/// pick the spec, so every descriptor shares one `new`, `name`,
/// `collection_def`/`with_collection_def`, and `bind` body.
///
/// The framework reads every [`StructuralIdentity`] token straight off
/// `Cell`'s axes, so a kind cannot misstate the identity it registers.
///
/// # Exposure
///
/// This trait names the [`StateDescriptor`] impl's `Handle` associated type, a
/// public interface, so it is `pub` — but defining collection kinds is
/// deliberately unexposed: a kind's `handle` receives a [`CellScope`], and the
/// scope's view-minting surface (`CellScope::typed`) is crate-internal, so
/// while a downstream impl can be registered and bound, the handle it mints
/// cannot reach any cell. Users compose cell types (codec + resolver) instead
/// — that surface is fully public.
pub trait CollectionSpec {
    /// This kind's durable discriminator.
    const KIND: CollectionKindId;

    /// The cell type stored in this kind's data cells. The framework reads its
    /// identity tokens off it.
    type Cell: CellType;

    /// The typed handle [`Descriptor::bind`] returns over session `S`.
    type Handle<S: CellSession>;

    /// Mints the handle over a bound [`CellScope`]. The scope pins the
    /// collection's partition; the kind projects the typed views it needs from
    /// it (see `CellScope::typed`).
    fn handle<S: CellSession>(scope: CellScope<S>) -> Self::Handle<S>;
}

/// The one descriptor skeleton every collection kind shares: an interned name,
/// operational settings, and a zero-sized [`CollectionSpec`] `K` supplying the
/// per-kind identity and handle. The three public names
/// ([`ValueDescriptor`]/[`MapDescriptor`]/[`DequeDescriptor`]) are aliases over
/// this type.
///
/// A plain `Copy` value (the name is interned — see [`Descriptor::new`] for
/// the retention rationale) so descriptors are cheap to build wherever they
/// are needed.
#[derive(Educe)]
#[educe(Clone(bound = ""), Copy, Debug(bound = ""))]
pub struct Descriptor<K> {
    name: &'static str,
    def: CollectionDef,
    #[educe(Debug(ignore))]
    _marker: PhantomData<fn() -> K>,
}

impl<K> Descriptor<K> {
    /// Declares a collection named `name`.
    ///
    /// Bound-free by design: construction needs no [`CollectionSpec`] bound
    /// (identity/handle surface only at bind and on the handle's `get`/`set`),
    /// so a consumer-layer alias can build a descriptor over a bespoke spec.
    ///
    /// `name` may be any runtime string — FFI clients register collections at
    /// client startup from host-language names — and is interned (the
    /// [`Topic`](crate::Topic) idiom) to the pool's canonical `&'static str`,
    /// which is what keeps the descriptor `Copy`; descriptor names are a
    /// bounded set fixed at consumer build, so pool entries living for the
    /// process is the intended retention. `name` is not validated here; an
    /// empty name fails loudly at registration, the fallible boundary.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: Intern::<str>::from(name).as_ref(),
            def: CollectionDef::new(None),
            _marker: PhantomData,
        }
    }
}

impl<K: CollectionSpec> DescriptorIdentity for Descriptor<K> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity::of::<K::Cell>(K::KIND)
    }
}

impl<K: CollectionSpec> StateDescriptor for Descriptor<K> {
    type Handle<S: CellSession> = K::Handle<S>;

    fn bind<S: CellSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        // The scope carries the binding descriptor's `state_type` so its cell
        // ops address the right namespace.
        let name = session.verify_state_registration(
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )?;
        Ok(K::handle(CellScope::new(
            session.clone(),
            self.state_type(),
            name,
        )))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }

    fn with_collection_def(mut self, def: CollectionDef) -> Self {
        self.def = def;
        self
    }
}

/// Descriptor for a codec-backed single value collection.
///
/// Generic over a [`CellType`] `T` — a plain [`Codec`] (the default
/// [`JsonCodec`] stores [`serde_json::Value`] cells, the same default as the
/// consumer's message payload) or a codec paired with a resolver via
/// [`WithResolver`]. Declare via [`value_state`] (see [`Descriptor::new`] for
/// the `name` contract); for a typed cell, declare a codec
/// (`CartCodec: Codec<Payload = Cart>`) and annotate the binding
/// `ValueDescriptor<CartCodec>`.
pub type ValueDescriptor<T = JsonCodec> = Descriptor<ValueKind<T>>;

/// The Value [`CollectionSpec`]: a single [`UnitKey`]-addressed cell of type
/// `T`.
pub struct ValueKind<T>(PhantomData<fn() -> T>);

impl<T: CellType<Key = UnitKey>> CollectionSpec for ValueKind<T> {
    type Cell = T;
    type Handle<S: CellSession> = ValueHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Value;

    fn handle<S: CellSession>(scope: CellScope<S>) -> ValueHandle<S, T> {
        ValueHandle::new(&scope)
    }
}

/// Declares a codec-backed value collection named `name` (JSON by
/// default — annotate the binding with `ValueDescriptor<MyCell>` to pick
/// another cell type). See [`Descriptor::new`] for the `name` contract.
#[must_use]
pub fn value_state<T>(name: &str) -> ValueDescriptor<T>
where
    T: CellType<Key = UnitKey>,
{
    ValueDescriptor::new(name)
}

/// A cell store scoped to ONE collection partition (the unit of atomicity).
///
/// Pins the collection's `(state_type, name)` once at bind and forwards by
/// [`CellKey`], so a collection handle addresses only cells within its own
/// partition and **cannot escape it** (the `CollectionScopeContainment`
/// invariant — the segment/key are injected by the session, the wrapped
/// session is private). A kind projects the typed views it needs from a scope
/// with `Self::typed`; the raw byte ops stay module-private, so a cell's
/// bytes are only ever spoken through its codecs — an [`OrderedKeyCodec`] for
/// the address, a [`Codec`] for the value. Cheap `Clone`.
///
/// The type is `pub` because it names a parameter of the public
/// [`CollectionSpec::handle`], but it is *sealed*: its constructor is
/// crate-internal and its fields private, so downstream code can hold one only
/// where the framework hands it in and can never mint one — the containment
/// invariant survives exposure.
#[derive(Clone)]
pub struct CellScope<S> {
    session: S,
    state_type: StateType,
    name: StateName,
}

impl<S> CellScope<S> {
    /// Binds a scope to one collection partition (see the type doc for the
    /// `CollectionScopeContainment` invariant this establishes). Bound-free —
    /// construction reads nothing.
    pub(in crate::state::descriptor) fn new(
        session: S,
        state_type: StateType,
        name: StateName,
    ) -> Self {
        Self {
            session,
            state_type,
            name,
        }
    }
}

impl<S: CellSession> CellScope<S> {
    /// Projects a typed view over this scope's cells in `section` for cell type
    /// `T`. A kind projects one view per cell family, each in its own section
    /// (a Map projects its entries and its meta cells from the same scope
    /// into different sections).
    pub(crate) fn typed<T>(&self, section: Section) -> CellView<S, T> {
        CellView {
            scope: self.clone(),
            section,
            _marker: PhantomData,
        }
    }

    /// The bound session, for a typed view to extract a resolver context from.
    pub(in crate::state::descriptor) fn session(&self) -> &S {
        &self.session
    }

    /// Whether this collection carries a TTL — a cheap, allocation-free
    /// registry lookup the Map bound refresh consults per `set`.
    pub(in crate::state::descriptor) fn has_ttl(&self) -> bool {
        self.session.collection_has_ttl(self.state_type, &self.name)
    }

    /// This collection's Map keyset bound — a cheap registry lookup the Map
    /// keyset transition consults per `set`/`stream`.
    pub(in crate::state::descriptor) fn keyset_limit(&self) -> usize {
        self.session
            .collection_keyset_limit(self.state_type, &self.name)
    }

    /// Reads one cell's visible committed bytes. Demands a read permit
    /// (`GateWitness`); `_permit` is a terminal token — the borrow is not
    /// threaded into the unwitnessed [`CellSession`] trait, but the returned
    /// future's edition-2024 lifetime capture still binds it to the gate.
    pub(in crate::state::descriptor) async fn raw_get(
        &self,
        _permit: &OpPermit<'_>,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.get(self.state_type, &self.name, cell).await
    }

    /// Scans this collection's cells in `coordinate` order. Unwitnessed by
    /// design: a scan drives gate-free (the stream takes the gate only for its
    /// init metadata read; see
    /// [`SessionGate`](crate::state::session::sealed::SessionGate)'s
    /// chunked stream contract).
    pub(in crate::state::descriptor) fn raw_scan<'a>(
        &'a self,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        self.session.scan(self.state_type, &self.name, scan)
    }

    /// Buffers a set of one cell's bytes. Demands a mutate permit
    /// (`GateWitness`); see [`Self::raw_get`] for the `_permit` token.
    pub(in crate::state::descriptor) async fn raw_set(
        &self,
        _permit: &MutatePermit<'_>,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .set(self.state_type, &self.name, cell, value)
            .await
    }

    /// Buffers a clear of one cell. Demands a mutate permit (`GateWitness`).
    pub(in crate::state::descriptor) async fn raw_clear(
        &self,
        _permit: &MutatePermit<'_>,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session.clear(self.state_type, &self.name, cell).await
    }

    /// Buffers a dirty clear marker over one section; see
    /// [`CellSession::clear_section`]. Demands a mutate permit (`GateWitness`).
    pub(in crate::state::descriptor) async fn clear_section(
        &self,
        _permit: &MutatePermit<'_>,
        section: Section,
    ) -> Result<(), StateAccessError> {
        ensure_live(&self.session)?;
        self.session
            .clear_section(self.state_type, &self.name, section)
            .await
    }

    /// Durably commits this collection's buffered ops mid-handler.
    /// At-least-once; see [`CellSession::commit`] for the contract. Demands a
    /// mutate permit (`GateWitness`).
    pub(in crate::state::descriptor) async fn raw_commit(
        &self,
        _permit: &MutatePermit<'_>,
    ) -> Result<StoreOutcome, StateAccessError> {
        ensure_live(&self.session)?;
        self.session.commit(self.state_type, &self.name).await
    }

    /// Discards this collection's uncommitted buffered ops mid-handler; see
    /// [`CellSession::rollback`] for the contract. Unwitnessed by design: the
    /// session owns rollback's gate acquire, so a handle-held permit would
    /// re-enter the non-reentrant mutex and deadlock. The terminated- and
    /// closed-session guards live in the session impl (as a `NoOp`), not
    /// here: the infallible signature cannot surface an error the way
    /// `ensure_live` does for the fallible ops.
    pub(in crate::state::descriptor) async fn raw_rollback(&self) -> StoreOutcome {
        self.session.rollback(self.state_type, &self.name).await
    }
}

/// A typed cell interface over one section of one collection partition: the
/// [`OrderedKeyCodec`] + [`Codec`] + [`CellResolver`] of a [`CellType`] `T`
/// applied to a [`CellScope`]'s raw bytes. It owns both byte codecs, so a kind
/// never speaks a key or value byte: `get`/`set`/`clear` encode the typed key
/// to its coordinate; `get` then decodes and resolves the cell, `set` lowers
/// then encodes it; `scan` decodes each yielded key and resolves each value.
/// Every op guards on session termination.
///
/// The one op bound `for<'s> ContextOf<'s, T>: FromSession<'s, S>` sits on the
/// op impl block: it is what lets `get`/`scan` extract the resolver's context
/// from the session for any lifetime.
pub(crate) struct CellView<S, T> {
    scope: CellScope<S>,
    section: Section,
    _marker: PhantomData<fn() -> T>,
}

impl<S: Clone, T> Clone for CellView<S, T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            section: self.section,
            _marker: PhantomData,
        }
    }
}

impl<S: CellSession, T: CellType> CellView<S, T> {
    /// The collection's name, for the handles' operation spans.
    pub(in crate::state::descriptor) fn name(&self) -> &StateName {
        &self.scope.name
    }

    /// Acquires the session operation gate for a read — the top of every
    /// gated public read wrapper (the handles' `get`/`len`/stream inits). The
    /// returned [`OpPermit`] is the witness the view's read sinks demand.
    pub(in crate::state::descriptor) async fn read_permit(&self) -> OpPermit<'_> {
        self.scope.session().gate().read().await
    }

    /// Acquires the session operation gate for a mutator, erroring
    /// [`StateAccessError::SessionClosed`] once the settle boundary closed the
    /// session. The returned [`MutatePermit`] is the witness the view's
    /// mutating sinks demand.
    pub(in crate::state::descriptor) async fn mutate_permit(
        &self,
    ) -> Result<MutatePermit<'_>, StateAccessError> {
        self.scope.session().gate().mutate().await
    }

    /// The full cell address for `key` in this view's section — the sole place
    /// a typed key is lowered to its order-preserving coordinate.
    fn cell(&self, key: &KeyOf<T>) -> CellKey {
        CellKey {
            section: self.section,
            coordinate: <T::Key as OrderedKeyCodec>::encode(key),
        }
    }

    /// Whether this view's collection carries a TTL (see
    /// [`CellScope::has_ttl`]).
    pub(crate) fn has_ttl(&self) -> bool {
        self.scope.has_ttl()
    }

    /// This view's collection Map keyset bound (see
    /// [`CellScope::keyset_limit`]).
    pub(crate) fn keyset_limit(&self) -> usize {
        self.scope.keyset_limit()
    }

    /// Buffers a dirty clear marker over this view's whole section: every
    /// cell reads as deleted from this program point, and later `set`s
    /// repopulate. See [`CellSession::clear_section`] for the transactional
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(crate) async fn clear_all(
        &self,
        permit: &MutatePermit<'_>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        Ok(self.scope.clear_section(permit, self.section).await?)
    }

    /// Discards this collection's uncommitted buffered ops mid-handler — every
    /// typed view over the scope, not just this view's cells; the discard twin
    /// of [`Self::commit`]. See [`CellSession::rollback`] for the contract
    /// (the session owns the gate acquire, so this stays permit-free).
    pub(crate) async fn rollback(&self) -> StoreOutcome {
        self.scope.raw_rollback().await
    }
}

impl<S, T> CellView<S, T>
where
    S: CellSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// Reads, decodes, and resolves the visible committed value at `key` — the
    /// point-op surface, recomposing the witnessed [`Self::get_bytes`] fetch
    /// with the gate-free [`Self::resolve_bytes`] decode+resolve.
    ///
    /// Written in the desugared `-> impl Future + Send` form for two reasons an
    /// `async fn` could not express:
    /// - the future holds the resolver's [`ContextOf`] GAT projection across
    ///   the resolve await, which rustc issue #100013 cannot infer `Send` for
    ///   through an `async fn`;
    /// - the key is lowered to its coordinate *before* the async block (inside
    ///   [`Self::get_bytes`]), so only the owned [`CellKey`] — never the
    ///   borrowed `&KeyOf<T>` — is captured into the future.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent)
    /// when the cell bytes do not decode, or a resolution error from the
    /// resolver.
    pub(crate) fn get<'a>(
        &'a self,
        permit: &'a OpPermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send + 'a
    {
        let fetch = self.get_bytes(permit, key);
        async move {
            match fetch.await? {
                Some(bytes) => Ok(Some(self.resolve_bytes(bytes).await?)),
                None => Ok(None),
            }
        }
    }

    /// Reads one cell's visible committed RAW bytes (witnessed) — the gated
    /// half of [`Self::get`]: the overlay check → `raw_get` → cache-fill
    /// publish, with no decode. Streams call this per chunk under a per-chunk
    /// permit and resolve the returned bytes gate-free via
    /// [`Self::resolve_bytes`].
    ///
    /// Desugared `-> impl Future + Send` and lowering the key to its
    /// [`CellKey`] *before* the async block, so only the owned coordinate — not
    /// the borrowed `&KeyOf<T>` — crosses the `raw_get` await.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(crate) fn get_bytes<'a>(
        &'a self,
        permit: &'a OpPermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<Option<Bytes>, CellStateError<CellCodecError<T>>>> + Send + 'a
    {
        let cell = self.cell(key);
        async move { Ok(self.scope.raw_get(permit, &cell).await?) }
    }

    /// Decodes and resolves raw cell bytes into the exposed value — the
    /// gate-free half of [`Self::get`]. Takes no permit: a resolver's only
    /// session capability is `()` or `&Loader` ([`FromSession`]), never a cell
    /// op, so resolution touches no cell state and needs no gate (the same
    /// property [`Self::get`] has always relied on — it resolves *under* the
    /// permit today, so a gate-re-entering resolver would already deadlock).
    /// Streams resolve each chunk through this after dropping the fetch permit;
    /// [`Self::scan`]'s per-item body shares it.
    ///
    /// Desugared `-> impl Future + Send + 'a` (with the synchronous decode
    /// hoisted before the async block) so the `Send` bound is **stated**, not
    /// inferred: a `.map(|item| cooperative(...resolve_bytes...))` fan-out
    /// requires the per-item futures `Send` for a higher-ranked lifetime, which
    /// an `async fn`'s inferred `Send` is "not general enough" to satisfy
    /// across a `tokio::spawn`. The explicit bound also removes the need
    /// for the `manual_async_fn` shape a single-async-block `async fn`
    /// would trip.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the bytes do not decode, or a
    /// resolution error from the resolver.
    fn resolve_bytes<'a>(
        &'a self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<ResolvedOf<T>, CellStateError<CellCodecError<T>>>> + Send + 'a
    {
        let stored = decode_cell::<T::Codec>(bytes);
        async move {
            let stored = stored.map_err(CellStateError::Codec)?;
            let ctx = <ContextOf<'a, T> as FromSession<'a, S>>::from_session(self.scope.session());
            Ok(<T::Resolver as CellResolver>::resolve(ctx, stored).await?)
        }
    }

    /// Lowers `value` through the resolver, encodes it, and buffers a set at
    /// `key`.
    ///
    /// Desugared like [`Self::get`]: the key is lowered to its coordinate and
    /// the value through the resolver *before* the async block, so only owned
    /// values cross the buffering await (a borrowed `&KeyOf<T>` never does).
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the cell fails to encode, or an
    /// access error from the session.
    pub(crate) fn set<'a>(
        &'a self,
        permit: &'a MutatePermit<'_>,
        key: &KeyOf<T>,
        value: WriteOf<'_, T>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<T>>>> + Send + 'a {
        let cell = self.cell(key);
        let stored = <T::Resolver as CellResolver>::stored_from(value);
        async move {
            let buf = encode_cell::<T::Codec>(stored).map_err(CellStateError::Codec)?;
            Ok(self.scope.raw_set(permit, &cell, &buf).await?)
        }
    }

    /// Buffers a clear of the cell at `key`.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(crate) fn clear<'a>(
        &'a self,
        permit: &'a MutatePermit<'_>,
        key: &KeyOf<T>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<T>>>> + Send + 'a {
        let cell = self.cell(key);
        async move { Ok(self.scope.raw_clear(permit, &cell).await?) }
    }

    /// Durably commits this collection's buffered ops mid-handler — the
    /// single `commit()` home, draining the whole collection's buffered ops
    /// (every typed view over the scope), not just this view's cells.
    /// At-least-once; see [`CellSession::commit`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    pub(crate) async fn commit(
        &self,
        permit: &MutatePermit<'_>,
    ) -> Result<StoreOutcome, CellStateError<CellCodecError<T>>> {
        Ok(self.scope.raw_commit(permit).await?)
    }

    /// Scans this section's cells in key order over the typed range
    /// `[start, end]` (direction-relative; see [`Scan`]), decoding each key and
    /// resolving each value, yielding `(KeyOf<T>, ResolvedOf<T>)`. The borrowed
    /// bound keys are encoded to owned coordinates on the stream's first poll
    /// and never touched again.
    ///
    /// Items may be prefetched and resolved up to
    /// [`SHARD_FANOUT_CONCURRENCY`](crate::state::SHARD_FANOUT_CONCURRENCY)
    /// ahead of the consumer; the window is ordered (`buffered`, not
    /// `buffer_unordered`), so cells arrive in key order. The stream terminates
    /// at the first error.
    pub(crate) fn scan<'a>(
        &'a self,
        start: ScanEdge<&'a KeyOf<T>>,
        dir: Direction,
        end: ScanEdge<&'a KeyOf<T>>,
        limit: Option<usize>,
    ) -> impl Stream<Item = ScanItem<T>> + Send + 'a {
        try_stream! {
            ensure_live(self.scope.session())?;
            let this = self;
            // Encode the direction-relative edges once, here — the owned
            // coordinates outlive the scan the generator drives to completion
            // below. (`Scan::start`/`end` follow `dir`; `Scan` itself derives
            // the byte-order low/high.)
            let start = encode_edge::<T::Key>(start);
            let end = encode_edge::<T::Key>(end);
            let scan = Scan {
                section: self.section,
                start: start.as_ref(),
                dir,
                end: end.as_ref(),
                limit,
            };
            // `cooperative` inline in the producing closure (a `.map(cooperative)`
            // stage trips a higher-ranked-lifetime error on the non-`'static`
            // per-item futures); `buffered` keeps key order. Resolution shares
            // `resolve_bytes` with the point-op `get` — one gate-free path.
            let inner = self
                .scope
                .raw_scan(scan)
                .map(|item| {
                    cooperative(async move {
                        let (cell, bytes) = item?;
                        let key = <T::Key as OrderedKeyCodec>::decode(cell.coordinate.as_bytes())
                            .map_err(CellStateError::Key)?;
                        let resolved = this.resolve_bytes(bytes).await?;
                        Ok::<_, CellStateError<CellCodecError<T>>>((key, resolved))
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }
}

/// Typed, owned handle over a codec-backed value collection — a thin newtype
/// over a `CellView` addressing the single Value cell.
///
/// Owns a clone of the binding session (`Clone + Send + Sync + 'static` — an
/// FFI requirement); the cell type's codec runs only at the edges (`get`
/// decodes, `set` encodes) and its resolver maps the decoded cell to and from
/// the exposed value. Every operation guards on session termination.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct ValueHandle<S, T> {
    view: CellView<S, T>,
}

impl<S: CellSession, T> ValueHandle<S, T> {
    /// Wraps a bound [`CellScope`] as the typed view over the single
    /// [`UnitKey`]-addressed Value cell. Bound-free in `T` so
    /// [`StateDescriptor::bind`] can mint it without the op bound.
    fn new(scope: &CellScope<S>) -> Self {
        Self {
            view: scope.typed(VALUE_SECTION),
        }
    }
}

impl<S, T> ValueHandle<S, T>
where
    S: CellSession,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    /// Reads, decodes, and resolves the current visible value.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent)
    /// when the cell bytes do not decode, or a resolution error from the
    /// resolver.
    #[instrument(name = "value.get", skip_all, fields(collection = self.view.name().as_str()), err)]
    pub async fn get(&self) -> Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>> {
        let permit = self.view.read_permit().await;
        self.view.get(&permit, &()).await
    }

    /// Lowers `value` through the resolver, encodes it, and buffers a set.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the cell fails to encode, or
    /// an access error from the session.
    #[instrument(name = "value.set", skip_all, fields(collection = self.view.name().as_str()), err)]
    pub async fn set(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        let permit = self.view.mutate_permit().await?;
        self.view.set(&permit, &(), value).await
    }

    /// Buffers a clear operation.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "value.clear", skip_all, fields(collection = self.view.name().as_str()), err)]
    pub async fn clear(&self) -> Result<(), CellStateError<CellCodecError<T>>> {
        let permit = self.view.mutate_permit().await?;
        self.view.clear(&permit, &()).await
    }

    /// Durably commits the buffered op mid-handler, so it survives a restart
    /// after failure. At-least-once; see [`CellSession::commit`] for the
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "value.commit", skip_all, fields(collection = self.view.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, CellStateError<CellCodecError<T>>> {
        let permit = self.view.mutate_permit().await?;
        self.view.commit(&permit).await
    }

    /// Discards the buffered uncommitted op, reverting reads to the last
    /// [`commit`](Self::commit) — or the pre-event committed value if none.
    /// Infallible; see [`CellSession::rollback`] for the contract.
    #[instrument(name = "value.rollback", skip_all, fields(collection = self.view.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome {
        self.view.rollback().await
    }
}

/// Decodes a cell's bytes as `C::Payload`. Parses in place when the `Bytes` is
/// uniquely owned (zero-copy, the production path — every backend decode mints
/// a fresh `Bytes`); falls back to a copy for a shared clone (the in-memory
/// test backend). The single decode path every typed cell view shares.
pub(in crate::state::descriptor) fn decode_cell<C: Codec>(
    cell: Bytes,
) -> Result<C::Payload, C::Error> {
    match cell.try_into_mut() {
        Ok(mut buf) => C::with_cached_local(|codec| codec.deserialize(&mut buf)),
        Err(cell) => {
            let mut buf = cell.to_vec();
            C::with_cached_local(|codec| codec.deserialize(&mut buf))
        }
    }
}

/// Encodes `payload` into the pooled, reusable serialize buffer, returning the
/// guard so the caller hands its bytes to a cell `set` before the guard drops
/// (returning the buffer to the pool). The guard owns its buffer, so it is
/// `Send` and rides the write across an await. The single encode path every
/// typed cell view shares.
pub(in crate::state::descriptor) fn encode_cell<C: Codec>(
    payload: C::Payload,
) -> Result<SerializeBufGuard, C::Error> {
    let mut buf = SerializeBufGuard::acquire();
    C::with_cached_local(|codec| codec.serialize(payload, &mut buf))?;
    Ok(buf)
}

/// Lowers a typed scan edge to its order-preserving coordinate edge. Called
/// once per scan, on the stream's first poll; the owned coordinate — not the
/// borrowed key — is what the running scan holds.
fn encode_edge<K: OrderedKeyCodec>(edge: ScanEdge<&K::Key>) -> ScanEdge<Coordinate> {
    edge.map(K::encode)
}

/// Guards every cell operation: a session whose partition is shutting
/// down or whose event is cancelled refuses state access with
/// [`StateAccessError::Terminated`].
fn ensure_live<S>(session: &S) -> Result<(), StateAccessError>
where
    S: CellSession,
{
    if session.is_terminated() {
        return Err(StateAccessError::Terminated);
    }
    Ok(())
}

/// Error returned by typed cell operations ([`ValueHandle`], `CellView`).
#[derive(Debug, Error)]
pub enum CellStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// The context refused or failed the state access.
    #[error(transparent)]
    Access(#[from] StateAccessError),

    /// The codec failed to encode or decode the cell.
    #[error("state codec failed")]
    Codec(#[source] E),

    /// A stored key coordinate did not decode back to a logical key. A scan
    /// alone surfaces this — `get`/`set`/`clear` only *encode* the caller's
    /// key, they never decode a stored one.
    #[error(transparent)]
    Key(#[from] KeyCodecError),
}

impl<E> ClassifyError for CellStateError<E>
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
            // A malformed stored coordinate will not decode on retry either.
            Self::Key(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
