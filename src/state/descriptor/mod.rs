//! Typed descriptors for keyed-state collections.
//!
//! A descriptor names a typed keyed-state collection — a plain `Copy` value
//! (names are interned). Build it with [`value_state`], registering it with
//! the consumer to mint a [`Registered`] capability handle. A handler binds
//! that handle via
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state)
//! to get a typed handle. That handle's `get` reads the value visible to this
//! event, and its `set` stages a write into the invocation's journal. `state`
//! takes the handle, never a raw descriptor, so a handler can reach only the
//! collections it registered.
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
//! value — directly; only its codecs do. The one decode/encode pair every
//! typed cell passes through lives in [`crate::state::collection`], which owns
//! that boundary.
//!
//! A [`CellResolver`] is **session-free**: it declares the capability it needs
//! as [`CellResolver::Context`] and the framework extracts that context from
//! the session via [`FromSession`]. Passing the whole session to a resolver was
//! the complection that once forced its durable token onto a separate trait;
//! with the context split out, [`CellResolver::RESOLVER_ID`] sits on the one
//! resolver trait as a plain const, symmetric with [`Codec::FORMAT_ID`].
//!
//! Every descriptor asserts a [`StructuralIdentity`] — the frozen
//! `(kind, codec id, resolver id, key codec id)` tuple. Swapping any of them
//! silently would change what a cell means: the codec types the stored cell,
//! the resolver maps it to and from the exposed value, and the key codec orders
//! keyed kinds. The codec and key-codec tokens are part of the *durable*
//! contract; the resolver token is checked in process only (see
//! [`StructuralIdentity::resolver_id`]).
//! The identity is checked at registration (same `(state_type, name)` ⇒ same
//! identity), at bind, and against the group-global durable identity table on
//! first use, so a process carrying an incompatible descriptor fails loudly
//! instead of silently misreading cells.
//!
//! # Exposure
//!
//! Users define codecs, resolvers, and cell types (all public). Defining
//! collection *kinds* stays deliberately unexposed. [`CollectionSpec`] is
//! nameable downstream, because it names a public associated type, but a marker
//! that only the layout macro emits *seals* it. A kind can therefore exist only
//! inside this crate, and never without a declared durable layout.
//!
//! [`StateDescriptor`] is sealed the same way, by the crate-private
//! `SealedDescriptor` supertrait. A downstream crate can register and bind the
//! framework's descriptors but cannot add its own impl. That is what keeps
//! identity honest: [`DescriptorIdentity`] is unsealed and
//! [`StructuralIdentity`]'s fields are `pub`, so without the seal a downstream
//! type could claim any (kind, format, resolver, key format) tuple for any name
//! and hand it to
//! [`KeyedStateConfiguration::register`](crate::consumer::KeyedStateConfiguration::register)
//! or [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//! The two seals cover different things — [`CollectionSpec`] seals cell *reach*
//! for kinds, this seals descriptor *authorship*.

use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateAccessError;
use crate::state::collection::sealed_spec::SealedSpec;
use crate::state::collection::{Collection, StateSession};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::registry::{CollectionDef, ReadCachePolicy, StateVisibility};
use crate::state::{CollectionKindId, CommitMode, StateType};
use crate::timers::duration::CompactDuration;
use educe::Educe;
use internment::Intern;
use std::error::Error;
use std::future::{Future, ready};
use std::marker::PhantomData;
use thiserror::Error;

pub mod deque;
pub mod map;
pub mod set;
mod value;

pub use deque::{DequeDescriptor, DequeHandle, DequeQuery, DequeStateError, deque_state};
pub use map::{MapDescriptor, MapHandle, MapStateError, map_state};
pub use set::{SetDescriptor, SetHandle, SetQuery, SetStateError, set_state};
pub use value::{ValueDescriptor, ValueHandle, ValueKind, value_state};

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
///
/// A resolver must never issue a session or collection operation. A point get
/// resolves while it holds the session gate. A resolver that re-entered the
/// non-reentrant gate would therefore deadlock. Other paths release admission
/// first: a point-get stream chunk releases it before its resolve fan-out, and
/// a range page runs gate-free. The contract still binds every resolver on
/// every path.
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

impl<'s, S: StateSession> FromSession<'s, S> for &'s S::Loader {
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

/// The borrowed key form that point operations accept for cell type `T`.
pub type BorrowedKeyOf<T> = <<T as CellType>::Key as OrderedKeyCodec>::Borrowed;

/// The codec error a cell type's `get`/`set` surface — the codec half of
/// [`CellStateError`].
pub type CellCodecError<T> = <<T as CellType>::Codec as Codec>::Error;

/// The value a cell type's `get` returns and its scan yields.
pub type ResolvedOf<T> = <<T as CellType>::Resolver as CellResolver>::Resolved;

/// The value a cell type's `set` takes.
pub type WriteOf<'a, T> = <<T as CellType>::Resolver as CellResolver>::Write<'a>;

/// The session capability a cell type's resolver borrows at resolve time.
pub type ContextOf<'s, T> = <<T as CellType>::Resolver as CellResolver>::Context<'s>;

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
/// lint), so no impl can exist outside this crate. A downstream crate can name
/// [`StateDescriptor`] in bounds and call `bind`, but cannot add an impl. See
/// the module's Exposure note for what that buys.
pub(crate) mod sealed {
    use super::Descriptor;

    /// The seal marker; see the module-level item's doc.
    pub trait SealedDescriptor {}

    impl<K> SealedDescriptor for Descriptor<K> {}
}

pub(crate) use sealed::SealedDescriptor;

/// A typed view over one keyed-state collection, bindable to any
/// [`StateSession`].
///
/// Handlers reach this through
/// [`EventContext::state`](crate::consumer::event_context::EventContext::state),
/// which binds against the context's per-event session. Binding validates the
/// collection through the session's engine — registration and structural
/// identity for the owner, the acquisition-validated descriptor for a published
/// reader — and returns an owned, `Clone` handle over the bound collection.
/// Each of that handle's methods runs as one scoped operation. A stream method
/// runs a planning operation, then drives its plan outside that operation.
///
/// Sealed by the crate-private `SealedDescriptor` supertrait; see the module's
/// Exposure note.
pub trait StateDescriptor: DescriptorIdentity + Copy + SealedDescriptor {
    /// Typed handle returned by [`Self::bind`]; owns a clone of the binding
    /// session.
    type Handle<S: StateSession>;

    /// Validates the collection against the session's engine and returns the
    /// typed handle.
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

    /// The operational settings this descriptor carries into registration, set
    /// via its fluent methods (see [`Self::ttl`]).
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

    /// Sets the collection's durable write TTL: the per-write Cassandra
    /// `USING TTL` that bounds how long stored state is retained. Registration
    /// validates it against the ceiling and the recovery delay. The granularity
    /// is seconds ([`CompactDuration`]), matching what Cassandra can store.
    ///
    /// This governs retention only, never read freshness. The read-only
    /// client's cache TTL is the separate [`Self::read_cache`] policy.
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

    /// Sets the collection's cross-group read visibility. The flag is
    /// reversible: `.published(false)` reverts to [`StateVisibility::Private`].
    /// A `Published` collection requires a configured subsystem, checked at
    /// consumer build.
    #[must_use]
    fn published(self, published: bool) -> Self {
        let mut def = self.collection_def();
        def.visibility = if published {
            StateVisibility::Published
        } else {
            StateVisibility::Private
        };
        self.with_collection_def(def)
    }

    /// Sets the **read-only client's cache policy**.
    ///
    /// A [`std::time::Duration`] sets this collection's TTL. Pass
    /// [`ReadCachePolicy::Disabled`] to read the durable store on every
    /// operation. Unset collections inherit the reader client's default.
    /// Sub-second TTLs are supported; a zero TTL is rejected at reader
    /// construction.
    ///
    /// Applies only in the read-only client, which consumes it from the
    /// descriptor *the reader itself* passes to `StateReader::new` /
    /// `client.state`. On the owning consumer it is inert: it never affects
    /// writes, owner reads, or the durable retention set by [`Self::ttl`].
    #[must_use]
    fn read_cache(self, policy: impl Into<ReadCachePolicy>) -> Self {
        let mut def = self.collection_def();
        def.read_cache = policy.into();
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
/// typed handle a bind mints from the bound [`Collection`]. One zero-sized impl
/// per collection kind ([`ValueKind`], [`map::MapKind`], [`deque::DequeKind`]);
/// the public [`ValueDescriptor`]/[`MapDescriptor`]/[`DequeDescriptor`] aliases
/// pick the spec, so every descriptor shares one `new`, `name`,
/// `collection_def`/`with_collection_def`, and `bind` body.
///
/// The framework reads every [`StructuralIdentity`] token straight off
/// `Cell`'s axes. `Cell` itself is hand-written, so it could name a family that
/// the layout does not declare. Each kind's frozen-layout assertion therefore
/// pins `Cell` to the key and payload tokens of the data family it addresses.
///
/// # Exposure
///
/// This trait names the [`StateDescriptor`] impl's `Handle` associated type, a
/// public interface, so it is `pub`. Defining collection kinds stays
/// deliberately unexposed, and structurally so. A crate-internal marker seals
/// the trait, and only
/// [`collection_layout!`](crate::state::collection::collection_layout) emits
/// that marker, so a kind cannot exist without a declared durable layout.
/// Users compose cell types (codec + resolver) instead. That surface is fully
/// public.
pub trait CollectionSpec: SealedSpec + Sized {
    /// This kind's durable discriminator.
    const KIND: CollectionKindId;

    /// The cell type stored in this kind's data cells. The framework reads its
    /// identity tokens off it.
    type Cell: CellType;

    /// The typed handle [`Descriptor::bind`] returns over session `S`.
    type Handle<S: StateSession>;

    /// Mints the handle over the already-validated binding. Infallible: every
    /// check the collection needs happened while the [`Collection`] was built.
    fn handle<S: StateSession>(collection: Collection<S, Self>) -> Self::Handle<S>;
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
    type Handle<S: StateSession> = K::Handle<S>;

    fn bind<S: StateSession>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
        // The binding carries the descriptor's `state_type`, so the
        // collection's commands address the right namespace.
        let collection = Collection::bind(
            session,
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )?;
        Ok(K::handle(collection))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }

    fn with_collection_def(mut self, def: CollectionDef) -> Self {
        self.def = def;
        self
    }
}

/// Error returned by a typed cell operation, which is one scoped collection
/// command.
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

    /// A stored key coordinate did not decode back to a logical key. Only a
    /// coordinate decode produces this error, so only a stream can raise it.
    /// Every point command encodes the caller's key and decodes no stored one.
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
