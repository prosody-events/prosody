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
//! value — directly; only its codecs do, and the compiler now enforces this:
//! the byte-level sinks live in the private `view` submodule as items private
//! to it, unreachable from the sibling collection kinds.
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
//! can receive the raw, gate-free [`CellRead`] session from `bind` and reach
//! cells outside the KV4 session gate. This closes the one hole
//! `CollectionSpec`'s Exposure note (on [`CollectionSpec`]) does not — that
//! seals cell *reach* for kinds, this seals descriptor *authorship*.

use crate::codec::{Codec, JsonCodec};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateAccessError;
use crate::state::cell_key::Section;
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::registry::{CollectionDef, ReadCache, StateVisibility};
use crate::state::session::{CellRead, CellWrite};
use crate::state::store::CELL_BATCH;
use crate::state::{CollectionKindId, CommitMode, StateType, StoreOutcome};
use crate::timers::duration::CompactDuration;
use educe::Educe;
use internment::Intern;
use std::error::Error;
use std::future::{Future, ready};
use std::marker::PhantomData;
use thiserror::Error;
use tracing::instrument;

pub mod deque;
pub mod map;
mod view;

pub use deque::{DequeDescriptor, DequeHandle, DequeStateError, deque_state};
pub use map::{MapDescriptor, MapHandle, MapStateError, map_state};
pub use view::CellScope;
pub(crate) use view::CellView;

/// Value's own section enum, lowered to the opaque [`Section`]. Value is a
/// one-cell collection, so it has exactly one section and addresses its single
/// cell at the empty coordinate.
#[repr(i8)]
enum ValueNs {
    Entries = 0,
}

/// The section holding a Value collection's single [`UnitKey`]-addressed cell.
const VALUE_SECTION: Section = Section::new(ValueNs::Entries as i8);

/// The point-get streams' chunk width: the granularity of both the per-chunk
/// gate hold (one read permit per chunk, dropped with the chunk future's scope
/// before any yield) and the batch read — each chunk's cells are fetched by
/// ONE `CellView::get_many` call (one Cassandra query / one fjall hop), whose
/// typed resolves then fan out under `RESOLVE_FANOUT`. Shared by
/// [`MapHandle::stream`](map::MapHandle::stream) and
/// [`DequeHandle::stream`](deque::DequeHandle::stream).
///
/// An alias of [`CELL_BATCH`] — the point-get
/// stream chunk width and the store batch-read width are one number, and the
/// `> 0` invariant the shared `CellView::scan_at` chunk source relies on
/// (`coords.by_ref().take(STREAM_CHUNK)` must take ≥ 1 coordinate per chunk) is
/// enforced once on `CELL_BATCH`.
pub(crate) const STREAM_CHUNK: usize = CELL_BATCH;

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
/// A resolver must never issue a session or collection operation: point gets
/// and point-get stream chunks resolve while holding the session gate (a scan
/// resolves gate-free, but the contract binds every resolver regardless of
/// path), so a resolver that re-entered the non-reentrant gate would deadlock.
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

impl<'s, S: CellRead> FromSession<'s, S> for &'s S::Loader {
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
/// own `bind` the raw, gate-free [`CellRead`] session and reach cells outside
/// the KV4 session gate.
pub(crate) mod sealed {
    use super::Descriptor;

    /// The seal marker; see the module-level item's doc.
    pub trait SealedDescriptor {}

    impl<K> SealedDescriptor for Descriptor<K> {}
}

pub(crate) use sealed::SealedDescriptor;

/// A typed view over one keyed-state collection, bindable to any
/// [`CellRead`] session.
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
/// [`verify_state_registration`]: CellRead::verify_state_registration
pub trait StateDescriptor: DescriptorIdentity + Copy + SealedDescriptor {
    /// Typed handle returned by [`Self::bind`]; owns a clone of the binding
    /// [`CellRead`] session.
    type Handle<S: CellRead>;

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
    fn bind<S: CellRead>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError>;

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

    /// Sets the collection's cross-group read visibility. A reversible flag —
    /// `.published(false)` reverts to [`StateVisibility::Private`], the first
    /// half of a source-of-truth handoff — not a one-way opt-in. A `Published`
    /// collection requires a configured subsystem, checked at consumer build.
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

    /// Sets the collection's read-side cache policy for cross-group readers.
    /// Inert on the owning consumer; consumed by the reader.
    #[must_use]
    fn read_cache(self, read_cache: ReadCache) -> Self {
        let mut def = self.collection_def();
        def.read_cache = read_cache;
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
    type Handle<S: CellRead>;

    /// Mints the handle over a bound [`CellScope`]. The scope pins the
    /// collection's partition; the kind projects the typed views it needs from
    /// it (see `CellScope::typed`).
    fn handle<S: CellRead>(scope: CellScope<S>) -> Self::Handle<S>;
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
    type Handle<S: CellRead> = K::Handle<S>;

    fn bind<S: CellRead>(self, session: &S) -> Result<Self::Handle<S>, StateAccessError> {
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
    type Handle<S: CellRead> = ValueHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Value;

    fn handle<S: CellRead>(scope: CellScope<S>) -> ValueHandle<S, T> {
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

impl<S: CellRead, T> ValueHandle<S, T> {
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
    S: CellRead,
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
}

impl<S, T> ValueHandle<S, T>
where
    S: CellWrite,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
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
    /// after failure. At-least-once; see [`CellWrite::commit`] for the
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
    /// Infallible; see [`CellWrite::rollback`] for the contract.
    #[instrument(name = "value.rollback", skip_all, fields(collection = self.view.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome {
        self.view.rollback().await
    }
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
