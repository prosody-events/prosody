//! Typed descriptors for keyed-state collections.
//!
//! A descriptor names a typed keyed-state collection. It is a plain `Copy`
//! value, because names are interned. Build one with [`value_state`] and
//! register it with the consumer to mint a [`Registered`] capability handle.
//! A handler binds that handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state)
//! and gets a typed handle.
//!
//! The typed handle's `get` reads the value visible to this event. Its `set`
//! stages a write into the invocation's journal. `state` takes the registered
//! handle, never a raw descriptor, so a handler reaches only the collections
//! it registered.
//!
//! # Composed cell types
//!
//! A [`CellType`] is the complete typed contract of a cell. It composes three
//! parts:
//!
//! - **Address:** an
//!   [`OrderedKeyCodec`](crate::state::order_codec::OrderedKeyCodec) maps a
//!   logical key to order-preserving bytes.
//!   [`UnitKey`](crate::state::order_codec::UnitKey) addresses a single-cell
//!   collection, and a real key codec addresses a keyed one.
//! - **Payload:** a [`Codec`] maps bytes to the stored value synchronously. The
//!   codec is the stored typing.
//! - **Resolution:** a [`CellResolver`] maps the stored value to the exposed
//!   value asynchronously.
//!
//! Every plain [`Codec`] is a complete cell type. The blanket impls address it
//! with [`UnitKey`](crate::state::order_codec::UnitKey) and make it its own
//! passthrough resolver. A value collection over `CartCodec: Codec<Payload =
//! Cart>` needs no other layer. A reference cell stores a durable pointer that
//! a resolver loads into a full value. Pair a codec with a resolver through
//! [`WithResolver`] to model one. The consumer layer's Kafka message cell is
//! that pairing.
//!
//! Lift a single-cell type through [`Keyed`] to address a family of cells by
//! key. `src/state` never reads or writes a cell's key or value bytes
//! directly. Only the codecs do, through the one decode/encode pair in
//! [`crate::state::collection`].
//!
//! A [`CellResolver`] is session-free. It declares the capability it needs as
//! [`CellResolver::Context`], and the framework extracts that context from the
//! session through [`FromSession`]. [`CellResolver::RESOLVER_ID`] is a plain
//! const on the resolver trait, symmetric with [`Codec::FORMAT_ID`].
//!
//! Every descriptor asserts a [`StructuralIdentity`]. It is the frozen
//! `(kind, codec id, resolver id, key codec id)` tuple. A silent change to any
//! part would change what a cell means. The codec types the stored cell, the
//! resolver maps it to the exposed value, and the key codec orders keyed
//! kinds. The codec and key-codec tokens are part of the durable contract. The
//! resolver token is checked in process only (see
//! [`StructuralIdentity::resolver_id`]).
//!
//! Registration checks that one `(state_type, name)` always carries one
//! identity. Bind checks it again, and first use checks it against the
//! group-global durable identity table. A process that carries an
//! incompatible descriptor fails loudly instead of misreading cells.
//!
//! # Exposure
//!
//! Users define codecs, resolvers, and cell types. All three are public.
//! Collection kinds stay unexposed. [`CollectionSpec`] is nameable downstream,
//! because it names a public associated type. A marker that only the layout
//! macro emits seals it. A kind can therefore exist only inside this crate,
//! and never without a declared durable layout.
//!
//! [`StateDescriptor`] is sealed the same way, by the crate-private
//! `SealedDescriptor` supertrait. A downstream crate can register and bind the
//! framework's descriptors but cannot add its own impl. The seal keeps
//! identity honest. [`DescriptorIdentity`] is unsealed and
//! [`StructuralIdentity`]'s fields are `pub`. Without the seal a downstream
//! type could claim any identity tuple for any name and hand it to
//! [`KeyedStateConfiguration::register`](crate::consumer::KeyedStateConfiguration::register)
//! or [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! The two seals cover different things. [`CollectionSpec`] seals cell reach
//! for kinds. `SealedDescriptor` seals descriptor authorship.

use crate::codec::Codec;
use crate::state::StateAccessError;
use crate::state::collection::sealed_spec::SealedSpec;
use crate::state::collection::{Collection, StateSession};
use crate::state::registry::{CollectionDef, ReadCachePolicy, StateVisibility};
use crate::state::{CollectionKindId, CommitMode, StateType};
use crate::timers::duration::CompactDuration;
use educe::Educe;
use internment::Intern;
use std::marker::PhantomData;

pub mod deque;
pub mod map;
pub mod set;
mod value;

pub use deque::{DequeDescriptor, DequeHandle, DequeStateError, deque_state};
pub use map::{MapDescriptor, MapHandle, MapStateError, map_state};
pub use set::{SetDescriptor, SetHandle, SetStateError, set_state};
pub use value::{ValueDescriptor, ValueHandle, ValueKind, value_state};

mod cell;
pub(crate) use cell::FanoutOf;
pub use cell::{
    BorrowedKeyOf, CellCodecError, CellResolver, CellStateError, CellType, ContextOf, FromSession,
    KeyOf, Keyed, ResolvedOf, WithResolver, WriteOf,
};

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
    /// [`UnitKey`](crate::state::order_codec::UnitKey)'s for single-cell kinds
    /// (Value), the kind's pinned index codec for Deque, the user's chosen
    /// key codec for Map. Frozen into the durable identity the same way
    /// `format_id` is, so a collection's key encoding can never silently
    /// change.
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
    /// rejects values below one second or above the Cassandra ceiling.
    /// The granularity is seconds ([`CompactDuration`]), as Cassandra requires.
    ///
    /// This governs retention only, never read freshness. The read-only
    /// client's cache TTL is the separate [`Self::read_cache`] policy.
    #[must_use]
    fn ttl(self, ttl: CompactDuration) -> Self {
        let mut def = self.collection_def();
        def.ttl = Some(ttl);
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
/// per-kind identity and handle. The public names ([`ValueDescriptor`],
/// [`MapDescriptor`], [`SetDescriptor`], [`DequeDescriptor`]) are aliases over
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

impl<K: CollectionSpec> Descriptor<K> {
    /// Binds this descriptor to the session's collection namespace.
    pub(crate) fn bind_collection<S: StateSession>(
        self,
        session: &S,
    ) -> Result<Collection<S, K>, StateAccessError> {
        Collection::bind(
            session,
            self.name,
            self.state_type(),
            &self.structural_identity(),
        )
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
        Ok(K::handle(self.bind_collection(session)?))
    }

    fn collection_def(&self) -> CollectionDef {
        self.def
    }

    fn with_collection_def(mut self, def: CollectionDef) -> Self {
        self.def = def;
        self
    }
}

#[cfg(test)]
pub(crate) mod tests;
