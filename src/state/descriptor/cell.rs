//! Cell key, codec, and resolver composition and operation errors.

use crate::codec::Codec;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateAccessError;
use crate::state::collection::StateSession;
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use std::error::Error;
use std::future::{Future, ready};
use std::marker::PhantomData;
use thiserror::Error;

/// A resolver maps a decoded cell (`Stored`) to and from the value a handle
/// exposes (`Resolved` and `Write`).
///
/// A resolver is a zero-sized strategy, never an instance. Every method is
/// static, and no method sees the session. A resolver declares the capability
/// that [`Self::resolve`] borrows as [`Self::Context`]: `()` for none, or
/// `&'s L` for a loader. The framework extracts that context from the session
/// through [`FromSession`]. The resolver token [`Self::RESOLVER_ID`] is a plain
/// const on this trait, like [`Codec::FORMAT_ID`].
///
/// A resolver is behavior over decoded payloads. It must never change what the
/// stored bytes mean, because [`Codec::FORMAT_ID`] describes them completely.
/// A payload that denotes something the format does not imply, such as a
/// reference, belongs in a dedicated codec. The message cell's `"message-ref"`
/// format is its own codec, and its resolver only fetches.
///
/// A resolver must never issue a session or collection operation. A point get
/// resolves while it holds the session gate. A resolver that re-entered the
/// non-reentrant gate would therefore deadlock. Other paths release admission
/// first: a point-get stream chunk releases it before its resolve fan-out, and
/// a range page runs gate-free. The contract still binds every resolver on
/// every path.
pub trait CellResolver {
    /// The decoded cell type this resolver maps from. [`CellType`] fixes it to
    /// the codec's payload.
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
    /// *is* the exposed value). Rides
    /// [`StructuralIdentity`](super::StructuralIdentity) for the
    /// **in-process** bind-time check (`verify_state_registration`), catching
    /// two same-named descriptors with different resolvers in one binary. It
    /// is deliberately not part of the durable identity. Resolvers are
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

/// Framework adapter that borrows a [`CellResolver::Context`] from a session.
/// Two impls exist, and their type shapes do not overlap: `()` borrows
/// nothing, and `&'s S::Loader` borrows the loader. A custom local context type
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

/// Every [`Codec`] is its own passthrough [`CellResolver`]. A plain codec is
/// therefore a complete [`CellType`] with no resolver slot to fill.
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
    /// The address codec. It is [`UnitKey`] for a single-cell type and a real
    /// key codec once lifted through [`Keyed`].
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

/// Pairs a codec with a resolver. This composes a reference cell without a
/// hand-written [`CellType`] impl. It is single-cell (`Key = UnitKey`). Lift
/// it through [`Keyed`] to address a family.
pub struct WithResolver<C, R>(PhantomData<fn() -> (C, R)>);

impl<C: Codec, R: CellResolver<Stored = C::Payload>> CellType for WithResolver<C, R> {
    type Codec = C;
    type Key = UnitKey;
    type Resolver = R;
}

/// Lifts a single-cell [`CellType`] into a family addressed by key codec `K`.
/// The payload and resolver stay the same. Together with the [`UnitKey`]
/// blanket impl, this encodes an optional key axis on stable Rust, where
/// associated-type defaults are unstable. Only single-cell types
/// (`Key = UnitKey`) can be lifted. A double-keyed composition would discard
/// the inner key axis, so it does not compile.
pub struct Keyed<K, T>(PhantomData<fn() -> (K, T)>);

impl<K: OrderedKeyCodec, T: CellType<Key = UnitKey>> CellType for Keyed<K, T> {
    type Codec = T::Codec;
    type Key = K;
    type Resolver = T::Resolver;
}

/// The logical key a cell type's ops address by. It is `()` for a single-cell
/// type.
pub type KeyOf<T> = <<T as CellType>::Key as OrderedKeyCodec>::Key;

/// The borrowed key view accepted by a cell type.
pub type BorrowedKeyOf<T> = <<T as CellType>::Key as OrderedKeyCodec>::Borrowed;

/// The codec error a cell type's `get` and `set` surface. It is the codec half
/// of [`CellStateError`].
pub type CellCodecError<T> = <<T as CellType>::Codec as Codec>::Error;

/// The value a cell type's `get` returns and its scan yields.
pub type ResolvedOf<T> = <<T as CellType>::Resolver as CellResolver>::Resolved;

/// The value a cell type's `set` takes.
pub type WriteOf<'a, T> = <<T as CellType>::Resolver as CellResolver>::Write<'a>;

/// The session capability a cell type's resolver borrows at resolve time.
pub type ContextOf<'s, T> = <<T as CellType>::Resolver as CellResolver>::Context<'s>;

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
