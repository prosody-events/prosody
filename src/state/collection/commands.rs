//! Typed commands available within collection admission.

use super::{CellAddress, CellFamily, CollectionLayout, StateSession};
use crate::state::descriptor::{
    BorrowedKeyOf, CellCodecError, CellStateError, CellType, ContextOf, FromSession, ResolvedOf,
    WriteOf,
};
use crate::state::store::CellBuffer;
use crate::state::{StateAccessError, StateName};
use std::future::Future;
use std::num::NonZeroUsize;

/// The read commands every scoped operation offers. Implemented by both
/// operation types, so one collection algorithm serves the owner session and
/// the published reader.
///
/// Every command takes `&mut self`: one public invocation is one explicit
/// top-to-bottom algorithm, and overlapping commands do not compile. Commands
/// that need concurrency provide it internally, after taking the one borrow.
pub(crate) trait CollectionRead: sealed_ops::CollectionOperation {
    /// The bound session type, which the resolver context is extracted from.
    type Session: StateSession;

    /// The layout brand every family argument is checked against.
    type Layout;

    /// The collection's canonical name. It is the operation-span field and
    /// the subject of a collection's degrade warnings.
    fn name(&self) -> &StateName;

    /// Whether the collection carries a durable TTL. The binding's captured
    /// settings answer this without I/O.
    fn has_ttl(&self) -> bool;

    /// The keyset bound: how many live members a map or set tracks before
    /// overflowing to a range scan. The binding's captured settings answer
    /// this without I/O.
    fn keyset_limit(&self) -> usize;

    /// The Deque push cap. A push evicts from the far end above this many
    /// window slots. `None` means unbounded. Reads the binding's captured
    /// settings, with no I/O.
    ///
    /// Only a write calls it today, as with [`has_ttl`](Self::has_ttl). Both
    /// stay here so the three binding-config accessors read as one group, and
    /// a read does call [`keyset_limit`](Self::keyset_limit).
    fn capacity(&self) -> Option<NonZeroUsize>;

    /// Reads, decodes, and resolves the visible value at `key`.
    ///
    /// # Errors
    ///
    /// An access error from the engine, a codec error (Permanent) when the
    /// cell bytes do not decode, or a resolution error from the resolver.
    fn get<T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;

    /// Reads, decodes, and resolves `keys` as one aligned batch: `result[i]`
    /// answers `keys[i]`, duplicates are answered per position, and an absent
    /// cell reads `None`.
    ///
    /// The cell reads run in sequential sub-batches. Two owner reads that can
    /// repair must not race on one collection's marker. The typed resolves fan
    /// out across the whole call in an order-preserving window.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    fn get_many<'a, T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        keys: impl IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;

    /// Tests `keys` for presence as one aligned batch. Each result answers the
    /// same input position. Duplicate keys keep their positions.
    ///
    /// # Errors
    ///
    /// Returns an engine access error.
    fn contains_many<'a, T: CellType>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        keys: impl IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    ) -> impl Future<Output = Result<CellBuffer<bool>, StateAccessError>> + Send;

    /// Whether a stored cell exists at `key`, **without decoding its value or
    /// running the resolver**. The guarantee is "no decode, no resolve", not
    /// "no I/O": a cold cache still reaches the store.
    ///
    /// # Errors
    ///
    /// An access error from the engine.
    fn contains<T: CellType>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send;
}

/// The mutation commands, implemented only by the write operation.
///
/// `set` and `clear` are synchronous. They encode and stage, and do no I/O, so
/// a future would add a suspension point without work. `set` can fail only at
/// typed encoding, and a point clear cannot fail after admission.
/// [`take`](Self::take) is the one exception, because it reads first.
pub(crate) trait CollectionWrite: CollectionRead {
    /// Reads, decodes, and resolves the value at `key`, then stages a clear of
    /// that cell. This is the one supported read-then-mutate composite.
    ///
    /// The read completes first. A read error stages nothing. `Ok(None)` still
    /// clears the addressed residue.
    ///
    /// The trait declares this method and gives no default body. A default body
    /// over an opaque `Self` cannot prove the returned future `Send` for its
    /// `&mut Self` and `&BorrowedKeyOf<T>` captures.
    ///
    /// # Errors
    ///
    /// As [`CollectionRead::get`].
    fn take<T>(
        &mut self,
        family: CellFamily<Self::Layout, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>> + Send
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, Self::Session>;

    /// Stages a write of `value` at its typed address.
    ///
    /// # Errors
    ///
    /// A codec error (Permanent) when the value fails to encode.
    fn set<T: CellType>(
        &mut self,
        address: CellAddress<Self::Layout, T>,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>>;

    /// Stages a clear at its typed address.
    fn clear<T: CellType>(&mut self, address: CellAddress<Self::Layout, T>);

    /// Stages an absence over the collection's whole declared layout. One
    /// payload-free journal entry expands to every active and reserved section
    /// at merge, so a removed family's legacy rows are erased too. From this
    /// program point the collection reads empty. Later commands in the same
    /// invocation repopulate it.
    fn clear_collection(&mut self)
    where
        Self::Layout: CollectionLayout;
}

/// Seals the author-facing command traits. Only the two operation types
/// implement them, so a helper bounded by them always receives real admission.
pub(crate) mod sealed_ops {
    /// The seal marker. See the module doc.
    pub trait CollectionOperation {}
}
