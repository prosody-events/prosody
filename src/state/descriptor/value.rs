//! The single-value collection.
//!
//! Value is the smallest complete collection: one durable family holding one
//! [`UnitKey`]-addressed cell. Its handle is the reference shape for a
//! collection author — a bound [`Collection`] plus marked methods whose bodies
//! are ordinary Rust over the scoped operation `op`.

use super::{CellCodecError, CellStateError, CellType, Descriptor, ResolvedOf, WriteOf};
use crate::codec::JsonCodec;
use crate::state::collection::{
    Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE, StateSession,
    WritableStateSession, collection_layout, collection_methods, same_token,
};
use crate::state::order_codec::UnitKey;
use crate::state::{CollectionKindId, StoreOutcome};
use educe::Educe;
use tracing::instrument;

collection_layout! {
    /// The Value collection kind: a single [`UnitKey`]-addressed cell of type
    /// `T`.
    pub struct ValueKind<T> {
        /// The value itself.
        #[id(0)]
        ENTRIES: T,
    }
}

/// Value's per-invocation mutation maximum: `set` and `clear` each stage
/// exactly one, and no Value method stages twice.
const VALUE_MAX_MUTATIONS: usize = 1;

const _: () = assert!(
    VALUE_MAX_MUTATIONS <= JOURNAL_INLINE,
    "a Value invocation must stay inside the journal's inline capacity"
);

/// Value's durable layout, frozen. The ids and format tokens below address
/// every Value cell ever written; changing one silently re-points existing
/// rows, and no type can compare this crate against yesterday's schema. The
/// pin is a compile-time assertion rather than a test so it cannot be filtered
/// out of a run.
const _: () = {
    let families = <ValueKind<JsonCodec> as CollectionLayout>::DESCRIPTOR;
    assert!(
        families.len() == 1,
        "Value declares exactly one cell family"
    );
    assert!(
        families[0].id() == 0,
        "Value's entries family is durably section 0"
    );
    assert!(
        same_token(families[0].key_format(), "unit.v1"),
        "Value's single cell is durably addressed by the unit key"
    );
    assert!(
        same_token(families[0].format(), "json"),
        "the default Value cell is durably JSON-encoded"
    );
    assert!(
        <ValueKind<JsonCodec> as CollectionLayout>::SECTIONS.len() == 1,
        "Value's reset domain is its one family"
    );
    assert!(
        <ValueKind<JsonCodec> as CollectionLayout>::RESERVED.is_empty(),
        "Value has never removed a family"
    );
};

/// Descriptor for a codec-backed single value collection.
///
/// Generic over a [`CellType`] `T` — a plain [`Codec`](crate::codec::Codec)
/// (the default [`JsonCodec`] stores [`serde_json::Value`] cells, the same
/// default as the consumer's message payload) or a codec paired with a resolver
/// via [`WithResolver`](super::WithResolver). Declare via [`value_state`] (see
/// [`Descriptor::new`] for the `name` contract); for a typed cell, declare a
/// codec (`CartCodec: Codec<Payload = Cart>`) and annotate the binding
/// `ValueDescriptor<CartCodec>`.
pub type ValueDescriptor<T = JsonCodec> = Descriptor<ValueKind<T>>;

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

impl<T: CellType<Key = UnitKey>> super::CollectionSpec for ValueKind<T> {
    type Cell = T;
    type Handle<S: StateSession> = ValueHandle<S, T>;

    const KIND: CollectionKindId = CollectionKindId::Value;

    fn handle<S: StateSession>(collection: Collection<S, Self>) -> ValueHandle<S, T> {
        ValueHandle { cells: collection }
    }
}

/// Typed, owned handle over a codec-backed value collection.
///
/// Owns the bound collection, whose session clone is `Clone + Send + Sync +
/// 'static` (an FFI requirement). The cell type's codec runs only at the edges
/// — `get` decodes, `set` encodes — and its resolver maps the decoded cell to
/// and from the exposed value.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct ValueHandle<S, T> {
    cells: Collection<S, ValueKind<T>>,
}

#[collection_methods(field = cells, session = S)]
impl<S, T> ValueHandle<S, T>
where
    S: StateSession,
    T: CellType<Key = UnitKey>,
{
    /// Reads, decodes, and resolves the current visible value.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session, a codec error (Permanent)
    /// when the cell bytes do not decode, or a resolution error from the
    /// resolver.
    #[instrument(name = "value.get", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn get(&self) -> Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>> {
        op.get(ValueKind::<T>::ENTRIES, &()).await
    }

    /// Lowers `value` through the resolver, encodes it, and stages a write.
    ///
    /// # Errors
    ///
    /// Returns a codec error (Permanent) when the cell fails to encode, or
    /// an access error from the session.
    #[instrument(name = "value.set", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn set(
        &self,
        value: WriteOf<'_, T>,
    ) -> Result<(), CellStateError<CellCodecError<T>>> {
        op.set(ValueKind::<T>::ENTRIES, &(), value)
    }

    /// Stages a clear of the value.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "value.clear", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), CellStateError<CellCodecError<T>>> {
        op.clear(ValueKind::<T>::ENTRIES, &());
        Ok(())
    }

    /// Durably commits the staged write mid-handler, so it survives a restart
    /// after failure. At-least-once; see
    /// [`CellWrite::commit`](crate::state::session::CellWrite::commit) for the
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(name = "value.commit", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, CellStateError<CellCodecError<T>>>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards the staged uncommitted write, reverting reads to the last
    /// [`commit`](Self::commit) — or the pre-event committed value if none.
    /// Infallible; see
    /// [`CellWrite::rollback`](crate::state::session::CellWrite::rollback) for
    /// the contract.
    #[instrument(name = "value.rollback", skip_all, fields(collection = self.cells.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
    }
}
