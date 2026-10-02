//! An ordered set that stores only membership.
//!
//! A set stores one zero-byte cell per member. It shares the map keyset
//! format and keeps the same membership rules.

use super::map::membership::{self, KeysetLayout};
use super::map::projected;
use super::map::{KeyItem, MapKeysetCodec, MapKeysetKey, MapStateError};
use super::{CollectionSpec, Descriptor, Keyed};
use crate::codec::{UnitCodec, UnitCodecError};
use crate::state::cell::Presence;
use crate::state::cell_key::Direction;
use crate::state::collection::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, StateSession,
    WritableStateSession, collection_layout, collection_methods, same_token, spec_matches,
};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::{BorrowedKeyQuery, CellBuffer, CollectionKindId, StateName, StoreOutcome};
use crate::state::{KeyQuery, KeyRead, ReadQuery, ReadSource};
use educe::Educe;
use futures::stream::Stream;
use std::borrow::Borrow;
use std::fmt::Display;
use tracing::{Span, field::Empty, info_span, instrument};

collection_layout! {
    /// The set collection kind has one keyset cell and one cell per member.
    pub struct SetKind<KC> {
        /// The keyset cell tracks current membership.
        #[id(0)]
        KEYSET: Keyed<MapKeysetKey, MapKeysetCodec>,
        /// Each member has one zero-byte cell.
        #[id(1)]
        MEMBERS: Keyed<KC, UnitCodec>,
    }
}

impl<KC: OrderedKeyCodec> KeysetLayout for SetKind<KC> {
    const KEYSET: CellFamily<Self, Keyed<MapKeysetKey, MapKeysetCodec>> = Self::KEYSET;
    const MEMBERS: CellFamily<Self, Self::Cell> = Self::MEMBERS;

    fn stream_span(collection: &StateName, dir: Direction, projection: &'static str) -> Span {
        info_span!("set.stream", collection = collection.as_str(), direction = ?dir, projection)
    }
}

type FrozenLayout = SetKind<I64KeyCodec>;
const _: () = {
    let families = <FrozenLayout as CollectionLayout>::DESCRIPTOR;
    assert!(families.len() == 2, "Set has two cell families");
    assert!(families[0].id() == 0, "Set uses section 0 for its keyset");
    assert!(
        same_token(families[0].key_format(), "map-keyset-key.v1"),
        "Set shares the map keyset address"
    );
    assert!(
        same_token(families[0].format(), "map-keyset.v1"),
        "Set shares the map keyset format"
    );
    assert!(families[1].id() == 1, "Set uses section 1 for members");
    assert!(
        spec_matches::<FrozenLayout>(families[1]),
        "Set members match the collection cell type"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::SECTIONS.len() == 2,
        "Set clear resets both sections"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::RESERVED.is_empty(),
        "Set has no reserved sections"
    );
};

/// Descriptor for a presence-only ordered set.
pub type SetDescriptor<KC> = Descriptor<SetKind<KC>>;

/// Error returned by set operations.
pub type SetStateError = MapStateError<UnitCodecError>;

impl<KC> CollectionSpec for SetKind<KC>
where
    KC: OrderedKeyCodec,
{
    type Cell = Keyed<KC, UnitCodec>;
    type Handle<S: StateSession> = SetHandle<S, KC>;

    const KIND: CollectionKindId = CollectionKindId::Set;

    fn handle<S: StateSession>(collection: Collection<S, Self>) -> SetHandle<S, KC> {
        SetHandle { cells: collection }
    }
}

/// Typed handle for a presence-only ordered set.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct SetHandle<S, KC> {
    cells: Collection<S, SetKind<KC>>,
}

#[collection_methods(field = cells, session = S)]
impl<S, KC> SetHandle<S, KC>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
{
    /// Inserts `key` into the set.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(name = "set.insert", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[write(op)]
    pub async fn insert(&self, key: &KC::Borrowed) -> Result<(), SetStateError>
    where
        KC::Borrowed: Display,
    {
        membership::insert(op, key, ()).await
    }

    /// Removes `key` from the set.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(name = "set.remove", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[write(op)]
    pub async fn remove(&self, key: &KC::Borrowed) -> Result<(), SetStateError>
    where
        KC::Borrowed: Display,
    {
        membership::remove(op, key).await
    }

    /// Tests whether `key` belongs to the set.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(name = "set.contains", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[read(op)]
    pub async fn contains(&self, key: &KC::Borrowed) -> Result<bool, SetStateError>
    where
        KC::Borrowed: Display,
    {
        Ok(op.contains(SetKind::<KC>::MEMBERS, key).await?)
    }

    /// Tests each key for membership in input order.
    ///
    /// # Errors
    ///
    /// Returns a key codec error (`Permanent`) when a key does not encode, or a
    /// session access error.
    #[instrument(name = "set.contains_many", skip_all, fields(collection = self.cells.name().as_str(), keys = Empty), err)]
    #[read(op)]
    pub async fn contains_many<'a, Q, I>(&self, keys: I) -> Result<CellBuffer<bool>, SetStateError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
    {
        let present = op
            .contains_many(SetKind::<KC>::MEMBERS, keys.into_iter().map(Borrow::borrow))
            .await?;
        Span::current().record("keys", present.len() as i64);
        Ok(present)
    }

    /// Removes all members.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.clear", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), SetStateError> {
        op.clear_collection();
        Ok(())
    }

    /// Builds a query over live members in ascending key order.
    pub fn keys<'a>(
        &'a self,
    ) -> KeyRead<
        'a,
        KC,
        impl ReadSource<
            Query = BorrowedKeyQuery<'a, KC>,
            Output: Stream<Item = KeyItem<SetKind<KC>>> + Send,
        > + Clone
        + use<'a, S, KC>,
    > {
        ReadQuery::new(KeyQuery::new(), move |query: BorrowedKeyQuery<'a, KC>| {
            projected::<_, _, Presence>(&self.cells, query)
        })
    }

    /// Reports whether the set has no live members.
    ///
    /// This reads the member section, not the keyset. After a split commit
    /// leaves keyset residue, it can report a live member that `keys` does not
    /// list.
    ///
    /// # Errors
    ///
    /// Returns a key codec error or a session access error.
    #[instrument(name = "set.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, SetStateError> {
        membership::is_empty(&self.cells).await
    }

    /// Commits buffered set operations.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.commit", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn commit(&self) -> Result<StoreOutcome, SetStateError>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards buffered set operations.
    #[instrument(name = "set.rollback", skip_all, fields(collection = self.cells.name().as_str()))]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
    }
}

/// Declares a presence-only ordered set named `name`.
#[must_use]
pub fn set_state<KC>(name: &str) -> SetDescriptor<KC>
where
    KC: OrderedKeyCodec,
{
    SetDescriptor::new(name)
}

impl<KC> Descriptor<SetKind<KC>> {
    /// Sets the number of live members the set tracks before overflow.
    /// The map descriptor's `keyset_limit` documents the shared contract.
    #[must_use]
    pub fn keyset_limit(mut self, limit: usize) -> Self {
        self.def.keyset_limit = limit;
        self
    }
}
