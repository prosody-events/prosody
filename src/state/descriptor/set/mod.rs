//! An ordered set that stores only membership.
//!
//! A set stores one zero-byte cell per member. It shares the map keyset
//! format and keeps the same membership rules.

use super::map::membership::{self, KeysetLayout};
use super::map::{MapKeysetCodec, MapKeysetKey, MapStateError, Query};
use super::{CollectionSpec, Descriptor, Keyed};
use crate::codec::{UnitCodec, UnitCodecError};
use crate::state::cell::Presence;
use crate::state::cell_key::{Direction, ScanEdge};
use crate::state::collection::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE,
    Plan, StateSession, WritableStateSession, collection_layout, collection_methods, same_token,
    spec_matches,
};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::fmt::Display;
use std::num::NonZeroUsize;
use tracing::{Instrument, info_span, instrument};

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
}

type FrozenLayout = SetKind<I64KeyCodec>;
const SET_MAX_MUTATIONS: usize = 2;
const _: () = assert!(
    SET_MAX_MUTATIONS <= JOURNAL_INLINE,
    "a set operation must fit in the inline journal"
);
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

type SetItem<KC> = Result<<KC as OrderedKeyCodec>::Key, SetStateError>;

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

/// A directional set member stream query.
///
/// Build one with [`SetHandle::query`]. Finish it with [`keys`](Self::keys).
/// `from` and `to` include their member. `after` and `before` exclude their
/// member. State all edges in iteration order. A later call for the same edge
/// replaces the earlier call. A start past the end yields an empty stream.
#[must_use]
pub struct SetQuery<'a, S, KC> {
    handle: &'a SetHandle<S, KC>,
    query: Query,
}

impl<'a, S, KC> SetQuery<'a, S, KC>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
{
    /// Starts at `key`.
    pub fn from(mut self, key: &KC::Key) -> Self {
        self.query.start = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Starts after `key`.
    pub fn after(mut self, key: &KC::Key) -> Self {
        self.query.start = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Stops at `key`.
    pub fn to(mut self, key: &KC::Key) -> Self {
        self.query.end = ScanEdge::Included(KC::encode(key));
        self
    }

    /// Stops before `key`.
    pub fn before(mut self, key: &KC::Key) -> Self {
        self.query.end = ScanEdge::Excluded(KC::encode(key));
        self
    }

    /// Sets the maximum number of present members.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    pub(crate) fn new(handle: &'a SetHandle<S, KC>, query: Query) -> Self {
        Self { handle, query }
    }

    /// Streams live members in the query direction.
    pub fn keys(self) -> impl Stream<Item = SetItem<KC>> + 'a {
        let span = info_span!(
            "set.keys",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.query.dir,
        );
        try_stream! {
            let plan = self.handle.stream_plan(&self.query).instrument(span.clone()).await?;
            let inner = plan.with_limit(self.query.limit).projected::<Presence>();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
    }
}

#[collection_methods(field = cells, session = S)]
impl<S, KC> SetHandle<S, KC>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
{
    /// Inserts `key` into the set.
    ///
    /// # Errors
    ///
    /// Returns a codec error or a session access error.
    #[instrument(name = "set.insert", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[write(op)]
    pub async fn insert(&self, key: KC::Key) -> Result<(), SetStateError> {
        membership::insert(op, &key, ()).await
    }

    /// Removes `key` from the set.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.remove", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[write(op)]
    pub async fn remove(&self, key: &KC::Key) -> Result<(), SetStateError> {
        membership::remove(op, key).await
    }

    /// Tests whether `key` belongs to the set.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.contains", skip_all, fields(collection = self.cells.name().as_str(), set.key = %key), err)]
    #[read(op)]
    pub async fn contains(&self, key: &KC::Key) -> Result<bool, SetStateError> {
        Ok(op.contains(SetKind::<KC>::MEMBERS, key).await?)
    }

    /// Tests each key for membership in input order.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.contains_many", skip_all, fields(collection = self.cells.name().as_str(), keys = keys.len() as i64), err)]
    #[read(op)]
    pub async fn contains_many(&self, keys: &[KC::Key]) -> Result<Vec<bool>, SetStateError> {
        Ok(op
            .contains_many(SetKind::<KC>::MEMBERS, keys)
            .await?
            .into_vec())
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

    #[read(op)]
    async fn stream_plan(
        &self,
        query: &Query,
    ) -> Result<Plan<S, Keyed<KC, UnitCodec>>, SetStateError> {
        membership::plan(op, query).await
    }

    /// Streams live members in the direction `dir`.
    pub fn keys(&self, dir: Direction) -> impl Stream<Item = SetItem<KC>> + '_ {
        self.query(dir).keys()
    }

    /// Builds a directional set query.
    pub fn query(&self, dir: Direction) -> SetQuery<'_, S, KC> {
        SetQuery::new(self, Query::new(dir))
    }

    /// Reports whether the set has no live members.
    ///
    /// # Errors
    ///
    /// Returns a key codec error or a session access error.
    #[instrument(name = "set.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, SetStateError> {
        let plan = self
            .cells
            .read(async |op| {
                op.range(
                    SetKind::<KC>::MEMBERS,
                    ScanEdge::Unbounded,
                    Direction::Forward,
                    ScanEdge::Unbounded,
                )
                .with_limit(Some(NonZeroUsize::MIN))
            })
            .await;
        let keys = plan.projected::<Presence>();
        futures::pin_mut!(keys);
        Ok(keys.next().await.transpose()?.is_none())
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
    /// Sets the maximum member count for tracked reads. Larger sets use
    /// scans.
    #[must_use]
    pub fn keyset_limit(mut self, limit: usize) -> Self {
        self.def.keyset_limit = limit;
        self
    }
}

#[cfg(test)]
mod tests;
