//! An ordered set that stores only membership.
//!
//! A set stores one zero-byte cell per member. It shares the map keyset
//! format and keeps the same membership rules.

use super::map::{
    Keyset, KeysetLayout, MapKeysetCodec, MapKeysetKey, MapStateError, PriorKeyset,
    decoded_key_list, is_oversized, read_keyset_state, subtract_keyset, update_keyset,
};
use super::{CollectionSpec, Descriptor, Keyed};
use crate::codec::{UnitCodec, UnitCodecError};
use crate::state::cell_key::{Direction, ScanEdge};
use crate::state::collection::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, Constraints,
    JOURNAL_INLINE, Plan, StateSession, WritableStateSession, collection_layout,
    collection_methods, same_token, spec_matches,
};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::borrow::Borrow;
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
    dir: Direction,
    constraints: Constraints,
}

impl<'a, S, KC> SetQuery<'a, S, KC>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Borrowed: Display,
{
    /// Starts at `key`.
    pub fn from<Q>(mut self, key: &Q) -> Self
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        self.constraints.start = ScanEdge::Included(KC::encode(key.borrow()));
        self
    }

    /// Starts after `key`.
    pub fn after<Q>(mut self, key: &Q) -> Self
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        self.constraints.start = ScanEdge::Excluded(KC::encode(key.borrow()));
        self
    }

    /// Stops at `key`.
    pub fn to<Q>(mut self, key: &Q) -> Self
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        self.constraints.end = ScanEdge::Included(KC::encode(key.borrow()));
        self
    }

    /// Stops before `key`.
    pub fn before<Q>(mut self, key: &Q) -> Self
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        self.constraints.end = ScanEdge::Excluded(KC::encode(key.borrow()));
        self
    }

    /// Sets the maximum number of present members.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.constraints.limit = Some(limit);
        self
    }

    /// Replaces all query constraints.
    pub(crate) fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Streams live members in the query direction.
    pub fn keys(self) -> impl Stream<Item = SetItem<KC>> + 'a {
        let span = info_span!(
            "set.keys",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.dir,
        );
        try_stream! {
            let plan = self.handle.stream_plan(self.dir).instrument(span.clone()).await?;
            let inner = plan.keys(self.constraints);
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
    KC::Borrowed: Display,
{
    /// Inserts `key` into the set.
    ///
    /// # Errors
    ///
    /// Returns a codec error or a session access error.
    #[instrument(name = "set.insert", skip_all, fields(collection = self.cells.name().as_str(), set.key = %<Q as Borrow<KC::Borrowed>>::borrow(key)), err)]
    #[write(op)]
    pub async fn insert<Q>(&self, key: &Q) -> Result<(), SetStateError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        let coordinate = KC::encode(key.borrow());
        let prior = read_keyset_state(op).await?;
        op.set(SetKind::<KC>::MEMBERS, key, ())?;
        update_keyset(op, coordinate, prior)
    }

    /// Removes `key` from the set.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.remove", skip_all, fields(collection = self.cells.name().as_str(), set.key = %<Q as Borrow<KC::Borrowed>>::borrow(key)), err)]
    #[write(op)]
    pub async fn remove<Q>(&self, key: &Q) -> Result<(), SetStateError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        let coordinate = KC::encode(key.borrow());
        let prior = read_keyset_state(op).await?;
        op.clear(SetKind::<KC>::MEMBERS, key);
        subtract_keyset(op, &coordinate, prior)
    }

    /// Tests whether `key` belongs to the set.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.contains", skip_all, fields(collection = self.cells.name().as_str(), set.key = %<Q as Borrow<KC::Borrowed>>::borrow(key)), err)]
    #[read(op)]
    pub async fn contains<Q>(&self, key: &Q) -> Result<bool, SetStateError>
    where
        Q: Borrow<KC::Borrowed> + ?Sized,
    {
        Ok(op.contains(SetKind::<KC>::MEMBERS, key).await?)
    }

    /// Tests each key for membership in input order. The result reserves the
    /// iterator's lower size bound. An exact-size iterator allocates once.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(name = "set.contains_many", skip_all, fields(collection = self.cells.name().as_str()), err)]
    #[read(op)]
    pub async fn contains_many<'a, I, Q>(&self, keys: I) -> Result<Vec<bool>, SetStateError>
    where
        I: IntoIterator<Item = &'a Q>,
        I::IntoIter: Send,
        Q: Borrow<KC::Borrowed> + Sync + ?Sized + 'a,
    {
        Ok(op
            .contains_many(SetKind::<KC>::MEMBERS, keys.into_iter())
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
        dir: Direction,
    ) -> Result<Plan<S, Keyed<KC, UnitCodec>>, SetStateError> {
        // Keep this plan local because its member family differs from Map's value
        // family.
        let coordinates = match read_keyset_state(op).await? {
            PriorKeyset::Absent => {
                return Ok(Plan::Points(op.coordinates(
                    SetKind::<KC>::MEMBERS,
                    Vec::new(),
                    dir,
                )));
            }
            PriorKeyset::Malformed | PriorKeyset::Decoded(Keyset::Overflowed) => {
                return Ok(Plan::Scan(op.range(SetKind::<KC>::MEMBERS, dir)));
            }
            PriorKeyset::Decoded(Keyset::Tracked(coordinates)) => coordinates,
        };
        if is_oversized(&coordinates, op.keyset_limit()) {
            return Ok(Plan::Scan(op.range(SetKind::<KC>::MEMBERS, dir)));
        }
        let Some(mut keys) = decoded_key_list::<KC>(&coordinates) else {
            return Ok(Plan::Scan(op.range(SetKind::<KC>::MEMBERS, dir)));
        };
        if dir == Direction::Backward {
            keys.reverse();
        }
        Ok(Plan::Points(op.coordinates(
            SetKind::<KC>::MEMBERS,
            keys,
            dir,
        )))
    }

    /// Streams live members in the direction `dir`.
    pub fn keys(&self, dir: Direction) -> impl Stream<Item = SetItem<KC>> + '_ {
        self.query(dir).keys()
    }

    /// Builds a directional set query.
    pub fn query(&self, dir: Direction) -> SetQuery<'_, S, KC> {
        SetQuery {
            handle: self,
            dir,
            constraints: Constraints::default(),
        }
    }

    /// Reports whether the set has no live members.
    ///
    /// # Errors
    ///
    /// Returns a key codec error or a session access error.
    #[instrument(name = "set.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, SetStateError> {
        let keys = self
            .query(Direction::Forward)
            .limit(NonZeroUsize::MIN)
            .keys();
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
