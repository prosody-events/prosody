//! Shared map and set membership transitions and query plans.
//!
//! Only stream plans read the keyset. Point and batch reads address member
//! cells directly and never consult it.

use super::keyset::{KEYSET_BYTE_CEILING, is_oversized, tracked_frame_len};
use super::{Keyset, KeysetFrameError, MapKeysetCodec, MapKeysetKey, MapStateError, Query};
use crate::state::StateName;
use crate::state::cell::Presence;
use crate::state::cell_key::{Coordinate, Direction, ScanEdge};
use crate::state::collection::{
    CellFamily, Collection, CollectionRead, CollectionWrite, JOURNAL_INLINE, Plan, ReadOperation,
    StateSession,
};
use crate::state::descriptor::{
    CellCodecError, CellStateError, CollectionSpec, KeyOf, Keyed, WriteOf,
};
use futures::StreamExt;
use std::error::Error;
use std::num::NonZeroUsize;
use tracing::{Span, warn};

/// Insert and remove each stage one member mutation and one keyset write.
/// Clear stages one layout reset.
const KEYSET_MAX_MUTATIONS: usize = 2;
const _: () = assert!(
    KEYSET_MAX_MUTATIONS <= JOURNAL_INLINE,
    "a map or set operation must fit in the inline journal"
);

/// A collection whose keyset and members share one atomic mutation.
/// The layout fixes both families at compile time.
/// Concrete descriptor and query methods keep this trait out of public bounds.
pub(crate) trait KeysetLayout: CollectionSpec {
    /// The family whose cells define membership.
    const MEMBERS: CellFamily<Self, Self::Cell>;

    /// The keyset family for this layout.
    const KEYSET: CellFamily<Self, Keyed<MapKeysetKey, MapKeysetCodec>>;

    /// The span every stream under this layout runs in.
    /// A span name must be a literal, so a trait constant cannot replace this
    /// function.
    fn stream_span(collection: &StateName, dir: Direction, projection: &'static str) -> Span;
}

/// The member cell selected by an operation's layout.
type MemberOf<C> = <<C as CollectionRead>::Layout as CollectionSpec>::Cell;

/// Reports whether the member family has no live cells.
pub(crate) async fn is_empty<S, L>(
    cells: &Collection<S, L>,
) -> Result<bool, MapStateError<CellCodecError<L::Cell>>>
where
    S: StateSession,
    L: KeysetLayout,
{
    let plan = cells
        .read(async |op| {
            op.range(
                L::MEMBERS,
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

/// Stages the member and keyset in one admitted operation.
/// Read the prior keyset before the member write becomes visible.
pub(crate) async fn insert<C>(
    op: &mut C,
    key: &KeyOf<MemberOf<C>>,
    value: WriteOf<'_, MemberOf<C>>,
) -> Result<(), MapStateError<CellCodecError<MemberOf<C>>>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
{
    let prior = read_keyset_state(op).await?;
    let address = C::Layout::MEMBERS.at(key);
    let keyset = prior.insert(
        address.coordinate(),
        op.keyset_limit(),
        op.has_ttl(),
        op.name(),
    );
    write_keyset(op, keyset)?;
    op.set(address, value).map_err(Into::into)
}

/// Removes the member and updates its keyset in one admitted operation.
pub(crate) async fn remove<C>(
    op: &mut C,
    key: &KeyOf<MemberOf<C>>,
) -> Result<(), MapStateError<CellCodecError<MemberOf<C>>>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
{
    let prior = read_keyset_state(op).await?;
    let address = C::Layout::MEMBERS.at(key);
    write_keyset(op, prior.remove(address.coordinate()))?;
    op.clear(address);
    Ok(())
}

/// The keyset before a member mutation. Invalid frames select a scan and heal
/// on insertion.
enum PriorKeyset {
    /// No keyset cell exists for a fresh or expired collection.
    Absent,

    /// A well-formed keyset.
    Decoded(Keyset),

    /// The stored frame did not decode. The next insertion repairs it.
    Malformed,
}

/// Selects bounded point reads or a range scan from the shared keyset.
pub(crate) async fn plan<S, L>(
    op: &mut ReadOperation<'_, S, L>,
    query: &Query,
) -> Result<Plan<S, L::Cell>, MapStateError<CellCodecError<L::Cell>>>
where
    S: StateSession,
    L: KeysetLayout,
{
    let keyset = read_keyset_state(op).await?;
    let range = || {
        op.range(
            L::MEMBERS,
            query.start.clone(),
            query.dir,
            query.end.clone(),
        )
    };
    let coordinates = match keyset {
        PriorKeyset::Absent => {
            return Ok(op.coordinates(L::MEMBERS, Vec::new()));
        }
        // Overflowed falls to the scan with no warning; Malformed already
        // warned in `read_keyset_state`.
        PriorKeyset::Malformed | PriorKeyset::Decoded(Keyset::Overflowed) => {
            return Ok(range());
        }
        PriorKeyset::Decoded(Keyset::Tracked(coordinates)) => coordinates,
    };
    if is_oversized(&coordinates, op.keyset_limit()) {
        warn!(
            collection = op.name().as_str(),
            "keyset exceeds the registered limit; use a range scan"
        );
        return Ok(range());
    }
    Ok(op.coordinates(L::MEMBERS, query.select(coordinates)))
}

/// Reads the keyset. Invalid frames select a scan; access errors propagate.
async fn read_keyset_state<C>(
    op: &mut C,
) -> Result<PriorKeyset, MapStateError<CellCodecError<MemberOf<C>>>>
where
    C: CollectionRead,
    C::Layout: KeysetLayout,
{
    match op.get(C::Layout::KEYSET, &()).await {
        Ok(None) => Ok(PriorKeyset::Absent),
        Ok(Some(keyset)) => Ok(PriorKeyset::Decoded(keyset)),
        Err(CellStateError::Codec(_)) => {
            warn!(
                collection = op.name().as_str(),
                "keyset frame did not decode; use a range scan"
            );
            Ok(PriorKeyset::Malformed)
        }
        Err(err) => Err(keyset_err(err)),
    }
}

impl PriorKeyset {
    /// Returns a replacement only when membership or its TTL changes.
    fn insert(
        self,
        coordinate: &Coordinate,
        limit: usize,
        ttl: bool,
        collection: &StateName,
    ) -> Option<Keyset> {
        let mut keys = match self {
            Self::Absent => Vec::with_capacity(1),
            Self::Malformed => return Some(Keyset::Overflowed),
            Self::Decoded(Keyset::Overflowed) => return ttl.then_some(Keyset::Overflowed),
            Self::Decoded(Keyset::Tracked(keys)) => keys,
        };
        let frame_len = tracked_frame_len(&keys);
        // Apply a lowered bound before duplicate detection.
        if keys.len() > limit || frame_len.is_none_or(|len| len > KEYSET_BYTE_CEILING) {
            warn!(
                collection = collection.as_str(),
                "keyset exceeds its bound; store Overflowed"
            );
            return Some(Keyset::Overflowed);
        }
        match keys.binary_search(coordinate) {
            Ok(_) => ttl.then_some(Keyset::Tracked(keys)),
            Err(position) => {
                let len = frame_len
                    .and_then(|len| len.checked_add(4))
                    .and_then(|len| len.checked_add(coordinate.as_bytes().len()));
                if keys.len() == limit || len.is_none_or(|len| len > KEYSET_BYTE_CEILING) {
                    return Some(Keyset::Overflowed);
                }
                keys.insert(position, coordinate.clone());
                Some(Keyset::Tracked(keys))
            }
        }
    }

    /// Removes known membership. Invalid frames become overflowed.
    fn remove(self, coordinate: &Coordinate) -> Option<Keyset> {
        match self {
            Self::Malformed => Some(Keyset::Overflowed),
            Self::Decoded(Keyset::Tracked(mut keys)) => {
                let Ok(position) = keys.binary_search(coordinate) else {
                    return None;
                };
                keys.remove(position);
                Some(Keyset::Tracked(keys))
            }
            Self::Absent | Self::Decoded(Keyset::Overflowed) => None,
        }
    }
}

/// Stages a replacement frame in the member's admitted operation.
fn write_keyset<C>(
    op: &mut C,
    keyset: Option<Keyset>,
) -> Result<(), MapStateError<CellCodecError<MemberOf<C>>>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
{
    if let Some(keyset) = keyset {
        op.set(C::Layout::KEYSET.at(&()), keyset)
            .map_err(keyset_err)?;
    }
    Ok(())
}

/// Preserves access and key errors. Maps frame errors to the separate keyset
/// error variant.
fn keyset_err<E>(err: CellStateError<KeysetFrameError>) -> MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    match err {
        CellStateError::Access(e) => CellStateError::Access(e).into(),
        CellStateError::Key(e) => CellStateError::Key(e).into(),
        CellStateError::Codec(e) => MapStateError::KeysetFrame(e),
    }
}
