//! Shared map and set membership transitions and query plans.

use super::keyset::{KEYSET_BYTE_CEILING, is_oversized, tracked_frame_len};
use super::{Keyset, KeysetFrameError, MapKeysetCodec, MapKeysetKey, MapStateError, Query};
use crate::state::cell_key::Coordinate;
use crate::state::collection::{
    CellFamily, CollectionRead, CollectionWrite, Plan, ReadOperation, StateSession,
};
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, CollectionSpec, KeyOf, Keyed, WriteOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use std::error::Error;
use std::slice::from_ref;
use tracing::warn;

/// A collection whose keyset and members share one atomic mutation.
/// The layout fixes both families at compile time.
pub(crate) trait KeysetLayout: CollectionSpec {
    /// The family whose cells define membership.
    const MEMBERS: CellFamily<Self, Self::Cell>;

    /// The keyset family for this layout.
    const KEYSET: CellFamily<Self, Keyed<MapKeysetKey, MapKeysetCodec>>;
}

/// The member cell selected by an operation's layout.
type MemberOf<C> = <<C as CollectionRead>::Layout as CollectionSpec>::Cell;

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
    let coordinate = <MemberOf<C> as CellType>::Key::encode(key);
    let prior = read_keyset_state(op).await?;
    op.set(C::Layout::MEMBERS, key, value)?;
    update_keyset(op, coordinate, prior)
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
    let coordinate = <MemberOf<C> as CellType>::Key::encode(key);
    let prior = read_keyset_state(op).await?;
    op.clear(C::Layout::MEMBERS, key);
    subtract_keyset(op, &coordinate, prior)
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
async fn read_keyset_state<C, E>(op: &mut C) -> Result<PriorKeyset, MapStateError<E>>
where
    C: CollectionRead,
    C::Layout: KeysetLayout,
    E: Error + Send + Sync + 'static,
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

/// Adds a coordinate and enforces both keyset bounds.
/// Every insertion with a TTL refreshes the keyset, including unchanged or
/// overflowed membership.
fn update_keyset<C, E>(
    op: &mut C,
    coordinate: Coordinate,
    prior: PriorKeyset,
) -> Result<(), MapStateError<E>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
    E: Error + Send + Sync + 'static,
{
    let limit = op.keyset_limit();
    let ttl = op.has_ttl();
    match prior {
        // Malformed → heal to Overflowed (already warned at read).
        PriorKeyset::Malformed => write_keyset(op, Keyset::Overflowed),
        // A fresh singleton must fit both bounds.
        PriorKeyset::Absent => {
            if is_oversized(from_ref(&coordinate), limit) {
                write_keyset(op, Keyset::Overflowed)
            } else {
                write_keyset(op, Keyset::Tracked(vec![coordinate]))
            }
        }
        // Overflowed is one-way: no write, except the TTL refresh.
        PriorKeyset::Decoded(Keyset::Overflowed) => {
            if ttl {
                write_keyset(op, Keyset::Overflowed)
            } else {
                Ok(())
            }
        }
        PriorKeyset::Decoded(Keyset::Tracked(keys)) => {
            update_tracked(op, coordinate, keys, limit, ttl)
        }
    }
}

/// Removes a tracked coordinate. Removal can reduce an oversized frame below
/// its limit. Unknown membership stays overflowed until clear or expiry.
fn subtract_keyset<C, E>(
    op: &mut C,
    coordinate: &Coordinate,
    prior: PriorKeyset,
) -> Result<(), MapStateError<E>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
    E: Error + Send + Sync + 'static,
{
    match prior {
        PriorKeyset::Malformed => write_keyset(op, Keyset::Overflowed),
        PriorKeyset::Decoded(Keyset::Tracked(mut keys)) => match keys.binary_search(coordinate) {
            Ok(position) => {
                keys.remove(position);
                write_keyset(op, Keyset::Tracked(keys))
            }
            Err(_) => Ok(()),
        },
        PriorKeyset::Absent | PriorKeyset::Decoded(Keyset::Overflowed) => Ok(()),
    }
}

/// Checks bounds before duplicate detection, then inserts a new coordinate in
/// order.
fn update_tracked<C, E>(
    op: &mut C,
    coordinate: Coordinate,
    mut keys: Vec<Coordinate>,
    limit: usize,
    ttl: bool,
) -> Result<(), MapStateError<E>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
    E: Error + Send + Sync + 'static,
{
    // Oversized first — collapse even when `coordinate` is already listed.
    if is_oversized(&keys, limit) {
        warn!(
            collection = op.name().as_str(),
            "keyset exceeds its bound; store Overflowed"
        );
        return write_keyset(op, Keyset::Overflowed);
    }
    match keys.binary_search(&coordinate) {
        // Already tracked: no content change — rewrite only to refresh TTL.
        Ok(_) => {
            if ttl {
                write_keyset(op, Keyset::Tracked(keys))
            } else {
                Ok(())
            }
        }
        Err(position) => {
            let would_exceed = keys.len() + 1 > limit
                || tracked_frame_len(&keys)
                    .and_then(|len| len.checked_add(4))
                    .and_then(|len| len.checked_add(coordinate.as_bytes().len()))
                    .is_none_or(|len| len > KEYSET_BYTE_CEILING);
            if would_exceed {
                return write_keyset(op, Keyset::Overflowed);
            }
            keys.insert(position, coordinate);
            write_keyset(op, Keyset::Tracked(keys))
        }
    }
}

/// Stages the shared keyset frame.
fn write_keyset<C, E>(op: &mut C, keyset: Keyset) -> Result<(), MapStateError<E>>
where
    C: CollectionWrite,
    C::Layout: KeysetLayout,
    E: Error + Send + Sync + 'static,
{
    op.set(C::Layout::KEYSET, &(), keyset).map_err(keyset_err)
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
