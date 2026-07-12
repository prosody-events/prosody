//! Ordered key→value map collection.
//!
//! A Map stores one cell per entry, keyed by an order-preserving encoding of
//! the logical key, plus loose min/max bounds that anchor an ordered scan. The
//! [`MapHandle`] composes the uniform `CellView` typed cell interface — there
//! is no Map-specific store, session, or backend. Build a descriptor with
//! [`map_state`], register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two sections separate bookkeeping from data (`MapNs`):
//!
//! * `Meta` holds **three** cells: the two bounds, addressed by
//!   `MapBound::Min`/`MapBound::Max`, each storing an extreme entry's *key*
//!   through `KC` itself (a key codec is its own payload codec — the
//!   byte-identity law on `OrderedKeyCodec`); plus the keyset at its own fixed
//!   coordinate. A bound cell's payload bytes are `KC::encode(key)` — the very
//!   bytes the entry cell is addressed by — so a bound reads back as a logical
//!   `KC::Key`. The two bounds are stored separately (not one cell) so each
//!   ratchets independently. An empty Map has no `Meta` cells.
//! * `Entries` holds one cell per key, addressed by the key codec's
//!   order-preserving coordinate and typed by the entries cell type
//!   [`Keyed`]`<KC, V>`.
//!
//! # Invariant: the keyset is a loose superset, `Overflowed` is one-way
//!
//! The keyset cell tracks a map's membership so a small map's `stream` becomes
//! enumerable point gets (all cached when warm) instead of a durable range
//! scan. It is a **loose superset**: `set` adds the key (a read-modify-write of
//! the cell, riding the same operation gate as the entry write), `remove` never
//! subtracts (it stays the blind entry clear it always was), and iteration
//! point-gets each listed key and **skips** the ones that read absent. So every
//! staleness the keyset can exhibit is an over-report that costs at most one
//! cached absent read — never a wrong or dropped answer.
//!
//! A `Tracked` keyset holds at most the registered `keyset_limit` distinct
//! keys, stored sorted by their order-preserving coordinate, so iteration is
//! key order in both directions with no read-time sort. The first `set` past
//! the limit (or past the module's encoded-byte ceiling) writes the one-way
//! `Overflowed` sentinel — until `clear` or TTL death of the whole map — and
//! iteration falls back to the bounds-anchored scan. The bound is the
//! **distinct-key universe since `clear`**, not the live size: a
//! rotating-identifier map overflows permanently and should not expect cached
//! iteration.
//!
//! The keyset is an optimization cell, so a malformed or oversized stored frame
//! **degrades** iteration to the scan (with a warning) and is healed by the
//! next `set` — it never errors upward; the bound cells keep their stricter
//! `Permanent` posture. Membership is durable data co-staged with the entry
//! writes in the same settle batch, so there is no in-RAM structure to bound.
//!
//! # Invariant: min/max are a loose outward superset
//!
//! `set` ratchets the min/max bounds **outward only** and never reads the entry
//! being written, so a blind last-writer-wins entry write is preserved and the
//! bounds may name a key that was since removed — a *superset* of the live key
//! range, never a subset. `stream` anchors **both** edges of its scan on the
//! bounds — `[min, max]` ascending, mirrored descending — reading them
//! concurrently, and relies on the store hiding cleared cells, so it yields
//! exactly the live entries in key order regardless of stale bounds: a residue
//! beyond the live range is never reached, and a live key can never sit outside
//! its bounds, so bounding the scan to `[min, max]` drops no entry. When either
//! bound is absent the map holds no live entries (see the TTL-refresh
//! invariant), so no scan is issued at all. The bounds are therefore
//! self-healing — never an exact count, by design (a count would force a
//! read-before-write on every mutation). [`MapHandle::clear`] removes the
//! bounds with the entries, so the absent-bounds ⇔ empty-map reading survives
//! a cleared map.
//!
//! # Invariant: on a TTL'd map the bounds outlive every entry
//!
//! A bound cell carries the collection's TTL, but an entry `set` within the
//! existing bounds would refresh only the entry's TTL — so, left alone, the
//! bounds could expire while live entries remain. To prevent that skew, on a
//! collection with a TTL every `set` rewrites *both* bound cells (with the
//! ratcheted extremes), refreshing their TTL. So a bound cell always lives at
//! least as long as the newest entry, and **absent bounds ⇔ no live entries**.
//! A non-TTL'd map keeps the extend-only write — nothing expires, so the
//! invariant holds vacuously.
//!
//! The keyset cell rides the identical rule: on a TTL'd map every `set`
//! rewrites the keyset too — **including** the already-tracked and `Overflowed`
//! no-write fast paths, which must not suppress the refresh — so the keyset
//! outlives every entry. An absent keyset then means pre-keyset-or-expired, and
//! iteration safely falls back to the bounds scan. A non-TTL'd map writes the
//! keyset only on a content change.

use super::{
    CellCodecError, CellScope, CellStateError, CellType, CellView, CollectionSpec, ContextOf,
    Descriptor, FromSession, Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{Codec, JsonCodec};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::CellKey;
use crate::state::cell_key::{Coordinate, Direction, ScanEdge, Section};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::session::CellSession;
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use bytes::Bytes;
use educe::Educe;
use futures::stream::{self, Stream, StreamExt};
use std::error::Error;
use std::fmt::Display;
use std::marker::PhantomData;
use std::slice::from_ref;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::{Instrument, info_span, instrument, warn};

/// The `Meta` section, holding the two bound cells and the keyset cell.
const META_SECTION: Section = Section::new(MapNs::Meta as i8);

/// The `Entries` section, holding one cell per key.
const ENTRY_SECTION: Section = Section::new(MapNs::Entries as i8);

/// A `set` whose updated keyset frame would exceed this writes `Overflowed`
/// instead: `keyset_limit` keys of unbounded encoded length must not produce an
/// unbounded meta cell, so the byte size is capped independently of the count.
const KEYSET_BYTE_CEILING: usize = 64 * 1024;

/// Concurrency width of the Tracked stream's ordered point gets
/// (`.buffered(KEYSET_PREFETCH)`): bounded and named, sized to overlap the
/// durable round-trips of a cold keyset (a warm keyset's fjall hits gain
/// nothing and lose nothing), not for throughput. Mirrors the deque window's
/// prefetch.
const KEYSET_PREFETCH: usize = 16;

/// Keyset frame tag for a [`Keyset::Tracked`] payload. Frozen wire byte, pinned
/// by `map_keyset_cell_bytes_are_frozen`.
const TRACKED_TAG: u8 = 0;

/// Keyset frame tag for the one-way [`Keyset::Overflowed`] sentinel. Frozen
/// wire byte, pinned by `map_keyset_cell_bytes_are_frozen`.
const OVERFLOWED_TAG: u8 = 1;

/// The most keyset entries any acceptable frame can hold — a ceiling-sized
/// frame with the minimum four-byte length prefix per entry. Caps the decoder's
/// upfront `with_capacity` so a pathological oversized stored frame can never
/// drive a prealloc that panics or aborts (the parse degrades either way; this
/// only bounds the reservation).
const KEYSET_MAX_ENTRIES: usize = KEYSET_BYTE_CEILING / 4;

/// Map's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column),
/// pinned by `map_layout_is_frozen` and the [`TryFrom`] round-trip.
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MapNs {
    /// Bookkeeping: the min/max bound cells.
    Meta = 0,

    /// Data: one cell per key.
    Entries = 1,
}

impl From<MapNs> for i8 {
    fn from(section: MapNs) -> Self {
        section as i8
    }
}

impl TryFrom<i8> for MapNs {
    type Error = UnknownMapSection;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Entries),
            _ => Err(UnknownMapSection(value)),
        }
    }
}

/// The two bound cells' logical address within the `Meta` section: the loose
/// lower and upper bounds of the live key range. Its encoding (`Min`→`[0]`,
/// `Max`→`[1]`) is a frozen durable contract, pinned by `map_layout_is_frozen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MapBound {
    /// The loose lower bound of the live key range.
    Min = 0,

    /// The loose upper bound of the live key range.
    Max = 1,
}

/// Address codec for the two [`MapBound`] cells. Module-fixed by the Map kind
/// (the section byte already pins the meta cells), so its
/// [`FORMAT_ID`](Codec::FORMAT_ID) never rides a collection's durable identity.
/// The meta cells are only ever read at their two fixed addresses, so `decode`
/// is a defensive round-trip, never a scan step.
#[derive(Clone, Copy, Debug, Default)]
struct MapBoundKey;

impl OrderedKeyCodec for MapBoundKey {
    type Key = MapBound;

    fn encode(key: &MapBound) -> Coordinate {
        // `from_static` — the two bound coordinates are compile-time constants,
        // so the meta-op address builder allocates nothing (behavior pinned by
        // `map_layout_is_frozen`).
        Coordinate::from_bytes(Bytes::from_static(match key {
            MapBound::Min => &[0],
            MapBound::Max => &[1],
        }))
    }

    fn decode(bytes: &[u8]) -> Result<MapBound, KeyCodecError> {
        match bytes {
            [0] => Ok(MapBound::Min),
            [1] => Ok(MapBound::Max),
            &[actual] => Err(KeyCodecError::BadDiscriminant { actual }),
            _ => Err(KeyCodecError::BadLength {
                expected: 1,
                actual: bytes.len(),
            }),
        }
    }
}

/// The payload half of `MapBoundKey` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction. Only the
/// *entries* key codec rides a bound cell's payload; this impl exists to
/// satisfy the supertrait, not to store bounds by.
impl Codec for MapBoundKey {
    type Error = KeyCodecError;
    type Payload = MapBound;

    const FORMAT_ID: &'static str = "map-bound.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<MapBound, KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(&mut self, payload: MapBound, buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&payload).as_bytes());
        Ok(())
    }
}

/// The keyset cell's logical address within the `Meta` section: a single fixed
/// coordinate `[2]`, beside `MapBound::Min`=`[0]` and `MapBound::Max`=`[1]`.
/// Its encoding is a frozen durable contract, pinned by `map_layout_is_frozen`.
/// Module-fixed by the Map kind, so its [`FORMAT_ID`](Codec::FORMAT_ID) never
/// rides a collection's durable identity.
#[derive(Clone, Copy, Debug, Default)]
struct MapKeysetKey;

impl OrderedKeyCodec for MapKeysetKey {
    type Key = ();

    fn encode((): &()) -> Coordinate {
        Coordinate::from_bytes(Bytes::from_static(&[2]))
    }

    fn decode(bytes: &[u8]) -> Result<(), KeyCodecError> {
        match bytes {
            [2] => Ok(()),
            &[actual] => Err(KeyCodecError::BadDiscriminant { actual }),
            _ => Err(KeyCodecError::BadLength {
                expected: 1,
                actual: bytes.len(),
            }),
        }
    }
}

/// The payload half of `MapKeysetKey` — delegates to `encode`/`decode`, so the
/// byte-identity law on [`OrderedKeyCodec`] holds by construction. Exists to
/// satisfy the supertrait; the keyset cell is only ever addressed at its one
/// fixed coordinate.
impl Codec for MapKeysetKey {
    type Error = KeyCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "map-keyset-key.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<(), KeyCodecError> {
        Self::decode(buf)
    }

    fn serialize(&mut self, (): (), buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&()).as_bytes());
        Ok(())
    }
}

/// A map's decoded keyset cell — the membership tracked since the last `clear`.
///
/// `Tracked` lists at most the registered `keyset_limit` coordinates,
/// **strictly ascending bytewise** (sorted and unique), bounded by the limit
/// and the [`KEYSET_BYTE_CEILING`] at write time. `Overflowed` is the one-way
/// sentinel: once a `set` past the bound writes it, iteration falls back to the
/// durable bounds scan until `clear` (or TTL death of the whole map). See the
/// module's loose-superset invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Keyset {
    /// The distinct-key coordinates tracked since `clear`, strictly ascending.
    Tracked(Vec<Coordinate>),

    /// The map overflowed its keyset bound; membership is no longer tracked.
    Overflowed,
}

/// The keyset cell's payload codec — the ONE decoder. Module-fixed by the Map
/// kind, so its [`FORMAT_ID`](Codec::FORMAT_ID) never rides a collection's
/// durable identity (the entries key codec `KC` alone does).
#[derive(Clone, Copy, Debug, Default)]
struct MapKeysetCodec;

impl Codec for MapKeysetCodec {
    type Error = KeysetFrameError;
    type Payload = Keyset;

    const FORMAT_ID: &'static str = "map-keyset.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Keyset, KeysetFrameError> {
        // The trait boundary hands a `&mut [u8]`, which cannot alias into
        // `Bytes`; one upfront copy of the frame (bounded by
        // `KEYSET_BYTE_CEILING`) buys zero-copy coordinate slicing for the rest
        // of the parse.
        decode_keyset(&Bytes::copy_from_slice(buf))
    }

    fn serialize(&mut self, payload: Keyset, buf: &mut Vec<u8>) -> Result<(), KeysetFrameError> {
        match payload {
            Keyset::Overflowed => {
                buf.reserve(1);
                buf.push(OVERFLOWED_TAG);
            }
            Keyset::Tracked(keys) => {
                let count =
                    u32::try_from(keys.len()).map_err(|_| KeysetFrameError::CountOverflow)?;
                // Exact length with checked arithmetic *before* any reserve.
                let total = tracked_frame_len(&keys).ok_or(KeysetFrameError::CountOverflow)?;
                buf.reserve(total);
                buf.push(TRACKED_TAG);
                buf.extend_from_slice(&count.to_be_bytes());
                for coordinate in &keys {
                    let len = u32::try_from(coordinate.as_bytes().len())
                        .map_err(|_| KeysetFrameError::CountOverflow)?;
                    buf.extend_from_slice(&len.to_be_bytes());
                    buf.extend_from_slice(coordinate.as_bytes());
                }
            }
        }
        Ok(())
    }
}

/// The keyset cell's state as read at the top of a `set`, folding the typed
/// get's malformed arm into data so [`MapHandle::update_keyset`] is one match.
enum PriorKeyset {
    /// No keyset cell (a fresh map, or pre-keyset/TTL-expired rows).
    Absent,

    /// A well-formed keyset.
    Decoded(Keyset),

    /// The stored frame did not decode — degrade reads, heal on the next `set`.
    Malformed,
}

/// The arm [`MapHandle::stream`] takes: materialize the tracked keys' entries
/// by point gets (in `dir` order), or fall back to the bounds-anchored scan.
enum StreamPlan<K> {
    /// Point-get each listed key (already reversed for a backward stream).
    Tracked(Vec<K>),

    /// Degrade to the bounds scan.
    Scan,
}

/// One item [`MapHandle::stream`] yields: a decoded key paired with its
/// resolved value, or the error that ended the stream.
type MapStreamItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), MapStateError<CellCodecError<V>>>;

/// Descriptor for a codec-backed ordered map collection. Generic over an
/// [`OrderedKeyCodec`] `KC` (the key encoding, frozen into the identity) and a
/// value [`CellType`] `V` — a plain [`Codec`] (JSON by default) or a codec
/// paired with a resolver via [`WithResolver`](super::WithResolver). Declare
/// via [`map_state`].
pub type MapDescriptor<KC, V = JsonCodec> = Descriptor<MapKind<KC, V>>;

/// The Map [`CollectionSpec`]: one cell per key plus the min/max bound cells;
/// the key codec is frozen into the identity.
pub struct MapKind<KC, V>(PhantomData<fn() -> (KC, V)>);

impl<KC, V> CollectionSpec for MapKind<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    type Cell = Keyed<KC, V>;
    type Handle<S: CellSession> = MapHandle<S, KC, V>;

    const KIND: CollectionKindId = CollectionKindId::Map;

    fn handle<S: CellSession>(scope: CellScope<S>) -> MapHandle<S, KC, V> {
        MapHandle {
            entries: scope.typed(ENTRY_SECTION),
            meta: scope.typed(META_SECTION),
            keyset: scope.typed(META_SECTION),
        }
    }
}

/// Typed, owned handle over a codec-backed ordered map — a thin composition
/// over three typed `CellView`s: `entries` (the per-key data cells, typed
/// [`Keyed`]`<KC, V>`), `meta` (the min/max bound cells, each storing an
/// extreme entry's key), and `keyset` (the membership cell — see the module's
/// loose-superset invariant). Every operation guards on session termination
/// through the views. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct MapHandle<S, KC, V> {
    entries: CellView<S, Keyed<KC, V>>,
    meta: CellView<S, Keyed<MapBoundKey, KC>>,
    keyset: CellView<S, Keyed<MapKeysetKey, MapKeysetCodec>>,
}

// `KC::Key: Display` exists only so the operation spans can record the map
// key as a joinable attribute (Debug would quote strings); every real key
// (`String`, `i64`, `u64`) already satisfies it, and no other map machinery
// needs it.
impl<S, KC, V> MapHandle<S, KC, V>
where
    S: CellSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, V>: FromSession<'s, S>,
{
    /// Reads and resolves the value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when the cell does not decode, a
    /// resolution error, or an access error from the session.
    #[instrument(
        name = "map.get",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn get(
        &self,
        key: &KC::Key,
    ) -> Result<Option<ResolvedOf<V>>, MapStateError<CellCodecError<V>>> {
        let _permit = self.entries.read_permit().await;
        Ok(self.entries.get(key).await?)
    }

    /// Streams the live entries in key order — ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]. Each
    /// entry's value is resolved as it is yielded.
    ///
    /// A `Tracked` keyset within its bound is the fast arm: iteration
    /// **materializes** the listed keys' entries by bounded concurrent point
    /// gets under one gate hold (the split-stream bounded contract), skipping
    /// any that read absent (the loose-superset skip), so a warm small map
    /// streams entirely from cache with zero durable scans. An `Overflowed`,
    /// absent, malformed, oversized, or otherwise undecodable keyset degrades
    /// to the bounds-anchored scan: bounded to `[min, max]`, so a stale bound
    /// never drops a live entry, only ever costs an extra scanned-but-cleared
    /// cell; both bounds absent ⇒ empty map ⇒ no scan.
    pub fn stream(&self, dir: Direction) -> impl Stream<Item = MapStreamItem<KC, V>> + '_ {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing scan ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "map.stream",
            collection = self.entries.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            // The split stream contract (see `SessionGate`): the gate is held
            // for the init keyset read and — on the bounded arm — the whole
            // materialization (every listed entry, ≤ the keyset bound in
            // memory), released before the first yield; the scan arm drops the
            // permit after the bounds read and streams per-item live. Neither
            // holds the gate across a yield to user code.
            let permit = self.entries.read_permit().instrument(span.clone()).await;
            let plan = self.stream_plan(dir).instrument(span.clone()).await?;
            match plan {
                StreamPlan::Tracked(keys) => {
                    // Concurrent ordered point gets: the gate hold above is the
                    // safety argument, the per-cell reads commute across
                    // distinct coordinates, and `buffered(KEYSET_PREFETCH)`
                    // overlaps a cold keyset's durable round-trips in key order.
                    let len = keys.len();
                    let gets = stream::iter(keys)
                        .map(|key| {
                            cooperative(async move {
                                let value = self.entries.get(&key).await?;
                                Ok::<_, MapStateError<CellCodecError<V>>>(value.map(|v| (key, v)))
                            })
                        })
                        .buffered(KEYSET_PREFETCH);
                    futures::pin_mut!(gets);
                    let mut buffer: Vec<(KC::Key, ResolvedOf<V>)> = Vec::with_capacity(len);
                    while let Some(item) = gets.next().instrument(span.clone()).await {
                        // A `None` is a removed/expired key: skipped, never an
                        // error (the loose-superset skip).
                        if let Some(entry) = item? {
                            buffer.push(entry);
                        }
                    }
                    drop(permit);
                    for entry in buffer {
                        yield entry;
                    }
                }
                StreamPlan::Scan => {
                    let (min, max) = tokio::try_join!(
                        self.read_bound(MapBound::Min).instrument(span.clone()),
                        self.read_bound(MapBound::Max).instrument(span.clone())
                    )?;
                    drop(permit);
                    let (Some(min), Some(max)) = (min, max) else {
                        return;
                    };
                    let (start, end) = match dir {
                        Direction::Forward => (ScanEdge::Included(&min), ScanEdge::Included(&max)),
                        Direction::Backward => (ScanEdge::Included(&max), ScanEdge::Included(&min)),
                    };
                    let inner = self.entries.scan(start, dir, end, None);
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().instrument(span.clone()).await {
                        yield item?;
                    }
                }
            }
        }
    }

    /// Reads the keyset cell and decides the stream's arm: a `Tracked` keyset
    /// within the registered limit and byte ceiling whose every coordinate
    /// decodes to a canonical key becomes the point-get materialization (keys
    /// in `dir` order); anything else — `Overflowed`, absent, malformed,
    /// oversized, or a coordinate that fails to decode or re-encode — degrades
    /// to the bounds scan (with a warning on the degradations that are not
    /// simply absent/overflowed). A keyset-read access error propagates; it
    /// never silently degrades.
    async fn stream_plan(
        &self,
        dir: Direction,
    ) -> Result<StreamPlan<KC::Key>, MapStateError<CellCodecError<V>>> {
        // Absent/Overflowed fall to the scan with no warning; Malformed already
        // warned in `read_keyset_state`.
        let PriorKeyset::Decoded(Keyset::Tracked(coordinates)) = self.read_keyset_state().await?
        else {
            return Ok(StreamPlan::Scan);
        };
        let limit = self.keyset.keyset_limit();
        if coordinates.len() > limit
            || tracked_frame_len(&coordinates).is_none_or(|len| len > KEYSET_BYTE_CEILING)
        {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset frame is oversized for the registered limit; degrading to the bounds \
                 scan until the next set heals it"
            );
            return Ok(StreamPlan::Scan);
        }
        let Some(mut keys) = decoded_key_list::<KC>(&coordinates) else {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset holds a coordinate that is not canonical for its key codec; degrading \
                 to the bounds scan until the next set heals it"
            );
            return Ok(StreamPlan::Scan);
        };
        // Coordinates are stored strictly ascending, so forward is key order
        // and backward is its reverse — no read-time sort.
        if dir == Direction::Backward {
            keys.reverse();
        }
        Ok(StreamPlan::Tracked(keys))
    }

    /// Reads a bound cell's stored extreme key (`None` when absent). The bound
    /// stores the key through `KC` itself (byte-identity law), so it reads
    /// back as a logical `KC::Key`.
    async fn read_bound(
        &self,
        bound: MapBound,
    ) -> Result<Option<KC::Key>, MapStateError<CellCodecError<V>>> {
        self.meta.get(&bound).await.map_err(meta_err)
    }

    /// Inserts or overwrites `key`'s value (a blind last-writer-wins write —
    /// the entry is never read first), ratcheting the min/max bounds outward
    /// and folding `key` into the keyset (see the module's loose-superset
    /// invariant).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, or an
    /// access error from the session.
    #[instrument(
        name = "map.set",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn set(
        &self,
        key: KC::Key,
        value: WriteOf<'_, V>,
    ) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Key: Clone,
    {
        // ONE permit across the entry set, the bound ratchet, AND the keyset
        // update: each is a read-modify-write over meta cells, so an
        // interleaved sibling `set` could lose an extension or a keyset entry
        // without the shared hold.
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        // The three meta reads are independent point gets; read them
        // concurrently *before* any write (own writes are visible to own
        // reads, so reading bounds after the ratchet would misclassify every
        // fresh map's first set).
        let coordinate = KC::encode(&key);
        let (min, max, prior) = tokio::try_join!(
            self.read_bound(MapBound::Min),
            self.read_bound(MapBound::Max),
            self.read_keyset_state(),
        )?;
        let bounds_present = min.is_some() || max.is_some();
        self.entries.set(&key, value).await?;
        self.ratchet_bounds(key, min, max).await?;
        self.update_keyset(coordinate, bounds_present, prior).await
    }

    /// Removes `key` (a blind clear; the bounds are left untouched, so they
    /// remain a loose superset).
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.remove",
        skip_all,
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn remove(&self, key: &KC::Key) -> Result<(), MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        Ok(self.entries.clear(key).await?)
    }

    /// Removes every entry, both bounds, and the keyset: within the event the
    /// map reads empty from this program point (later `set`s repopulate it,
    /// starting a fresh `Tracked` keyset); committed, exactly the repopulated
    /// entries survive; aborted, the map is untouched. O(handler writes) — the
    /// entry section rides the durable section clear; only the
    /// fixed-cardinality bookkeeping cells take the per-cell path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.clear",
        skip_all,
        fields(collection = self.entries.name().as_str()),
        err
    )]
    pub async fn clear(&self) -> Result<(), MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        self.entries.clear_all().await?;
        self.meta.clear(&MapBound::Min).await.map_err(meta_err)?;
        self.meta.clear(&MapBound::Max).await.map_err(meta_err)?;
        self.keyset.clear(&()).await.map_err(keyset_err)?;
        Ok(())
    }

    /// Durably commits this map's buffered ops mid-handler — entries and
    /// bound ratchets together, in one batch. At-least-once; see
    /// [`CellSession::commit`] for the contract.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.commit",
        skip_all,
        fields(collection = self.entries.name().as_str()),
        err
    )]
    pub async fn commit(&self) -> Result<StoreOutcome, MapStateError<CellCodecError<V>>> {
        let _permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        Ok(self.entries.commit().await?)
    }

    /// Discards this map's buffered uncommitted ops — entries and bound
    /// ratchets together — reverting reads to the last
    /// [`commit`](Self::commit), or the pre-event committed state if none.
    /// Infallible; see [`CellSession::rollback`] for the contract.
    #[instrument(
        name = "map.rollback",
        skip_all,
        fields(collection = self.entries.name().as_str())
    )]
    pub async fn rollback(&self) -> StoreOutcome {
        self.entries.rollback().await
    }

    /// Ratchets the min/max bounds outward to `key`, given the bounds already
    /// read by the caller (`set` reads all three meta cells concurrently up
    /// front), then writes the ratcheted extremes.
    ///
    /// On a **TTL'd** collection both bound cells are rewritten every `set`,
    /// even when neither extreme moves: that unconditional rewrite refreshes
    /// the bounds' TTL so they outlive every entry (the module's
    /// TTL-refresh invariant). On a **non-TTL'd** collection only a bound
    /// that `key` extends by [`Ord`] is written — nothing expires, so a
    /// rewrite would be pure churn. Either way the conditional sets stay
    /// sequential (cheap in-memory buffering, nothing to overlap): `key` is
    /// cloned into the lower write and moved into the upper.
    async fn ratchet_bounds(
        &self,
        key: KC::Key,
        min: Option<KC::Key>,
        max: Option<KC::Key>,
    ) -> Result<(), MapStateError<CellCodecError<V>>>
    where
        KC::Key: Clone,
    {
        if self.meta.has_ttl() {
            // Compute both ratcheted extremes before the moves: `new_min` may
            // clone `key`, `new_max` consumes it.
            let new_min = min.filter(|min| *min <= key).unwrap_or_else(|| key.clone());
            let new_max = max.filter(|max| *max >= key).unwrap_or(key);
            self.meta
                .set(&MapBound::Min, new_min)
                .await
                .map_err(meta_err)?;
            self.meta
                .set(&MapBound::Max, new_max)
                .await
                .map_err(meta_err)?;
            return Ok(());
        }
        let extend_min = min.is_none_or(|min| key < min);
        let extend_max = max.is_none_or(|max| key > max);
        match (extend_min, extend_max) {
            (true, true) => {
                // Both bounds extend: clone once for Min, move into Max.
                self.meta
                    .set(&MapBound::Min, key.clone())
                    .await
                    .map_err(meta_err)?;
                self.meta.set(&MapBound::Max, key).await.map_err(meta_err)?;
            }
            (true, false) => self.meta.set(&MapBound::Min, key).await.map_err(meta_err)?,
            (false, true) => self.meta.set(&MapBound::Max, key).await.map_err(meta_err)?,
            (false, false) => {}
        }
        Ok(())
    }

    /// Reads the keyset cell, folding a malformed frame into
    /// [`PriorKeyset::Malformed`] (with a warning) so the caller degrades
    /// rather than errors. An access error propagates; a key-decode error
    /// cannot arise (the cell is read at its one fixed coordinate).
    async fn read_keyset_state(&self) -> Result<PriorKeyset, MapStateError<CellCodecError<V>>> {
        match self.keyset.get(&()).await {
            Ok(None) => Ok(PriorKeyset::Absent),
            Ok(Some(keyset)) => Ok(PriorKeyset::Decoded(keyset)),
            Err(CellStateError::Codec(_)) => {
                warn!(
                    collection = self.entries.name().as_str(),
                    "map keyset frame did not decode; degrading to the bounds scan until the next \
                     set heals it"
                );
                Ok(PriorKeyset::Malformed)
            }
            Err(err) => Err(keyset_err(err)),
        }
    }

    /// Folds `coordinate` into the keyset given the pre-read prior state and
    /// whether the bounds are present (the transition table for the module's
    /// loose-superset invariant). Runs under the caller's `set` permit.
    ///
    /// The size check runs *before* the already-tracked fast path, so an
    /// oversized stored `Tracked` collapses to `Overflowed` even when
    /// `coordinate` is already listed. On a TTL'd collection the
    /// already-tracked and `Overflowed` no-write paths still rewrite the
    /// cell, refreshing its TTL (the module's TTL-refresh invariant).
    async fn update_keyset(
        &self,
        coordinate: Coordinate,
        bounds_present: bool,
        prior: PriorKeyset,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        let limit = self.keyset.keyset_limit();
        let ttl = self.keyset.has_ttl();
        match prior {
            // Malformed → heal to Overflowed (already warned at read).
            PriorKeyset::Malformed => self.write_keyset(Keyset::Overflowed).await,
            // Absent + bounds present → membership unknown, conservatively
            // Overflowed. Absent + bounds absent → the empty map: a fresh
            // singleton, itself subject to the limit/ceiling.
            PriorKeyset::Absent => {
                if bounds_present || !fits_fresh(&coordinate, limit) {
                    self.write_keyset(Keyset::Overflowed).await
                } else {
                    self.write_keyset(Keyset::Tracked(vec![coordinate])).await
                }
            }
            // Overflowed is one-way: no write, except the TTL refresh.
            PriorKeyset::Decoded(Keyset::Overflowed) => {
                if ttl {
                    self.write_keyset(Keyset::Overflowed).await
                } else {
                    Ok(())
                }
            }
            PriorKeyset::Decoded(Keyset::Tracked(keys)) => {
                self.update_tracked(coordinate, keys, limit, ttl).await
            }
        }
    }

    /// The `Tracked` arm of [`Self::update_keyset`]: size check →
    /// already-present fast path → insert-sorted with the would-exceed
    /// check.
    async fn update_tracked(
        &self,
        coordinate: Coordinate,
        keys: Vec<Coordinate>,
        limit: usize,
        ttl: bool,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        // Oversized first — collapse even when `coordinate` is already listed.
        if keys.len() > limit
            || tracked_frame_len(&keys).is_none_or(|len| len > KEYSET_BYTE_CEILING)
        {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset exceeded its bound; collapsing to Overflowed"
            );
            return self.write_keyset(Keyset::Overflowed).await;
        }
        match keys.binary_search(&coordinate) {
            // Already tracked: no content change — rewrite only to refresh TTL.
            Ok(_) => {
                if ttl {
                    self.write_keyset(Keyset::Tracked(keys)).await
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
                    return self.write_keyset(Keyset::Overflowed).await;
                }
                let mut updated = Vec::with_capacity(keys.len() + 1);
                updated.extend_from_slice(&keys[..position]);
                updated.push(coordinate);
                updated.extend_from_slice(&keys[position..]);
                self.write_keyset(Keyset::Tracked(updated)).await
            }
        }
    }

    /// Buffers a keyset-cell write, re-homing its error under the map's type.
    async fn write_keyset(&self, keyset: Keyset) -> Result<(), MapStateError<CellCodecError<V>>> {
        self.keyset.set(&(), keyset).await.map_err(keyset_err)
    }
}

/// Declares a codec-backed ordered map collection named `name` over key codec
/// `KC` and value cell type `V` (JSON values by default). See
/// [`Descriptor::new`](super::Descriptor::new) for the `name` contract.
#[must_use]
pub fn map_state<KC, V>(name: &str) -> MapDescriptor<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    MapDescriptor::new(name)
}

/// Re-homes a meta-cell access error under the map's value-codec error
/// parameter. The `meta` view's payload codec is the entries key codec `KC`,
/// so a meta codec failure means a stored bound's key bytes no longer decode —
/// a key-decode failure, folded under [`CellStateError::Key`].
fn meta_err<E>(err: CellStateError<KeyCodecError>) -> MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    match err {
        CellStateError::Access(e) => CellStateError::Access(e).into(),
        CellStateError::Codec(e) | CellStateError::Key(e) => CellStateError::Key(e).into(),
    }
}

/// Re-homes a keyset-cell access error under the map's value-codec error
/// parameter. A `Codec` failure is a malformed keyset frame, routed to
/// [`MapStateError::KeysetFrame`] (`Permanent`); its access half joins the
/// entries' [`Cell`](MapStateError::Cell) arm. The key half cannot arise (the
/// keyset cell is read at its one fixed coordinate) but is forwarded for
/// exhaustiveness.
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

/// Whether a fresh single-key keyset fits the registered `limit` and the byte
/// ceiling — `false` when `limit == 0` (tracking disabled) or the one key's
/// frame is already oversized.
fn fits_fresh(coordinate: &Coordinate, limit: usize) -> bool {
    limit >= 1
        && tracked_frame_len(from_ref(coordinate)).is_some_and(|len| len <= KEYSET_BYTE_CEILING)
}

/// The exact encoded byte length of a `Tracked` frame over `keys`
/// (`1` tag + `4` count + `Σ(4 len + coordinate bytes)`), or `None` on `usize`
/// overflow — the single length arithmetic the serializer and both size checks
/// share.
fn tracked_frame_len(keys: &[Coordinate]) -> Option<usize> {
    let mut total = 1usize.checked_add(4)?;
    for coordinate in keys {
        total = total
            .checked_add(4)?
            .checked_add(coordinate.as_bytes().len())?;
    }
    Some(total)
}

/// Decodes each stored coordinate to its logical key, returning `None` if any
/// coordinate fails [`KC::decode`](OrderedKeyCodec::decode) or is not canonical
/// (re-encoding the decoded key must reproduce the stored bytes — a
/// contract-breaking aliasing codec could otherwise collapse two coordinates
/// onto one key and yield an entry twice). The caller degrades the stream to
/// the scan on `None`. Sized once (`with_capacity`), bounded by the keyset
/// limit.
fn decoded_key_list<KC: OrderedKeyCodec>(coordinates: &[Coordinate]) -> Option<Vec<KC::Key>> {
    let mut keys = Vec::with_capacity(coordinates.len());
    for coordinate in coordinates {
        let key = KC::decode(coordinate.as_bytes()).ok()?;
        if KC::encode(&key) != *coordinate {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

/// Parses an untrusted keyset frame. Every length is bounds-checked with
/// overflow-safe arithmetic, `with_capacity` is capped by what the remaining
/// bytes can hold (never the raw count field), coordinates are sliced zero-copy
/// out of `bytes`, and the frame must be strictly ascending, unique, and free
/// of trailing bytes. The single decode implementation (behind
/// [`MapKeysetCodec::deserialize`]).
fn decode_keyset(bytes: &Bytes) -> Result<Keyset, KeysetFrameError> {
    let buf = bytes.as_ref();
    match buf.first().copied() {
        None => Err(KeysetFrameError::Truncated),
        Some(OVERFLOWED_TAG) => {
            if buf.len() == 1 {
                Ok(Keyset::Overflowed)
            } else {
                Err(KeysetFrameError::TrailingBytes)
            }
        }
        Some(TRACKED_TAG) => decode_tracked(bytes),
        Some(other) => Err(KeysetFrameError::UnknownTag(other)),
    }
}

/// The `Tracked` body of [`decode_keyset`] — `count` then `count`
/// length-prefixed coordinates.
fn decode_tracked(bytes: &Bytes) -> Result<Keyset, KeysetFrameError> {
    let buf = bytes.as_ref();
    let count_bytes: [u8; 4] = buf
        .get(1..5)
        .ok_or(KeysetFrameError::Truncated)?
        .try_into()
        .map_err(|_| KeysetFrameError::Truncated)?;
    let count = u32::from_be_bytes(count_bytes) as usize;
    // Every entry costs ≥ 4 bytes of length prefix, so the capacity the frame
    // can actually hold is `remaining / 4` — never the raw count field, which an
    // adversary could inflate to `u32::MAX`. Also cap by the most entries any
    // acceptable frame holds, so a pathological oversized stored frame cannot
    // drive a prealloc that panics or aborts (the parse degrades regardless).
    let cap = count
        .min(buf.len().saturating_sub(5) / 4)
        .min(KEYSET_MAX_ENTRIES);
    let mut keys: Vec<Coordinate> = Vec::with_capacity(cap);
    let mut offset = 5usize;
    let mut prev: Option<&[u8]> = None;
    for _ in 0..count {
        let len_end = offset.checked_add(4).ok_or(KeysetFrameError::Truncated)?;
        let len_bytes: [u8; 4] = buf
            .get(offset..len_end)
            .ok_or(KeysetFrameError::Truncated)?
            .try_into()
            .map_err(|_| KeysetFrameError::Truncated)?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        let coord_end = len_end
            .checked_add(len)
            .ok_or(KeysetFrameError::Truncated)?;
        let coordinate = buf
            .get(len_end..coord_end)
            .ok_or(KeysetFrameError::Truncated)?;
        if let Some(previous) = prev
            && coordinate <= previous
        {
            return Err(KeysetFrameError::Unsorted);
        }
        keys.push(Coordinate::from_bytes(bytes.slice(len_end..coord_end)));
        prev = Some(coordinate);
        offset = coord_end;
    }
    if offset != buf.len() {
        return Err(KeysetFrameError::TrailingBytes);
    }
    Ok(Keyset::Tracked(keys))
}

/// Error converting an `i8` that matches no [`MapNs`] variant.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown map section discriminant: {0}")]
struct UnknownMapSection(i8);

/// Error returned by [`MapHandle`] operations.
///
/// The entry and bound cells go through the `CellView` interface, so their
/// failures — access, value-codec, or a stored key that no longer decodes — are
/// already a [`CellStateError`]. A corrupt keyset *frame* is the keyset codec's
/// own error, kept separate because it never surfaces from a well-behaved
/// handle: a malformed frame degrades reads to the scan (never errors upward)
/// and the encoder's guards are bounded away by the registration cap. The arm
/// exists to keep the keyset view's error mapping total. (Mirrors
/// [`DequeStateError::MetaFrame`](super::deque::DequeStateError::MetaFrame).)
#[derive(Debug, Error)]
pub enum MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    /// A typed cell op failed: an access error, a value-codec failure, or a
    /// stored key that did not decode.
    #[error(transparent)]
    Cell(#[from] CellStateError<E>),

    /// The stored keyset frame was corrupt (see the type doc — never produced
    /// by a well-behaved handle).
    #[error(transparent)]
    KeysetFrame(#[from] KeysetFrameError),
}

impl<E> ClassifyError for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cell(e) => e.classify_error(),
            // A corrupt keyset frame will not decode on retry.
            Self::KeysetFrame(_) => ErrorCategory::Permanent,
        }
    }
}

/// Error decoding or encoding a Map keyset frame. Always `Permanent`: a
/// malformed frame will not start decoding on retry, and the encoder's count
/// guard is structurally unreachable behind the registration limit.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum KeysetFrameError {
    /// The frame's leading tag byte was neither `Tracked` nor `Overflowed`.
    #[error("unknown map keyset tag: {0}")]
    UnknownTag(u8),

    /// The frame ended before a declared count or coordinate length.
    #[error("truncated map keyset frame")]
    Truncated,

    /// Bytes remained after the frame's declared contents.
    #[error("trailing bytes after map keyset frame")]
    TrailingBytes,

    /// Two coordinates were out of strictly-ascending order (unsorted or a
    /// duplicate).
    #[error("map keyset coordinates are not strictly ascending")]
    Unsorted,

    /// A key count or coordinate length exceeded `u32` (structurally
    /// unreachable behind the registration limit).
    #[error("map keyset count exceeds u32")]
    CountOverflow,
}

impl ClassifyError for KeysetFrameError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// Test-only: the entry cell at key coordinate `coordinate`, so a test can
/// seed raw entry cells directly — including a coordinate that does not decode
/// as the collection's key codec — to exercise the real store's scan path.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: ENTRY_SECTION,
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the `(Min, Max)` bound cells, so a test can read the stored
/// bounds directly and assert the loose-superset containment invariant.
#[cfg(test)]
pub(crate) fn bound_cells() -> (CellKey, CellKey) {
    let cell = |bound: MapBound| CellKey {
        section: META_SECTION,
        coordinate: MapBoundKey::encode(&bound),
    };
    (cell(MapBound::Min), cell(MapBound::Max))
}

/// Test-only: the keyset cell at its frozen address (`Meta` section,
/// coordinate `[2]`), so a test can read the stored keyset frame directly.
#[cfg(test)]
pub(crate) fn keyset_cell() -> CellKey {
    CellKey {
        section: META_SECTION,
        coordinate: MapKeysetKey::encode(&()),
    }
}

impl<KC, V> Descriptor<MapKind<KC, V>> {
    /// Sets the Map keyset bound: the number of distinct keys **since `clear`**
    /// (not the live size) this map tracks before overflowing to the durable
    /// bounds scan. Default `128`, validated `<= 4096` at registration; `0`
    /// disables tracking (every map overflows on its first `set`). A
    /// rotating-identifier map exhausts the bound permanently, so it should not
    /// expect cached iteration.
    ///
    /// Available on Map registrations only — a keyset bound on a Value or Deque
    /// is uncompilable, since this inherent method exists only at this type.
    #[must_use]
    pub fn keyset_limit(mut self, limit: usize) -> Self {
        self.def.keyset_limit = limit;
        self
    }
}

#[cfg(test)]
mod tests;
