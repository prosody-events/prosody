//! Ordered key→value map collection.
//!
//! A Map stores one cell per entry, keyed by an order-preserving encoding of
//! the logical key, plus one keyset cell tracking current membership so a small
//! map's `stream` becomes point gets instead of a durable scan. Every
//! [`MapHandle`] method runs as one scoped collection operation over the bound
//! collection. Build a descriptor with [`map_state`],
//! register it with the consumer, and bind the
//! [`Registered`](super::Registered) handle through
//! [`EventContext::state`](crate::consumer::event_context::EventContext::state).
//!
//! # Layout
//!
//! Two declared cell families (see [`MapKind`]):
//!
//! * `KEYSET` holds **one** cell: the keyset, at the fixed coordinate `[2]`.
//!   Coordinates `[0]`/`[1]` are a retired gap; `MapKeysetKey` owns that rule.
//!   A Map that has never been written has no keyset cell.
//! * `ENTRIES` holds one cell per key, addressed by the key codec's
//!   order-preserving coordinate and typed by the entries cell type
//!   [`Keyed`]`<KC, V>`.
//!
//! [`clear`](MapHandle::clear) is one whole-layout reset: it erases **both**
//! declared sections, so the retired coordinates go with the keyset and the
//! entries.
//!
//! # Invariant: the keyset tracks current membership, `Overflowed` is one-way
//!
//! The keyset cell tracks a map's membership so a small map's `stream` becomes
//! enumerable point gets (all cached when warm) instead of a durable range
//! scan. It tracks **current** membership: `set` adds the key and `remove`
//! subtracts it (each a read-modify-write of the cell, staged in the same
//! invocation as the entry write). The only over-report is across TTL expiry
//! (a present keyset may briefly outlive its entries) or the one-way
//! `Overflowed`; iteration still point-gets each listed key and **skips** the
//! ones that read absent, so a stale entry costs at most one cached absent
//! read, never a wrong or dropped answer.
//!
//! A `Tracked` keyset stores its keys sorted by their order-preserving
//! coordinate, so iteration is key order in both directions with no read-time
//! sort. A `set` that would push a `Tracked` frame past the registered
//! `keyset_limit`, or past the module's encoded-byte ceiling, writes the
//! one-way `Overflowed` sentinel instead. Iteration then falls back to the
//! full-section (`Unbounded`-edged) scan until `clear`, or until the whole map
//! dies of TTL. A frame that a *lowered* `keyset_limit` left above the new
//! bound degrades the same way, until removals shrink it back under.
//!
//! The bound is the **live distinct-key count**. Because `remove` subtracts, a
//! rotating map whose live size stays under the limit keeps cached iteration
//! forever, and a `remove` that drops a `Tracked` frame back under the limit
//! heals it. Removal heals. A map that ever exceeds the limit in one
//! incarnation overflows permanently: scan-rebuild re-entry from `Overflowed`
//! is deliberately not implemented, so recovery needs `clear` or TTL death.
//!
//! The keyset is an optimization cell, so a malformed or oversized stored frame
//! **degrades** iteration to the full-section scan (with a warning) and is
//! healed by the next `set` — it never errors upward. Membership is durable
//! data co-staged with the entry writes under one settle marker, so there is
//! no in-RAM structure to bound.
//!
//! # Invariant: on a TTL'd map the keyset outlives every entry
//!
//! The keyset cell carries the collection's TTL, but an entry `set` of an
//! already-tracked key would refresh only the entry's TTL — so, left alone, the
//! keyset could expire while live entries remain. To prevent that skew, on a
//! collection with a TTL every `set` rewrites the keyset — **including** the
//! already-tracked and `Overflowed` no-write fast paths, which must not
//! suppress the refresh — so the keyset always lives at least as long as the
//! newest entry. A non-TTL'd map writes the keyset only on a content change;
//! nothing expires, so the invariant holds vacuously.

use super::{
    CellCodecError, CellStateError, CellType, CollectionSpec, ContextOf, Descriptor, FromSession,
    Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{Codec, JsonCodec};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::CellKey;
use crate::state::cell_key::{Coordinate, Direction};
use crate::state::collection::{
    Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE, Plan,
    StateSession, WritableStateSession, collection_layout, collection_methods, same_token,
    spec_matches,
};
use crate::state::order_codec::{I64KeyCodec, KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::{CollectionKindId, StateAccessError, StoreOutcome};
use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::slice::from_ref;
use thiserror::Error;
use tracing::{Instrument, info_span, instrument, warn};

collection_layout! {
    /// The Map collection kind: one keyset cell plus one cell per key. The key
    /// codec `KC` is frozen into the collection's durable identity.
    pub struct MapKind<KC, V> {
        /// The keyset cell — current membership (see the module's
        /// current-membership invariant).
        #[id(0)]
        KEYSET: Keyed<MapKeysetKey, MapKeysetCodec>,
        /// One cell per key.
        #[id(1)]
        ENTRIES: Keyed<KC, V>,
    }
}

/// The instantiation the frozen-layout pin and the test-only cell-address
/// helpers read their sections and format tokens from. A family's durable
/// section and its declared codecs come from the layout, never from the type
/// parameters, so every instantiation answers identically.
type FrozenLayout = MapKind<I64KeyCodec, JsonCodec>;

/// Map's declared per-invocation mutation maximum: `set` and `remove` each
/// stage one entry mutation plus one keyset write; `clear` stages one
/// whole-layout reset. The assertion below pins the declaration against
/// [`JOURNAL_INLINE`]'s budget.
const MAP_MAX_MUTATIONS: usize = 2;

const _: () = assert!(
    MAP_MAX_MUTATIONS <= JOURNAL_INLINE,
    "a Map invocation must stay inside the journal's inline capacity"
);

/// Map's durable layout, frozen. The ids and the keyset family's format tokens
/// below address every Map cell ever written; changing one silently re-points
/// existing rows, and no type can compare this crate against yesterday's
/// schema. The entries family's key and payload tokens are the *user's* choice
/// and ride the collection's structural identity instead. The pin is a
/// compile-time assertion rather than a test so it cannot be filtered out of a
/// run.
const _: () = {
    let families = <FrozenLayout as CollectionLayout>::DESCRIPTOR;
    assert!(
        families.len() == 2,
        "Map declares exactly two cell families"
    );
    assert!(
        families[0].id() == 0,
        "Map's keyset family is durably section 0"
    );
    assert!(
        same_token(families[0].key_format(), "map-keyset-key.v1"),
        "the keyset cell is durably addressed by the Map keyset key"
    );
    assert!(
        same_token(families[0].format(), "map-keyset.v1"),
        "the keyset cell is durably encoded by the Map keyset frame codec"
    );
    assert!(
        families[1].id() == 1,
        "Map's entries family is durably section 1"
    );
    assert!(
        spec_matches::<FrozenLayout>(families[1]),
        "the spec's cell type addresses and encodes the entries family"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::SECTIONS.len() == 2,
        "Map's reset domain is its two families"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::RESERVED.is_empty(),
        "Map has never removed a family"
    );
};

/// A `set` whose updated keyset frame would exceed this writes `Overflowed`
/// instead: `keyset_limit` keys of unbounded encoded length must not produce an
/// unbounded meta cell, so the byte size is capped independently of the count.
const KEYSET_BYTE_CEILING: usize = 64 * 1024;

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

/// The keyset cell's logical address within its section: a single fixed
/// coordinate `[2]`. Coordinates `[0]`/`[1]` are a deliberately retired gap
/// (they once held two min/max bound cells; re-using a retired coordinate would
/// let a stale artifact alias an old frame as a keyset). Its encoding is a
/// frozen durable contract, pinned by `map_layout_is_frozen`. Module-fixed by
/// the Map kind, so its [`FORMAT_ID`](Codec::FORMAT_ID) never rides a
/// collection's durable identity.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MapKeysetKey;

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
/// Every input form writes or checks the same fixed, empty coordinate.
impl Codec for MapKeysetKey {
    type Error = KeyCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "map-keyset-key.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<(), KeyCodecError> {
        Self::decode(buf)
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<(), KeyCodecError> {
        Self::decode(&buf)
    }

    fn serialize_ref(&mut self, (): &(), buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        buf.extend_from_slice(Self::encode(&()).as_bytes());
        Ok(())
    }
}

/// A map's decoded keyset cell — its current tracked membership.
///
/// `Tracked` lists coordinates **strictly ascending bytewise** (sorted and
/// unique). The count and [`KEYSET_BYTE_CEILING`] bounds are enforced at
/// `set`: a `set` that would push the frame past either writes `Overflowed`
/// instead — the one-way sentinel, after which iteration falls back to the
/// full-section (`Unbounded`-edged) scan until `clear` (or TTL death of the
/// whole map). Because the bound is read from the *current* registration, a
/// redeploy that lowers `keyset_limit` can leave a stored `Tracked` frame above
/// it; reads then degrade to the same full-section scan until removals shrink
/// the frame back under the bound or a `set` collapses it. See the module's
/// current-membership invariant.
///
/// # Invariant: `KeysetPresence`
///
/// A **live entry cell implies a present keyset cell** — equivalently, an
/// absent keyset implies no live entries, so `stream` may return empty with
/// zero entry reads (an empty [`Points`](Plan::Points) plan — zero
/// coordinates, so no point gets and no scan). Three rules hold it:
///
/// * every `set` leaves a keyset cell present. It writes one whenever the
///   pre-write read is `Absent` or `Malformed`, or whenever the frame must
///   change, staged with its entry write in one collection-grain store write.
///   Otherwise it relies on the already-present cell. At the settle boundary
///   that write is provisional under one recovery marker, so it is recoverable
///   whatever the batching. A mid-handler [`commit`](MapHandle::commit) writes
///   them resolved and marker-free, in one atomic batch within the batch
///   budget. An over-budget commit can crash mid-split and strand an entry
///   ahead of its keyset. That is the collection-grain over-budget residual on
///   [`CellStore`](crate::state::store::CellStore): only the idempotent handler
///   re-run reconstructs it, never the store;
/// * [`clear`](MapHandle::clear) erases the whole layout, keyset included;
/// * on a TTL'd map every `set` refreshes the keyset (above), so it expires no
///   earlier than the newest entry.
///
/// The converse is **not** an invariant: `remove` may leave `Tracked([])`, TTL
/// expiry may over-report, and `Overflowed` may outlive its last entry — a
/// present keyset over an empty map is legal and merely does bounded extra work
/// (one absent read per stale coordinate, or one empty-yield scan).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Keyset {
    /// The distinct-key coordinates currently tracked, strictly ascending.
    Tracked(Vec<Coordinate>),

    /// The map overflowed its keyset bound; membership is no longer tracked.
    Overflowed,
}

/// The keyset cell's payload codec — the ONE decoder. Module-fixed by the Map
/// kind, so its [`FORMAT_ID`](Codec::FORMAT_ID) never rides a collection's
/// durable identity (the entries key codec `KC` alone does).
/// Owned decoding transfers the frame allocation into coordinate slices.
/// Borrowed decoding copies once to create those owned slices.
/// Both serializers read coordinates without copying their backing storage.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MapKeysetCodec;

impl Codec for MapKeysetCodec {
    type Error = KeysetFrameError;
    type Payload = Keyset;

    const FORMAT_ID: &'static str = "map-keyset.v1";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Keyset, KeysetFrameError> {
        // The trait boundary hands a `&mut [u8]`, which cannot alias into
        // `Bytes`; one upfront copy of the frame — sized by the stored cell,
        // which a lowered bound or an older writer can leave above
        // `KEYSET_BYTE_CEILING` — buys zero-copy coordinate slicing for the
        // rest of the parse.
        decode_keyset(&Bytes::copy_from_slice(buf))
    }

    fn deserialize_owned(&mut self, buf: BytesMut) -> Result<Keyset, KeysetFrameError> {
        // Freezing transfers the allocation into each coordinate slice.
        decode_keyset(&buf.freeze())
    }

    fn deserialize_bytes(&mut self, buf: Bytes) -> Result<Keyset, KeysetFrameError> {
        decode_keyset(&buf)
    }

    fn serialize_ref(
        &mut self,
        payload: &Keyset,
        buf: &mut Vec<u8>,
    ) -> Result<(), KeysetFrameError> {
        encode_keyset(payload, buf)
    }
}

fn encode_keyset(payload: &Keyset, buf: &mut Vec<u8>) -> Result<(), KeysetFrameError> {
    match payload {
        Keyset::Overflowed => {
            buf.reserve(1);
            buf.push(OVERFLOWED_TAG);
        }
        Keyset::Tracked(keys) => {
            let count = u32::try_from(keys.len()).map_err(|_| KeysetFrameError::CountOverflow)?;
            let total = tracked_frame_len(keys).ok_or(KeysetFrameError::CountOverflow)?;
            buf.reserve(total);
            buf.push(TRACKED_TAG);
            buf.extend_from_slice(&count.to_be_bytes());
            for coordinate in keys {
                let len = u32::try_from(coordinate.as_bytes().len())
                    .map_err(|_| KeysetFrameError::CountOverflow)?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(coordinate.as_bytes());
            }
        }
    }
    Ok(())
}

/// The keyset cell's state as read at the top of a `set`, folding the typed
/// get's malformed arm into data so [`update_keyset`] is one match.
enum PriorKeyset {
    /// No keyset cell (a fresh map, or TTL-expired rows).
    Absent,

    /// A well-formed keyset.
    Decoded(Keyset),

    /// The stored frame did not decode — degrade reads, heal on the next `set`.
    Malformed,
}

/// One item [`MapHandle::stream`] yields: a decoded key paired with its
/// resolved value, or the error that ended the stream.
type MapStreamItem<KC, V> =
    Result<(<KC as OrderedKeyCodec>::Key, ResolvedOf<V>), MapStateError<CellCodecError<V>>>;

/// One item [`MapHandle::keys`] yields: a decoded key, or the error that ended
/// the stream. The presence-only, value-free twin of [`MapStreamItem`].
type MapKeyItem<KC, V> = Result<<KC as OrderedKeyCodec>::Key, MapStateError<CellCodecError<V>>>;

/// Descriptor for a codec-backed ordered map collection. Generic over an
/// [`OrderedKeyCodec`] `KC` (the key encoding, frozen into the identity) and a
/// value [`CellType`] `V` — a plain [`Codec`] (JSON by default) or a codec
/// paired with a resolver via [`WithResolver`](super::WithResolver). Declare
/// via [`map_state`].
pub type MapDescriptor<KC, V = JsonCodec> = Descriptor<MapKind<KC, V>>;

impl<KC, V> CollectionSpec for MapKind<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    type Cell = Keyed<KC, V>;
    type Handle<S: StateSession> = MapHandle<S, KC, V>;

    const KIND: CollectionKindId = CollectionKindId::Map;

    fn handle<S: StateSession>(collection: Collection<S, Self>) -> MapHandle<S, KC, V> {
        MapHandle { cells: collection }
    }
}

/// Typed, owned handle over a codec-backed ordered map.
///
/// Owns the bound collection, whose session clone is `Clone + Send + Sync +
/// 'static` (an FFI requirement). Each method opens exactly one scoped
/// operation; the streams run a short planning operation and then drive the
/// plan it returns. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct MapHandle<S, KC, V> {
    cells: Collection<S, MapKind<KC, V>>,
}

/// A directional map stream query.
///
/// Build one with [`MapHandle::query`]. Finish with [`keys`](Self::keys) or
/// [`entries`](Self::entries).
#[must_use]
pub struct MapQuery<'a, S, KC, V> {
    handle: &'a MapHandle<S, KC, V>,
    dir: Direction,
    limit: Option<NonZeroUsize>,
}

impl<'a, S, KC, V> MapQuery<'a, S, KC, V>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
{
    /// Sets the maximum number of present items that the stream yields.
    /// Absent rows are free. Fetch sizing cannot change an answer.
    pub fn limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Streams live entries in the query direction.
    pub fn entries(self) -> impl Stream<Item = MapStreamItem<KC, V>> + 'a
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, S>,
    {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing chunk ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "map.stream",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.dir,
        );
        try_stream! {
            // Init: `stream_plan` reads the keyset under an admission it drops
            // as it returns, before this `?` observes the result.
            let plan = self.handle.stream_plan(self.dir).instrument(span.clone()).await?;
            let plan = match self.limit {
                Some(limit) => plan.with_limit(limit),
                None => plan,
            };
            let inner = plan.entries();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
    }

    /// Streams live keys in the query direction.
    pub fn keys(self) -> impl Stream<Item = MapKeyItem<KC, V>> + 'a {
        let span = info_span!(
            "map.keys",
            collection = self.handle.cells.name().as_str(),
            direction = ?self.dir,
        );
        try_stream! {
            let plan = self.handle.stream_plan(self.dir).instrument(span.clone()).await?;
            let plan = match self.limit {
                Some(limit) => plan.with_limit(limit),
                None => plan,
            };
            let inner = plan.keys();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().instrument(span.clone()).await {
                yield item?;
            }
        }
    }
}

// `KC::Key: Display` exists only so the operation spans can record the map
// key as a joinable attribute (Debug would quote strings); every real key
// (`String`, `i64`, `u64`) already satisfies it, and no other map machinery
// needs it.
#[collection_methods(field = cells, session = S)]
impl<S, KC, V> MapHandle<S, KC, V>
where
    S: StateSession,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
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
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[read(op)]
    pub async fn get(
        &self,
        key: &KC::Key,
    ) -> Result<Option<ResolvedOf<V>>, MapStateError<CellCodecError<V>>> {
        Ok(op.get(MapKind::<KC, V>::ENTRIES, key).await?)
    }

    /// Whether a stored cell exists for `key`, **without decoding its value or
    /// running the resolver**.
    ///
    /// Answers "a stored cell exists," read through the event's dirty overlay
    /// (read-your-writes). On a writable session an uncommitted `set` → `true`,
    /// an uncommitted `remove` → `false`, `clear` hides entries, a `set` after
    /// `clear` → `true`, and `rollback` restores the prior view; a
    /// reader-bound handle has no overlay and answers from committed state
    /// alone. For a message-backed map
    /// this can return `true` even when the referenced Kafka message can no
    /// longer be fetched — presence is about the cell, not the message. The
    /// guarantee is "no value decode, no resolver run," **not** "no I/O": a
    /// cold cache still reaches the store.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.contains_key",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[read(op)]
    pub async fn contains_key(
        &self,
        key: &KC::Key,
    ) -> Result<bool, MapStateError<CellCodecError<V>>> {
        Ok(op.contains(MapKind::<KC, V>::ENTRIES, key).await?)
    }

    /// Reads the values for `keys` as one aligned batch — one result per input
    /// key, aligned index-wise (`results[i]` answers `keys[i]`; duplicate keys
    /// are answered per position; absent keys read `None`).
    ///
    /// The whole read runs inside one scoped operation, so no session-side
    /// mutation — a sibling operation under `join!`, an attempt reset, a settle
    /// close — can interleave anywhere inside this call. Isolation does not
    /// freeze wall-clock TTL passage; how deduplicated and cross-batch cells
    /// are timed is the store layer's batch-read observation contract, stated
    /// once on the internal batch-read verb. This method adds only the
    /// user-sized `Vec`, no observation behavior of its own.
    ///
    /// Keys are addressed directly — there is no keyset consult, so a key
    /// outside the tracked keyset simply reads `None`.
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when a cell does not decode, a
    /// resolution error, or an access error from the session. A terminated or
    /// closed session errors as a whole; no partial result is ever returned
    /// (the `Result<Vec<_>>` shape makes partiality unrepresentable).
    #[instrument(
        name = "map.get_many",
        skip_all,
        fields(collection = self.cells.name().as_str(), keys = keys.len() as i64),
        err
    )]
    #[read(op)]
    pub async fn get_many(
        &self,
        keys: &[KC::Key],
    ) -> Result<Vec<Option<ResolvedOf<V>>>, MapStateError<CellCodecError<V>>> {
        Ok(op
            .get_many(MapKind::<KC, V>::ENTRIES, keys)
            .await?
            .into_vec())
    }

    /// Tests `keys` for presence as one aligned batch. `results[i]` answers
    /// `keys[i]`. Duplicate keys keep their positions.
    ///
    /// # Errors
    ///
    /// Returns a session access error.
    #[instrument(
        name = "map.contains_many",
        skip_all,
        fields(collection = self.cells.name().as_str(), keys = keys.len() as i64),
        err
    )]
    #[read(op)]
    pub async fn contains_many(
        &self,
        keys: &[KC::Key],
    ) -> Result<Vec<bool>, MapStateError<CellCodecError<V>>> {
        Ok(op
            .contains_many(MapKind::<KC, V>::ENTRIES, keys)
            .await?
            .into_vec())
    }

    /// Inserts or overwrites `key`'s value (a blind last-writer-wins write —
    /// the entry is never read first) and folds `key` into the keyset (see the
    /// module's current-membership invariant).
    ///
    /// # Errors
    ///
    /// Returns a codec error (`Permanent`) when `value` does not encode, or an
    /// access error from the session.
    #[instrument(
        name = "map.set",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[write(op)]
    pub async fn set(
        &self,
        key: KC::Key,
        value: WriteOf<'_, V>,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        // Read the keyset *before* staging the entry: own writes are visible to
        // own reads, and the transition depends on the pre-set frame.
        //
        // `KC::encode` runs twice per mutation: once here, and once inside
        // `op.set`'s cell-key lowering — one bounded extra coordinate. Map is
        // the only collection that needs the coordinate at the call site. A
        // coordinate-addressed mutation command on `CollectionWrite` would
        // therefore serve one caller, so keep the second encode.
        let coordinate = KC::encode(&key);
        let prior = read_keyset_state(op).await?;
        op.set(MapKind::<KC, V>::ENTRIES, &key, value)?;
        update_keyset(op, coordinate, prior)
    }

    /// Removes `key` and subtracts it from the keyset (see the module's
    /// current-membership invariant): stages the entry clear, then rewrites a
    /// `Tracked` frame without the coordinate — which heals an oversized frame
    /// back toward the bound. An `Overflowed` or absent keyset is unchanged
    /// (membership is unknown / already empty); a malformed frame heals to
    /// `Overflowed`, exactly as `set` does.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.remove",
        skip_all,
        fields(collection = self.cells.name().as_str(), map.key = %key),
        err
    )]
    #[write(op)]
    pub async fn remove(&self, key: &KC::Key) -> Result<(), MapStateError<CellCodecError<V>>> {
        let coordinate = KC::encode(key);
        let prior = read_keyset_state(op).await?;
        op.clear(MapKind::<KC, V>::ENTRIES, key);
        subtract_keyset(op, &coordinate, prior)
    }

    /// Removes every entry and the keyset: within the event the map reads empty
    /// from this program point (later `set`s repopulate it, starting a fresh
    /// `Tracked` keyset); committed, exactly the repopulated entries survive;
    /// aborted, the map is untouched. O(handler writes) — one whole-layout
    /// reset covers both sections, so no cell takes a per-cell path.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.clear",
        skip_all,
        fields(collection = self.cells.name().as_str()),
        err
    )]
    #[write(op)]
    pub async fn clear(&self) -> Result<(), MapStateError<CellCodecError<V>>> {
        op.clear_collection();
        Ok(())
    }

    /// Reads the keyset cell and captures the stream's arm as an owned plan.
    ///
    /// An **absent** keyset means no live entries
    /// ([`KeysetPresence`](Keyset)). The stream then takes an empty
    /// [`Points`](Plan::Points) plan: zero coordinates, so zero point gets and
    /// no scan.
    ///
    /// A `Tracked` keyset becomes the chunked point-get arm, with the keys in
    /// `dir` order. It must sit within the registered limit and the byte
    /// ceiling, and every coordinate must decode to a canonical key.
    ///
    /// Anything else degrades to the full-section scan: `Overflowed`,
    /// malformed, oversized, or a coordinate that fails to decode or re-encode.
    /// Each degradation that is not simply overflowed also warns.
    ///
    /// A keyset-read access error propagates. It never silently degrades.
    #[read(op)]
    async fn stream_plan(
        &self,
        dir: Direction,
    ) -> Result<Plan<S, Keyed<KC, V>>, MapStateError<CellCodecError<V>>> {
        let coordinates = match read_keyset_state(op).await? {
            // Absent ⇒ no live entries: an empty tracked plan — zero
            // coordinates, so zero point gets and no scan.
            PriorKeyset::Absent => {
                return Ok(Plan::Points(
                    op.coordinates(MapKind::<KC, V>::ENTRIES, Vec::new()),
                ));
            }
            // Overflowed falls to the scan with no warning; Malformed already
            // warned in `read_keyset_state`.
            PriorKeyset::Malformed | PriorKeyset::Decoded(Keyset::Overflowed) => {
                return Ok(Plan::Scan(op.range(MapKind::<KC, V>::ENTRIES, dir)));
            }
            PriorKeyset::Decoded(Keyset::Tracked(coordinates)) => coordinates,
        };
        if is_oversized(&coordinates, op.keyset_limit()) {
            warn!(
                collection = op.name().as_str(),
                "map keyset frame is oversized for the registered limit; degrading to the \
                 full-section scan until the next set heals it"
            );
            return Ok(Plan::Scan(op.range(MapKind::<KC, V>::ENTRIES, dir)));
        }
        let Some(mut keys) = decoded_key_list::<KC>(&coordinates) else {
            warn!(
                collection = op.name().as_str(),
                "map keyset holds a coordinate that is not canonical for its key codec; degrading \
                 to the full-section scan until the next set heals it"
            );
            return Ok(Plan::Scan(op.range(MapKind::<KC, V>::ENTRIES, dir)));
        };
        // Coordinates are stored strictly ascending, so forward is key order
        // and backward is its reverse — no read-time sort.
        if dir == Direction::Backward {
            keys.reverse();
        }
        Ok(Plan::Points(
            op.coordinates(MapKind::<KC, V>::ENTRIES, keys),
        ))
    }

    /// Streams the live entries in key order — ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]. Each
    /// entry's value is resolved as it is yielded.
    ///
    /// # Per-arm consistency (a paged read, not a snapshot)
    ///
    /// A `Tracked` keyset within its bound is the fast arm: **key membership is
    /// snapshotted at init** (the one keyset read), then the listed keys are
    /// point-got in chunks of `CELL_BATCH`. Keys added after init are not
    /// yielded; **values are read live, chunk by chunk** — a key
    /// removed/cleared/expired after init reads absent (skipped, the
    /// current-membership skip) and an overwritten key yields the newer value
    /// when its chunk is fetched. So a warm small map streams entirely from
    /// cache with zero durable scans.
    ///
    /// An `Overflowed`, malformed, oversized, or otherwise undecodable keyset
    /// degrades to a **full-section** (`Unbounded`-edged) scan. That scan pages
    /// live: it snapshots the own dirty writes at init and reads the durable
    /// leg lazily. It hides cleared cells. It can also observe entries the
    /// handler itself mid-handler-commits ahead of the cursor. It still
    /// terminates, because a finite handler inserts finitely many coordinates
    /// ahead. That is a visibility semantic, not a termination hazard.
    ///
    /// An **absent** keyset means no live entries (the current-membership
    /// invariant), so the stream yields nothing with zero entry reads and no
    /// scan.
    ///
    /// Session admission is taken at init for the keyset read. The tracked
    /// (point) arm then takes it once per chunk, at most `CELL_BATCH` point
    /// reads each: a chunk's admission covers its batch fetch, and is released
    /// before the chunk is decoded and resolved. The degraded scan arm takes no
    /// admission after init and pages gate-free. Neither arm holds admission
    /// across a yield, for items and errors alike, so a handler may mutate this
    /// map between stream items without deadlock (`StreamYieldFree`, over the
    /// per-event session operation gate). Errors are chunk-atomic: a failing
    /// chunk yields none of its items (all its live entries, or only its
    /// error).
    pub fn stream(&self, dir: Direction) -> impl Stream<Item = MapStreamItem<KC, V>> + '_
    where
        for<'s> ContextOf<'s, V>: FromSession<'s, S>,
    {
        self.query(dir).entries()
    }

    /// Streams the live entries' **keys** in key order (ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]) —
    /// **without decoding or resolving any value**. So a message-backed map
    /// enumerates keys with **zero Kafka fetches**: the guarantee is "no value
    /// decode, no resolver run," not "no I/O" (the tracked arm still does a
    /// presence-only batch read; the degrade arm uses a presence-only scan).
    ///
    /// Presence-only: a key is yielded for every present cell, even one whose
    /// value would fail to decode or resolve (unlike [`stream`](Self::stream),
    /// which errors on such a value) — presence is about the cell, not the
    /// value (mirrors [`contains_key`](Self::contains_key)). The arm choice,
    /// per-arm consistency, and admission/fence posture are exactly
    /// [`stream`](Self::stream)'s (same keyset-plan decision); only the value
    /// work is dropped.
    pub fn keys(&self, dir: Direction) -> impl Stream<Item = MapKeyItem<KC, V>> + '_ {
        self.query(dir).keys()
    }

    /// Builds a directional stream query.
    pub fn query(&self, dir: Direction) -> MapQuery<'_, S, KC, V> {
        MapQuery {
            handle: self,
            dir,
            limit: None,
        }
    }

    /// Reports whether the map holds no live entries.
    ///
    /// # Errors
    ///
    /// Returns a key codec error or an access error from the session.
    #[instrument(name = "map.is_empty", skip_all, fields(collection = self.cells.name().as_str()), err)]
    pub async fn is_empty(&self) -> Result<bool, MapStateError<CellCodecError<V>>> {
        let keys = self
            .query(Direction::Forward)
            .limit(NonZeroUsize::MIN)
            .keys();
        futures::pin_mut!(keys);
        Ok(keys.next().await.transpose()?.is_none())
    }

    /// Durably commits this map's buffered ops mid-handler — entries and keyset
    /// together. At-least-once; the mid-handler durability section of the
    /// [`collection`](crate::state::collection) module states the contract,
    /// including the over-budget batch split.
    ///
    /// # Errors
    ///
    /// Returns an access error from the session.
    #[instrument(
        name = "map.commit",
        skip_all,
        fields(collection = self.cells.name().as_str()),
        err
    )]
    pub async fn commit(&self) -> Result<StoreOutcome, MapStateError<CellCodecError<V>>>
    where
        S: WritableStateSession,
    {
        Ok(self.cells.commit().await?)
    }

    /// Discards this map's buffered uncommitted ops — entries and keyset
    /// together — reverting reads to the last [`commit`](Self::commit), or the
    /// pre-event committed state if none. Infallible; the mid-handler
    /// durability section of the [`collection`](crate::state::collection)
    /// module states the contract.
    #[instrument(
        name = "map.rollback",
        skip_all,
        fields(collection = self.cells.name().as_str())
    )]
    pub async fn rollback(&self) -> StoreOutcome
    where
        S: WritableStateSession,
    {
        self.cells.rollback().await
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

/// Reads the keyset cell, folding a malformed frame into
/// [`PriorKeyset::Malformed`] (with a warning) so the caller degrades rather
/// than errors. An access error propagates; a key-decode error cannot arise
/// (the cell is read at its one fixed coordinate).
async fn read_keyset_state<C, KC, V>(
    op: &mut C,
) -> Result<PriorKeyset, MapStateError<CellCodecError<V>>>
where
    C: CollectionRead<Layout = MapKind<KC, V>>,
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    match op.get(MapKind::<KC, V>::KEYSET, &()).await {
        Ok(None) => Ok(PriorKeyset::Absent),
        Ok(Some(keyset)) => Ok(PriorKeyset::Decoded(keyset)),
        Err(CellStateError::Codec(_)) => {
            warn!(
                collection = op.name().as_str(),
                "map keyset frame did not decode; degrading to the full-section scan until the \
                 next set heals it"
            );
            Ok(PriorKeyset::Malformed)
        }
        Err(err) => Err(keyset_err(err)),
    }
}

/// Folds `coordinate` into the keyset given the pre-read prior state (the
/// `set`-side transition table for the module's current-membership invariant).
///
/// The size check runs *before* the already-tracked fast path, so an oversized
/// stored `Tracked` collapses to `Overflowed` even when `coordinate` is already
/// listed. On a TTL'd collection the already-tracked and `Overflowed` no-write
/// paths still rewrite the cell, refreshing its TTL (the module's TTL-refresh
/// invariant).
fn update_keyset<C, KC, V>(
    op: &mut C,
    coordinate: Coordinate,
    prior: PriorKeyset,
) -> Result<(), MapStateError<CellCodecError<V>>>
where
    C: CollectionWrite<Layout = MapKind<KC, V>>,
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    let limit = op.keyset_limit();
    let ttl = op.has_ttl();
    match prior {
        // Malformed → heal to Overflowed (already warned at read).
        PriorKeyset::Malformed => write_keyset(op, Keyset::Overflowed),
        // Absent → the empty map: a fresh singleton, itself subject to the
        // limit/ceiling.
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

/// Subtracts `coordinate` from the keyset given the pre-read prior state (the
/// `remove`-side transition table for the module's current-membership
/// invariant). A `Tracked` frame containing the coordinate is rewritten without
/// it — unconditionally, so a `remove` that drops an oversized frame back under
/// the bound heals it (removal heals) and a still-oversized one shrinks toward
/// it rather than forfeiting tracked iteration. A `Tracked` frame that does
/// not contain the coordinate, an `Overflowed` sentinel (membership unknown;
/// one-way until `clear`), and an absent keyset are all left untouched; a
/// malformed frame heals to `Overflowed`, exactly as `set` does.
fn subtract_keyset<C, KC, V>(
    op: &mut C,
    coordinate: &Coordinate,
    prior: PriorKeyset,
) -> Result<(), MapStateError<CellCodecError<V>>>
where
    C: CollectionWrite<Layout = MapKind<KC, V>>,
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
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

/// The `Tracked` arm of [`update_keyset`]: size check → already-present fast
/// path → insert-sorted with the would-exceed check.
fn update_tracked<C, KC, V>(
    op: &mut C,
    coordinate: Coordinate,
    mut keys: Vec<Coordinate>,
    limit: usize,
    ttl: bool,
) -> Result<(), MapStateError<CellCodecError<V>>>
where
    C: CollectionWrite<Layout = MapKind<KC, V>>,
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    // Oversized first — collapse even when `coordinate` is already listed.
    if is_oversized(&keys, limit) {
        warn!(
            collection = op.name().as_str(),
            "map keyset exceeded its bound; collapsing to Overflowed"
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

/// Stages a keyset-cell write, re-homing its error under the map's type.
fn write_keyset<C, KC, V>(
    op: &mut C,
    keyset: Keyset,
) -> Result<(), MapStateError<CellCodecError<V>>>
where
    C: CollectionWrite<Layout = MapKind<KC, V>>,
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    op.set(MapKind::<KC, V>::KEYSET, &(), keyset)
        .map_err(keyset_err)
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

/// Whether a stored `Tracked` frame no longer fits the current bound (the
/// registered limit or the byte ceiling). Shared by the read and write
/// paths — the stream plan degrades on it and [`update_tracked`] collapses on
/// it — so the two can never disagree about what "oversized" means.
fn is_oversized(keys: &[Coordinate], limit: usize) -> bool {
    keys.len() > limit || tracked_frame_len(keys).is_none_or(|len| len > KEYSET_BYTE_CEILING)
}

/// Decodes each stored coordinate to its logical key, returning `None` if any
/// coordinate fails [`KC::decode`](OrderedKeyCodec::decode) or is not canonical
/// (re-encoding the decoded key must reproduce the stored bytes — a
/// contract-breaking aliasing codec could otherwise collapse two coordinates
/// onto one key and yield an entry twice). The caller degrades the stream to
/// the scan on `None`. Sized once (`with_capacity`), bounded by the keyset
/// limit.
///
/// The canonicality re-encode costs one [`Coordinate`] per tracked key —
/// bounded by the registered limit and paid once per stream construction, not
/// per item — and is accepted over trusting the codec's byte-identity law,
/// because an aliasing codec would otherwise silently double-yield an entry.
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

/// Test-only: the entry cell at key coordinate `coordinate`, so a test can
/// seed raw entry cells directly — including a coordinate that does not decode
/// as the collection's key codec — to exercise the real store's scan path.
#[cfg(test)]
pub(crate) fn entry_cell_for(coordinate: &Coordinate) -> CellKey {
    CellKey {
        section: FrozenLayout::ENTRIES.section(),
        coordinate: coordinate.clone(),
    }
}

/// Test-only: the keyset cell at its frozen address (section 0, coordinate
/// `[2]`), so a test can read the stored keyset frame directly.
#[cfg(test)]
pub(crate) fn keyset_cell() -> CellKey {
    CellKey {
        section: FrozenLayout::KEYSET.section(),
        coordinate: MapKeysetKey::encode(&()),
    }
}

impl<KC, V> Descriptor<MapKind<KC, V>> {
    /// Sets the Map keyset bound: the number of **live** distinct keys this map
    /// tracks before overflowing to the full-section scan. Default `128`,
    /// validated `<= 4096` at registration; `0` disables tracking (every map
    /// overflows on its first `set`). Because `remove` subtracts, a rotating
    /// map whose live size stays under the bound keeps cached iteration; a
    /// map that ever exceeds the bound in one incarnation overflows
    /// permanently (until `clear` or TTL death), so a monotonically growing
    /// key universe should not expect cached iteration.
    ///
    /// The bound shapes the **owner's** arm choice only. A published reader
    /// binds the same map at the global validated ceiling, so a tracked keyset
    /// the owner's lowered bound degrades still streams as point gets there.
    ///
    /// Available on Map registrations only — a keyset bound on a Value or Deque
    /// is uncompilable, since this inherent method exists only at this type.
    #[must_use]
    pub fn keyset_limit(mut self, limit: usize) -> Self {
        self.def.keyset_limit = limit;
        self
    }
}

/// Error returned by [`MapHandle`] operations.
///
/// The entry cells go through the typed collection commands, so their
/// failures — access, value-codec, or a stored key that no longer decodes — are
/// already a [`CellStateError`]. A corrupt keyset *frame* is the keyset codec's
/// own error, kept separate because it never surfaces from a well-behaved
/// handle: a malformed frame degrades reads to the scan (never errors upward)
/// and the encoder's guards are bounded away by the registration cap. The arm
/// exists to keep the keyset cell's error mapping total. (Mirrors
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

/// An access refusal reaches the handle as the access arm of a cell error —
/// the shape the scoped write invocation's final fence reports.
impl<E> From<StateAccessError> for MapStateError<E>
where
    E: Error + Send + Sync + 'static,
{
    fn from(error: StateAccessError) -> Self {
        Self::Cell(CellStateError::Access(error))
    }
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

#[cfg(test)]
mod tests;
