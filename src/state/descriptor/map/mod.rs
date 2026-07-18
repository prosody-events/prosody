//! Ordered key→value map collection.
//!
//! A Map stores one cell per entry, keyed by an order-preserving encoding of
//! the logical key, plus one keyset cell tracking current membership so a small
//! map's `stream` becomes point gets instead of a durable scan. The
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
//! * `Meta` holds **one** cell: the keyset, at the fixed coordinate `[2]`.
//!   Coordinates `[0]`/`[1]` are a deliberately retired gap (they once held two
//!   min/max bound cells; re-using a retired coordinate would let a stale
//!   artifact alias an old frame). An empty Map has no `Meta` cell.
//! * `Entries` holds one cell per key, addressed by the key codec's
//!   order-preserving coordinate and typed by the entries cell type
//!   [`Keyed`]`<KC, V>`.
//!
//! # Invariant: the keyset tracks current membership, `Overflowed` is one-way
//!
//! The keyset cell tracks a map's membership so a small map's `stream` becomes
//! enumerable point gets (all cached when warm) instead of a durable range
//! scan. It tracks **current** membership: `set` adds the key and `remove`
//! subtracts it (each a read-modify-write of the cell, riding the same
//! operation gate as the entry write). The only over-report is across TTL
//! expiry (a present keyset may briefly outlive its entries) or the one-way
//! `Overflowed`; iteration still point-gets each listed key and **skips** the
//! ones that read absent, so a stale entry costs at most one cached absent
//! read, never a wrong or dropped answer.
//!
//! A `Tracked` keyset holds at most the registered `keyset_limit` distinct
//! keys, stored sorted by their order-preserving coordinate, so iteration is
//! key order in both directions with no read-time sort. A `set` that would push
//! a `Tracked` frame past the limit (or past the module's encoded-byte ceiling)
//! writes the one-way `Overflowed` sentinel — until `clear` or TTL death of the
//! whole map — and iteration falls back to the full-section (`Unbounded`-edged)
//! scan. The bound is the **live distinct-key count**: because `remove`
//! subtracts, a rotating map whose live size stays under the limit keeps cached
//! iteration forever (and a `remove` that drops a `Tracked` frame back under
//! the limit heals it — removal heals). A map that ever exceeds the limit in
//! one incarnation overflows permanently (scan-rebuild re-entry from
//! `Overflowed` is deliberately not implemented; it needs `clear` or TTL death
//! to recover).
//!
//! Bounds deleted (`KeysetPresence` makes them redundant): the map once carried
//! two min/max bound cells to anchor the fallback scan. They are gone — within
//! an incarnation they fenced no tombstone the `Tracked` arm ever wades (that
//! arm point-gets), and the accepted residual is the cross-incarnation
//! `Overflowed` corner, whose fallback is a full-section scan that may cross a
//! one-time tombstone wave (self-healing as those rows compact).
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
    CellCodecError, CellScope, CellStateError, CellType, CellView, CollectionSpec, ContextOf,
    Descriptor, FromSession, Keyed, ResolvedOf, WriteOf,
};
use crate::codec::{Codec, JsonCodec};
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state::cell_key::CellKey;
use crate::state::cell_key::{Coordinate, Direction, ScanEdge, Section};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, UnitKey};
use crate::state::session::{CellSession, MutatePermit, OpPermit};
use crate::state::{CollectionKindId, StoreOutcome};
use async_stream::try_stream;
use bytes::Bytes;
use educe::Educe;
use futures::stream::{Stream, StreamExt};
use std::error::Error;
use std::fmt::Display;
use std::marker::PhantomData;
use std::slice::from_ref;
use thiserror::Error;
use tracing::{Instrument, info_span, instrument, warn};

/// The `Meta` section, holding the keyset cell.
const META_SECTION: Section = Section::new(MapNs::Meta as i8);

/// The `Entries` section, holding one cell per key.
const ENTRY_SECTION: Section = Section::new(MapNs::Entries as i8);

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

/// Map's section enum, lowered to the opaque [`Section`]. Frozen: the
/// discriminants are a durable wire contract (the `section tinyint` column),
/// pinned by `map_layout_is_frozen` and the [`TryFrom`] round-trip.
#[repr(i8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MapNs {
    /// Bookkeeping: the keyset cell.
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

/// The keyset cell's logical address within the `Meta` section: a single fixed
/// coordinate `[2]`. Coordinates `[0]`/`[1]` are a deliberately retired gap
/// (they once held two min/max bound cells; re-using a retired coordinate would
/// let a stale artifact alias an old frame as a keyset). Its encoding is a
/// frozen durable contract, pinned by `map_layout_is_frozen`. Module-fixed by
/// the Map kind, so its [`FORMAT_ID`](Codec::FORMAT_ID) never rides a
/// collection's durable identity.
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

/// A map's decoded keyset cell — its current tracked membership.
///
/// `Tracked` lists at most the registered `keyset_limit` coordinates,
/// **strictly ascending bytewise** (sorted and unique), bounded by the limit
/// and the [`KEYSET_BYTE_CEILING`] at write time. `Overflowed` is the one-way
/// sentinel: once a `set` past the bound writes it, iteration falls back to the
/// full-section (`Unbounded`-edged) scan until `clear` (or TTL death of the
/// whole map). See the module's current-membership invariant.
///
/// # Invariant: `KeysetPresence`
///
/// A **live entry cell implies a present keyset cell** — equivalently, an
/// absent keyset implies no live entries, so `stream` may return empty with
/// zero entry reads (an empty [`Tracked`](StreamPlan::Tracked) list — zero
/// coordinates, so no point gets and no scan). Three rules hold it:
///
/// * every `set` leaves a keyset cell present — writing one whenever the
///   pre-write read is `Absent`/`Malformed` or the frame must change, staged
///   with its entry write in one collection-grain store write, and relying on
///   the already-present cell otherwise. At the settle boundary that write is
///   provisional under one recovery marker, so it is recoverable whatever the
///   batching; mid-handler [`commit`](MapHandle::commit) writes them resolved
///   and marker-free — one atomic batch within the batch budget, but an
///   over-budget commit can crash mid-split and strand an entry ahead of its
///   keyset (the collection-grain over-budget residual on
///   [`CellStore`](crate::state::store::CellStore), reconstructed only by the
///   idempotent handler re-run, never by the store);
/// * [`clear`](MapHandle::clear) erases both the entry section and the keyset;
/// * on a TTL'd map every `set` refreshes the keyset (above), so it expires no
///   earlier than the newest entry.
///
/// The converse is **not** an invariant: `remove` may leave `Tracked([])`, TTL
/// expiry may over-report, and `Overflowed` may outlive its last entry — a
/// present keyset over an empty map is legal and merely does bounded extra work
/// (one absent read per stale coordinate, or one empty-yield scan).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Keyset {
    /// The distinct-key coordinates currently tracked, strictly ascending.
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

/// The arm [`MapHandle::stream`] takes: point-get the tracked keys' entries
/// (in `dir` order), or degrade to the full-section scan. An absent keyset
/// (⇒ [`KeysetPresence`](Keyset) ⇒ no live entries) arrives here as an empty
/// [`Tracked`](StreamPlan::Tracked) list — zero coordinates, so zero point
/// gets and no scan, and its exhaustion still passes the scan-shell fence.
enum StreamPlan<K> {
    /// Point-get each listed key (already reversed for a backward stream).
    Tracked(Vec<K>),

    /// Degrade to the full-section (`Unbounded`-edged) scan.
    Scan,
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

/// The Map [`CollectionSpec`]: one cell per key plus one keyset cell;
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
            keyset: scope.typed(META_SECTION),
        }
    }
}

/// Typed, owned handle over a codec-backed ordered map — a thin composition
/// over two typed `CellView`s: `entries` (the per-key data cells, typed
/// [`Keyed`]`<KC, V>`) and `keyset` (the membership cell — see the module's
/// current-membership invariant). Every operation guards on session termination
/// through the views. Cheap `Clone`.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
pub struct MapHandle<S, KC, V> {
    entries: CellView<S, Keyed<KC, V>>,
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
        let permit = self.entries.read_permit().await;
        Ok(self.entries.get(&permit, key).await?)
    }

    /// Whether a stored cell exists for `key`, **without decoding its value or
    /// running the resolver**.
    ///
    /// Answers "a stored cell exists," read through the event's dirty overlay
    /// (read-your-writes): an uncommitted `set` → `true`, an uncommitted
    /// `remove` → `false`, `clear` hides entries, a `set` after `clear` →
    /// `true`, and `rollback` restores the prior view. For a message-backed map
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
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn contains_key(
        &self,
        key: &KC::Key,
    ) -> Result<bool, MapStateError<CellCodecError<V>>> {
        let permit = self.entries.read_permit().await;
        Ok(self
            .entries
            .contains(&permit, key)
            .await
            .map_err(CellStateError::Access)?)
    }

    /// Reads the values for `keys` as one isolated batch — one result per input
    /// key, aligned index-wise (`results[i]` answers `keys[i]`; duplicate keys
    /// are answered per position; absent keys read `None`).
    ///
    /// The whole read runs under one session-gate hold (the gate is a single
    /// exclusive per-event mutex every collection op acquires for its body), so
    /// no session-side mutation — a sibling op under `join!`, an attempt reset,
    /// a settle close — can interleave anywhere inside this call. Isolation
    /// does not freeze wall-clock TTL passage; how deduplicated and
    /// cross-batch cells are timed is the store layer's batch-read
    /// observation contract, stated once on the internal batch-read verb.
    /// This method adds only gate ownership and the user-sized `Vec`, no
    /// observation behavior of its own.
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
        fields(collection = self.entries.name().as_str(), keys = keys.len() as i64),
        err
    )]
    pub async fn get_many(
        &self,
        keys: &[KC::Key],
    ) -> Result<Vec<Option<ResolvedOf<V>>>, MapStateError<CellCodecError<V>>> {
        let permit = self.entries.read_permit().await;
        let resolved = self.entries.get_many(&permit, keys).await?;
        Ok(resolved.into_vec())
    }

    /// Streams the live entries in key order — ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]. Each
    /// entry's value is resolved as it is yielded.
    ///
    /// # Per-arm consistency (a paged read, not a snapshot)
    ///
    /// A `Tracked` keyset within its bound is the fast arm: **key membership is
    /// snapshotted at init** (the one keyset read), then the listed keys are
    /// point-got in gate-scoped chunks of `STREAM_CHUNK`. Keys added after
    /// init are not yielded; **values are read live, chunk by chunk** — a key
    /// removed/cleared/expired after init reads absent (skipped, the
    /// current-membership skip) and an overwritten key yields the newer value
    /// when its chunk is fetched. So a warm small map streams entirely from
    /// cache with zero durable scans. An `Overflowed`, malformed, oversized, or
    /// otherwise undecodable keyset degrades to a **full-section**
    /// (`Unbounded`-edged) scan: live pages (own dirty writes snapshotted at
    /// init, the durable leg lazily), hides cleared cells, and can observe
    /// entries the handler itself mid-handler-commits ahead of the cursor (it
    /// terminates because a finite handler inserts finitely many coordinates
    /// ahead — a visibility semantic, not a termination hazard). An **absent**
    /// keyset means no live entries (the current-membership invariant), so the
    /// stream yields nothing with zero entry reads (no scan).
    ///
    /// The gate is held only for the init keyset read and per chunk
    /// (≤ `STREAM_CHUNK` point reads each): each chunk is fetched, decoded, and
    /// resolved under one permit that dies with the chunk future's scope before
    /// any of its items reach user code, so the gate is never held across a
    /// yield (items and errors alike) — a handler may mutate this map between
    /// stream items without deadlock (`StreamYieldFree`; see
    /// [`SessionGate`](crate::state::session)). Errors are chunk-atomic: a
    /// failing chunk yields none of its items (all its live entries, or only
    /// its error).
    pub fn stream(&self, dir: Direction) -> impl Stream<Item = MapStreamItem<KC, V>> + '_ {
        // Hand-built span: `#[instrument]` cannot follow a returned `Stream`,
        // so each inner await is instrumented with a clone instead; the
        // span's recorded time is the stream's own work. Unlike the sibling
        // ops' `err`, failures are yielded per item rather than recorded on
        // the span — a failing chunk ends with an OK-status span, and the
        // yielded `Err` surfaces to the caller inside this span's scope.
        let span = info_span!(
            "map.stream",
            collection = self.entries.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            // Init: `stream_plan` reads the keyset plan under a permit it drops
            // as it returns, before this `?` observes the result.
            let plan = self.stream_plan(dir).instrument(span.clone()).await?;
            match plan {
                // The degrade fallback walks the whole entry section: no keyset
                // enumeration remains to fence it, so both edges are
                // `Unbounded`. `dir` still orders the walk; the scan drops the
                // gate after its own init and pages live thereafter.
                StreamPlan::Scan => {
                    let inner = self.entries.scan(
                        ScanEdge::<&KC::Key>::Unbounded,
                        dir,
                        ScanEdge::<&KC::Key>::Unbounded,
                        None,
                    );
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().instrument(span.clone()).await {
                        yield item?;
                    }
                }
                // Tracked keyset (possibly empty): point-get the listed keys
                // through the scan shell's coordinate source. An absent keyset
                // arrives here as an empty list, so its exhaustion is fenced
                // too. Chunking, the per-chunk permit, and skip-absent all live
                // in `CellView::scan_at`.
                StreamPlan::Tracked(keys) => {
                    let inner = self.entries.scan_at(keys.into_iter());
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().instrument(span.clone()).await {
                        yield item?;
                    }
                }
            }
        }
    }

    /// Streams the live entries' **keys** in key order (ascending for
    /// [`Direction::Forward`], descending for [`Direction::Backward`]) —
    /// **without decoding or resolving any value**. So a message-backed map
    /// enumerates keys with **zero Kafka fetches**: the guarantee is "no value
    /// decode, no resolver run," not "no I/O" (the tracked arm still does a
    /// presence-only batched read; the degrade arm still transfers each cell
    /// envelope and discards the value bytes).
    ///
    /// Presence-only: a key is yielded for every present cell, even one whose
    /// value would fail to decode or resolve (unlike [`stream`](Self::stream),
    /// which errors on such a value) — presence is about the cell, not the
    /// value (mirrors [`contains_key`](Self::contains_key)). The arm choice,
    /// per-arm consistency, and gate/fence posture are exactly
    /// [`stream`](Self::stream)'s (same keyset-plan decision); only the value
    /// work is dropped.
    pub fn keys(&self, dir: Direction) -> impl Stream<Item = MapKeyItem<KC, V>> + '_ {
        let span = info_span!(
            "map.keys",
            collection = self.entries.name().as_str(),
            direction = ?dir,
        );
        try_stream! {
            let plan = self.stream_plan(dir).instrument(span.clone()).await?;
            match plan {
                StreamPlan::Scan => {
                    let inner = self.entries.key_scan(dir);
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().instrument(span.clone()).await {
                        yield item?;
                    }
                }
                StreamPlan::Tracked(keys) => {
                    let inner = self.entries.key_scan_at(keys.into_iter());
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().instrument(span.clone()).await {
                        yield item?;
                    }
                }
            }
        }
    }

    /// Reads the keyset cell and decides the stream's arm: an **absent** keyset
    /// means no live entries ([`KeysetPresence`](Keyset)), so the stream takes
    /// an empty [`Tracked`](StreamPlan::Tracked) list — zero coordinates, so
    /// zero point gets and no scan; a `Tracked` keyset within the registered
    /// limit and byte ceiling whose every coordinate decodes to a canonical
    /// key becomes the chunked point-get arm (keys in `dir` order); anything
    /// else — `Overflowed`, malformed, oversized, or a coordinate that fails to
    /// decode or re-encode — degrades to the full-section scan (with a warning
    /// on the degradations that are not simply overflowed). A keyset-read
    /// access error propagates; it never silently degrades.
    async fn stream_plan(
        &self,
        dir: Direction,
    ) -> Result<StreamPlan<KC::Key>, MapStateError<CellCodecError<V>>> {
        // The read permit is a local, dropped as this future returns on every
        // path (like the point-op helpers): the caller's `?` observes the
        // result only after the permit is gone, so the gate never spans a yield.
        let permit = self.entries.read_permit().await;
        let coordinates = match self.read_keyset_state(&permit).await? {
            // Absent ⇒ no live entries: an empty Tracked list — zero
            // coordinates, so zero point gets and no scan.
            PriorKeyset::Absent => return Ok(StreamPlan::Tracked(Vec::new())),
            // Overflowed falls to the scan with no warning; Malformed already
            // warned in `read_keyset_state`.
            PriorKeyset::Malformed | PriorKeyset::Decoded(Keyset::Overflowed) => {
                return Ok(StreamPlan::Scan);
            }
            PriorKeyset::Decoded(Keyset::Tracked(coordinates)) => coordinates,
        };
        let limit = self.keyset.keyset_limit();
        if is_oversized(&coordinates, limit) {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset frame is oversized for the registered limit; degrading to the \
                 full-section scan until the next set heals it"
            );
            return Ok(StreamPlan::Scan);
        }
        let Some(mut keys) = decoded_key_list::<KC>(&coordinates) else {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset holds a coordinate that is not canonical for its key codec; degrading \
                 to the full-section scan until the next set heals it"
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
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn set(
        &self,
        key: KC::Key,
        value: WriteOf<'_, V>,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        let permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        // Read the keyset *before* the entry write: own writes are visible to
        // own reads, and the transition depends on the pre-set frame. `&permit`
        // (a `&MutatePermit`) deref-coerces to the `&OpPermit` the read helper
        // demands.
        let coordinate = KC::encode(&key);
        let prior = self.read_keyset_state(&permit).await?;
        self.entries.set(&permit, &key, value).await?;
        self.update_keyset(&permit, coordinate, prior).await
    }

    /// Removes `key` and subtracts it from the keyset (see the module's
    /// current-membership invariant): buffers the entry clear, then rewrites a
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
        fields(collection = self.entries.name().as_str(), map.key = %key),
        err
    )]
    pub async fn remove(&self, key: &KC::Key) -> Result<(), MapStateError<CellCodecError<V>>> {
        let permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        let coordinate = KC::encode(key);
        let prior = self.read_keyset_state(&permit).await?;
        self.entries.clear(&permit, key).await?;
        self.subtract_keyset(&permit, &coordinate, prior).await
    }

    /// Removes every entry and the keyset: within the event the map reads empty
    /// from this program point (later `set`s repopulate it, starting a fresh
    /// `Tracked` keyset); committed, exactly the repopulated entries survive;
    /// aborted, the map is untouched. O(handler writes) — the entry section
    /// rides the durable section clear; only the fixed-address keyset cell
    /// takes the per-cell path.
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
        let permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        self.entries.clear_all(&permit).await?;
        self.keyset.clear(&permit, &()).await.map_err(keyset_err)?;
        Ok(())
    }

    /// Durably commits this map's buffered ops mid-handler — entries and keyset
    /// together. At-least-once; see [`CellSession::commit`] for the contract,
    /// including the over-budget batch split.
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
        let permit = self
            .entries
            .mutate_permit()
            .await
            .map_err(CellStateError::Access)?;
        Ok(self.entries.commit(&permit).await?)
    }

    /// Discards this map's buffered uncommitted ops — entries and keyset
    /// together — reverting reads to the last [`commit`](Self::commit), or the
    /// pre-event committed state if none. Infallible; see
    /// [`CellSession::rollback`] for the contract.
    #[instrument(
        name = "map.rollback",
        skip_all,
        fields(collection = self.entries.name().as_str())
    )]
    pub async fn rollback(&self) -> StoreOutcome {
        self.entries.rollback().await
    }

    /// Reads the keyset cell, folding a malformed frame into
    /// [`PriorKeyset::Malformed`] (with a warning) so the caller degrades
    /// rather than errors. An access error propagates; a key-decode error
    /// cannot arise (the cell is read at its one fixed coordinate).
    async fn read_keyset_state(
        &self,
        permit: &OpPermit<'_>,
    ) -> Result<PriorKeyset, MapStateError<CellCodecError<V>>> {
        match self.keyset.get(permit, &()).await {
            Ok(None) => Ok(PriorKeyset::Absent),
            Ok(Some(keyset)) => Ok(PriorKeyset::Decoded(keyset)),
            Err(CellStateError::Codec(_)) => {
                warn!(
                    collection = self.entries.name().as_str(),
                    "map keyset frame did not decode; degrading to the full-section scan until \
                     the next set heals it"
                );
                Ok(PriorKeyset::Malformed)
            }
            Err(err) => Err(keyset_err(err)),
        }
    }

    /// Folds `coordinate` into the keyset given the pre-read prior state (the
    /// `set`-side transition table for the module's current-membership
    /// invariant).
    ///
    /// The size check runs *before* the already-tracked fast path, so an
    /// oversized stored `Tracked` collapses to `Overflowed` even when
    /// `coordinate` is already listed. On a TTL'd collection the
    /// already-tracked and `Overflowed` no-write paths still rewrite the
    /// cell, refreshing its TTL (the module's TTL-refresh invariant).
    async fn update_keyset(
        &self,
        permit: &MutatePermit<'_>,
        coordinate: Coordinate,
        prior: PriorKeyset,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        let limit = self.keyset.keyset_limit();
        let ttl = self.keyset.has_ttl();
        match prior {
            // Malformed → heal to Overflowed (already warned at read).
            PriorKeyset::Malformed => self.write_keyset(permit, Keyset::Overflowed).await,
            // Absent → the empty map: a fresh singleton, itself subject to the
            // limit/ceiling.
            PriorKeyset::Absent => {
                if fits_fresh(&coordinate, limit) {
                    self.write_keyset(permit, Keyset::Tracked(vec![coordinate]))
                        .await
                } else {
                    self.write_keyset(permit, Keyset::Overflowed).await
                }
            }
            // Overflowed is one-way: no write, except the TTL refresh.
            PriorKeyset::Decoded(Keyset::Overflowed) => {
                if ttl {
                    self.write_keyset(permit, Keyset::Overflowed).await
                } else {
                    Ok(())
                }
            }
            PriorKeyset::Decoded(Keyset::Tracked(keys)) => {
                self.update_tracked(permit, coordinate, keys, limit, ttl)
                    .await
            }
        }
    }

    /// Subtracts `coordinate` from the keyset given the pre-read prior state
    /// (the `remove`-side transition table for the module's current-membership
    /// invariant). A `Tracked` frame containing the coordinate is rewritten
    /// without it — unconditionally, so a `remove` that drops an oversized
    /// frame back under the bound heals it (removal heals); the rewrite
    /// only shrinks the frame, so the byte ceiling can never be exceeded by
    /// it. A `Tracked` frame that does not contain the coordinate, an
    /// `Overflowed` sentinel (membership unknown; one-way until `clear`),
    /// and an absent keyset are all left untouched; a malformed frame heals
    /// to `Overflowed`, exactly as `set` does.
    async fn subtract_keyset(
        &self,
        permit: &MutatePermit<'_>,
        coordinate: &Coordinate,
        prior: PriorKeyset,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        match prior {
            PriorKeyset::Malformed => self.write_keyset(permit, Keyset::Overflowed).await,
            PriorKeyset::Decoded(Keyset::Tracked(mut keys)) => {
                match keys.binary_search(coordinate) {
                    Ok(position) => {
                        keys.remove(position);
                        self.write_keyset(permit, Keyset::Tracked(keys)).await
                    }
                    Err(_) => Ok(()),
                }
            }
            PriorKeyset::Absent | PriorKeyset::Decoded(Keyset::Overflowed) => Ok(()),
        }
    }

    /// The `Tracked` arm of [`Self::update_keyset`]: size check →
    /// already-present fast path → insert-sorted with the would-exceed
    /// check.
    async fn update_tracked(
        &self,
        permit: &MutatePermit<'_>,
        coordinate: Coordinate,
        keys: Vec<Coordinate>,
        limit: usize,
        ttl: bool,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        // Oversized first — collapse even when `coordinate` is already listed.
        if is_oversized(&keys, limit) {
            warn!(
                collection = self.entries.name().as_str(),
                "map keyset exceeded its bound; collapsing to Overflowed"
            );
            return self.write_keyset(permit, Keyset::Overflowed).await;
        }
        match keys.binary_search(&coordinate) {
            // Already tracked: no content change — rewrite only to refresh TTL.
            Ok(_) => {
                if ttl {
                    self.write_keyset(permit, Keyset::Tracked(keys)).await
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
                    return self.write_keyset(permit, Keyset::Overflowed).await;
                }
                let mut updated = Vec::with_capacity(keys.len() + 1);
                updated.extend_from_slice(&keys[..position]);
                updated.push(coordinate);
                updated.extend_from_slice(&keys[position..]);
                self.write_keyset(permit, Keyset::Tracked(updated)).await
            }
        }
    }

    /// Buffers a keyset-cell write, re-homing its error under the map's type.
    async fn write_keyset(
        &self,
        permit: &MutatePermit<'_>,
        keyset: Keyset,
    ) -> Result<(), MapStateError<CellCodecError<V>>> {
        self.keyset
            .set(permit, &(), keyset)
            .await
            .map_err(keyset_err)
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

/// Whether a stored `Tracked` frame no longer fits the current bound (the
/// registered limit or the byte ceiling). Shared by the read and write
/// paths — `stream_plan` degrades on it and `update_tracked` collapses on
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
/// The entry cells go through the `CellView` interface, so their failures —
/// access, value-codec, or a stored key that no longer decodes — are already a
/// [`CellStateError`]. A corrupt keyset *frame* is the keyset codec's own
/// error, kept separate because it never surfaces from a well-behaved handle: a
/// malformed frame degrades reads to the scan (never errors upward) and the
/// encoder's guards are bounded away by the registration cap. The arm exists to
/// keep the keyset view's error mapping total. (Mirrors
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
    /// Sets the Map keyset bound: the number of **live** distinct keys this map
    /// tracks before overflowing to the full-section scan. Default `128`,
    /// validated `<= 4096` at registration; `0` disables tracking (every map
    /// overflows on its first `set`). Because `remove` subtracts, a rotating
    /// map whose live size stays under the bound keeps cached iteration; a
    /// map that ever exceeds the bound in one incarnation overflows
    /// permanently (until `clear` or TTL death), so a monotonically growing
    /// key universe should not expect cached iteration.
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
