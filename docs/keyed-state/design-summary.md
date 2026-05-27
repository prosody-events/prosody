# Keyed State for Prosody

## Problem

Prosody is a Kafka consumer with a timer system. Handlers receive messages or fired timers, do work, and finish. Keyed
state gives them durable per-key collections:

```rust
ctx.value("counter").set(count + 1).await?;
ctx.map("seen").put(message_id, timestamp).await?;
```

State must survive process restarts, tolerate at-least-once redelivery, and use only the Cassandra and timer systems
Prosody already owns.

Three properties of the existing harness shape the design:

- **One handler at a time per key.** `KeyManager` already serializes message and timer events on the same
  `(segment, key)`. Single-writer is a property to use, not engineer around.
- **At-least-once redelivery.** Kafka redelivers until offset commit; timers re-fire until the store row is deleted.
  Handlers must be idempotent; state inherits the same property.
- **A 2PC seam already exists.** `FallibleHandler` fires exactly one of `after_commit` or `after_abort` per dispatch,
  after the framework decides whether the dispatch is final. Hook firing is best-effort: process crash or storage outage
  can skip it. Hooks are the fast path, never the source of truth.

The durable source of truth is Cassandra. A collection partition may contain an applied value plus a sealed WAL.
Recovery
asks the upstream commit oracle whether the event that sealed the WAL committed, then applies or rolls back from that
answer.

Fjall is the first local workspace implementation, but the core design depends on traits, not Fjall itself. Each data
structure has one datatype trait plus small capability traits. Fjall can implement the datatype trait as a dirty tracker
in one namespace and as a write-through cache over Cassandra in another. Memory implements the same traits for fast
tests. If Fjall disappears on restart, revocation, corruption, or format mismatch, Cassandra still contains the truth.

---

## Architecture

Keyed state has four separate responsibilities:

1. **Collection kind modules.** Each data structure owns its operation type, applied representation, overlay, and
   kind-specific apply logic.
2. **Datatype traits.** Kind-specific storage interfaces (`ValueStore`, `MapStore`, `DequeStore`) for normal collection
   operations. Cassandra, memory, Fjall dirty trackers, and Fjall write-through caches can all implement these traits.
3. **Capability traits.** Small traits for behavior that not every datatype implementation has: pending op streaming,
   durable WAL transitions, and direct apply.
4. **Combinators.** `Layered*Store` composes a cache with a backing store. `Transaction*Store` composes a dirty tracker
   with a durable store and is the interface used by handler-facing handles.

```text
StateHandle
  -> TransactionMapStore<Dirty, Durable>
       Dirty:   MapStore + PendingOpSource<MapKind>
       Durable: MapStore + DurableWalStore<MapKind> + DirectApplyStore<MapKind>

Durable may be:
  CassandraMapStore
  MemoryMapStore
  LayeredMapStore<FjallMapStore, CassandraMapStore>
```

The Fjall dirty tracker and Fjall write-through cache may share one assignment-scoped Fjall keyspace, but they use
different namespaces and are composed in different places:

- Dirty implementation failures before seal/direct apply fail the handler, because the current attempt's buffered
  operations
  would otherwise be lost.
- Fjall write-through cache failures after the backing durable store succeeds degrade to invalidation or cache
  disablement; they do not change durable semantics.

---

## Core Invariants

These invariants are load-bearing. Code should name them near the owning types and tests should exercise them directly.

1. **Single writer per state key.** At most one handler can mutate a `(segment, key)` in this process, and Kafka
   partition ownership gives the same guarantee system-wide for live owners. Keyed state does not need LWTs,
   distributed locks, or compare-and-set writes.
2. **The backing durable store is authoritative.** In production this is Cassandra. Fjall may speed up reads and spill
   dirty state, but durable recovery depends only on the backing durable store plus the commit oracle.
3. **Kind identity is typed.** `CollectionId<K>`, `WalBlob<K>`, `WalOpStream<K>`, `DirtyCollection<K>`, and capability
   traits all carry the collection kind in the type system. A Value WAL cannot contain Map operations.
4. **Durable WAL state is total.** A collection partition is either `Idle(applied)` or
   `Sealed(applied, event, wal_blob)`. Partial WAL columns are corrupt, never a valid state.
5. **Payload encoding is collection metadata.** Every stored value cell in a collection is decoded with that
   collection's `PayloadEncoding`. A collection with applied payload cells or a sealed WAL must have exactly one
   payload encoding. Per-cell bytes carry the serialized `StoredPayload` enum, not a separate format or kind column.
6. **Commit mode is fixed per event scope.** A collection has one `CommitMode` while a handler is running. Runtime mode
   changes, if ever supported, take effect only when creating a new `EventScope`.
7. **Only SEALED WAL asks the oracle.** Direct applies and flushed operations never consult `CommitManager`. Recovery
   consults the oracle only for `DurableState::Sealed`.
8. **Pending index is only a hint.** `keyed_state_pending` helps recovery find SEALED partitions. Its presence never
   proves a WAL exists; the collection partition is authoritative.
9. **Hooks are non-authoritative.** `after_commit` and `after_abort` may be skipped. Correctness must still follow from
   first-touch recovery or the recovery timer.
10. **Rollback only rolls back sealed WAL.** Aborting a handler drops unsealed local operations. It rolls back only
    collections that successfully reached the sealed state.
11. **Direct apply is at-least-once.** `flush()` and `CommitMode::Direct` may be observed more than once if the process
    loses the storage response and the handler retries. Users opt into this contract.
12. **Backend physical metadata is private.** Cassandra static columns belong to Cassandra store implementations.
    Layered caches observe only datatype trait results plus their own coverage metadata.
13. **Collection scans are streaming.** Range and iteration APIs return async streams. Store implementations must not
    materialize an unbounded collection or range in memory.
14. **Recovery is idempotent.** Running recovery repeatedly on the same collection produces the same visible state as
    running it once.

---

## Types

### Collection Kinds

The WAL frame format is shared, but operation types are kind-specific.

```rust
trait CollectionKind: Copy + Send + Sync + 'static {
    const ID: CollectionKindId;
    type Op;
    type Applied;
    type Overlay;
    type LookupKey;
    type OverlayRead;
}

struct ValueKind;
struct MapKind;
struct DequeKind;

enum DirtyRead<T> {
    Hit(T),
    Miss,
}
```

Each kind owns the types that make sense for that data structure:

```rust
enum ValueOp {
    Set { payload: StoredPayload },
    Clear,
}

enum MapOp {
    Put { key: EncodedMapKey, payload: StoredPayload },
    Remove { key: EncodedMapKey },
    Clear,
}

enum DequeOp {
    PutAt { index: DequeIndex, payload: StoredPayload },
    RemoveAt { index: DequeIndex },
    Clear,
}
```

`EncodedMapKey` preserves the user's `Ord` in lexicographic byte order. `DequeIndex` encodes `i64` so byte order sorts
the same way as numeric order.

### Identity

Use typed identities instead of loose parameter lists:

```rust
struct StateKey {
    segment_id: SegmentId,
    key: Key,
}

struct CollectionId<K: CollectionKind> {
    state_key: StateKey,
    state_type: StateType,
    name: StateName,
    _kind: PhantomData<K>,
}

enum StateType {
    Application = 0,
    // Middleware-owned namespaces use 1+.
}
```

The Cassandra rows still store `kind tinyint` where a shared table needs it, but Rust APIs use `CollectionId<K>` so the
wrong kind cannot be passed to a kind-specific store method.

### Event References

`EventRef` identifies the upstream event that sealed a WAL:

```rust
enum EventRef {
    Message { dedup_id: Uuid },
    Timer {
        timer_type: TimerType,
        time: CompactDateTime,
        tag: i32,
    },
}

enum CommitDecision {
    Committed,
    NotCommitted,
}
```

Message recovery asks the deduplication store whether `dedup_id` exists. Timer recovery compares the WAL tag against
the current timer-store tag:

- row absent -> committed
- current tag equals WAL tag -> not committed
- current tag differs from WAL tag -> committed and rescheduled

Recovery branches on `CommitDecision`, not a bare boolean.

### Durable State

Cassandra can physically contain nulls, but Rust exposes only this state:

```rust
enum DurableState<K: CollectionKind> {
    Idle { applied: K::Applied },
    Sealed {
        applied: K::Applied,
        wal: SealedWal<K>,
    },
}

struct SealedWal<K: CollectionKind> {
    event: EventRef,
    wal: WalBlob<K>,
    payload_encoding: PayloadEncoding,
}
```

Decoding rules:

```text
wal_event/wal_ops/wal_format all NULL          -> Idle
all WAL columns + payload_encoding are valid   -> Sealed
anything else                                  -> StateError::CorruptWal
```

For `Idle`, `payload_encoding` is required if applied payload cells exist. For `Sealed`, `payload_encoding` is always
required so recovery can apply the WAL into an empty collection after restart without consulting process-local defaults.

### Local State

Inside one handler invocation, each touched collection is in exactly one local state:

```rust
struct CollectionRef<K: CollectionKind> {
    id: CollectionId<K>,
    ttl: Option<CompactDuration>,
    commit_mode: CommitMode,
    scope: EventScopeId,
}

struct DirtyCollection<K: CollectionKind> {
    collection: CollectionRef<K>,
    op_count: NonZeroU64,
}

struct SealedCollection<K: CollectionKind> {
    collection: CollectionRef<K>,
    event: EventRef,
}

enum LocalTx<K: CollectionKind> {
    Clean(CollectionRef<K>),
    Dirty(DirtyCollection<K>),
    Sealed(SealedCollection<K>),
    Finished,
}
```

`Clean` means only "this collection has no local operations in the current event scope." It does not carry committed
data. Applied state lives in Cassandra, the write-through committed cache, or any later read cache. Only
`DirtyCollection<K>` can be sealed, directly applied, or flushed. Only `SealedCollection<K>` can be consumed by
`after_commit` or `after_abort`.

These types are transition capabilities, not data containers:

- `CollectionRef<K>` proves the collection kind, identity, commit mode, event scope, and the application's
  per-collection TTL.
- `DirtyCollection<K>` proves a non-empty dirty op stream exists in the dirty workspace for that collection.
- `SealedCollection<K>` proves the dirty stream was sealed to Cassandra under one `EventRef`.
- `Finished` invalidates handles after the event scope ends.

No local type stores committed collection data. That keeps stale committed state out of the transaction state machine.

The application supplies `CollectionRef::ttl` at the handler boundary. Cassandra carries the TTL via `USING TTL ?` on
every durable write that creates or refreshes a cell. Reads do not return the TTL — recovery callers re-supply it from
the application's collection-definition registry (Slice 8+); the row does not retain the original TTL value. Identity
and equality of `CollectionRef<K>` ignore the TTL field: two refs that point at the same collection compare equal even
when their TTLs differ. `ttl: None` writes without `USING TTL` and is a first-class value: it covers (1) indefinite
retention and (2) the Cassandra over-20-year overflow fallback, where a computed TTL above 630_720_000 seconds collapses
to `None` at the wiring layer.

**`CollectionDef` registry (Slice 8+).** The middleware also owns a builder-time
`HashMap<StateName, CollectionDef>` plus a middleware-wide `default_ttl`. `CollectionDef::ttl: Option<CompactDuration>`
records the per-collection override: `Some(d)` binds `d` on every write to that collection (including recovery
writes), and `None` is an explicit opt-out — the middleware will not invent a default for that name. Unregistered
collections use the middleware's `default_ttl`. `ctx.value(name)` resolves the TTL at handle-construction time, so
seal / `apply_sealed` / `rollback_sealed` see the registered value (or the middleware default) without each store
having to consult the registry.

**Constructor-supplied `default_ttl`.** Every production keyed-state store (`CassandraValueStore`,
`MemoryDurableValueStore`, `RecoveringValueStore`) owns a `default_ttl: Option<CompactDuration>` field, set at
construction. The store threads this value through `ValueStore::set` / `ValueStore::clear` (which build
`CollectionRef::new(id.clone(), self.default_ttl)` for the underlying `direct_apply` write) and through every recovery
write it issues (`apply_sealed` / `rollback_sealed`). No store reaches into a sibling type for its TTL: production wiring
sources `Some(cassandra_store.base_ttl())` (or `None` when a per-collection override or overflow fallback applies) once
at build time and passes the same value into each store's constructor. The store has a single constructor —
`CollectionRef::new(id, ttl: Option<CompactDuration>)` — so the TTL choice is always explicit at the callsite; `None` is
a deliberate value, never a forgotten one. Forgetting the TTL on a write that should carry one corrupts the durable
retention contract (cells written without `USING TTL` live forever), which is why the keyed-state stores never invent a
default and the `*_no_ttl` query variants are reachable only when `collection.ttl()` is genuinely `None`.

---

## Encoding

Cassandra uses Cassandra schema for durable structure and collection-level format tags for opaque bytes. Dirty datatype
implementations use the same operation encoding for dirty op records so seal can copy frames into the WAL stream without
re-encoding operations.

### Encoding Columns

Use checked Rust enums over Cassandra `smallint` columns. Cassandra has no native enum type, and `smallint` gives a
compact, explicit durable representation.

```rust
#[repr(i16)]
enum PayloadEncoding {
    MsgpackV1 = 1,
    MsgpackZstdV1 = 2,
}

#[repr(i16)]
enum WalFormat {
    MsgpackStreamV1 = 1,
    MsgpackStreamZstdV1 = 2,
}
```

`PayloadEncoding` describes every stored payload cell in one collection partition. `WalFormat` describes the `wal_ops`
stream. They evolve independently.

### Stored Payloads

```rust
enum StoredPayload {
    Inline(Bytes),
    KafkaMessage(KafkaMessageRef),
}

struct KafkaMessageRef {
    topic: Topic,
    partition: Partition,
    offset: Offset,
}
```

`StoredPayload` is the physical value stored by Cassandra, memory, Fjall, dirty overlays, and WAL operations. Stores do
not interpret or resolve payload variants. They copy, stream, cache, seal, apply, and roll back `StoredPayload` values
as opaque values with a stable serialized enum shape.

`Inline` contains bytes produced by the configured state payload codec. `KafkaMessage` contains only exact Kafka
coordinates. Loading that reference into a full message is a higher-layer concern and is intentionally outside the store
traits.

The serialized enum determines the payload variant; there is no separate value-kind column. New payload variants can be
added without changing datatype store traits.

### WAL Stream

`wal_ops` is a MsgPack stream, optionally zstd-compressed:

```text
MsgPack(WalHeader<K>)
MsgPack(K::Op)
MsgPack(K::Op)
...
```

```rust
struct NonEmptyOps<T> {
    first: T,
    rest: Vec<T>,
}

struct OpStream<K: CollectionKind> {
    ops: BoxStream<'static, Result<K::Op, StateError>>,
    count: NonZeroU64,
}

struct WalHeader<K: CollectionKind> {
    version: u16,
    kind: CollectionKindId,
    op_count: NonZeroU64,
    _kind: PhantomData<K>,
}

struct WalBlob<K: CollectionKind> {
    bytes: Bytes,
    format: WalFormat,
    _kind: PhantomData<K>,
}

struct WalOpStream<K: CollectionKind> {
    header: WalHeader<K>,
    ops: OpStream<K>,
}

struct WalEnvelope<K: CollectionKind> {
    header: WalHeader<K>,
    ops: NonEmptyOps<K::Op>,
}
```

The decoder is kind-specific:

```rust
decode_wal::<ValueKind>(blob)
```

It validates:

- `wal_format` is known.
- the header kind matches `ValueKind::ID`;
- `op_count > 0`;
- every frame decodes as `ValueOp`;
- no trailing garbage remains after the expected operation count.

Production apply/recovery should prefer `WalOpStream<K>` so large WALs can be decoded incrementally. `WalEnvelope<K>` is
the small, materialized representation useful in tests and small helper paths; it obeys the same kind and non-empty
invariants.

Use map-style/named MsgPack records for durable WAL types. New fields may be added with defaults. Existing variants and
field meanings are never repurposed. Breaking changes use a new `WalFormat`.

### Compression

- Stored payload cells use the collection's `PayloadEncoding`.
- `PayloadEncoding::MsgpackZstdV1` compresses the serialized `StoredPayload` envelope. Cassandra table compression may
  still compress the row again; payload-level compression is useful when values are copied through WAL or cache layers.
- WAL blobs default to `WalFormat::MsgpackStreamZstdV1`.
- Dirty op values are already MsgPack frames. Seal streams those frames into the WAL encoder and compresses as it
  writes.

This avoids accumulating an uncompressed WAL in memory. The final compressed WAL blob still must fit in the Cassandra
cell until chunked WAL storage is added.

---

## Datatype And Capability Traits

Each data structure has one datatype trait for normal collection operations. Implementations may be durable stores,
dirty trackers, write-through caches, or in-memory test stores. Extra behavior is expressed through small capability
traits.

Reads use `Read<T>` rather than `Option<T>` because partial layers exist:

```rust
enum Read<T> {
    Present(T),
    Absent,
    Unknown,
}
```

- Cassandra and fully populated memory stores return `Present` or `Absent`.
- Dirty trackers return `Unknown` when they have no local opinion about a key.
- Fjall write-through caches may return `Unknown` outside covered ranges, then fall through to their backing store.

The generic transition capabilities are:

```rust
trait PendingOpSource<K: CollectionKind>: Clone + Send + Sync + 'static {
    type Error;

    async fn pending_ops(&self, id: CollectionId<K>) -> Result<Option<OpStream<K>>, Self::Error>;
    async fn clear_pending_ops(&self, id: CollectionId<K>) -> Result<(), Self::Error>;
}

trait DurableWalStore<K: CollectionKind>: Clone + Send + Sync + 'static {
    type Error;

    async fn read_partition(&self, id: &CollectionId<K>) -> Result<DurableState<K>, Self::Error>;
    async fn seal(
        &self,
        collection: &CollectionRef<K>,
        event: EventRef,
        ops: impl IntoIterator<Item=K::Op>,
    ) -> Result<SealedCollection<K>, Self::Error>;

    async fn apply_sealed(
        &self,
        collection: &CollectionRef<K>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error>;

    async fn rollback_sealed(
        &self,
        collection: &CollectionRef<K>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error>;
}

trait DirectApplyStore<K: CollectionKind>: Clone + Send + Sync + 'static {
    type Error;

    async fn direct_apply(
        &self,
        collection: &CollectionRef<K>,
        ops: impl IntoIterator<Item=K::Op>,
    ) -> Result<StoreOutcome, Self::Error>;
}
```

Write methods take `&CollectionRef<K>` so the TTL supplied at handler construction reaches durable storage on every
mutating call. Read paths stay on `&CollectionId<K>` because reads do not need the TTL.

Concrete datatype traits are named per kind:

```rust
trait MapStore: Clone + Send + Sync + 'static {
    type Error;
    type RangeStream: Stream<Item=Result<MapReadEntry, Self::Error>> + Send;

    async fn get(&self, id: CollectionId<MapKind>, key: EncodedMapKey)
                 -> Result<Read<StoredPayload>, Self::Error>;

    async fn range(&self, id: CollectionId<MapKind>, range: MapKeyRange)
                   -> Result<MapRangeRead<Self::RangeStream>, Self::Error>;

    async fn put(&self, id: CollectionId<MapKind>, key: EncodedMapKey, value: StoredPayload)
                 -> Result<(), Self::Error>;

    async fn remove(&self, id: CollectionId<MapKind>, key: EncodedMapKey)
                    -> Result<(), Self::Error>;

    async fn clear(&self, id: CollectionId<MapKind>) -> Result<(), Self::Error>;
}
```

Map range reads distinguish a complete committed answer from a dirty overlay:

```rust
enum MapRangeRead<S> {
    Complete(S),
    Overlay(S),
    Unknown,
}

enum MapReadEntry {
    Present { key: EncodedMapKey, value: StoredPayload },
    Removed { key: EncodedMapKey },
}
```

`Complete` means the stream is authoritative for the requested range. `Overlay` means the stream contains only local
changes and tombstones that must be merged over a committed range. `Unknown` means this layer cannot answer the range.
This keeps "partial range accidentally returned as complete" out of the type system.

Range and iteration APIs are always streaming. A Cassandra range query, Fjall range scan, dirty overlay scan, or memory
reference implementation may buffer small implementation details, but it must not realize an unbounded range as a
collection before returning results. Deque range and iteration APIs follow the same shape with indexed entries.

Cassandra emits `Complete` streams with only `Present` entries and never returns `Unknown` for point reads. A dirty
Fjall
map returns `Overlay` for ranges, may return `Unknown` for untouched point keys, and may emit `Removed` entries. A
committed Fjall cache returns `Complete` only when coverage metadata proves the whole requested range is covered;
otherwise it returns `Unknown`, and `LayeredMapStore` consults the backing store.

### Combinators

`LayeredMapStore<C, S>` composes a cache-like map with a backing map:

```rust
struct LayeredMapStore<C, S> {
    cache: C,
    store: S,
}
```

It implements `MapStore` when `C: MapStore` and `S: MapStore`. It implements `DurableWalStore<MapKind>` and
`DirectApplyStore<MapKind>` when the backing store implements those capabilities, delegating the durable operation and
then patching or invalidating the cache.

`TransactionMapStore<D, S>` composes dirty state with durable state:

```rust
struct TransactionMapStore<D, S> {
    dirty: D,
    durable: S,
    tx: LocalTx<MapKind>,
}
```

It requires:

```rust
D: MapStore + PendingOpSource<MapKind>
S: MapStore + DurableWalStore<MapKind> + DirectApplyStore<MapKind>
```

and implements handler-facing behavior:

```text
get/range:
  read dirty first
  for get, Present/Absent wins and Unknown falls through to durable
  for range, Overlay is merged over a durable Complete range; Unknown reads durable unchanged

put/remove/clear:
  write dirty only

seal:
  dirty.pending_ops -> build WalBlob -> durable.seal -> dirty.clear_pending_ops

flush/direct:
  dirty.pending_ops -> durable.direct_apply -> dirty.clear_pending_ops

abort:
  dirty.clear_pending_ops
```

A separate pending-index trait can be shared by all kinds:

```rust
trait PendingIndexStore: Clone + Send + Sync + 'static {
    type Error;

    async fn scan_pending(&self, state_key: StateKey) -> Result<Vec<PendingCollection>, Self::Error>;
    async fn delete_pending(&self, id: UntypedCollectionId) -> Result<(), Self::Error>;
}
```

`read_partition` validates any sealed WAL header against `K::ID` before exposing it to kind-specific code.
`PendingCollection` is untyped because the pending index scans across kinds. Recovery immediately dispatches each row to
the kind module that owns that `CollectionKindId`, which converts it to a typed `CollectionId<K>`.

---

## Dirty Datatype Implementations

The dirty datatype implementation is an event-local, spillable transaction log plus a read overlay. It implements the
same datatype trait as durable stores, and it also implements `PendingOpSource<K>` so seal/direct apply can stream the
pending operations. It is not durable state and is not reused after a restart. The local state machine's
`DirtyCollection<K>` is only a typed handle into this workspace; it does not store the operations in memory.

The Fjall dirty implementation uses this layout within the assignment keyspace:

```text
dirty_ops/<scope>/<collection>/<seq>       -> MsgPack(K::Op)
dirty_overlay/<scope>/<collection>/...     -> K::Overlay record or StoredPayload
dirty_meta/<scope>/<collection>/next_seq   -> u64
dirty_meta/<scope>/<collection>/op_count   -> u64
```

A user mutation performs one short dirty-store transaction or batch. For Fjall:

```text
put dirty_ops[seq] = MsgPack(K::Op)
update dirty_overlay
update next_seq and op_count
```

The first write calls `begin(CollectionRef<K>, K::Op) -> DirtyCollection<K>`. Later writes call
`append_op(DirtyCollection<K>, K::Op) -> DirtyCollection<K>`. That keeps "dirty with zero operations" unrepresentable.

No long-lived dirty transaction crosses handler awaits. Fjall transactions or batches are local atomic updates only.

Dirty datatype invariants:

- `dirty_ops` is authoritative for the current attempt's pending writes.
- `dirty_overlay` is derived from `dirty_ops` and may be rebuilt from it.
- Dirty data is scoped to exactly one `EventScopeId`.
- Dirty data is deleted after seal, direct apply, `flush()`, or abort.
- Dirty implementation errors before seal/direct apply fail the handler.

### Seal From Dirty Ops

WAL mode success streams dirty ops into a typed WAL blob:

```text
scan dirty_ops/<scope>/<collection>/ in sequence order
write MsgPack(WalHeader<K>)
copy each MsgPack(K::Op) frame
optionally zstd-compress
store.seal(id, event, WalBlob<K>)
delete dirty prefixes after seal succeeds
```

The dirty implementation does not build or retain an in-memory `WalBuilder`.

### Direct Apply And Flush

Direct mode and `flush()` stream dirty ops into the kind-specific direct apply path:

```text
scan dirty ops
decode as K::Op
store.direct_apply(id, OpStream<K>)
delete dirty prefixes after durable success
```

`flush()` applies one collection's dirty ops and returns that collection to `Clean`; the handler may continue and create
new dirty ops afterward.

---

## Fjall Store Implementations

Fjall implements the same datatype store traits as Cassandra and memory. It is not a second state machine and it does
not know Cassandra's physical table invariants. Higher-level combinators decide whether a Fjall-backed datatype store is
being used as a dirty tracker, a committed cache, or a plain local test store.

Slice 6 lands `FjallValueStore` in the **committed cache** role only. The dirty Value workspace remains the in-memory
`MemoryDirtyValueStore`; a Fjall dirty tracker is design Slice 7+ territory. The cache fronts an authoritative
`MemoryDurableValueStore` or `CassandraValueStore` through `LayeredValueStore<FjallValueStore, _>`.

```rust
struct FjallMapStore {
    keyspace: FjallKeyspace,
}
```

`FjallMapStore` implements `MapStore`. When it is used as the dirty tracker, it is paired with
`PendingOpSource<MapKind>`. When it is used as the committed cache, it is paired with an authoritative store through
`LayeredMapStore<FjallMapStore, CassandraMapStore>` or `LayeredMapStore<FjallMapStore, MemoryMapStore>`.

### Cache Key Layout

The Fjall cache lays out each cached cell as:

```text
[16-byte collection hash][inner key bytes]
```

The collection hash is `xxh3_128(segment_id || 0x00 || key_bytes || 0x00 || state_type || 0x00 || name_bytes)`,
serialized big-endian for stable cross-platform ordering. Inner key bytes are empty for Value; Map and Deque (future
slices) append their kind-specific inner key. This layout supports point reads (Value), prefix scans (collection-wide
invalidation), and ordered range scans (Map/Deque). See `src/state/fjall/codec.rs::collection_prefix` for the
rationale.

Each cached value cell is a single tag byte (`0x00 = Absent`, `0x01 = Present`) optionally followed by an
`encode_payload(StoredPayload, MsgpackZstdV1)` blob. A missing entry decodes as `Read::Unknown`; the absent tag
distinguishes "known absent" from "never observed."

The write-through behavior belongs to the layered combinator:

```text
get/range:
  read cache
  if cache returns Present/Absent, return it
  if cache returns Unknown, read backing store and populate cache

seal:
  backing.seal succeeds
  mark collection SEALED in cache or invalidate collection

apply_sealed:
  backing.apply_sealed succeeds
  patch cache if simple; otherwise invalidate collection

rollback_sealed:
  backing.rollback_sealed succeeds
  clear sealed marker or invalidate collection

direct_apply:
  backing.direct_apply succeeds
  patch cache if simple; otherwise invalidate collection
```

Cache failure after a backing-store success must not fail the durable operation. The wrapper invalidates the affected
collection or disables the assignment workspace and returns success.

Cassandra's static columns are Cassandra-internal implementation details. `CassandraMapStore` and
`CassandraDequeStore` load and maintain their own `count`, cursor, and any future min/max key statics. The Fjall cache
does not mirror those statics as part of its correctness contract; it tracks only local cache entries, cache coverage,
and enough invalidation metadata to decide whether a cached absence is known or unknown.

### Assignment Scope

Use one Fjall database per consumer process and one assignment-scoped Fjall keyspace or directory per owned Kafka
partition epoch:

```text
topic / partition / assignment_epoch
```

On partition revocation, stop handlers, drop the workspace, and delete the assignment data. On process restart or cache
format mismatch, delete old workspaces. Stale Fjall data is never addressable by a new assignment.

### Coverage For Sorted Collections

Fjall's committed cache is a positive cache unless coverage proves absence. For Map and Deque:

```text
Inside a covered range, Fjall is authoritative for presence and absence.
Outside a covered range, Fjall absence means unknown.
```

Map cache:

```text
committed_map/<collection>/<encoded_map_key> -> StoredPayload
coverage/<collection>/<start>               -> end
```

Deque cache:

```text
committed_deque/<collection>/<encoded_idx> -> StoredPayload
coverage/<collection>/<start_idx>          -> end_idx
```

Reads:

```text
get(k):
  if k is covered -> Fjall hit or miss is authoritative
  else Fjall hit is usable, miss returns Unknown

range(a..b):
  if a..b is fully covered, return Complete(scan Fjall range)
  otherwise return Unknown

Layered range(a..b):
  if cache returns Complete, use it
  if cache returns Unknown, stream a..b from backing store
  opportunistically write each streamed item into Fjall
  mark a..b covered and normalize coverage ranges only after the backing stream completes successfully

iter():
  range(MIN..MAX)
```

Coverage is a completion marker, not an intent marker. A range becomes covered only after every authoritative item in
that range has been streamed from the backing store and reflected in the cache. If the backing stream, cache write, or
consumer drops early, the range remains uncovered or is invalidated; later reads fall through to the backing store.

Dirty reads are handled by `TransactionMapStore` before committed cache reads. After Map or Deque durable writes, v1
invalidates that collection's committed entries and coverage. Value can be patched exactly.

---

## Handler API

Handlers reach keyed state through `KeyedStateMiddleware<D, Sc, O, P>`, a
[`HandlerMiddleware`](src/consumer/middleware/mod.rs) that wraps an inner handler and constructs a
`KeyedStateContext<C, D>` per event. The wrapped context implements `EventContext` (delegating every method
to the inner context) and the extension trait `KeyedStateAccess`. Handlers that want to access state add
`+ KeyedStateAccess` to their context bound:

```rust
async fn on_message<C>(&self, ctx: C, msg: ConsumerMessage<P>, _: DemandType)
    -> Result<Self::Output, Self::Error>
where
    C: EventContext + KeyedStateAccess,
{
    let counter = ctx.value("counter")?;
    counter.set(StoredPayload::Inline(Bytes::from_static(b"42"))).await?;
    Ok(())
}
```

`ctx.value(name) -> ValueHandle` returns a handle that drives a `TransactionValueStore` for one
`(event, collection)` pair. Repeated `value(name)` calls in the same dispatch return handles bound to the
same transaction; dirty ops accumulate across calls. The handle exposes `get`, `set`, and `clear` as
`async fn`s.

The middleware sits below any rescue, retry, or defer middleware and above the user handler. It owns:

- The composed durable bundle (`Layered<Fjall, Recovering<Cassandra, CommitOracle>>` in production).
- The read-side `PendingIndexScanner` for `StateRecovery` sweeps.
- A `CommitOracle` (the existing `CommitManager`).
- A `CollectionDefRegistry` for per-collection TTL overrides.
- A `CommitMode` (Wal or Direct, fixed at builder time today).
- A `recovery_delay: CompactDuration` (default 30s) added to `now()` when scheduling `StateRecovery`.

## Valid Transitions

Every collection has a fixed commit mode for the lifetime of an `EventScope`:

```rust
enum CommitMode {
    Wal,
    Direct,
}
```

- `Wal` is the default. Handler success seals dirty ops under the upstream event.
- `Direct` bypasses the WAL. Handler success directly applies dirty ops.
- `flush()` is an explicit direct apply of one dirty collection, regardless of mode.

### Handler Body

```text
Clean --write(K::Op)--> Dirty
Dirty --more writes--> Dirty
Dirty --flush()--> Clean
Clean --flush()--> Clean
```

Reads merge:

```text
dirty datatype store -> durable datatype store
```

When the durable store is layered, its first read goes through the committed Fjall cache and falls through to Cassandra
or memory only on `Unknown`.

If a storage read finds `DurableState::Sealed`, first-touch recovery resolves it before returning data.

### WAL-Mode Success

```text
Dirty --handler Ok--> Sealed(event)
Sealed(event) --after_commit--> Clean
Sealed(event) --after_abort--> Clean
Sealed(event) --crash--> DurableState::Sealed
```

Seal performs:

1. Build `WalBlob<K>` by streaming dirty ops.
2. `INSERT` into `keyed_state_pending`.
3. `UPDATE` the collection partition to `Sealed(event, wal_blob)`.
4. Schedule a `StateRecovery` timer for `(segment, key)`.
5. Delete dirty workspace prefixes after seal succeeds.

The seal `UPDATE` is one Cassandra row mutation; the full compacted WAL must fit in one cell. The framework does not
pre-validate WAL size — oversized seals fail with whatever the driver returns, wrapped as `Durable(...)`.

After the framework commits the message or timer, `after_commit` calls `store.apply_sealed`. If the framework aborts
the event, `after_abort` calls `store.rollback_sealed`.

**Apply.** `apply_sealed` drains the WAL incrementally rather than in one mutation:

```text
loop until wal_ops is empty:
    peel next batch of N ops off the front of wal_ops
    in one atomic row mutation:
        data     = fold(data, peeled)
        wal_ops  = encode(remaining)
        when remaining is empty, also clear wal_event and payload_encoding
```

Every iteration is one row update on the same partition, atomic by virtue of single-row Cassandra semantics. The batch
size N is chosen so each mutation fits comfortably under the driver's frame size. Apply is naturally restartable: if
the process dies mid-loop, the durable row is left in `DurableState::Sealed { applied: partial, wal: remaining }`;
recovery re-asks the oracle (still says `Committed`), and `apply_sealed` resumes from the partial state. Rollback
remains a single `UPDATE` that clears the WAL columns; the entry's `data` is the pre-seal authoritative state by
construction, so no per-op work is required to roll back.

The memory store applies in one shot — equivalent to the trivial case of `N = ∞`. Slice 1 establishes the trait
contract; the chunked loop is a Slice 5 (Cassandra) implementation detail.

### Direct-Mode Success

```text
Dirty --handler Ok--> Clean
Dirty --handler Err--> Clean
```

On success, dirty ops are applied directly through `store.direct_apply`. There is no sealed state and no recovery work.
If the handler returns an error before direct apply starts, dirty prefixes are deleted.

### Recovery

```text
DurableState::Sealed(event, wal)
    oracle says Committed    -> store.apply_sealed(event)   -> Idle(applied)
    oracle says NotCommitted -> store.rollback_sealed(event) -> Idle(original)

pending index row + Idle partition -> delete stale pending index row
```

Recovery is triggered by first touch or by the durable `StateRecovery` timer. Both paths run the same transition.

### `StateRecovery` timer lifecycle

`StateRecovery` is `TimerType::StateRecovery = 3`. The middleware schedules at most one per event and only
in `CommitMode::Wal`:

```text
Wal handler Ok with >=1 seal:
  context.schedule(now + recovery_delay, TimerType::StateRecovery)

after_commit(Ok(_)) with sealed list:
  for each sealed CollectionRef:
    durable.apply_sealed(collection_ref, event)
  context.clear_scheduled(TimerType::StateRecovery)

after_commit(Err(_)) / after_abort(*):
  for each sealed CollectionRef:
    durable.rollback_sealed(collection_ref, event)
  context.clear_scheduled(TimerType::StateRecovery)

StateRecovery fires:
  for entry in scanner.scan_pending(state_key):
    if entry.kind != Value: WARN and skip   # Slice 9+
    match durable.read_partition(id):
      Idle    -> durable.delete_pending(id)         # stale pending row
      Sealed  -> oracle.resolve(...) -> apply | rollback
  context.clear_scheduled(TimerType::StateRecovery)
```

Direct mode has no `StateRecovery` schedule call by construction — the `CommitMode::Direct` branch in the
middleware does not have access to the helper that schedules it.

---

## Cassandra Storage Layout

The WAL design uses one table per collection kind plus a shared pending index.

```cql
CREATE TYPE event_ref (
    kind          tinyint,    -- 0=MSG, 1=TMR
    msg_dedup_id uuid,       -- MSG
    timer_type   tinyint,    -- TMR
    time         int,        -- TMR
    tag          int         -- TMR
);

CREATE TABLE keyed_state_value (
    segment_id  uuid,
    key         text,
    state_type  tinyint,
    name        text,

    data             blob,
    payload_encoding smallint,

    wal_event    frozen<event_ref>,
    wal_ops      blob,
    wal_format   smallint,

    PRIMARY KEY ((segment_id, key, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy' }
  AND compression = { 'class': 'ZstdCompressor' };

CREATE TABLE keyed_state_map (
    segment_id  uuid,
    key         text,
    state_type  tinyint,
    name        text,

    map_key      blob,
    value        blob,

    payload_encoding smallint            STATIC,
    count            bigint              STATIC,
    wal_event        frozen<event_ref>   STATIC,
    wal_ops          blob                STATIC,
    wal_format       smallint            STATIC,
    wal_count        bigint              STATIC,

    PRIMARY KEY ((segment_id, key, state_type, name), map_key)
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy' }
  AND compression = { 'class': 'ZstdCompressor' };

CREATE TABLE keyed_state_deque (
    segment_id   uuid,
    key          text,
    state_type   tinyint,
    name         text,

    idx          bigint,
    value        blob,

    payload_encoding smallint            STATIC,
    first            bigint              STATIC,
    last             bigint              STATIC,
    wal_event        frozen<event_ref>   STATIC,
    wal_ops          blob                STATIC,
    wal_format       smallint            STATIC,

    PRIMARY KEY ((segment_id, key, state_type, name), idx)
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy' }
  AND compression = { 'class': 'ZstdCompressor' };

CREATE TABLE keyed_state_pending (
    segment_id  uuid,
    key         text,
    state_type  tinyint,
    kind        tinyint,
    name        text,

    PRIMARY KEY ((segment_id, key), state_type, kind, name)
) WITH compaction = { 'class': 'UnifiedCompactionStrategy' };
```

**Payload encoding.** `payload_encoding` is required whenever a collection contains any `data` / `value` cells or a
sealed WAL. It is row-local for Value and STATIC for Map and Deque. The encoded cell is a `StoredPayload`; the enum
discriminant inside the blob determines whether the value is inline bytes or a Kafka message reference. Unknown encoding
discriminants are permanent decode errors. When `CollectionRef::ttl()` is `Some(_)`, the store binds the TTL via
`USING TTL ?` on writes; `None` writes the columns without a TTL clause.

**WAL columns.** `wal_event`, `wal_ops`, `wal_format`, and `payload_encoding` are written together in one partition
update when sealing. Rust decodes them into `DurableState<K>`; callers do not handle independent `Option`s.

**Pending index ordering.** Seal inserts the pending-index row before writing the WAL. Apply and rollback clear the WAL
before deleting the pending-index row. A crash mid-pair can leave a stale index entry, but cannot lose a SEALED WAL.

**Map keys.** `map_key` is an encoded byte key whose lexicographic order matches the user's `Ord`. A custom `MapKey`
implementation must prove canonical encoding, lossless decoding, and order preservation by property test.

**Map count.** Map stores a STATIC `count`. Seal writes the post-apply count to `wal_count`; apply lifts `wal_count`
into
`count` while applying mutations and clearing WAL.

**Deque cursors.** Deque stores STATIC `first` and `last`. Empty deque means both are null. Non-empty deque means both
are non-null and `first <= last`.

**Cassandra-owned statics.** Each Cassandra datatype store owns the static metadata that makes its table efficient:
`count` for Map, `first`/`last` for Deque, and any future min/max key or cursor statics. The Cassandra store loads,
validates, repairs, and updates those fields behind the datatype trait. Cache layers do not share responsibility for
them and do not need to know they exist.

### Backend Contract

Cassandra and memory preserve the same observable ordering:

- `seal` writes pending index first, then WAL columns.
- `apply_sealed` applies kind-specific data and clears WAL in the collection partition, then deletes the pending index.
- `rollback_sealed` clears WAL first, then deletes the pending index.
- `direct_apply` writes only applied data. It never touches WAL columns or the pending index.

The memory backend should preserve these orderings so property tests can exercise stale index rows and SEALED partitions
the same way they occur with Cassandra.

---

## Crash Robustness

### WAL Mode

| Crash point                                    | Durable shape                               | Resolution                                                                    |
|------------------------------------------------|---------------------------------------------|-------------------------------------------------------------------------------|
| During handler body                            | Dirty implementation only, no Cassandra WAL | Event retries and recomputes local ops.                                       |
| After some seals, before durability marker     | Some collections SEALED                     | Oracle says not committed; recovery rolls them back.                          |
| After durability marker, before `after_commit` | SEALED WAL                                  | Oracle says committed; recovery applies it.                                   |
| During `after_commit`                          | Some applied, some SEALED                   | Remaining SEALED collections apply through recovery.                          |
| During `after_abort`                           | Some rolled back, some SEALED               | Remaining SEALED collections roll back through recovery.                      |
| Pending index written, WAL not written         | Stale pending row + IDLE partition          | Recovery deletes the stale pending row.                                       |
| WAL written, pending index not visible         | SEALED partition                            | First-touch recovery resolves it; seal ordering avoids this in normal writes. |

The event's durability marker is the cross-collection commit point. Keyed state never decides whether that marker
landed; it asks the commit oracle identified by `EventRef`.

### Direct Mode And `flush()`

Direct apply has no rollback protocol:

| Crash point                                     | Result                                                |
|-------------------------------------------------|-------------------------------------------------------|
| Before direct apply starts                      | Dirty ops are lost; event may retry.                  |
| During direct apply                             | Storage may or may not contain the mutation.          |
| After direct apply, before response is observed | Event may retry and user code may direct-apply again. |

This is correct by contract. Direct mode and `flush()` are for users who intentionally want an early or WAL-free
durability boundary and can make their handler logic idempotent at the application level.

### Local Workspace

| Event                                       | Behavior                                               |
|---------------------------------------------|--------------------------------------------------------|
| Process restart                             | Delete old workspaces; Cassandra recovers truth.       |
| Partition revocation                        | Stop handlers, drop workspace, delete assignment data. |
| Cache format mismatch                       | Delete workspace and reload from Cassandra.            |
| Committed-cache corruption                  | Invalidate affected prefix or disable cache.           |
| Dirty implementation corruption before seal | Fail handler; redelivery recomputes dirty ops.         |

Fjall is never a recovery authority.

---

## Commit Oracles

Recovery uses `CommitManager`, a read-only facade over the two upstream commit sources.

### Messages

The deduplication middleware computes a UUID from the event identity at the dispatch gate. WAL stores that UUID.
Recovery asks whether the UUID exists in the deduplication table.

```text
dedup row exists -> Committed
dedup row absent -> NotCommitted
```

### Timers

The timer WAL stores `(timer_type, time, tag)`. `tag` is a random `i32` stored on timer rows and rotated when
`complete()` commits a `FiringRescheduled` timer.

```text
timer row absent          -> Committed
current tag == WAL tag    -> NotCommitted
current tag != WAL tag    -> Committed
```

The tag distinguishes "the original fire has not committed" from "the fire committed and rescheduled a new row at the
same logical coordinates."

---

## Adding Collection Kinds

A new data structure should be a local addition. It should not require changes to `EventScope`, commit-oracle logic,
pending-index recovery, or the WAL state machine.

Each kind module owns:

1. **Kind marker.** A `CollectionKind` implementation with `Op`, `Applied`, and `Overlay` associated types.
2. **Operation schema.** A closed, MsgPack-serializable `K::Op` enum. Variants are additive; existing tags are never
   reshaped or repurposed.
3. **Applied model.** The durable representation decoded from Cassandra or memory into `K::Applied`.
4. **Overlay model.** The dirty read-your-writes representation in Fjall.
5. **Pure apply logic.** Deterministic functions that fold op streams into applied state or concrete storage writes.
6. **Store adapter.** Kind-specific `read_partition`, `apply_sealed`, and `direct_apply` behavior over the shared
   `seal`, `rollback`, pending-index, and recovery protocol.
7. **Datatype trait implementation.** Cassandra, memory, and optional local stores implement the same kind-specific
   trait. Additional capabilities such as `PendingOpSource`, `DurableWalStore`, and `DirectApplyStore` are implemented
   only where they are meaningful.
8. **Property tests.** Model tests plus memory/Cassandra equivalence tests for the kind's invariants.

This keeps future Set, SortedSet, Counter, or approximate structures from turning the shared recovery machinery into a
large union of collection-specific cases.

---

## Implementation Slices

Implement in thin slices so invariants are testable before full middleware integration.

1. **Typed Value protocol with memory store.** Add `CollectionKind`, typed identity, `EventRef`, `CommitMode`,
   `DurableState`, `LocalTx`, `ValueOp`, memory-backed store, direct apply, seal, apply, rollback, and recovery.
2. **MsgPack payload and WAL encoding.** Add `PayloadEncoding`, `WalFormat`, `StoredPayload`,
   `WalBlob<ValueKind>`, streaming encode/decode, and property tests for malformed and mismatched kind/format cases.
3. **Datatype and capability traits.** Add the Value datatype trait plus `PendingOpSource`, `DurableWalStore`, and
   `DirectApplyStore`. Implement memory as both dirty tracker and durable reference store.
4. **Shared property tests.** Drive random traces against the memory store and a pure model. Assert recovery
   idempotence,
   stale-index cleanup, WAL/direct behavior, dirty-workspace behavior, and visible state after every transition.
5. **Cassandra Value store.** (**Complete.**) Schema migration, `CassandraValueStore`, `event_ref` UDT serde, row
   decoder, per-collection TTL on `CollectionRef<K>`, and a write-side `PendingIndexStore` trait. The shared
   memory/Cassandra property suite drives equivalence by construction.
6. **Fjall Value store and combinators.** (**Complete for cache role.**) `FjallValueStore` implements
   `ValueStore` over a fjall partition; `LayeredValueStore<Cache, Backing>` patches the cache on
   `apply_sealed`/`direct_apply` and leaves it untouched on `seal`/`rollback_sealed`. Cache failures never surface
   as the outer error: a failed cache read falls through to the backing store (and auto-repairs the cell via the
   miss-then-populate cycle); a failed cache write after a successful backing write is logged at WARN and the entry
   is invalidated. This keeps the layered store's `ValueStore::Error` equal to the backing's error type, which the
   shared `DurableBundle` requires. The `value_test_suite::run_*` runners exercise
   `LayeredValueStore<FjallValueStore, MemoryDurableValueStore>` (default CI) and
   `LayeredValueStore<FjallValueStore, CassandraValueStore>` (gated on `INTEGRATION_TESTS`) so layering is
   behavior-preserving by construction. The Fjall dirty workspace is deferred to a later slice.
7. **First-touch integration.** (**Complete.**) `RecoveringValueStore<Inner, Oracle>` resolves SEALED Value
   partitions before reads return. The combinator wraps any durable Value store + a `CommitOracle`; `ValueStore::get`
   reads `inner.read_partition`, dispatches `Sealed { wal, .. }` through the oracle, calls `apply_sealed` or
   `rollback_sealed` on the inner using a `CollectionRef` built from the wrapper's constructor-supplied `default_ttl`,
   then returns the resolved value. Every other method passes through. `impl CommitOracle for CommitManager<D, T>`
   bridges the existing message/timer bool oracles into `CommitDecision`. Production composition stays test-only at
   this slice; Slice 8 wires the production builder. The slice also threads `default_ttl: Option<CompactDuration>`
   through every keyed-state store's constructor so production writes stop hardcoding `None` and the `*_no_ttl` query
   variants fire only when the collection truly opted into indefinite retention (or the overflow fallback applies).
   The durable `StateRecovery` timer, the `scan_pending` sweep, and the per-collection TTL registry remain Slice 8.
8. **Middleware and recovery timer.** (**Complete.**) `KeyedStateMiddleware<D, Sc, O, P>` wires handler success
   and abort hooks to `apply_sealed` / `rollback_sealed`, schedules the durable `StateRecovery` timer after
   seal in WAL mode, and routes the timer's fire back through `scan_pending` + the commit oracle. The middleware
   constructs a `KeyedStateContext<C, D>` per event that wraps the inner `EventContext` and exposes
   `ctx.value(name)` through the extension trait `KeyedStateAccess`. Repeated `value(name)` calls on the same
   context return handles that share a `TransactionValueStore` so dirty ops accumulate. The middleware owns a
   `CollectionDefRegistry`: registered collections bind a per-collection TTL on every write; unregistered
   collections fall back to the middleware-wide `default_ttl`. `CommitMode::Direct` literally cannot schedule
   the recovery timer — that branch calls `direct_apply` and returns; only the WAL branch calls
   `context.schedule(TimerType::StateRecovery)`. The recovery handler reads each pending entry's partition; an
   `Idle` partition is a stale pending row and is removed via `delete_pending`, a `Sealed` partition is
   resolved via the oracle. Non-Value kinds are logged at WARN and skipped (Slice 9+ plugs them in). The
   memory and Cassandra durable Value stores both implement the new read-side `PendingIndexScanner` trait that
   yields a `PendingEntry { state_type, kind, name }` stream for one `(segment, key)` partition.
9. **Map and Deque.** Add kind-specific modules after the protocol is proven for Value.

Seal writes the full compacted WAL into one Cassandra row mutation. Apply drains the WAL incrementally (see
§"WAL-Mode Success" — Apply) so even large WALs commit safely, but the initial seal write is one-shot. Oversized seals
fail with whatever the Cassandra driver returns (wrapped as `Durable(...)`); the framework does not pre-validate WAL
size. Chunked WAL **storage** — splitting one WAL across multiple rows — remains future work.

---

## Testing

Tests should be invariant-driven.

- **State machine properties.** Generate traces containing writes, `flush`, success in WAL mode, success in Direct mode,
  abort, commit hook, abort hook, crash, first-touch recovery, and recovery sweeps.
- **Encoding properties.** Verify unknown payload encodings and WAL formats, missing `payload_encoding` when cells or
  SEALED WAL require it, kind mismatches, empty WAL streams, bad MsgPack, and trailing bytes become structured errors.
- **Stored payload properties.** Verify all payload variants round-trip through Cassandra, memory, Fjall, dirty ops, and
  WAL apply without interpretation or variant-specific store behavior.
- **Streaming properties.** Verify range and iteration APIs can be consumed incrementally, do not require all values in
  memory, and mark cache coverage only after the authoritative stream completes successfully.
- **Dirty tracker properties.** Verify dirty ops are authoritative, overlay can be rebuilt from ops, seal streams ops
  in order, and abort/flush/seal delete the right prefixes.
- **Layered cache properties.** Verify `Layered*Store<Fjall*Store, S>` returns the same visible results as `S` for
  every generated trace, including cache invalidation and coverage cases.
- **Memory/Cassandra equivalence.** Use the existing store-test pattern: memory is fast and runs broadly; Cassandra runs
  the same property suite against a test keyspace.
- **Oracle matrix.** For SEALED WAL, verify committed message, uncommitted message, committed timer, uncommitted timer,
  and committed-and-rescheduled timer decisions.
- **Pending-index cases.** Verify stale pending rows are deleted and missing pending rows do not prevent first-touch
  recovery.
- **Direct-mode contract.** Verify Direct mode and `flush()` never create WAL and never call the oracle. Tests should
  document that replaying direct mutations is user-visible at-least-once behavior.

Property-test iteration counts should come from QuickCheck's default environment handling; do not hardcode counts in
test bodies.

---

## Open Questions

- **Direct mode API.** The mode should be explicit enough that users cannot accidentally opt out of WAL protection. A
  builder-level declaration is preferable to a per-call flag.
- **Map count strategy.** Map count can stay exact by point-loading unknown keys before `put`/`remove`; a future
  optimization can batch those checks at seal time.
- **Deque size ceiling.** V1 assumes deque partitions fit in memory or Fjall-backed iteration on first touch. Larger
  queues should use Map keyed by sequence number until a streaming deque design exists.
- **Chunked WAL storage.** `apply_sealed` drains the WAL incrementally (see §"WAL-Mode Success" — Apply), so even
  large WALs commit safely without splitting storage. Chunked WAL *storage* — one logical WAL spread across multiple
  rows — is still future work; required if a single seal write would exceed the Cassandra cell ceiling.
