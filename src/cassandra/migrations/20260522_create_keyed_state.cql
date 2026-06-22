-- Migration: create_keyed_state
-- Timestamp: 20260522
-- Purpose: Provision the durable storage for the uniform keyed-state CELL STORE
--          plus the group-global descriptor identity table. One Cassandra
--          partition (segment_id, key, state_type, name) is ONE collection; each
--          clustering row (namespace, order_key) is ONE cell. A cell is one
--          provisional cell: `data` is the committed value once `event` is null,
--          or the in-flight event's outcome while `event` is set, with
--          `prev_data` carrying the committed value the event superseded. The
--          `event_ref` UDT carries the upstream event identity (Kafka message or
--          timer row) that owns a provisional cell.

CREATE TYPE {{KEYSPACE}}.event_ref (
    kind         tinyint,
    msg_dedup_id uuid,
    timer_type   tinyint,
    time         int,
    tag          int
);

-- One partition = one collection (one transactional cell store). Clustering
-- columns address a cell within it: `namespace` discriminates sub-structures
-- (Meta vs Entries), `order_key` orders cells within a namespace by unsigned
-- lexicographic byte order (= the order-preserving codec's contract). A
-- single-namespace forward scan is a contiguous clustering range:
--   WHERE <pk>=… AND namespace = ? AND order_key >= ?  (no ALLOW FILTERING).
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_CELL}} (
    segment_id uuid,
    key        text,
    state_type tinyint,
    name       text,

    namespace  tinyint,
    order_key  blob,

    data       blob,
    prev_data  blob,
    encoding   smallint,
    version    int,

    event      frozen<event_ref>,

    PRIMARY KEY ((segment_id, key, state_type, name), namespace, order_key)
) WITH CLUSTERING ORDER BY (namespace ASC, order_key ASC)
  AND compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };

-- Frozen structural identity per (consumer group, state_type, collection name).
-- This table exists solely to make a collection's identity immutable: once a
-- collection has been used, its (kind, codec, key_codec, resolver) can never
-- silently change. Validated by point-read; registered on first use with
-- `INSERT … IF NOT EXISTS` (the one authorized keyed-state LWT — once per
-- collection per group, never on the hot path). A registered descriptor that
-- disagrees with the stored row refuses to dispatch (Permanent), so an
-- incompatible deploy fails loudly instead of silently misreading cells.
-- `codec_id` (value codec) is always present (every cell is codec-produced);
-- `key_codec_id` is the order-preserving Map key codec, NULL for Value/Deque;
-- `resolver_id` is NULL for the passthrough resolver, a token otherwise. `kind`
-- is validated, not part of the key, so a name pins exactly one kind per
-- state_type.
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_IDENTITY}} (
    group_id     text,
    state_type   tinyint,
    name         text,

    kind         tinyint,
    resolver_id  text,
    codec_id     text,
    key_codec_id text,

    PRIMARY KEY ((group_id, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
