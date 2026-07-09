-- Migration: create_keyed_state
-- Timestamp: 20260522
-- Purpose: Provision the durable storage for the uniform keyed-state CELL STORE
--          plus the group-global descriptor identity table. One Cassandra
--          partition (segment_id, key, state_type, name) is ONE collection; each
--          `kind = 0` clustering row (section, coordinate) is ONE cell, and a
--          co-clustered `kind = 1` bare row indexes each provisional
--          coordinate for bounded recovery. A cell is one
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
-- columns address a cell within it: `kind` splits the partition into two
-- disjoint front-to-back ranges — `kind = 0` (Cell) rows carry the full cell
-- columns, `kind = 1` (Index) rows are bare provisional-coordinate markers.
-- `section` groups a collection's cells into disjoint sub-structures
-- (bookkeeping vs data), `coordinate` orders cells within a section by unsigned
-- lexicographic byte order (= the order-preserving codec's contract). All three
-- are opaque to the cell store — the collection layer owns section/coordinate
-- meaning; `kind` is bound as a constant clustering predicate, never a value.
-- `kind` leads so the Index rows form one contiguous front-of-partition range
-- disjoint from the Cell rows: recovery reads only that range (cost ∝
-- #provisional, never #cells), then point-reads each coordinate's Cell row. A
-- single-section forward scan is a contiguous clustering range:
--   WHERE <pk>=… AND kind = 0 AND section = ? AND coordinate >= ? (no ALLOW
-- FILTERING).
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_CELL}} (
    segment_id uuid,
    key        text,
    state_type tinyint,
    name       text,

    kind       tinyint,
    section    tinyint,
    coordinate blob,

    data       blob,
    prev_data  blob,
    encoding   smallint,
    version    int,

    event      frozen<event_ref>,

    PRIMARY KEY ((segment_id, key, state_type, name), kind, section, coordinate)
) WITH CLUSTERING ORDER BY (kind ASC, section ASC, coordinate ASC)
  AND compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };

-- Frozen structural identity per (consumer group, state_type, collection name).
-- This table exists solely to make a collection's identity immutable: once a
-- collection has been used, its (kind, key format, payload format) can never
-- silently change. Validated by point-read; registered on first use with
-- `INSERT … IF NOT EXISTS` (the one authorized keyed-state LWT — once per
-- collection per group, never on the hot path). A registered descriptor that
-- disagrees with the stored row refuses to dispatch (Permanent), so an
-- incompatible deploy fails loudly instead of silently misreading cells.
-- `format_id` (the payload byte format) and `key_format_id` (the cell address
-- byte format — the unit codec's token for single-cell kinds) are always
-- present and together fully describe the collection's bytes; resolvers are
-- application behavior, deliberately not recorded. `kind` is validated, not
-- part of the key, so a name pins exactly one kind per state_type.
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_IDENTITY}} (
    group_id      text,
    state_type    tinyint,
    name          text,

    kind          tinyint,
    format_id     text,
    key_format_id text,

    PRIMARY KEY ((group_id, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
