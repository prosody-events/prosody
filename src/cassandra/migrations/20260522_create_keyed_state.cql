-- Migration: create_keyed_state
-- Timestamp: 20260522
-- Purpose: Provision the durable storage for keyed-state Value collections
--          plus the group-global descriptor identity table. Each value row is
--          one provisional cell: `data` is the committed value once `event`
--          is null, or the in-flight event's outcome while `event` is set,
--          with `prev_data` carrying the committed value the event superseded.
--          The `event_ref` UDT carries the upstream event identity (Kafka
--          message or timer row) that owns a provisional cell.

CREATE TYPE {{KEYSPACE}}.event_ref (
    kind         tinyint,
    msg_dedup_id uuid,
    timer_type   tinyint,
    time         int,
    tag          int
);

CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_VALUE}} (
    segment_id uuid,
    key        text,
    state_type tinyint,
    name       text,

    data       blob,
    prev_data  blob,
    encoding   smallint,
    version    int,

    event      frozen<event_ref>,

    PRIMARY KEY ((segment_id, key, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };

-- Frozen structural identity per (consumer group, state_type, collection name).
-- This table exists solely to make a collection's identity immutable: once a
-- collection has been used, its (kind, codec, resolver) can never silently
-- change. Validated by point-read; registered on first use with
-- `INSERT … IF NOT EXISTS` (the one authorized keyed-state LWT — once per
-- collection per group, never on the hot path). A registered descriptor that
-- disagrees with the stored row refuses to dispatch (Permanent), so an
-- incompatible deploy fails loudly instead of silently misreading cells.
-- `codec_id` is always present (every cell is codec-produced); `resolver_id`
-- is NULL for the passthrough resolver, a token otherwise. `kind` is validated,
-- not part of the key, so a name pins exactly one kind per state_type.
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_IDENTITY}} (
    group_id    text,
    state_type  tinyint,
    name        text,

    kind        tinyint,
    resolver_id text,
    codec_id    text,

    PRIMARY KEY ((group_id, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
