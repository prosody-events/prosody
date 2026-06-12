-- Migration: create_keyed_state
-- Timestamp: 20260522
-- Purpose: Provision the durable storage for keyed-state Value collections
--          plus a shared pending index and the per-segment descriptor
--          identity table. The `event_ref` UDT carries the upstream event
--          identity (Kafka message or timer row) that owns a sealed WAL.

CREATE TYPE {{KEYSPACE}}.event_ref (
    kind         tinyint,
    msg_dedup_id uuid,
    timer_type   tinyint,
    time         int,
    tag          int
);

CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_VALUE}} (
    segment_id       uuid,
    key              text,
    state_type       tinyint,
    name             text,

    data             blob,
    payload_encoding smallint,
    identity_version int,

    wal_event        frozen<event_ref>,
    wal_ops          blob,
    wal_format       smallint,

    PRIMARY KEY ((segment_id, key, state_type, name))
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };

CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_PENDING}} (
    segment_id  uuid,
    key         text,
    state_type  tinyint,
    kind        tinyint,
    name        text,

    PRIMARY KEY ((segment_id, key), state_type, kind, name)
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };

-- Frozen structural identity per (segment, collection name). Written once
-- on first use by the partition's single owner (no LWT needed); a process
-- whose registered descriptor disagrees with the stored identity refuses
-- to dispatch (Permanent), so an incompatible deploy fails loudly instead
-- of silently misreading cells. `codec_id` is the codec's open-dispatch
-- string token (NULL for framework-defined cells). `version` is an
-- append-only clustering column: only version 1 is written today; future
-- identity migrations append new versions and stamp value rows
-- (`identity_version`) per key as they migrate.
CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_DESCRIPTOR}} (
    segment_id    uuid,
    name          text,
    version       int,

    kind          tinyint,
    cell_kind     smallint,
    codec_id      text,

    PRIMARY KEY ((segment_id), name, version)
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
