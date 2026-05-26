-- Migration: create_keyed_state
-- Timestamp: 20260522
-- Purpose: Provision the durable storage for keyed-state Value collections
--          plus a shared pending index. The `event_ref` UDT carries the
--          upstream event identity (Kafka message or timer row) that owns
--          a sealed WAL.

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
