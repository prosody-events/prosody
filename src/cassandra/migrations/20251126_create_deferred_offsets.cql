-- Migration: create_deferred_offsets
-- Timestamp: 20251126
-- Replaces: 20251117_create_deferred_messages.cql

-- Segment metadata for observability and reconstruction
create table {{KEYSPACE}}.{{TABLE_DEFERRED_SEGMENTS}}
(
    id             uuid,
    topic          text,
    partition      int,
    consumer_group text,
    primary key ((id))
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

-- Deferred offsets per key within a segment
create table {{KEYSPACE}}.{{TABLE_DEFERRED_OFFSETS}}
(
    segment_id  uuid,
    key         text,
    offset      bigint,
    retry_count int static,
    primary key ((segment_id, key), offset)
)
    with clustering order by (offset asc)
     and compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
