-- Migration: add_timer_types
-- Timestamp: 20251023

-- Add version column to segments table
alter table {{KEYSPACE}}.{{TABLE_SEGMENTS}} add version tinyint static;

-- V2 time-based index with timer type support
create table {{KEYSPACE}}.{{TABLE_TYPED_SLABS}}
(
    segment_id uuid,
    slab_size  int,
    id         int,
    timer_type tinyint,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, slab_size, id), timer_type, key, time)
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

-- V2 key-based index with timer type support
create table {{KEYSPACE}}.{{TABLE_TYPED_KEYS}}
(
    segment_id uuid,
    key        text,
    timer_type tinyint,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, key), timer_type, time)
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
