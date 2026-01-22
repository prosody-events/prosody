-- Migration: create_deferred_timers
-- Timestamp: 20251223
-- Description: Creates the deferred_timers table for timer defer middleware

-- Deferred timers per key within a segment
-- Uses same segment metadata table as deferred_offsets (deferred_segments)
-- L10: Leveled compaction optimal for high DELETE rate, long TTLs, FIFO reads
create table {{KEYSPACE}}.{{TABLE_DEFERRED_TIMERS}}
(
    segment_id    uuid,
    key           text,
    original_time int,
    span          frozen<map<text, text>>,
    retry_count   int static,
    primary key ((segment_id, key), original_time)
)
    with clustering order by (original_time asc)
     and compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' };
