-- Migration: create_deduplication
-- Timestamp: 20260319
-- Description: Creates the deduplication table for pipeline consumer message dedup

create table {{KEYSPACE}}.{{TABLE_DEDUPLICATION}}
(
    id uuid,
    primary key ((id))
)
    -- TWCS with a 1-day window and a default 7-day TTL: SSTables are bucketed
    -- by write time and dropped whole once every row inside has expired. The
    -- window is intentionally smaller than the TTL so that expiry is granular
    -- (one day's worth of data drops at a time rather than all 7 days at once).
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = {
         'class': 'TimeWindowCompactionStrategy',
         'compaction_window_unit': 'DAYS',
         'compaction_window_size': 1
     };
