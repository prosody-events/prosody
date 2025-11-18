-- Migration: create_deferred_messages
-- Timestamp: 20251117

-- Deferred messages table for consumer middleware retry tracking
create table {{KEYSPACE}}.{{TABLE_DEFERRED_MESSAGES}}
(
    key_id      uuid,
    offset      bigint,
    retry_count int static,
    primary key (key_id, offset)
)
    with clustering order by (offset asc)
     and compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
