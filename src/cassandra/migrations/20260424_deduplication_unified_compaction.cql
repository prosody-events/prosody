-- Migration: deduplication_unified_compaction
-- Timestamp: 20260424
-- Purpose: Switch deduplication table from TWCS to UnifiedCompactionStrategy.
--          TWCS cannot be repaired safely with incremental repair, and a
--          manual compaction under TWCS is disastrous (collapses all windows
--          into one SSTable, defeating whole-file TTL expiry).

alter table {{KEYSPACE}}.{{TABLE_DEDUPLICATION}}
    with compaction = {
        'class': 'UnifiedCompactionStrategy',
        'scaling_parameters': 'T4',
        'expired_sstable_check_frequency_seconds': '300'
    };
