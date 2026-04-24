-- Migration: deduplication_unified_compaction
-- Timestamp: 20260424
-- Purpose: Switch deduplication table from TWCS to UnifiedCompactionStrategy
--          and drop gc_grace_seconds to 1 day.
--
--          TWCS cannot be repaired safely with incremental repair, and a
--          manual compaction under TWCS is disastrous (collapses all windows
--          into one SSTable, defeating whole-file TTL expiry).
--
--          gc_grace_seconds = 86400 is safe here because this table is
--          TTL-only — there are no DELETEs. The only tombstones come from
--          TTL expiry, so the usual concern (user-issued deletes resurrecting
--          on a lagging replica) does not apply. A short grace period lets
--          expired-row tombstones be purged sooner.

alter table {{KEYSPACE}}.{{TABLE_DEDUPLICATION}}
    with compaction = {
        'class': 'UnifiedCompactionStrategy',
        'scaling_parameters': 'T4',
        'expired_sstable_check_frequency_seconds': '300'
    }
     and gc_grace_seconds = 86400;
