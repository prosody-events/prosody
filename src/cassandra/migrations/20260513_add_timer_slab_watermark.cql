-- Migration: add_timer_slab_watermark
-- Timestamp: 20260513
-- Purpose: Add a static `slab_watermark` column to timer_segments so the slab
--          loader can start its scan from `slab_watermark + 1` rather than 0
--          on every fresh partition startup. This eliminates the tombstone
--          scan that grows linearly with the number of completed-and-deleted
--          slabs.
--
-- Invariant: when `slab_watermark` is non-NULL, it is strictly less than
-- every `slab_id` clustering row present for the same `id`. NULL means
-- the column was never written; readers treat it as "scan from slab 0".
--
-- Rollback compatibility:
--   Forward-only. Pre-PR code never reads `slab_watermark`, so stale
--   watermark values written by new code cannot mislead it. If new code
--   advances watermark high, then a rollback to old code creates past-time
--   slabs below the watermark, then re-deploys new code, those slabs are
--   silently skipped — acceptable, but worth noting.

ALTER TABLE {{KEYSPACE}}.{{TABLE_SEGMENTS}} ADD slab_watermark int static;
