-- Migration: add_deferred_next_hints
-- Timestamp: 20260409
-- Purpose: Add static columns to both deferred stores so get_next is a single
--          static-column read (zero tombstone scan) rather than an unbounded
--          LIMIT 1 clustering scan.
--
-- For offsets: a scalar next_offset is sufficient (no per-row auxiliary data).
-- For timers: a UDT bundles time + span atomically so time/span cannot drift.
--
-- Rollback compatibility:
--   This migration is forward-only; the added static column and UDT linger on
--   rollback. Reverting to pre-PR code is safe **only** because:
--     (1) the pre-PR `get_next` reader uses
--         `maybe_first_row::<(Option<i64>, Option<i32>)>()` which explicitly
--         handles `offset = NULL` (static column present, no clustering rows);
--     (2) the pre-PR code never reads `next_offset` or `next_timer`, so stale
--         static hints written by the new code cannot mislead it.
--   Future changes to either reader must preserve these invariants or ship
--   their own rollback plan.

CREATE TYPE IF NOT EXISTS {{KEYSPACE}}.deferred_next_timer (
    time int,
    span frozen<map<text, text>>
);

ALTER TABLE {{KEYSPACE}}.{{TABLE_DEFERRED_OFFSETS}} ADD (
    next_offset bigint static
);

ALTER TABLE {{KEYSPACE}}.{{TABLE_DEFERRED_TIMERS}} ADD (
    next_timer frozen<deferred_next_timer> static
);
