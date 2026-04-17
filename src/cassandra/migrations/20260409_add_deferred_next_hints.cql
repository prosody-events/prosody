-- Migration: add_deferred_next_hints
-- Timestamp: 20260409
-- Purpose: Add static columns to both deferred stores so get_next is a single
--          static-column read (zero tombstone scan) rather than an unbounded
--          LIMIT 1 clustering scan.
--
-- For offsets: a scalar next_offset is sufficient (no per-row auxiliary data).
-- For timers: a UDT bundles time + span atomically so time/span cannot drift.

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
