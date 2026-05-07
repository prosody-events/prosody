-- Migration: add_tag
-- Timestamp: 20260506
-- Purpose: Add tag int column to timer tables for commit oracle support.
--          The tag is a random 32-bit integer rotated on each complete()
--          from FiringRescheduled state, enabling three-way oracle decisions:
--          row absent → committed; tag matches → not committed;
--          tag mismatches → committed-and-rescheduled.
--
--          Pre-migration rows read NULL, surfaced as Some(0) by the
--          deserializer. The first complete()-from-FiringRescheduled rotates
--          legacy rows off 0 to a non-zero i32.

alter table {{KEYSPACE}}.{{TABLE_TYPED_KEYS}} add (tag int);
alter table {{KEYSPACE}}.{{TABLE_TYPED_SLABS}} add (tag int);
