-- Migration: add_tag
-- Timestamp: 20260506
-- Purpose: Add the commit-oracle `tag` column to the timer tables and to the
--          `key_timer_state` UDT.
--
--          The tag is a random 32-bit integer rotated on each complete()
--          from FiringRescheduled state, enabling three-way oracle decisions:
--          row absent → committed; tag matches → not committed;
--          tag mismatches → committed-and-rescheduled.
--
--          The clustering-row column carries the tag for Overflow timers; the
--          UDT field carries it for Inline (singleton) timers. Pre-migration
--          values read NULL and are normalised to 0 at the deserialiser
--          boundary. The first complete()-from-FiringRescheduled rotates any
--          legacy 0 to a non-zero i32.

alter table {{KEYSPACE}}.{{TABLE_TYPED_KEYS}} add (tag int);
alter table {{KEYSPACE}}.{{TABLE_TYPED_SLABS}} add (tag int);
alter type {{KEYSPACE}}.key_timer_state add tag int;
