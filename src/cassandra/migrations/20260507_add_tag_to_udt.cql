-- Migration: add_tag_to_udt
-- Timestamp: 20260506b
-- Purpose: Add tag int to key_timer_state UDT so Inline timers carry their
--          commit-oracle tag alongside the time and span. Without this,
--          current_tag() returns None for Inline (singleton) timers because
--          only clustering rows carry the tag column — the UDT did not.
--
--          Pre-migration rows have tag=NULL, normalised to 0 at the
--          deserialiser boundary (consistent with the tag=NULL→0 convention
--          for the timer_typed_keys clustering-row tag column).

alter type {{KEYSPACE}}.key_timer_state add tag int;
