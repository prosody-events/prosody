-- Migration: add_singleton_slots
-- Timestamp: 20260217
-- Purpose: Add static singleton slot support to reduce tombstones for single-timer keys

-- Create UDT for timer slot data
-- Stores time and span context for singleton timer entries
create type if not exists {{KEYSPACE}}.timer_slot (
    time int,
    span frozen<map<text, text>>
);

-- Add static map column for singleton timers
-- Key = timer_type (tinyint), Value = frozen timer_slot
-- Static column means one value per partition (segment_id, key)
alter table {{KEYSPACE}}.{{TABLE_TYPED_KEYS}} add (
    singleton_timers map<tinyint, frozen<timer_slot>> static
);
