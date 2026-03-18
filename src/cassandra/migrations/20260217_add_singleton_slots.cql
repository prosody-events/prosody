-- Migration: add_singleton_slots
-- Timestamp: 20260217
-- Purpose: Add static timer state support to reduce tombstones for single-timer keys

-- Create UDT for timer state data
-- Stores inline flag, time and span context for timer state entries
create type if not exists {{KEYSPACE}}.key_timer_state (
    inline boolean,
    time int,
    span frozen<map<text, text>>
);

-- Add static map column for timer state
-- Key = timer_type (tinyint), Value = frozen key_timer_state
-- Static column means one value per partition (segment_id, key)
alter table {{KEYSPACE}}.{{TABLE_TYPED_KEYS}} add (
    state map<tinyint, frozen<key_timer_state>> static
);
