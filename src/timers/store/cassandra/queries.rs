//! Prepared CQL statement management for Cassandra timer storage.
//!
//! This module provides the [`Queries`] struct which contains all prepared
//! Cassandra CQL statements needed for timer storage operations. Prepared
//! statements are created once during initialization and reused for all
//! database operations, providing better performance and security.
//!
//! The module handles:
//! - Cassandra session creation and configuration
//! - Schema migration execution via the embedded migrator
//! - Preparation of all CQL statements for CRUD operations
//! - Load balancing and retry policy configuration
//! - TTL and non-TTL variants of insert statements

#![allow(dead_code, reason = "fields used in tests")]

use crate::cassandra::{
    TABLE_KEYS, TABLE_SEGMENTS, TABLE_SLABS, TABLE_TYPED_KEYS, TABLE_TYPED_SLABS,
};
use crate::cassandra_queries;

cassandra_queries! {
    /// Container for all prepared Cassandra CQL statements used by the timer store.
    ///
    /// This struct holds the Cassandra session and all prepared statements needed
    /// for timer storage operations. Prepared statements provide better performance
    /// and security compared to ad-hoc query strings.
    ///
    /// The struct contains prepared statements for:
    /// - Segment management (create, read, delete)
    /// - Slab operations (insert, delete, range queries)
    /// - Trigger operations for both time-based and key-based indices
    /// - TTL and non-TTL variants for data lifecycle management
    pub struct Queries {
        /// Inserts a new segment with id, name, `slab_size`, and version
        insert_segment: (
            "INSERT INTO $keyspace.{} (id, name, slab_size, version) VALUES (?, ?, ?, ?)",
            TABLE_SEGMENTS
        ),

        /// Gets segment metadata by ID
        get_segment: (
            "SELECT name, slab_size, version FROM $keyspace.{} WHERE id = ? LIMIT 1",
            TABLE_SEGMENTS
        ),

        /// Deletes a segment by ID
        delete_segment: (
            "DELETE FROM $keyspace.{} WHERE id = ?",
            TABLE_SEGMENTS
        ),

        /// Gets all slab IDs for a segment
        get_slabs: (
            "SELECT slab_id FROM $keyspace.{} WHERE id = ?",
            TABLE_SEGMENTS
        ),

        /// Gets slab IDs within a range for a segment
        get_slab_range: (
            "SELECT slab_id FROM $keyspace.{} WHERE id = ? AND slab_id >= ? AND slab_id <= ?",
            TABLE_SEGMENTS
        ),

        /// Inserts a slab with TTL
        insert_slab: (
            "INSERT INTO $keyspace.{} (id, slab_id) VALUES (?, ?) USING TTL ?",
            TABLE_SEGMENTS
        ),

        /// Deletes a slab
        delete_slab: (
            "DELETE FROM $keyspace.{} WHERE id = ? AND slab_id = ?",
            TABLE_SEGMENTS
        ),

        /// Gets all triggers of a specific type in a slab
        get_slab_triggers: (
            "SELECT key, time, timer_type, span FROM $keyspace.{} WHERE segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ?",
            TABLE_TYPED_SLABS
        ),

        /// Inserts a trigger into a slab with TTL
        insert_slab_trigger: (
            "INSERT INTO $keyspace.{} (segment_id, slab_size, id, timer_type, key, time, span) VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            TABLE_TYPED_SLABS
        ),

        /// Deletes a specific trigger from a slab
        delete_slab_trigger: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND slab_size = ? AND id = ? AND timer_type = ? AND key = ? AND time = ?",
            TABLE_TYPED_SLABS
        ),

        /// Clears all triggers from a slab
        clear_slab_triggers: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND slab_size = ? AND id = ?",
            TABLE_TYPED_SLABS
        ),

        /// Gets all scheduled times for a key and timer type
        get_key_times: (
            "SELECT time FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?",
            TABLE_TYPED_KEYS
        ),

        /// Gets all triggers for a key and timer type
        get_key_triggers: (
            "SELECT key, time, timer_type, span FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?",
            TABLE_TYPED_KEYS
        ),

        /// Gets ALL triggers in a slab across all timer types
        get_slab_triggers_all_types: (
            "SELECT key, time, timer_type, span FROM $keyspace.{} WHERE segment_id = ? AND slab_size = ? AND id = ?",
            TABLE_TYPED_SLABS
        ),

        /// Gets ALL triggers for a key across all timer types
        get_key_triggers_all_types: (
            "SELECT key, time, timer_type, span FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Inserts a trigger into the key index with TTL
        insert_key_trigger: (
            "INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?",
            TABLE_TYPED_KEYS
        ),

        /// Deletes a specific trigger from the key index
        delete_key_trigger: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ? AND time = ?",
            TABLE_TYPED_KEYS
        ),

        /// Clears all triggers for a key and timer type
        clear_key_triggers: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?",
            TABLE_TYPED_KEYS
        ),

        /// Clears all triggers for a key across ALL timer types
        clear_key_triggers_all_types: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Inserts a slab without TTL
        insert_slab_no_ttl: (
            "INSERT INTO $keyspace.{} (id, slab_id) VALUES (?, ?)",
            TABLE_SEGMENTS
        ),

        /// Inserts a trigger into a slab without TTL
        insert_slab_trigger_no_ttl: (
            "INSERT INTO $keyspace.{} (segment_id, slab_size, id, timer_type, key, time, span) VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_TYPED_SLABS
        ),

        /// Inserts a trigger into the key index without TTL
        insert_key_trigger_no_ttl: (
            "INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?)",
            TABLE_TYPED_KEYS
        ),

        /// Updates segment version
        update_segment_version: (
            "UPDATE $keyspace.{} SET version = ?, slab_size = ? WHERE id = ?",
            TABLE_SEGMENTS
        ),

        /// Enumerates active v1 slabs for a segment
        get_slabs_v1: (
            "SELECT slab_id FROM $keyspace.{} WHERE id = ?",
            TABLE_SEGMENTS
        ),

        /// Retrieves v1 triggers from a slab
        get_slab_triggers_v1: (
            "SELECT key, time, span FROM $keyspace.{} WHERE segment_id = ? AND id = ?",
            TABLE_SLABS
        ),

        /// Deletes v1 slab metadata
        delete_slab_metadata_v1: (
            "DELETE FROM $keyspace.{} WHERE id = ? AND slab_id = ?",
            TABLE_SEGMENTS
        ),

        /// Deletes a single v1 trigger from a slab
        delete_slab_trigger_v1: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND id = ? AND key = ? AND time = ?",
            TABLE_SLABS
        ),

        /// Clears all v1 triggers for a slab
        clear_slab_triggers_v1: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND id = ?",
            TABLE_SLABS
        ),

        /// Deletes a single v1 trigger from the key index
        delete_key_trigger_v1: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND time = ?",
            TABLE_KEYS
        ),

        /// Clears v1 triggers for a key
        clear_key_triggers_v1: (
            "DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_KEYS
        ),

        /// Inserts a v1 trigger into the key index
        insert_key_trigger_v1: (
            "INSERT INTO $keyspace.{} (segment_id, key, time, span) VALUES (?, ?, ?, ?)",
            TABLE_KEYS
        ),

        /// Gets v1 triggers for a key
        get_key_triggers_v1: (
            "SELECT key, time, span FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_KEYS
        ),

        /// Inserts a v1 slab ID (for testing)
        insert_slab_v1: (
            "INSERT INTO $keyspace.{} (id, slab_id) VALUES (?, ?)",
            TABLE_SEGMENTS
        ),

        /// Inserts a v1 trigger (for testing)
        insert_slab_trigger_v1: (
            "INSERT INTO $keyspace.{} (segment_id, id, key, time, span) VALUES (?, ?, ?, ?, ?)",
            TABLE_SLABS
        ),

        /// Inserts a V1 segment without version (for testing)
        insert_segment_v1: (
            "INSERT INTO $keyspace.{} (id, name, slab_size) VALUES (?, ?, ?)",
            TABLE_SEGMENTS
        ),

        // =========================================================================
        // State Column Operations (Inline/Overflow Timer State)
        // =========================================================================

        /// Gets the `state` static map column for a key partition
        get_state: (
            "SELECT state FROM $keyspace.{} WHERE segment_id = ? AND key = ? LIMIT 1",
            TABLE_TYPED_KEYS
        ),

        /// Gets a single entry from the `state` static map column for a key partition
        get_state_entry: (
            "SELECT state[?] FROM $keyspace.{} WHERE segment_id = ? AND key = ? LIMIT 1",
            TABLE_TYPED_KEYS
        ),

        /// Inserts a trigger into clustering columns with TTL
        insert_key_trigger_clustering: (
            "INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?",
            TABLE_TYPED_KEYS
        ),

        /// Inserts a trigger into clustering columns without TTL
        insert_key_trigger_clustering_no_ttl: (
            "INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?)",
            TABLE_TYPED_KEYS
        ),

        /// Removes a state entry for a single timer type (returns to Absent for that type)
        remove_state_entry: (
            "DELETE state[?] FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Clears the entire `state` static column (all timer types at once)
        clear_state: (
            "DELETE state FROM $keyspace.{} WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Sets inline timer state (static column only) with TTL — no DELETE, no BATCH
        set_state_inline: (
            "UPDATE $keyspace.{} USING TTL ? SET state[?] = ? WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Sets inline timer state (static column only) without TTL — no DELETE, no BATCH
        set_state_inline_no_ttl: (
            "UPDATE $keyspace.{} SET state[?] = ? WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Sets overflow state marker (static column only) without TTL
        set_state_overflow: (
            "UPDATE $keyspace.{} SET state[?] = ? WHERE segment_id = ? AND key = ?",
            TABLE_TYPED_KEYS
        ),

        /// Reads up to 2 remaining trigger times for a key/type (demotion check)
        count_key_triggers: (
            "SELECT time FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ? LIMIT 2",
            TABLE_TYPED_KEYS
        ),

        /// Reads the first remaining trigger (time + span) for a key/type (inline demotion)
        peek_first_key_trigger: (
            "SELECT time, span FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ? LIMIT 1",
            TABLE_TYPED_KEYS
        ),

        /// BATCH: Clear clustering rows and set inline state with TTL
        batch_clear_and_set_inline: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?; \
             UPDATE $keyspace.{} USING TTL ? SET state[?] = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

        /// BATCH: Clear clustering rows and set inline state without TTL
        batch_clear_and_set_inline_no_ttl: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?; \
             UPDATE $keyspace.{} SET state[?] = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

        /// BATCH: Insert two clustering rows (promoted + new) and set overflow state with TTL.
        /// Per-statement USING TTL applies to each INSERT; the UPDATE has no TTL.
        batch_promote_and_set_overflow: (
            "BEGIN UNLOGGED BATCH \
             INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?; \
             INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?) USING TTL ?; \
             UPDATE $keyspace.{} SET state[?] = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

        /// BATCH: Insert two clustering rows (promoted + new) and set overflow state without TTL
        batch_promote_and_set_overflow_no_ttl: (
            "BEGIN UNLOGGED BATCH \
             INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?); \
             INSERT INTO $keyspace.{} (segment_id, key, timer_type, time, span) VALUES (?, ?, ?, ?, ?); \
             UPDATE $keyspace.{} SET state[?] = ? WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

        /// BATCH: Clear clustering rows for a single timer type + remove state entry.
        /// Same partition — no cross-partition overhead.
        batch_clear_key_triggers: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ? AND timer_type = ?; \
             DELETE state[?] FROM $keyspace.{} WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

        /// BATCH: Clear all clustering rows + clear entire state column.
        /// Same partition — no cross-partition overhead.
        batch_clear_key_triggers_all_types: (
            "BEGIN UNLOGGED BATCH \
             DELETE FROM $keyspace.{} WHERE segment_id = ? AND key = ?; \
             DELETE state FROM $keyspace.{} WHERE segment_id = ? AND key = ?; \
             APPLY BATCH",
            TABLE_TYPED_KEYS, TABLE_TYPED_KEYS
        ),

    }
}
