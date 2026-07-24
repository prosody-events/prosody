-- Migration: create_keyed_state_publication
-- Timestamp: 20260722
-- Purpose: Provision the routing-only keyed-state PUBLICATION table. One row
--          advertises that a consumer group publishes a collection's state
--          under a topic: the partition key ((subsystem, state_type, name)) is
--          the logical collection a reader addresses — state_type namespaces
--          names exactly as keyed_state_identity does — and each
--          (group_id, topic) clustering row names one source carrying only
--          ROUTING FACTS (partition_count).
--          Rows are maintained by a plain idempotent upsert performed just
--          before the first durable state write per (collection, topic) — no
--          LWT, no TTL, no client-set timestamp. Identity is DELIBERATELY
--          ABSENT: a freely-upsertable row cannot hold frozen data, so a reader
--          validates a source's descriptor identity against its
--          keyed_state_identity row instead. Source resolution reads exactly one
--          partition's clustering rows.

CREATE TABLE {{KEYSPACE}}.{{TABLE_KEYED_STATE_PUBLICATION}} (
    subsystem       text,
    state_type      tinyint,
    name            text,
    group_id        text,
    topic           text,
    partition_count int,

    PRIMARY KEY ((subsystem, state_type, name), group_id, topic)
) WITH CLUSTERING ORDER BY (group_id ASC, topic ASC)
  AND compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
