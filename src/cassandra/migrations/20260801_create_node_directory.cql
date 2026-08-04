-- Migration: create_node_directory
-- Timestamp: 20260801
-- Purpose: Provision the node directory that every prosody process registers
--          itself in. {{TABLE_NODE_DIRECTORY}} holds one row per live process,
--          keyed by a node id that the process mints at startup and never
--          reuses. Each write is therefore an unconditional last-write-wins
--          upsert: there is no lease to acquire and nothing to fence. A row
--          carries a TTL and the process rewrites it well inside that TTL, so a
--          dead process expires with no application logic. A clean shutdown
--          deletes the row.
--
--          kafka_cluster_id sits beside group_id because Kafka scopes a group
--          id to its cluster: two unrelated clusters can each run a group named
--          `orders`, and the group alone names no set of processes.
--
--          No conditional write appears here, and no statement supplies its own
--          write timestamp: the session's monotonic generator stamps every
--          write. The deployment's general-purpose compaction applies.
--          Time-window compaction is wrong for this table because a process
--          overwrites its row in place across windows, and gc_grace_seconds
--          stays at the default because it governs resurrection risk rather
--          than tombstone noise.

CREATE TABLE {{KEYSPACE}}.{{TABLE_NODE_DIRECTORY}} (
    node_id          uuid PRIMARY KEY,
    direct_host      text,
    direct_port      int,
    advertised_host  text,
    advertised_port  int,
    network          text,
    kafka_cluster_id text,
    group_id         text,
    hostname         text
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
