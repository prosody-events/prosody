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
--          {{TABLE_NODES_BY_GROUP}} indexes the live members of one consumer
--          group. It carries no column beyond its key: a listing resolves each
--          member through {{TABLE_NODE_DIRECTORY}} and treats an unresolvable
--          member as a stale index entry. kafka_cluster_id belongs in the key
--          because Kafka scopes a group id to its cluster. Two unrelated
--          clusters can each run a group named `orders`, and a key without the
--          cluster makes each read the other's members as its own peers. The
--          shard column, a value derived from node_id, spreads one group's
--          renewals, expiries and shutdown tombstones over a fixed number of
--          partitions.
--
--          No conditional write appears here, and no statement supplies its own
--          write timestamp: the session's monotonic generator stamps every
--          write. The deployment's general-purpose compaction applies.
--          Time-window compaction is wrong for these tables because a process
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

CREATE TABLE {{KEYSPACE}}.{{TABLE_NODES_BY_GROUP}} (
    kafka_cluster_id text,
    group_id         text,
    shard            int,
    node_id          uuid,

    PRIMARY KEY ((kafka_cluster_id, group_id, shard), node_id)
) WITH compaction  = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' }
  AND compression = { 'class': 'ZstdCompressor' };
