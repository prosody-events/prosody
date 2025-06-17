-- Migration: create_timers
-- Timestamp: 20250613

-- use this keyspace configuration only for development
create keyspace if not exists prosody with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

create table prosody.segments
(
    id        uuid,
    name      text static,
    slab_size int static,
    slab_id   int,
    primary key ((id), slab_id)
)
    with compression = { 'sstable_compression': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

create table prosody.slabs
(
    segment_id uuid,
    id         int,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, id), key, time)
)
    with compression = { 'sstable_compression': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

create table prosody.keys
(
    segment_id uuid,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, key), time)
)
    with compression = { 'sstable_compression': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
