-- Migration: create_timers
-- Timestamp: 20250613

create table segments
(
    id        uuid,
    name      text static,
    slab_size int static,
    slab_id   int,
    primary key ((id), slab_id)
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

create table slabs
(
    segment_id uuid,
    id         int,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, id), key, time)
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy' };

create table keys
(
    segment_id uuid,
    key        text,
    time       int,
    span       frozen<map<text, text>>,
    primary key ((segment_id, key), time)
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = { 'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T2' };
