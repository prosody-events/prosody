create table {{KEYSPACE}}.{{TABLE_DEDUPLICATION}}
(
    id uuid,
    primary key ((id))
)
    with compression = { 'class': 'ZstdCompressor' }
     and compaction = {
         'class': 'TimeWindowCompactionStrategy',
         'compaction_window_unit': 'DAYS',
         'compaction_window_size': 1
     };
