# Defer repair

`repair.py` finds deferred keys that no retry timer will drain, and arms
them.

A deferred key holds a queue of messages in `deferred_offsets`, or a queue of
application timers in `deferred_timers`. A retry timer of type
`DeferredMessage` (1) or `DeferredTimer` (2) drains each queue.

## How the scheduler finds a timer

The scheduler loads every row in `timer_typed_slabs` whose slab is
registered in `timer_segments` and is above `slab_watermark`. It fires a
loaded row without a read of the key row.

The script reads the retry timers that the key row shows, the same way the
library reads them. A missing `state` entry shows no timer. An inline entry
shows one timer. An overflow entry shows the clustering rows. The script
calls a timer loadable when its slab and its slab row meet the rule above.
This takes a fixed number of reads per key. A slab row that the key row does
not show is not found, so such a key gets one more timer, which is safe.

In every version from v0.1.0, the loader reads slabs up to `now + P`. The
preload window P is at most `max(60 s, slab_size)`. A new timer starts at
the next slab after `now + max(5 slabs, 10 minutes)`, so the loader always
reads it later, when its slab enters the preload window.

Each segment has its own slab size. The script reads `slab_size` from the
segment row in `timer_segments` and uses that value for each key in the
segment. Never assume one slab size for all segments.

## Classes

| Class | Meaning |
|-------|---------|
| `HEALTHY` | A loadable timer at a recent or future time. |
| `STALE` | A loadable timer, but every loadable time is older than `--stale`. |
| `GHOST` | The key row shows timers, but none is loadable. |
| `NO_TIMER` | The key row shows no timer. |
| `NO_SEGMENT` | The defer segment metadata or the timer segment is missing. |
| `OLD_LAYOUT` | The timer segment layout is older than V3. |

`GHOST` and `NO_TIMER` keys have no retry timer that will ever fire.

The `summary` column is separate from the class. A queue is `UNREADABLE`
when it has rows but both summary columns are NULL: `next_offset` or
`next_timer`, and `retry_count`. The defer store reads only those columns,
so it reads such a queue as empty. The next retry fire then deletes the
queue and its entries never run. Versions before v0.2.1 can write this
state. The report counts these keys under `summary`.

The report counts the entries that will never run under `never_fire`: all
`GHOST`, `NO_TIMER` and `UNREADABLE` keys. `HEALTHY` and `STALE` keys keep a
loadable retry timer. A `STALE` key is not an error: its group is off, or
its handler keeps failing for that key.

`STALE` is not a stranded key. A loadable timer in the past means that the
segment has no running owner, or that the owner retries the fired timer in
memory. Read the `watermark_age_days` column. A large value means that no
owner advances the segment.

## The summary repair

The `arm` command repairs each `UNREADABLE` queue before it adds a timer.
It repairs these queues even when they already have a loadable timer. The write
copies `repair_legacy_partition` in each defer store:

- Messages: set `next_offset` to the first offset, with the retention as TTL.
- Timers: set `next_timer` to the first row's time and span, with
  `calculate_ttl(time)` as TTL.

The script reads the summary columns again just before the write. It skips
a queue that a consumer made readable during the scan. It never arms a key
whose repair did not succeed.

A running consumer keeps a cache of each key's next entry, and the cache
can hold "no queue". The script cannot clear that cache. If a consumer read
an `UNREADABLE` key since its partition assignment, a retry fire can still
delete the queue until the cache entry goes or the partition moves.

## The arm write

After the summary repair, the `arm` command adds one retry timer to each
`GHOST`, `NO_TIMER` and `STALE` key. It never removes a timer. A `STALE`
key keeps a loadable timer, but a past timer that has not fired is in doubt,
and a second retry timer is safe. An extra retry timer is safe: the
first fire on a non-empty queue calls `clear_and_schedule`, which leaves one
timer. A fire on an empty queue does nothing.

The write copies the library statements in
`src/timers/store/cassandra/queries.rs`, in this order:

1. `insert_slab`: the slab registration.
2. `insert_slab_trigger`: the slab row.
3. `write::upsert`: the key row. The branch follows the current `state`
   entry, the same as `KeyUpsertTransition`.

The slab row and the key row get the same random tag. The TTLs follow
`calculate_ttl`. A partial write is safe. A registration alone holds no
trigger, and a slab row fires without a key row.

The command is idempotent. A second run finds each armed key `HEALTHY` and
skips it. Just before each write, the script reads the key again and skips a
key that a running consumer armed during the scan.

## Run

`run.sh` starts the script as a Kubernetes Job in the dev or prod cluster.
It prints the Job name.

```sh
scripts/defer-repair/run.sh dev audit full
scripts/defer-repair/run.sh prod audit sample --samples 100 --rows 100
scripts/defer-repair/run.sh prod arm
scripts/defer-repair/run.sh prod arm --apply --group herald-worker --limit 10
```

The Job runs in a pod apart from the Cassandra pods, with a fixed memory
limit. Do not run the script inside a Cassandra pod: a script that uses too
much memory can cause the database node to fail.

The Job restarts the script after a failure. Each command is safe to run
again. The audit only reads, and `arm` skips each key that an earlier run
fixed.

The script writes everything to stdout, and the Job keeps its log for one
week. Each line starts with its kind:

- `report,`: one CSV row for each live deferred key, after a header row.
- `action,`: one CSV row for each planned or applied repair and timer.
- `summary `: the JSON summary, on the last line.

To save the results:

```sh
kubectl --context <ctx> -n <ns> logs job/<name> > run.log
grep '^report,' run.log | cut -d, -f2- > report.csv
grep '^summary ' run.log | cut -c9- > summary.json
```

Use `--qps` to set the maximum statement rate. Use `--group` and `--limit`
to arm a small set first. `--spread` sets the time, in seconds, over which
the new timers fire. It starts at the first usable slab and defaults to one
day, so a large replay does not all fire at once. Set `--retention` to the value of
`PROSODY_CASSANDRA_RETENTION` when a service changes it.
