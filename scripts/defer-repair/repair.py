#!/usr/bin/env python3
"""Find deferred keys that no retry timer will drain, and arm them.

A deferred key holds a queue in `deferred_offsets` (messages) or in
`deferred_timers` (application timers). A retry timer of type
`DeferredMessage` (1) or `DeferredTimer` (2) drains the queue.

The scheduler fires every trigger row in `timer_typed_slabs` whose slab is
registered in `timer_segments` and is above `slab_watermark`. The script
reads the retry timers that the key row shows, and calls a timer loadable
when its slab and slab row meet that rule. The check takes a fixed number
of reads per key.

Classes:
  HEALTHY     a loadable timer at a recent or future time
  STALE       a loadable timer, but every loadable time is older than --stale
  GHOST       the key row shows timers, but none is loadable
  NO_TIMER    the key row shows no timer
  NO_SEGMENT  the defer segment metadata or the timer segment is missing
  OLD_LAYOUT  the timer segment layout is older than V3

The summary column shows how the defer store reads the queue:
  readable    the store starts at the first row
  UNREADABLE  the queue has rows, but both summary columns (`next_offset` or
              `next_timer`, and `retry_count`) are NULL
  HIDDEN      the next-entry summary is after the first row

The store reads an UNREADABLE queue as empty. It starts a HIDDEN queue at the
summary and never reads the rows before it. In both cases a retry fire that
finds no later row deletes the whole queue. Versions before v0.2.1 can write
both states: a later deferral to an UNREADABLE queue makes it HIDDEN.

The report counts the entries that never run under `never_fire`: all entries
of GHOST, NO_TIMER and UNREADABLE keys, and the hidden entries of HIDDEN keys.

Commands:
  audit full | audit sample      read only; write a report
  arm                            show the plan; add --apply to write

The arm command first repairs each UNREADABLE and HIDDEN key with the
store's own legacy repair write. It then adds one retry timer to each GHOST, NO_TIMER
and STALE key. It never removes a timer. A second run finds each key readable and
HEALTHY, and skips it. A slab row that the key row does not show is not
found. Such a key gets one more timer, which is safe.
"""

import argparse
import collections
import csv
import glob
import json
import os
import random
import sys
import time
import uuid


def add_driver_to_path():
    driver_zip = max(glob.glob("/opt/cassandra/lib/cassandra-driver-internal-only-*.zip"))
    version = os.path.basename(driver_zip)[len("cassandra-driver-internal-only-"):-4]
    sys.path.insert(0, os.path.join(driver_zip, "cassandra-driver-" + version))
    for lib in ("futures", "geomet", "pure_sasl", "wcwidth"):
        for path in glob.glob("/opt/cassandra/lib/%s-*.zip" % lib):
            sys.path.insert(0, path)


add_driver_to_path()

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT  # noqa: E402
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy  # noqa: E402
from cassandra.query import BatchStatement, BatchType, SimpleStatement  # noqa: E402

KS = "prosody"
MIN_TOKEN = -(2 ** 63)
MAX_TOKEN = 2 ** 63 - 1
TOKEN_SPACE = 2 ** 64
# Cassandra's maximum TTL. The library binds 0 (no expiry) above it.
MAX_TTL = 630_720_000
# The oldest timer layout whose `state` column has one meaning.
MIN_SEGMENT_VERSION = 3
# The loader reads up to `now + P`, where P is at most max(60 s, slab_size).
# A new timer is at least this many slabs and this many seconds ahead.
MIN_LEAD_SLABS = 3
MIN_LEAD_SECONDS = 600
# Keys whose retry timer never fires.
STRANDED = ("GHOST", "NO_TIMER")
# Keys that get a new retry timer. A STALE key keeps a loadable timer, but a
# past timer that has not fired is in doubt. A second retry timer beyond the
# preload window is safe, so the script arms these keys too.
ARMABLE = STRANDED + ("STALE",)

# (defer table, clustering column, retry timer type, next-entry summary column)
TABLES = (
    ("deferred_offsets", "offset", 1, "next_offset"),
    ("deferred_timers", "original_time", 2, "next_timer"),
)


class DeferredNextTimer:
    """The `deferred_next_timer` UDT."""

    def __init__(self, time, span):
        self.time = time
        self.span = span


class KeyTimerState:
    """The `key_timer_state` UDT. `inline = False` marks Overflow."""

    def __init__(self, inline, time, span, tag):
        self.inline = inline
        self.time = time
        self.span = span
        self.tag = tag


class Throttle:
    """Keeps the statement rate at or below `qps`."""

    def __init__(self, qps):
        self.interval = 1.0 / qps
        self.next_at = time.monotonic()

    def wait(self):
        now = time.monotonic()
        if now < self.next_at:
            time.sleep(self.next_at - now)
        self.next_at = max(now, self.next_at) + self.interval


class Store:
    """Prepared statements. Each write copies a statement in
    `src/timers/store/cassandra/queries.rs`."""

    def __init__(self, session, throttle, retention):
        self.session = session
        self.throttle = throttle
        self.retention = retention
        self.statements = 0
        p = session.prepare
        self.q_timer_segment = p(
            "SELECT name, slab_size, version, slab_watermark FROM %s.timer_segments "
            "WHERE id = ? ORDER BY slab_id DESC LIMIT 1" % KS)
        self.q_registered = p(
            "SELECT slab_id FROM %s.timer_segments WHERE id = ? AND slab_id = ?" % KS)
        self.q_slab_row = p(
            "SELECT time FROM %s.timer_typed_slabs WHERE segment_id = ? AND slab_size = ? "
            "AND id = ? AND timer_type = ? AND key = ? AND time = ?" % KS)
        self.q_state = p(
            "SELECT state[?] FROM %s.timer_typed_keys WHERE segment_id = ? AND key = ? "
            "ORDER BY timer_type DESC, time DESC LIMIT 1" % KS)
        self.q_key_rows = p(
            "SELECT time, span, tag FROM %s.timer_typed_keys "
            "WHERE segment_id = ? AND key = ? AND timer_type = ?" % KS)
        # insert_slab
        self.w_slab = p(
            "INSERT INTO %s.timer_segments (id, slab_id) VALUES (?, ?) USING TTL ?" % KS)
        # insert_slab_trigger
        self.w_slab_trigger = p(
            "INSERT INTO %s.timer_typed_slabs (segment_id, slab_size, id, timer_type, key, "
            "time, span, tag) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?" % KS)
        # set_state_inline and set_state_overflow
        self.w_state = p(
            "UPDATE %s.timer_typed_keys USING TTL ? SET state[?] = ? "
            "WHERE segment_id = ? AND key = ?" % KS)
        # insert_key_trigger
        self.w_key_trigger = p(
            "INSERT INTO %s.timer_typed_keys (segment_id, key, timer_type, time, span, tag) "
            "VALUES (?, ?, ?, ?, ?, ?) USING TTL ?" % KS)
        # Defer store: get_next_static, probe_min and the legacy repair write
        # in `src/consumer/middleware/defer/{message,timer}/store/cassandra/queries.rs`.
        self.q_next_static = {
            "deferred_offsets": p(
                "SELECT next_offset, retry_count FROM %s.deferred_offsets "
                "WHERE segment_id = ? AND key = ? ORDER BY offset DESC LIMIT 1" % KS),
            "deferred_timers": p(
                "SELECT next_timer, retry_count FROM %s.deferred_timers "
                "WHERE segment_id = ? AND key = ? ORDER BY original_time DESC LIMIT 1" % KS),
        }
        self.q_probe_min = {
            "deferred_offsets": p(
                "SELECT offset FROM %s.deferred_offsets "
                "WHERE segment_id = ? AND key = ? LIMIT 1" % KS),
            "deferred_timers": p(
                "SELECT original_time, span FROM %s.deferred_timers "
                "WHERE segment_id = ? AND key = ? LIMIT 1" % KS),
        }
        self.w_repair_next = {
            "deferred_offsets": p(
                "UPDATE %s.deferred_offsets USING TTL ? SET next_offset = ? "
                "WHERE segment_id = ? AND key = ?" % KS),
            "deferred_timers": p(
                "UPDATE %s.deferred_timers USING TTL ? SET next_timer = ? "
                "WHERE segment_id = ? AND key = ?" % KS),
        }
        for q in (self.q_timer_segment, self.q_registered, self.q_slab_row,
                  self.q_state, self.q_key_rows,
                  self.w_slab, self.w_slab_trigger, self.w_state, self.w_key_trigger,
                  *self.q_next_static.values(), *self.q_probe_min.values(),
                  *self.w_repair_next.values()):
            q.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    def run(self, statement, params=None):
        self.throttle.wait()
        self.statements += 1
        return list(self.session.execute(statement, params))

    def ttl(self, anchor):
        """`calculate_ttl`: remaining time to `anchor` plus retention."""
        ttl = max(anchor - int(time.time()), 0) + self.retention
        return ttl if ttl <= MAX_TTL else 0

    def scan(self, table, clustering, next_column, start=None, limit=None):
        """Streams rows in token order. Throttles once per page."""
        where = " WHERE token(segment_id, key) > %s" % start if start is not None else ""
        cql = ("SELECT token(segment_id, key) AS tk, segment_id, key, %s, %s AS next, "
               "retry_count, writetime(retry_count) AS wt FROM %s.%s%s"
               % (clustering, next_column, KS, table, where))
        if limit is not None:
            cql += " LIMIT %d" % limit
        stmt = SimpleStatement(cql, fetch_size=500,
                               consistency_level=ConsistencyLevel.LOCAL_ONE)
        self.throttle.wait()
        result = self.session.execute(stmt)
        while True:
            self.statements += 1
            yield from result.current_rows
            if not result.has_more_pages:
                return
            self.throttle.wait()
            result.fetch_next_page()


class Segment:
    """The statics of one timer segment."""

    def __init__(self, timer_id, row):
        self.id = timer_id
        self.slab_size = row.slab_size
        self.version = row.version
        self.watermark = row.slab_watermark

    def above_watermark(self, slab_id):
        return self.watermark is None or slab_id > self.watermark

    def watermark_age_days(self, now):
        if self.watermark is None:
            return ""
        return round((now - (self.watermark + 1) * self.slab_size) / 86400, 1)


class Repair:
    def __init__(self, store, stale_secs):
        self.store = store
        self.stale_secs = stale_secs
        self.now = int(time.time())
        self.defer_segments = {}
        self.segments = {}

    def load_defer_segments(self):
        stmt = SimpleStatement(
            "SELECT id, consumer_group, topic, partition FROM %s.deferred_segments" % KS,
            fetch_size=500, consistency_level=ConsistencyLevel.LOCAL_ONE)
        for row in self.store.session.execute(stmt):
            self.defer_segments[row.id] = (row.consumer_group, row.topic, row.partition)

    def segment(self, defer_segment_id):
        """`Segment::for_partition`: UUIDv5(URL, "{group}:{topic}/{partition}")."""
        meta = self.defer_segments.get(defer_segment_id)
        if meta is None:
            return None, None
        timer_id = uuid.uuid5(uuid.NAMESPACE_URL, "%s:%s/%s" % meta)
        if timer_id not in self.segments:
            rows = self.store.run(self.store.q_timer_segment, (timer_id,))
            found = rows and rows[0].slab_size
            self.segments[timer_id] = Segment(timer_id, rows[0]) if found else None
        return meta, self.segments[timer_id]

    def visible_timers(self, segment, key, timer_type):
        """The key-row timers the library reads (`read::times`).

        A missing state entry is Absent and shows no timer, even when
        clustering rows exist. Inline shows its one time. Overflow shows the
        clustering rows.
        """
        state = self.state(segment, key, timer_type)
        if state is None:
            return state, []
        if state.inline:
            return state, [state.time]
        rows = self.store.run(self.store.q_key_rows, (segment.id, key, timer_type))
        return state, [row.time for row in rows]

    def state(self, segment, key, timer_type):
        rows = self.store.run(self.store.q_state, (timer_type, segment.id, key))
        return rows[0][0] if rows else None

    def fresh(self, segment):
        """Reads the segment statics again.

        A consumer can raise the watermark, lower it for a timer in a past
        slab, or migrate the slab size during a long run. Read this after the
        key row, so a change that came before the key read is seen.
        """
        rows = self.store.run(self.store.q_timer_segment, (segment.id,))
        if not rows or not rows[0].slab_size:
            return segment
        return Segment(segment.id, rows[0])

    def slab_status(self, segment, key, timer_type, t):
        """Returns "loadable", or each reason that no loader reads the timer.

        A retry timer starts above the watermark: its time is `now + backoff`.
        The watermark passes its slab only when no owner holds the timer in
        memory, and a new owner loads from `watermark + 1`.
        """
        slab_id = t // segment.slab_size
        reasons = []
        if not segment.above_watermark(slab_id):
            reasons.append("below_watermark")
        if not self.store.run(self.store.q_registered, (segment.id, slab_id)):
            reasons.append("unregistered")
        if not self.store.run(self.store.q_slab_row,
                              (segment.id, segment.slab_size, slab_id, timer_type, key, t)):
            reasons.append("no_slab_row")
        return "+".join(reasons) or "loadable"

    def classify(self, defer_segment_id, key, timer_type):
        """Returns (meta, segment, class, detail)."""
        meta, segment = self.segment(defer_segment_id)
        if segment is None:
            return meta, None, "NO_SEGMENT", ""
        if segment.version is None or segment.version < MIN_SEGMENT_VERSION:
            return meta, segment, "OLD_LAYOUT", ""
        klass, detail = self.timer_class(segment, key, timer_type, self.now)
        return meta, segment, klass, detail

    def timer_class(self, segment, key, timer_type, now):
        """Classifies the retry timers that the key row shows."""
        _, times = self.visible_timers(segment, key, timer_type)
        if not times:
            return "NO_TIMER", ""
        segment = self.fresh(segment)
        status = {t: self.slab_status(segment, key, timer_type, t) for t in times}
        detail = ";".join("%d:%s" % (t, status[t]) for t in sorted(times))
        loadable = [t for t in times if status[t] == "loadable"]
        if not loadable:
            return "GHOST", detail
        if max(loadable) >= now - self.stale_secs:
            return "HEALTHY", detail
        return "STALE", detail

    def still_stranded(self, segment, key, timer_type):
        """Re-reads the key just before a write. True when it still needs a timer.

        A live consumer can arm the key during a long scan.
        """
        klass, _ = self.timer_class(segment, key, timer_type, int(time.time()))
        return klass in ARMABLE

    def arm(self, segment, key, timer_type, lead_slabs, spread):
        """Adds one retry timer. Returns its time.

        Order: slab registration, slab row, key row. A partial write is safe.
        A registration alone holds no trigger. A slab row fires even without
        a key row, and its completion deletes both rows.

        The new time differs from every timer the key row shows, so no write
        replaces an existing timer.
        """
        store = self.store
        size = segment.slab_size
        now = int(time.time())
        lead = max(lead_slabs * size, MIN_LEAD_SECONDS)
        first = ((now + lead) // size + 1) * size
        _, existing = self.visible_timers(segment, key, timer_type)
        fire = random.randint(first, first + spread - 1)
        while fire in existing:
            fire = random.randint(first, first + spread - 1)
        slab_id = fire // size
        tag = random.choice((-1, 1)) * random.randint(1, 2 ** 31 - 1)

        store.run(store.w_slab, (segment.id, slab_id, store.ttl((slab_id + 1) * size)))
        store.run(store.w_slab_trigger, (segment.id, size, slab_id, timer_type, key, fire,
                                         {}, tag, store.ttl((slab_id + 1) * size)))
        self.upsert_key(segment, key, timer_type, fire, tag)
        return fire

    def upsert_key(self, segment, key, timer_type, fire, tag):
        """`write::upsert`, one branch per `KeyUpsertTransition`."""
        store = self.store
        state = self.state(segment, key, timer_type)
        new = KeyTimerState(True, fire, {}, tag)

        if state is None or (state.inline and state.time == fire):
            # WriteInline
            store.run(store.w_state, (store.ttl(fire), timer_type, new, segment.id, key))
        elif state.inline:
            # PromoteToOverflow: both rows and the marker share one TTL.
            ttl = store.ttl(max(state.time, fire))
            batch = BatchStatement(batch_type=BatchType.UNLOGGED,
                                   consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            batch.add(store.w_key_trigger, (segment.id, key, timer_type, state.time,
                                            state.span or {}, state.tag or 0, ttl))
            batch.add(store.w_key_trigger, (segment.id, key, timer_type, fire, {}, tag, ttl))
            batch.add(store.w_state, (ttl, timer_type, KeyTimerState(False, None, None, None),
                                      segment.id, key))
            store.run(batch)
        else:
            # UpsertClustering
            store.run(store.w_key_trigger,
                      (segment.id, key, timer_type, fire, {}, tag, store.ttl(fire)))

    def repair_summary(self, table, defer_segment_id, key):
        """Points the next-entry summary of a queue at its first row.

        Copies `repair_legacy_partition` in each defer store. The statics
        and the first row are read again first. Returns "repaired",
        "readable" when the store already starts at the first row, or
        "empty" when no queue row is left.
        """
        store = self.store
        first = store.run(store.q_probe_min[table], (defer_segment_id, key))
        if not first or first[0][0] is None:
            return "empty"
        rows = store.run(store.q_next_static[table], (defer_segment_id, key))
        next_value, retry_count = (rows[0][0], rows[0].retry_count) if rows else (None, None)
        if summary_state(table, next_value, retry_count, first[0][0]) == "readable":
            return "readable"

        if table == "deferred_offsets":
            # The message store binds the retention, not an anchored TTL.
            ttl, value = store.retention, first[0].offset
        else:
            # The timer store anchors the TTL on the referenced row.
            row = first[0]
            ttl, value = store.ttl(row.original_time), DeferredNextTimer(
                row.original_time, row.span or {})
        store.run(store.w_repair_next[table], (ttl, value, defer_segment_id, key))
        return "repaired"


def partitions(rows, clustering):
    """Groups token-ordered rows into one record per partition."""
    current = None
    for row in rows:
        ident = (row.segment_id, row.key)
        if current is None or current["id"] != ident:
            if current is not None:
                yield current
            current = {"id": ident, "token": row.tk, "entries": [], "next": row.next,
                       "retry_count": row.retry_count, "written": row.wt}
        value = getattr(row, clustering)
        if value is not None:
            current["entries"].append(value)
    if current is not None:
        yield current


def summary_state(table, next_value, retry_count, first):
    """Returns how the defer store reads a queue whose first row is `first`.

    `read_next_static` returns no entry when both summary columns are NULL,
    and does not probe the queue rows. When only `next_*` is NULL, the store
    probes the first row and repairs the summary itself. Otherwise the store
    starts at the summary, and a FIFO completion reads only later rows.
    """
    if next_value is None:
        return "UNREADABLE" if retry_count is None else "readable"
    start = next_value if table == "deferred_offsets" else next_value.time
    return "HIDDEN" if start > first else "readable"


def never_run(table, part, summary):
    """Counts the queue entries that the defer store never reads."""
    if summary == "UNREADABLE":
        return len(part["entries"])
    if summary == "HIDDEN":
        start = part["next"] if table == "deferred_offsets" else part["next"].time
        return sum(1 for entry in part["entries"] if entry < start)
    return 0


def batches(repair, args, table, clustering, next_column):
    """Yields (partitions, token span) for the chosen scan mode."""
    store = repair.store
    if args.scan == "full":
        rows = store.scan(table, clustering, next_column)
        yield partitions(rows, clustering), TOKEN_SPACE
        return
    for _ in range(args.samples):
        start = random.randint(MIN_TOKEN, MAX_TOKEN - 1)
        rows = list(store.scan(table, clustering, next_column, start, args.rows))
        parts = list(partitions(rows, clustering))
        if len(rows) == args.rows:
            parts = parts[:-1]  # the last partition can be cut short
        if parts:
            yield parts, parts[-1]["token"] - start


class Tally:
    """Counts keys and queued entries per label."""

    def __init__(self):
        self.keys = collections.Counter()
        self.entries = collections.Counter()

    def add(self, label, queued):
        self.keys[label] += 1
        self.entries[label] += queued

    def report(self, scale=None):
        report = {k: {"keys": self.keys[k], "queued_entries": self.entries[k]}
                  for k in self.keys}
        if scale is not None:
            for k, v in report.items():
                v["estimated_keys"] = round(v["keys"] * scale)
                v["estimated_queued_entries"] = round(v["queued_entries"] * scale)
        return report


def process_table(repair, args, table, clustering, timer_type, next_column, writer, actions):
    classes = Tally()
    summaries = Tally()
    never_fire = Tally()
    by_group = collections.defaultdict(Tally)
    residue = 0
    span_total = 0
    touched = collections.Counter()

    for parts, span in batches(repair, args, table, clustering, next_column):
        span_total += span
        for part in parts:
            if not part["entries"]:
                residue += 1
                continue
            segment_id, key = part["id"]
            meta, segment, klass, detail = repair.classify(segment_id, key, timer_type)
            group, topic, partition = meta or ("?", "?", "?")
            queued = len(part["entries"])
            summary = summary_state(table, part["next"], part["retry_count"],
                                    min(part["entries"]))
            classes.add(klass, queued)
            summaries.add(summary, queued)
            if klass in STRANDED:
                never_fire.add(group, queued)
            elif summary != "readable":
                never_fire.add(group, never_run(table, part, summary))
            by_group[group].add(klass, queued)
            if summary != "readable":
                by_group[group].add(summary, queued)
            writer.writerow([table, klass, summary, group, topic, partition,
                             segment.version if segment else "",
                             segment.watermark_age_days(repair.now) if segment else "",
                             key, queued, min(part["entries"]), part["retry_count"],
                             part["written"], detail])

            if actions is None:
                continue
            if args.group and group not in args.group:
                continue
            if args.key and key not in args.key:
                continue
            if args.exclude_key and key in args.exclude_key:
                continue
            if args.limit is not None and touched["keys"] >= args.limit:
                continue
            outcome = act(repair, args, table, segment_id, segment, key, timer_type, klass,
                          summary)
            if outcome is None:
                continue
            repaired, fire = outcome
            touched["keys"] += 1
            touched["repaired"] += repaired == "repaired"
            touched["armed"] += fire != ""
            actions.writerow([int(time.time()), "applied" if args.apply else "planned",
                              table, klass, summary, group, topic, partition, key,
                              timer_type, segment.slab_size if segment else "",
                              repaired, fire])

    scale = TOKEN_SPACE / span_total if span_total else 0
    return {
        "live_keys": sum(classes.keys.values()),
        "residue_partitions": residue,
        "token_fraction_covered": span_total / TOKEN_SPACE,
        "classes": classes.report(scale),
        "summary": summaries.report(scale),
        "never_fire": {
            "keys": sum(never_fire.keys.values()),
            "queued_entries": sum(never_fire.entries.values()),
            "by_group": never_fire.report(),
        },
        "by_group": {g: t.report() for g, t in sorted(by_group.items())},
        "touched": dict(touched),
    }


def act(repair, args, table, defer_segment_id, segment, key, timer_type, klass, summary):
    """Repairs and arms one key. Returns (repair outcome, fire time) or None.

    The summary repair runs first. A key whose repair does not succeed is
    never armed, because a retry fire would delete its queue.
    """
    needs_repair = summary != "readable"
    needs_timer = klass in ARMABLE
    if not needs_repair and not needs_timer:
        return None

    repaired = ""
    if needs_repair:
        repaired = repair.repair_summary(table, defer_segment_id, key) if args.apply \
            else "planned"
        if repaired == "empty":
            return repaired, ""

    if not needs_timer or not repair.still_stranded(segment, key, timer_type):
        return repaired, ""
    fire = "planned"
    if args.apply:
        fire = repair.arm(repair.fresh(segment), key, timer_type, args.lead_slabs,
                          args.spread)
    return repaired, fire


def connect():
    cluster = Cluster(
        [os.environ["CASS_HOST"]],
        auth_provider=PlainTextAuthProvider(os.environ["CASS_USER"], os.environ["CASS_PASS"]),
        execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=os.environ["CASS_DC"])),
            request_timeout=30)},
        protocol_version=5)
    cluster.register_user_type(KS, "key_timer_state", KeyTimerState)
    cluster.register_user_type(KS, "deferred_next_timer", DeferredNextTimer)
    return cluster, cluster.connect()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("audit", "arm"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--qps", type=float, default=50.0, help="maximum statement rate")
        cmd.add_argument("--stale", type=int, default=6 * 3600,
                         help="seconds after which a loadable past timer counts as STALE")
        cmd.add_argument("--retention", type=int, default=365 * 86400,
                         help="PROSODY_CASSANDRA_RETENTION in seconds")
    audit = sub.choices["audit"]
    audit.add_argument("scan", choices=("full", "sample"))
    audit.add_argument("--samples", type=int, default=200)
    audit.add_argument("--rows", type=int, default=200)
    arm = sub.choices["arm"]
    arm.add_argument("--apply", action="store_true", help="write; without it, only plan")
    arm.add_argument("--group", action="append", help="arm only this group (repeatable)")
    arm.add_argument("--key", action="append", help="arm only this key (repeatable)")
    arm.add_argument("--exclude-key", action="append", help="never arm this key (repeatable)")
    arm.add_argument("--limit", type=int, help="arm at most this many keys per table")
    arm.add_argument("--lead-slabs", type=int, default=5,
                     help="slabs between now and the first slab a new timer can use")
    arm.add_argument("--spread", type=int, default=86400,
                     help="seconds after the first usable slab over which new timers spread")
    args = parser.parse_args()
    if args.command == "arm":
        args.scan = "full"
        if args.lead_slabs < MIN_LEAD_SLABS:
            parser.error("--lead-slabs must be at least %d" % MIN_LEAD_SLABS)
        if args.spread < 1:
            parser.error("--spread must be at least 1")
    else:
        args.apply, args.group, args.limit = False, None, None
        args.key, args.exclude_key = None, None
    return args


class Lines:
    """Writes CSV rows to stdout, each prefixed with its kind.

    Kubernetes keeps a Job's stdout, so the log is the report. Split it with
    `grep '^report,'` and `grep '^action,'`.
    """

    def __init__(self, kind, header):
        self.kind = kind
        self.writer = csv.writer(sys.stdout)
        self.writerow(header)

    def writerow(self, row):
        self.writer.writerow([self.kind] + row)


def main():
    args = parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    cluster, session = connect()
    repair = Repair(Store(session, Throttle(args.qps), args.retention), args.stale)
    repair.load_defer_segments()

    report = Lines("report", ["table", "class", "summary", "group", "topic", "partition",
                              "segment_version", "watermark_age_days", "key", "queue_len",
                              "head", "retry_count", "retry_count_written", "timers"])
    actions = None
    if args.command == "arm":
        actions = Lines("action", ["at", "mode", "table", "class", "summary", "group", "topic",
                                   "partition", "key", "timer_type", "slab_size", "repair",
                                   "fire"])

    summary = {"command": args.command, "scan": args.scan, "apply": args.apply,
               "now": repair.now, "tables": {}}
    for table, clustering, timer_type, next_column in TABLES:
        summary["tables"][table] = process_table(
            repair, args, table, clustering, timer_type, next_column, report, actions)
    summary["statements"] = repair.store.statements
    print("summary " + json.dumps(summary, default=str))
    cluster.shutdown()


if __name__ == "__main__":
    main()
