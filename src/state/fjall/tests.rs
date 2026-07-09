//! Boundary tests for the fjall cell cache decode path.
//!
//! The flagship is the **read-path uniqueness invariant**: a present cell read
//! back from the fjall decode path is uniquely owned
//! (`try_into_mut().is_ok()`). This pins the production fast path
//! `CellView::get` relies on — the fjall cache decode mints a fresh `Bytes`,
//! so the read parses in place with zero copy — and guards against a future
//! layer re-introducing a shared clone that would silently demote the read to
//! the copying fallback.

use super::codec::cell_key;
use super::test_db;
use super::{CacheRead, Clock, FjallCellCache, FjallClient, FjallClientError, ScanHit};
use crate::Topic;
use crate::state::cell::Committed;
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, Section};
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::fresh_collection;
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use fjall::{Database, KeyspaceCreateOptions};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Read-path uniqueness invariant over the fjall cache: a present cell read
/// back from the decode path is uniquely owned, across random non-empty
/// payloads.
#[test]
fn prop_fjall_present_cell_is_uniquely_owned() {
    async fn check(payload: Vec<u8>) -> Result<bool> {
        let store = test_db::cache("value_cache")?;
        let c = fresh_collection("uniq")?;
        let cell = value_cell();
        store
            .put(&c, &cell, &Committed::new(Some(Bytes::from(payload))), 0)
            .await?;
        let CacheRead::Hit(committed) = store.get(&c, &cell).await? else {
            return Err(eyre!("expected a cache hit"));
        };
        let Some(bytes) = committed.into_inner() else {
            return Err(eyre!("expected a present cell"));
        };
        Ok(bytes.try_into_mut().is_ok())
    }

    fn prop(payload: Vec<u8>) -> TestResult {
        if payload.is_empty() {
            return TestResult::discard();
        }
        match TEST_RUNTIME.block_on(check(payload)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("present cell was a shared clone, not uniquely owned"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>) -> TestResult);
}

/// End-to-end through the cache store: a present cell written via the committed
/// cache is stored `[0x01][expiry: u64 BE][raw payload]` byte-for-byte.
/// `partition.get` returns the logical value (fjall decompresses any on-disk
/// LZ4 transparently), so an equal-to-raw result proves the app layer dropped
/// its zstd frame — a zstd frame would differ from the raw tail for any
/// payload — and pins the expiry header position.
#[test]
fn stored_cells_are_raw_tagged_payload_with_expiry() -> Result<()> {
    const EXPIRY: u64 = 1_700_000_000_000;
    let payload = b"a raw, uncompressed keyed-state payload".as_slice();
    let mut expected = vec![0x01_u8];
    expected.extend_from_slice(&EXPIRY.to_be_bytes());
    expected.extend_from_slice(payload);

    let (database, cache_partition, index_partition) = test_db::keyspace_pair("value_cache")?;
    let c = fresh_collection("raw")?;
    let cell = value_cell();

    let cache = FjallCellCache::new(database, cache_partition.clone(), index_partition);
    TEST_RUNTIME.block_on(cache.put(
        &c,
        &cell,
        &Committed::new(Some(Bytes::copy_from_slice(payload))),
        EXPIRY,
    ))?;
    let cache_raw = cache_partition
        .get(cell_key(&c, &cell))?
        .ok_or_else(|| eyre!("cache cell missing"))?;
    assert_eq!(
        cache_raw.as_ref(),
        expected.as_slice(),
        "cache cell not raw"
    );

    Ok(())
}

/// An expired present entry reads back as a miss (`None`) under a clock
/// advanced past its stamped expiry; the same entry with a `0`-never expiry, or
/// read at a time before expiry, stays a hit. Drives the read-side TTL check
/// with a deterministic [`Clock::Fixed`], no sleep.
#[test]
fn expired_entry_reads_as_miss() -> Result<()> {
    use super::Clock;
    use color_eyre::eyre::Report;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let now = Arc::new(AtomicU64::new(1_000));
    let cache = test_db::cache_with_clock("ttl_value", Clock::Fixed(now.clone()))?;
    let c = fresh_collection("ttl")?;
    let cell = value_cell();
    let payload = Committed::new(Some(Bytes::from_static(b"v")));

    TEST_RUNTIME.block_on(async {
        // Stamp an entry that expires at 2_000ms.
        cache.put(&c, &cell, &payload, 2_000).await?;
        // Before expiry: a hit.
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Hit(_)),
            "live entry must hit"
        );
        // At/after expiry: reported Expired (an entry exists, floor-expired).
        now.store(2_000, Ordering::Relaxed);
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Expired),
            "expired entry must read as Expired"
        );
        // A `never` (0) expiry never expires, even far in the future.
        cache.put(&c, &cell, &payload, 0).await?;
        now.store(u64::MAX, Ordering::Relaxed);
        assert!(
            matches!(cache.get(&c, &cell).await?, CacheRead::Hit(_)),
            "a never-expiry entry must always hit"
        );
        Ok::<_, Report>(())
    })?;
    Ok(())
}

/// The fixed instant the scan-fixture clock reads; entries stamped `1` are
/// expired, entries stamped `0` never expire.
const FIXTURE_NOW: u64 = 10_000;

/// The kind of entry seeded at one coordinate of a [`ScanFixture`].
#[derive(Clone, Debug)]
enum EntryKind {
    /// A live present cell carrying a one-byte payload.
    Present(u8),
    /// A cleared (`Absent`-tagged) entry — the scan skips it.
    Absent,
    /// A present cell whose stamped expiry has passed — the scan yields it as
    /// [`ScanHit::Expired`].
    Expired(u8),
}

/// A comparable projection of one [`ScanHit`] (coordinate bytes + payload).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Hit {
    Present(Vec<u8>, Vec<u8>),
    Expired(Vec<u8>),
}

impl From<ScanHit> for Hit {
    fn from(hit: ScanHit) -> Self {
        match hit {
            ScanHit::Present(cell, bytes) => {
                Self::Present(cell.coordinate.as_bytes().to_vec(), bytes.to_vec())
            }
            ScanHit::Expired(cell) => Self::Expired(cell.coordinate.as_bytes().to_vec()),
        }
    }
}

/// A generated `scan_present` case: entries over a small coordinate alphabet
/// (so collisions, adjacency and boundary hits recur), an ordered bound pair,
/// a direction, and an optional limit.
#[derive(Clone, Debug)]
struct ScanFixture {
    entries: Vec<(Vec<u8>, EntryKind)>,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    backward: bool,
    limit: Option<usize>,
}

/// A coordinate over the small alphabet: 0–2 bytes, each in `0..4`.
fn fixture_coord(g: &mut Gen) -> Vec<u8> {
    let len = usize::arbitrary(g) % 3;
    (0..len).map(|_| u8::arbitrary(g) % 4).collect()
}

impl Arbitrary for ScanFixture {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 24;
        let entries = (0..len)
            .map(|_| {
                let kind = match u8::arbitrary(g) % 4 {
                    0 => EntryKind::Absent,
                    1 => EntryKind::Expired(u8::arbitrary(g)),
                    _ => EntryKind::Present(u8::arbitrary(g)),
                };
                (fixture_coord(g), kind)
            })
            .collect();
        // An ordered bound pair; equal endpoints stay doubly-Included so the
        // pair is a valid (possibly empty) range for every range consumer.
        let (mut a, mut b) = (fixture_coord(g), fixture_coord(g));
        if a > b {
            mem::swap(&mut a, &mut b);
        }
        let equal = a == b;
        let pick = |g: &mut Gen, c: Vec<u8>| match u8::arbitrary(g) % 3 {
            0 => Bound::Unbounded,
            1 => Bound::Included(c),
            _ if equal => Bound::Included(c),
            _ => Bound::Excluded(c),
        };
        Self {
            lo: pick(g, a),
            hi: pick(g, b),
            entries,
            backward: bool::arbitrary(g),
            limit: bool::arbitrary(g).then(|| usize::arbitrary(g) % 6),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let unlimited = self.limit.map(|_| Self {
            limit: None,
            ..self.clone()
        });
        let this = self.clone();
        let prefixes = (0..self.entries.len()).map(move |n| Self {
            entries: this.entries[..n].to_vec(),
            ..this.clone()
        });
        Box::new(unlimited.into_iter().chain(prefixes))
    }
}

/// What the scan must yield: the seeded entries within the bounds in the scan
/// direction — `Absent` skipped, `Expired` reported — truncated at the limit.
fn model_hits(fixture: &ScanFixture, seeded: &BTreeMap<Vec<u8>, EntryKind>) -> Vec<Hit> {
    let mut hits: Vec<Hit> = seeded
        .range::<Vec<u8>, _>((fixture.lo.as_ref(), fixture.hi.as_ref()))
        .filter_map(|(coord, kind)| match kind {
            EntryKind::Present(v) => Some(Hit::Present(coord.clone(), vec![*v])),
            EntryKind::Expired(_) => Some(Hit::Expired(coord.clone())),
            EntryKind::Absent => None,
        })
        .collect();
    if fixture.backward {
        hits.reverse();
    }
    if let Some(limit) = fixture.limit {
        hits.truncate(limit);
    }
    hits
}

/// Chunked `scan_present` answer-vs-oracle (the hop/re-seek arithmetic): over
/// random entries, bounds, direction and limit, the hopping drain — driven at
/// a tiny per-hop budget so a single scan crosses many re-seeks — must yield
/// exactly the model's ordered hits, and the production hop size must agree.
/// This is the regression for the chunking that replaced the unbounded
/// whole-interval collect: a re-seek that skipped or repeated a row, broke at
/// a section boundary, or mis-counted the limit falsifies the equality.
#[test]
fn prop_scan_present_hops_match_model() {
    /// A hop budget small enough that most fixtures need several re-seeks.
    const TEST_HOP_ROWS: usize = 3;

    async fn check(fixture: ScanFixture) -> Result<bool> {
        let now = Arc::new(AtomicU64::new(FIXTURE_NOW));
        let cache = test_db::cache_with_clock("scan_hop", Clock::Fixed(now))?;
        let c = fresh_collection("hop")?;
        let section = Section::new(0);

        // Later duplicates overwrite earlier ones, in fjall and model alike.
        let mut seeded: BTreeMap<Vec<u8>, EntryKind> = BTreeMap::new();
        for (coord, kind) in &fixture.entries {
            seeded.insert(coord.clone(), kind.clone());
            let cell = CellKey {
                section,
                coordinate: Coordinate::from_bytes(coord.clone()),
            };
            let (value, expiry) = match kind {
                EntryKind::Present(v) => (Committed::new(Some(Bytes::from(vec![*v]))), 0),
                EntryKind::Absent => (Committed::new(None), 0),
                EntryKind::Expired(v) => (Committed::new(Some(Bytes::from(vec![*v]))), 1),
            };
            cache.put(&c, &cell, &value, expiry).await?;
        }

        let want = model_hits(&fixture, &seeded);
        let lo = fixture.lo.clone().map(Coordinate::from_bytes);
        let hi = fixture.hi.clone().map(Coordinate::from_bytes);
        let dir = if fixture.backward {
            Direction::Backward
        } else {
            Direction::Forward
        };
        let (start, end) = if fixture.backward {
            (hi.as_ref(), lo.as_ref())
        } else {
            (lo.as_ref(), hi.as_ref())
        };
        let scan = Scan {
            section,
            start,
            dir,
            end,
            limit: fixture.limit,
        };
        let tiny = drain_hits(cache.scan_present_hopping(&c, scan, TEST_HOP_ROWS)).await?;
        let production = drain_hits(cache.scan_present(&c, scan)).await?;
        Ok(tiny == want && production == want)
    }

    fn prop(fixture: ScanFixture) -> TestResult {
        match TEST_RUNTIME.block_on(check(fixture)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(ScanFixture) -> TestResult);
}

/// Collects a `scan_present` stream into comparable hits.
async fn drain_hits<S>(stream: S) -> Result<Vec<Hit>>
where
    S: futures::Stream<Item = Result<ScanHit, super::FjallCellCacheError>>,
{
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(Hit::from(item?));
    }
    Ok(out)
}

/// Warm-index batch round-trip: `index_record_batch` of arbitrary (duplicate-
/// prone) coordinates followed by `index_clear_batch` of an arbitrary subset
/// must leave `index_snapshot` holding exactly the recorded-minus-cleared set —
/// the batch ops must agree with the model a sequence of single-key
/// `index_record`s would produce (one atomic hop instead of N).
#[test]
fn prop_index_batches_round_trip_the_snapshot() {
    fn cells_of(coords: &[u8]) -> Vec<CellKey> {
        coords
            .iter()
            .map(|&b| CellKey {
                section: Section::new(0),
                coordinate: Coordinate::from_bytes(vec![b]),
            })
            .collect()
    }

    async fn check(record: Vec<u8>, clear: Vec<u8>) -> Result<bool> {
        let cache = test_db::cache("index_batch")?;
        let c = fresh_collection("batch")?;
        let recorded = cells_of(&record);
        let cleared = cells_of(&clear);
        cache.index_record_batch(&c, recorded.iter()).await?;
        cache.index_clear_batch(&c, cleared.iter()).await?;

        let want: BTreeSet<u8> = record
            .iter()
            .filter(|b| !clear.contains(b))
            .copied()
            .collect();
        let mut got: Vec<u8> = cache
            .index_snapshot(&c)
            .await?
            .into_iter()
            .map(|cell| cell.coordinate.as_bytes()[0])
            .collect();
        got.sort_unstable();
        got.dedup();
        Ok(got.into_iter().eq(want))
    }

    fn prop(record: Vec<u8>, clear: Vec<u8>) -> TestResult {
        match TEST_RUNTIME.block_on(check(record, clear)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Vec<u8>, Vec<u8>) -> TestResult);
}

/// `for_workspace` must *retain* the workspace it is handed, not extract the
/// cache handle and drop the workspace.
///
/// This is the one ownership decision the type system does not enforce: both
/// `new` (bare handle, no workspace) and `for_workspace` return `Self`, so a
/// `for_workspace` rewritten to `Self::new(ws.cache_handle().clone())` compiles
/// — and silently deletes the cache partition the moment the dropped
/// workspace's `Drop` runs. The cache is a hint over the durable lower store,
/// so that degrades every op to a backing read with no other test failing. We
/// move the
/// workspace in with no other binding to it and confirm — through the keyspace,
/// the only channel a `Drop` side-effect is observable on — that the partition
/// is still live after construction. A discarding `for_workspace` would show
/// zero.
#[test]
fn for_workspace_retains_the_workspace() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = FjallClient::open(dir.path())?;
    let database = client.database().clone();
    let live_cache_partitions = || {
        database
            .list_keyspace_names()
            .iter()
            .filter(|name| name.starts_with("value_cache_"))
            .count()
    };

    let workspace = client.workspace(Topic::from("orders.v1"), 0)?;
    let _cache = FjallCellCache::for_workspace(workspace);
    assert_eq!(
        live_cache_partitions(),
        1,
        "for_workspace must keep the workspace alive, not drop it on return"
    );
    Ok(())
}

/// The startup sweep reaps every stale `value_*` keyspace — and only those.
/// Stale keyspaces are seeded through a raw [`Database`] (bypassing
/// [`FjallClient`], whose workspaces would delete them on drop), modeling a
/// crashed prior process.
#[test]
fn open_sweeps_stale_value_keyspaces() -> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let database = Database::builder(dir.path()).open()?;
        for name in ["value_cache_deadbeef", "value_index_deadbeef", "unrelated"] {
            database
                .keyspace(name, KeyspaceCreateOptions::default)?
                .insert(b"stale", b"row")?;
        }
    }

    let client = FjallClient::open(dir.path())?;
    let names = client.database().list_keyspace_names();
    assert!(
        !names.iter().any(|name| name.starts_with("value_")),
        "open must sweep every stale value_* keyspace, found {names:?}"
    );
    assert!(
        names.iter().any(|name| &**name == "unrelated"),
        "the sweep must reap only value_* keyspaces, found {names:?}"
    );
    Ok(())
}

/// Born-cold invariant of [`FjallClient::workspace`]: re-assigning the same
/// `(topic, partition)` mints fresh keyspace names — a name is never
/// re-derived, so a new workspace can never open a prior assignment's data.
#[test]
fn workspace_names_are_never_reused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = FjallClient::open(dir.path())?;
    let database = client.database().clone();
    let value_names = || -> BTreeSet<String> {
        database
            .list_keyspace_names()
            .iter()
            .filter(|name| name.starts_with("value_"))
            .map(|name| (**name).to_owned())
            .collect()
    };

    let first = client.workspace(Topic::from("orders.v1"), 0)?;
    let first_names = value_names();
    assert_eq!(
        first_names.len(),
        2,
        "a workspace owns a cache + index pair"
    );
    drop(first);

    let _second = client.workspace(Topic::from("orders.v1"), 0)?;
    let second_names = value_names();
    assert_eq!(
        second_names.len(),
        2,
        "the re-assigned workspace owns a fresh cache + index pair — without this the disjoint \
         check below passes vacuously if the new keyspaces never appear"
    );
    assert!(
        first_names.is_disjoint(&second_names),
        "re-assigning the same (topic, partition) must mint fresh names, got {first_names:?} then \
         {second_names:?}"
    );
    Ok(())
}

/// Two clients on one `cache_dir` fail fast with [`CacheDirInUse`]: fjall's
/// exclusive directory lock is what makes the startup sweep safe, so
/// contention must surface as a clear, permanent configuration error.
///
/// [`CacheDirInUse`]: FjallClientError::CacheDirInUse
#[test]
fn open_fails_clearly_when_cache_dir_is_in_use() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let _first = FjallClient::open(dir.path())?;
    let second = FjallClient::open(dir.path());
    assert!(
        matches!(second, Err(FjallClientError::CacheDirInUse { .. })),
        "a second client on a live cache_dir must fail with CacheDirInUse, got {second:?}"
    );
    Ok(())
}
