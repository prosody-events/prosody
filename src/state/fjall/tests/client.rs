//! Keyspace lifecycle, client directories, and cache sizing.

use super::*;
use crate::ByteSize;
use crate::state::config::KeyedStateConfiguration;
use quickcheck::{Arbitrary, Gen};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use tokio::runtime::Builder;
use tokio::sync::OnceCell;

/// Most operations in one lifecycle trace. Each assignment creates a keyspace,
/// which costs one directory `fsync`.
const MAX_TRACE_OPS: usize = 8;

/// One client shared by every lifecycle iteration. Each iteration ends with no
/// live slot and an empty database, so iterations do not interact.
static CLIENT: OnceCell<(FjallClient, TempDir)> = OnceCell::const_new();

/// One step of a lifecycle trace.
#[derive(Clone, Debug)]
enum Op {
    /// Assigns a partition: takes a new slot.
    Assign,
    /// Revokes the live slot at this index, modulo the live count.
    Revoke(usize),
    /// Waits for the lifecycle task, then checks the model.
    Settle,
}

/// A bounded sequence of lifecycle steps.
#[derive(Clone, Debug)]
struct Trace(Vec<Op>);

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 | 1 => Self::Assign,
            2 | 3 => Self::Revoke(usize::arbitrary(g)),
            _ => Self::Settle,
        }
    }
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % MAX_TRACE_OPS + 1;
        Self((0..len).map(|_| Op::arbitrary(g)).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(Self))
    }
}

/// Lifecycle model: after the lifecycle task settles, every live slot holds a
/// keyspace, the database holds exactly those keyspaces, and no keyspace name
/// serves two assignments. Revokes often land before the create runs, so the
/// trace covers creates for assignments that already ended.
#[test]
fn prop_keyspaces_track_live_slots() {
    async fn check(trace: Trace) -> Result<bool> {
        let (client, _) = CLIENT
            .get_or_try_init(|| async {
                let dir = tempfile::tempdir()?;
                let client = FjallClient::open(&config(dir.path(), None)?).await?;
                Ok::<_, Report>((client, dir))
            })
            .await?;
        let mut live: Vec<(usize, CacheSlot)> = Vec::new();
        let mut owners: HashMap<String, usize> = HashMap::new();

        for (step, op) in trace.0.iter().chain([&Op::Settle]).enumerate() {
            match op {
                Op::Assign => live.push((step, client.slot())),
                Op::Revoke(index) => {
                    if !live.is_empty() {
                        live.swap_remove(index % live.len());
                    }
                }
                Op::Settle => {
                    client.settled().await?;
                    let mut held = BTreeSet::new();
                    for (id, slot) in &live {
                        let Some(cache) = slot.attached() else {
                            return Ok(false);
                        };
                        let name = cache.keyspace().name().to_string();
                        if *owners.entry(name.clone()).or_insert(*id) != *id {
                            return Ok(false);
                        }
                        held.insert(name);
                    }
                    if held != keyspace_names(client.database()) {
                        return Ok(false);
                    }
                }
            }
        }

        live.clear();
        client.settled().await?;
        Ok(keyspace_names(client.database()).is_empty())
    }

    fn prop(trace: Trace) -> TestResult {
        match TEST_RUNTIME.block_on(check(trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }

    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}

/// Clients share a `cache_dir` without contention: each opens its database in
/// its own fresh subdirectory. fjall removes each subdirectory when the last
/// handle to that database drops. Dropping the runtime drops the lifecycle
/// tasks, and with them the last handles.
#[test]
fn clients_own_fresh_directories_that_fjall_removes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = config(dir.path(), None)?;
    let entries = || -> Result<usize> { Ok(fs::read_dir(dir.path())?.count()) };

    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let first = FjallClient::open(&config).await?;
        let second = FjallClient::open(&config).await?;
        let slot = first.slot();
        first.settled().await?;
        assert!(
            slot.attached().is_some(),
            "the settled slot holds a keyspace"
        );
        assert_eq!(entries()?, 2, "each client opens its own subdirectory");
        drop((slot, first, second));
        Ok::<_, Report>(())
    })?;
    drop(runtime);

    assert_eq!(entries()?, 0, "fjall must remove every client directory");
    Ok(())
}

/// `owned_cache_size` sets fjall's block-cache capacity. `None` leaves the
/// builder untouched, so it matches an untouched control database rather than
/// a hardcoded fjall default. 7 MiB differs from that default.
#[test]
fn cache_size_reaches_the_block_cache() -> Result<()> {
    let control = tempfile::tempdir()?;
    let default = Database::builder(control.path()).open()?.cache_capacity();
    let seven_mib = NonZeroU64::new(7 * 1024 * 1024).ok_or_else(|| eyre!("7 MiB is nonzero"))?;
    for (size, expected) in [
        (None, default),
        (Some(ByteSize::new(seven_mib)), seven_mib.get()),
    ] {
        let dir = tempfile::tempdir()?;
        let client = TEST_RUNTIME.block_on(FjallClient::open(&config(dir.path(), size)?))?;
        assert_eq!(
            client.database().cache_capacity(),
            expected,
            "cache size {size:?}"
        );
    }
    Ok(())
}

/// A configuration for `cache_dir` with block-cache capacity `cache_size`.
/// It sets every size explicitly, so environment variables cannot change it.
fn config(cache_dir: &Path, cache_size: Option<ByteSize>) -> Result<KeyedStateConfiguration> {
    Ok(KeyedStateConfiguration::builder()
        .cache_dir(cache_dir.to_path_buf())
        .owned_cache_size(cache_size)
        .memtable_size(None)
        .build()?)
}

/// The names of every keyspace in `database`.
fn keyspace_names(database: &Database) -> BTreeSet<String> {
    database
        .list_keyspace_names()
        .iter()
        .map(ToString::to_string)
        .collect()
}
