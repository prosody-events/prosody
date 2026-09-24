//! Workspace ownership, startup sweeps, and cache sizing.

use super::*;

/// `for_workspace` must *retain* the workspace it is handed, not extract the
/// cache handle and drop the workspace.
///
/// This is the one ownership decision the type system does not enforce: both
/// `new` (bare handle, no workspace) and `for_workspace` return `Self`, so a
/// `for_workspace` rewritten to `Self::new(ws.cache_handle().clone())` compiles
/// — and silently deletes the cache partition the moment the dropped
/// workspace's `Drop` runs. The cache is a hint over the durable lower store,
/// so that degrades every op to a backing read with no other test failing. We
/// move the workspace in with no other binding to it and confirm — through the
/// keyspace, the only channel a `Drop` side-effect is observable on — that the
/// partition is still live after construction. A discarding `for_workspace`
/// would show zero.
#[test]
fn for_workspace_retains_the_workspace() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let client = FjallClient::open(dir.path(), None)?;
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

    let client = FjallClient::open(dir.path(), None)?;
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
    let client = FjallClient::open(dir.path(), None)?;
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
    let _first = FjallClient::open(dir.path(), None)?;
    let second = FjallClient::open(dir.path(), None);
    assert!(
        matches!(second, Err(FjallClientError::CacheDirInUse { .. })),
        "a second client on a live cache_dir must fail with CacheDirInUse, got {second:?}"
    );
    Ok(())
}

/// An explicit `cache_size_bytes` reaches fjall's block cache: the opened
/// database's `cache_capacity()` equals the bytes requested.
#[test]
fn explicit_cache_size_forwards() -> Result<()> {
    const SEVEN_MIB: u64 = 7 * 1024 * 1024; // deliberately != fjall's 32 MiB default
    let dir = tempfile::tempdir()?;
    let cap = NonZeroU64::new(SEVEN_MIB).ok_or_else(|| eyre!("SEVEN_MIB is nonzero"))?;
    let client = FjallClient::open(dir.path(), Some(cap))?;
    assert_eq!(
        client.database().cache_capacity(),
        SEVEN_MIB,
        "explicit cache_size_bytes must reach fjall's block cache verbatim"
    );
    Ok(())
}

/// `None` must omit `Builder::cache_size` entirely — proven by matching the
/// capacity of an untouched builder control, not a hardcoded 32 MiB (which
/// would freeze an upstream number).
#[test]
fn none_cache_size_matches_untouched_builder() -> Result<()> {
    let opened = tempfile::tempdir()?;
    let control = tempfile::tempdir()?;
    let default_capacity = FjallClient::open(opened.path(), None)?
        .database()
        .cache_capacity();
    let control_capacity = Database::builder(control.path()).open()?.cache_capacity();
    assert_eq!(
        default_capacity, control_capacity,
        "None must leave the builder untouched, matching fjall's own default"
    );
    Ok(())
}
