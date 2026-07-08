//! Process-shared fjall database for the keyed-state test suites.
//!
//! Creating a fjall [`Database`] (a tempdir open) and each keyspace (a
//! directory `fsync`, ~128 ms on APFS) is the dominant cost in the fjall-backed
//! suites; fjall has no in-memory mode, so the only lever is to stop
//! *creating*. This module hands every test **one** process-wide database whose
//! keyspaces are opened once and reused across a property's iterations. Row
//! isolation comes from a fresh v4 segment per iteration (in the
//! [`CollectionId`](crate::state::CollectionId)), never the keyspace name, so a
//! reused keyspace grows but never aliases another test's rows.
//!
//! # Isolation contract
//!
//! * A **non-clearing** test ([`cache`], [`cache_with_clock`]) may share any
//!   keyspace name: distinct v4 segments keep its rows disjoint from every
//!   other test's, even in one keyspace under `cargo test`'s thread-per-test.
//! * A **clearing** test ([`cold_cache`]) MUST pass a keyspace name no other
//!   test uses — `clear` wipes the whole keyspace. Quickcheck runs a property's
//!   iterations sequentially, so a single clearing test never races itself.
//!
//! The `TempDir` is held for the process lifetime (a process-scoped fixture,
//! not a leak); nextest's short-lived test processes leave it to the OS
//! reclaimer.

use super::workspace::keyspace_options;
use super::{Clock, FjallCellCache};
use color_eyre::eyre::{Result, eyre};
use fjall::{Database, Keyspace};
use std::sync::LazyLock;
use tempfile::TempDir;

/// One fjall database shared by every test in the process, created once.
static SHARED_DB: LazyLock<Result<(Database, TempDir), String>> = LazyLock::new(|| {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let database = Database::builder(dir.path())
        .open()
        .map_err(|e| e.to_string())?;
    Ok((database, dir))
});

/// The process-shared [`Database`] (see the module docs).
fn shared_database() -> Result<Database> {
    match &*SHARED_DB {
        Ok((db, _)) => Ok(db.clone()),
        Err(e) => Err(eyre!("shared fjall database init failed: {e}")),
    }
}

/// The shared database plus the `name` cache keyspace and its sibling
/// `name_index` warm-index keyspace, opened (get-or-create) and reused. Prefer
/// [`cache`]/[`cold_cache`]; use this only when a test needs the raw
/// [`Keyspace`] handles (e.g. to read a stored row byte-for-byte).
pub fn keyspace_pair(name: &str) -> Result<(Database, Keyspace, Keyspace)> {
    let database = shared_database()?;
    let cache = database.keyspace(name, keyspace_options)?;
    let index = database.keyspace(&format!("{name}_index"), keyspace_options)?;
    Ok((database, cache, index))
}

/// A **warm-reuse** [`FjallCellCache`] over the `name` keyspace pair: created
/// once and reused across iterations (no clear); distinct v4 segments keep
/// iterations disjoint. The default for every non-crash test.
pub fn cache(name: &str) -> Result<FjallCellCache> {
    let (database, cache, index) = keyspace_pair(name)?;
    Ok(FjallCellCache::new(database, cache, index))
}

/// Like [`cache`] but driven by a test-controlled [`Clock`].
pub fn cache_with_clock(name: &str, clock: Clock) -> Result<FjallCellCache> {
    let (database, cache, index) = keyspace_pair(name)?;
    Ok(FjallCellCache::with_clock(database, cache, index, clock))
}

/// A **cold** [`FjallCellCache`]: get-or-create the `name` keyspace pair, then
/// [`clear`](Keyspace::clear) both — modeling a fresh assignment epoch (a cold
/// cache over the same warm durable backing) without a keyspace-creation
/// `fsync`. Cache lookups key on `(segment, key, cell)`, never the keyspace
/// name, so a cleared reused keyspace is byte-for-byte a brand-new one. The
/// caller MUST own `name` exclusively (see the module's isolation contract).
pub fn cold_cache(name: &str) -> Result<FjallCellCache> {
    let (database, cache, index) = keyspace_pair(name)?;
    cache.clear()?;
    index.clear()?;
    Ok(FjallCellCache::new(database, cache, index))
}
