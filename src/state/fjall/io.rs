//! Blocking fjall reads and writes behind the cell cache.

use super::codec::{self, SECTION_PREFIX_LEN};
use super::{CacheRead, FjallCellCacheError};
use crate::state::cell::{CacheEntry, Committed, Projection, Read};
use crate::timers::duration::CompactDuration;
use ahash::RandomState;
use bytes::Bytes;
use fjall::{Database, Keyspace, OwnedWriteBatch, Slice};
use smallvec::SmallVec;
use std::collections::HashSet;
use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tracing::warn;

/// Rows examined per blocking hop of a chunked
/// [`delete_section`] walk: each hop collects
/// at most this many keys in one [`spawn_blocking`], deletes them in one
/// bounded write batch, then re-seeks from the last key it saw. A section
/// delete therefore holds O(hop) keys in RAM — never the whole section (the
/// bounded-RAM invariant) — while the synchronous fjall range guard still
/// never crosses an `.await`.
pub(super) const SCAN_HOP_ROWS: usize = 256;

/// Deletes the rows under `prefix` except `excluded`, in hops of at most
/// [`SCAN_HOP_ROWS`] keys.
///
/// Each hop runs in [`spawn_blocking`], deletes its keys in one bounded batch,
/// and resumes after the last examined key. The walk never holds the whole
/// section in RAM. Deleted keys disappear, so a retry can repeat the walk.
pub(super) async fn delete_section(
    database: Database,
    handle: Keyspace,
    prefix: [u8; SECTION_PREFIX_LEN],
    excluded: Arc<HashSet<SmallVec<[u8; 32]>, RandomState>>,
) -> Result<(), FjallCellCacheError> {
    let mut lo: Bound<Vec<u8>> = Bound::Included(prefix.to_vec());
    loop {
        let hop_handle = handle.clone();
        let hop_database = database.clone();
        let hop_excluded = excluded.clone();
        let hop_lo = lo;
        let resume = spawn_blocking(move || -> fjall::Result<Option<Vec<u8>>> {
            // A `Vec`: bounded by `SCAN_HOP_ROWS` and always spilling past
            // any small inline on this recovery/must-succeed-delete path.
            let mut doomed: Vec<Vec<u8>> = Vec::new();
            let mut resume: Option<Vec<u8>> = None;
            let mut examined = 0usize;
            for guard in hop_handle.range((hop_lo, Bound::Unbounded)) {
                let (key, _) = guard.into_inner()?;
                // The range's upper side is open; the prefix check is what
                // stops the walk at the section boundary.
                if !key.starts_with(&prefix) {
                    break;
                }
                examined += 1;
                if !hop_excluded.contains(key.as_ref()) {
                    doomed.push(key.to_vec());
                }
                if examined >= SCAN_HOP_ROWS {
                    resume = Some(key.to_vec());
                    break;
                }
            }
            let mut batch = OwnedWriteBatch::with_capacity(hop_database, doomed.len());
            for key in &doomed {
                batch.remove(&hop_handle, key.as_slice());
            }
            batch.commit()?;
            Ok(resume)
        })
        .await??;
        match resume {
            // The hop stopped on its budget; re-seek just past the last
            // examined key.
            Some(key) => lo = Bound::Excluded(key),
            None => return Ok(()),
        }
    }
}

/// Runs `fill` over a fresh [`OwnedWriteBatch`] against `handle` and
/// commits it, all in a single blocking hop — the shared ceremony behind
/// every all-or-nothing batch mutator except
/// [`FjallCellCache::commit_batch`](super::FjallCellCache::commit_batch), which
/// reads stage expiries inside its own closure, and the hopping
/// [`delete_section`].
pub(super) fn run_batch<F>(
    database: Database,
    handle: Keyspace,
    capacity: usize,
    fill: F,
) -> impl Future<Output = Result<(), FjallCellCacheError>> + Send + use<F>
where
    F: FnOnce(&mut OwnedWriteBatch, &Keyspace) -> fjall::Result<()> + Send + 'static,
{
    let task = spawn_blocking(move || {
        let mut batch = OwnedWriteBatch::with_capacity(database, capacity);
        fill(&mut batch, &handle)?;
        batch.commit()
    });
    async move {
        task.await??;
        Ok(())
    }
}

/// Borrows the owned payload for frame encoding.
pub(super) fn encode_frame(entry: &CacheEntry<Bytes>, expiry: u64) -> Bytes {
    let borrowed = match entry {
        CacheEntry::Absent => CacheEntry::Absent,
        CacheEntry::Exists => CacheEntry::Exists,
        CacheEntry::Value(bytes) => CacheEntry::Value(bytes.as_ref()),
    };
    codec::encode_frame(borrowed, expiry)
}

/// Reads the raw cell at `key`, or `None` when the key is absent — one
/// blocking hop. Generic over the key so a variable-length `SmallVec` cell key
/// and a fixed-size `[u8; N]` index key both read without a bridging copy.
pub(super) async fn read_cell(
    cache: &Keyspace,
    key: impl AsRef<[u8]> + Send + 'static,
) -> Result<Option<Slice>, FjallCellCacheError> {
    let cache = cache.clone();
    Ok(spawn_blocking(move || cache.get(key)).await??)
}

/// Writes `cell` at `key`, overwriting any existing cell — one blocking hop.
/// Generic over the key so a variable-length `SmallVec` cell key and a
/// fixed-size `[u8; N]` index key both write without a bridging copy.
pub(super) async fn write_cell(
    cache: &Keyspace,
    key: impl AsRef<[u8]> + Send + 'static,
    cell: Bytes,
) -> Result<(), FjallCellCacheError> {
    let cache = cache.clone();
    spawn_blocking(move || cache.insert(key.as_ref(), cell.as_ref())).await??;
    Ok(())
}

/// Whether an absolute `expiry` (millis; `0` = never) has passed at `now`.
fn expired(expiry: u64, now: u64) -> bool {
    expiry != codec::NEVER_EXPIRES && now >= expiry
}

/// Classifies a projected frame at `now` and gives each hit its remaining TTL.
/// Point and batch reads share this classifier.
pub(super) fn classify<P: Projection>(
    expiry: u64,
    read: Read<P::Payload>,
    now: u64,
) -> CacheRead<P> {
    let remaining = || {
        (expiry != codec::NEVER_EXPIRES).then(|| {
            CompactDuration::new(
                u32::try_from(expiry.saturating_sub(now) / 1_000).unwrap_or(u32::MAX),
            )
        })
    };
    match read {
        _ if expired(expiry, now) => CacheRead::Expired,
        Read::Unknown => CacheRead::Miss,
        Read::Present(payload) => CacheRead::Hit((Committed::new(Some(payload)), remaining())),
        Read::Absent => CacheRead::Hit((Committed::new(None), remaining())),
    }
}

/// Reads the absolute stage expiry stamped on the cell at `key` back from the
/// cache keyspace, or `None` when no entry exists or the read/decode fails —
/// the transform then deletes the entry in the same atomic batch so the next
/// read falls through and self-heals. Runs inside
/// [`commit_batch`](super::FjallCellCache::commit_batch)'s blocking closure, so
/// it uses the synchronous keyspace `get` directly.
pub(super) fn stage_expiry(handle: &Keyspace, key: &[u8]) -> Option<u64> {
    let raw = match handle.get(key) {
        Ok(raw) => raw,
        Err(error) => {
            warn!(%error, "committed-value cache commit expiry read failed; degrading");
            return None;
        }
    };
    match codec::frame_expiry(raw.as_deref()) {
        Ok(expiry) => expiry,
        Err(error) => {
            warn!(%error, "committed-value cache commit expiry decode failed; degrading");
            None
        }
    }
}

/// Rewrites each staged key's committed `data` at its stored stage expiry in
/// one atomic batch. A key with no readable stage entry is removed instead,
/// so the next read falls through to durable storage.
pub(super) fn commit_stage(
    database: Database,
    handle: &Keyspace,
    inputs: &[(SmallVec<[u8; 32]>, Option<Bytes>)],
) -> fjall::Result<()> {
    let mut batch = OwnedWriteBatch::with_capacity(database, inputs.len());
    for (key, data) in inputs {
        match stage_expiry(handle, key) {
            Some(expiry) => {
                let entry = data
                    .as_deref()
                    .map_or(CacheEntry::Absent, CacheEntry::Value);
                let frame = codec::encode_frame(entry, expiry);
                batch.insert(handle, key.as_slice(), frame.as_ref());
            }
            None => batch.remove(handle, key.as_slice()),
        }
    }
    batch.commit()
}
