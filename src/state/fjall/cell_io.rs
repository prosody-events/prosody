//! Shared async wrappers over fjall's synchronous per-cell I/O.
//!
//! The cache ([`FjallCellCache`](super::FjallCellCache)) stores one tagged
//! cell per key in a fjall [`Keyspace`]. fjall's API is synchronous, so
//! each call clones the cheap `Arc`-backed handle and dispatches the blocking
//! get/insert through [`tokio::task::spawn_blocking`]. The key is generic over
//! its byte width.

use super::error::FjallCellCacheError;
use bytes::Bytes;
use fjall::{Keyspace, Slice};
use tokio::task::spawn_blocking;

/// Reads the raw cell at `key`, or `None` when the key is absent.
pub(super) async fn read_cell<K>(
    cache: &Keyspace,
    key: K,
) -> Result<Option<Slice>, FjallCellCacheError>
where
    K: AsRef<[u8]> + Send + 'static,
{
    let cache = cache.clone();
    Ok(spawn_blocking(move || cache.get(key)).await??)
}

/// Writes `cell` at `key`, overwriting any existing cell.
pub(super) async fn write_cell<K>(
    cache: &Keyspace,
    key: K,
    cell: Bytes,
) -> Result<(), FjallCellCacheError>
where
    K: AsRef<[u8]> + Send + 'static,
{
    let cache = cache.clone();
    spawn_blocking(move || cache.insert(key.as_ref(), cell.as_ref())).await??;
    Ok(())
}
