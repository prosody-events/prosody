//! Shared async wrappers over fjall's synchronous per-cell I/O.
//!
//! Both the cache ([`FjallValueStore`](super::FjallValueStore)) and the
//! dirty overlay ([`FjallDirtyValueStore`](super::FjallDirtyValueStore))
//! store one tagged cell per key in a fjall [`PartitionHandle`]. fjall's API
//! is synchronous, so each call clones the cheap `Arc`-backed handle and
//! dispatches the blocking get/insert through
//! [`tokio::task::spawn_blocking`]. The key is generic over its byte width so
//! both the cache's collection prefix and the dirty overlay's wider
//! scope-qualified key share one path.

use super::error::FjallValueStoreError;
use bytes::Bytes;
use fjall::{PartitionHandle, Slice};
use tokio::task::spawn_blocking;

/// Reads the raw cell at `key`, or `None` when the key is absent.
pub(super) async fn read_cell<K>(
    partition: &PartitionHandle,
    key: K,
) -> Result<Option<Slice>, FjallValueStoreError>
where
    K: AsRef<[u8]> + Send + 'static,
{
    let partition = partition.clone();
    Ok(spawn_blocking(move || partition.get(key)).await??)
}

/// Writes `cell` at `key`, overwriting any existing cell.
pub(super) async fn write_cell<K>(
    partition: &PartitionHandle,
    key: K,
    cell: Bytes,
) -> Result<(), FjallValueStoreError>
where
    K: AsRef<[u8]> + Send + 'static,
{
    let partition = partition.clone();
    spawn_blocking(move || partition.insert(key.as_ref(), cell.as_ref())).await??;
    Ok(())
}

/// Removes the cell at `key`, so a later read finds the key absent.
///
/// Distinct from writing an `Absent` tag cell: a removed key decodes as
/// [`Read::Unknown`](crate::state::Read::Unknown) — "this layer holds no
/// answer" — while an `Absent` cell is an authoritative "the value is
/// cleared".
pub(super) async fn remove_cell<K>(
    partition: &PartitionHandle,
    key: K,
) -> Result<(), FjallValueStoreError>
where
    K: AsRef<[u8]> + Send + 'static,
{
    let partition = partition.clone();
    spawn_blocking(move || partition.remove(key.as_ref())).await??;
    Ok(())
}
