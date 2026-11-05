//! Admission proofs stored in the assignment's keyspace.

use super::{CacheSlot, FjallCellCacheError, codec, io};
use crate::Key;
use crate::state::backend::AdmissionChecks;
use bytes::Bytes;

/// Stores admission proofs as marker rows in the assignment's keyspace.
///
/// An empty or disabled slot holds no proofs: `contains` is `false`, and
/// `mark` and `unmark` do nothing. A disabled cache never reports a proof
/// again, and disablement never reverts. Keyspace deletion reclaims the rows.
#[derive(Clone)]
pub(crate) struct MarkerCheckSet(CacheSlot);

impl From<CacheSlot> for MarkerCheckSet {
    fn from(slot: CacheSlot) -> Self {
        Self(slot)
    }
}

impl AdmissionChecks for MarkerCheckSet {
    type Error = FjallCellCacheError;

    async fn contains(&self, key: &Key) -> Result<bool, Self::Error> {
        let Some(cache) = self.0.active() else {
            return Ok(false);
        };
        let keyspace = cache.keyspace().clone();
        let marker = codec::marker_key(key);
        io::blocking(move || keyspace.contains_key(marker)).await
    }

    async fn mark(&self, key: &Key) -> Result<(), Self::Error> {
        let Some(cache) = self.0.active() else {
            return Ok(());
        };
        io::write_cell(cache.keyspace(), codec::marker_key(key), Bytes::new()).await
    }

    async fn unmark(&self, key: &Key) -> Result<(), Self::Error> {
        let Some(cache) = self.0.active() else {
            return Ok(());
        };
        let keyspace = cache.keyspace().clone();
        let marker = codec::marker_key(key);
        io::blocking(move || keyspace.remove(marker))
            .await
            .inspect_err(|_| cache.disable())
    }
}
