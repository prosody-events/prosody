//! Bundles the partition stores and admission checks behind one type parameter.
//! The module remains private because these dependencies serve framework code.

use super::store::CellStore;
use crate::Key;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::ClassifyError;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};

/// Bundles partition dependencies behind one type parameter.
/// This trait exists for type-parameter compression across managers and
/// sessions.
pub trait StateBackend: Send + Sync + 'static {
    /// The shared message dedup store.
    type Dedup: DeduplicationStore;

    /// The shared descriptor-identity control-plane store, validated eagerly
    /// at acquisition. It is decoupled from the cell data store — the cell
    /// store does **not** implement [`DescriptorIdentityStore`].
    type Identity: DescriptorIdentityStore;

    /// The one uniform durable cell store (`Cached<CassandraStore>` in
    /// production, `MemoryCellStore` in tests). The session wraps it in the
    /// per-event dirty `Overlay`.
    type Cell: CellStore;
    /// Returns the shared message dedup store.
    fn dedup(&self) -> Self::Dedup;

    /// The assignment admission proof store.
    type Checks: AdmissionChecks;

    /// Returns the assignment admission proof store.
    fn checks(&self) -> Self::Checks;

    /// The shared descriptor-identity store.
    fn identity(&self) -> Self::Identity;

    /// The uniform durable cell store.
    fn cell(&self) -> Self::Cell;
}

/// The one concrete backend every factory mints; [`StateBackend`] projects its
/// store type so callers name only `B`.
#[derive(Clone, Debug)]
pub struct PartitionBackend<D, I, C, K> {
    dedup: D,
    identity: I,
    cell: C,
    checks: K,
}

impl<D, I, C, K> PartitionBackend<D, I, C, K> {
    /// Bundles the stores and admission checks.
    #[must_use]
    pub fn new(dedup: D, identity: I, cell: C, checks: K) -> Self {
        Self {
            dedup,
            identity,
            cell,
            checks,
        }
    }
}

impl<D, I, C, K> StateBackend for PartitionBackend<D, I, C, K>
where
    D: DeduplicationStore,
    I: DescriptorIdentityStore + Clone,
    C: CellStore,
    K: AdmissionChecks,
{
    type Cell = C;
    type Checks = K;
    type Dedup = D;
    type Identity = I;

    fn dedup(&self) -> D {
        self.dedup.clone()
    }

    fn checks(&self) -> K {
        self.checks.clone()
    }

    fn identity(&self) -> I {
        self.identity.clone()
    }

    fn cell(&self) -> C {
        self.cell.clone()
    }
}

/// Creates the keyed-state dependencies for each partition assignment.
/// The workspace lives until partition revocation.
pub trait StateBackendFactory<T>: Clone + Send + Sync + 'static {
    /// The per-partition backend bundle this factory mints.
    type Backend: StateBackend;

    /// Error returned when a partition's backend cannot be materialized.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Creates the backend for the partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the assignment workspace cannot open.
    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: T,
    ) -> Result<Self::Backend, Self::Error>;
}

/// Clones shared test stores for each partition.
#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SharedStateBackend<S, I, D> {
    cell: S,
    identity: I,
    dedup: D,
}

#[cfg(test)]
impl<S, I, D> SharedStateBackend<S, I, D> {
    /// Creates a backend factory that hands out clones of the supplied parts.
    #[must_use]
    pub fn new(cell: S, identity: I, dedup: D) -> Self {
        Self {
            cell,
            identity,
            dedup,
        }
    }
}

/// Shared test stores do not use the trigger handle.
#[cfg(test)]
impl<S, I, D, T> StateBackendFactory<T> for SharedStateBackend<S, I, D>
where
    S: CellStore + Clone,
    I: DescriptorIdentityStore + Clone,
    D: DeduplicationStore,
{
    type Backend = PartitionBackend<D, I, S, ()>;
    type Error = Infallible;

    fn for_partition(
        &self,
        _topic: Topic,
        _partition: Partition,
        _triggers: T,
    ) -> Result<Self::Backend, Self::Error> {
        Ok(PartitionBackend::new(
            self.dedup.clone(),
            self.identity.clone(),
            self.cell.clone(),
            (),
        ))
    }
}

/// Stores admission proofs for one partition assignment.
/// A checked key has completed admission, including local repairs for Permanent
/// errors. Each later settle resolves its collections or removes the proof.
/// Finalize removes it after a failed stage. Staged removes it when shutdown
/// interrupts promotion or rollback. A failed unmark must disable the proof
/// before it returns an error.
pub trait AdmissionChecks: Clone + Send + Sync + 'static {
    /// The classified storage error.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Tests whether admission completed for this key.
    fn contains<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send + use<'a, Self>;

    /// Records complete admission.
    fn mark<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;

    /// Removes admission proof before another dispatch can use it.
    fn unmark<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<'a, Self>;
}

/// Memory stores have no assignment workspace. Each event admits again.
impl AdmissionChecks for () {
    type Error = Infallible;

    fn contains<'a>(
        &'a self,
        _key: &'a Key,
    ) -> impl Future<Output = Result<bool, Self::Error>> + use<'a> {
        ready(Ok(false))
    }

    fn mark<'a>(
        &'a self,
        _key: &'a Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<'a> {
        ready(Ok(()))
    }

    fn unmark<'a>(
        &'a self,
        _key: &'a Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<'a> {
        ready(Ok(()))
    }
}
