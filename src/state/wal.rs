//! Write-ahead-log payloads and the non-empty operation list they wrap.
//!
//! A [`WalEnvelope`] is the materialized typed op stream; [`WalBlob`] is
//! its encoded bytes tagged with a [`WalFormat`]; [`SealedWal`] is the
//! durable sealed state (blob + owning [`EventRef`]). All three rest on
//! [`NonEmptyOps`], which makes the "a WAL never holds zero operations"
//! invariant a type-level fact.

use super::encoding::{self, EncodingError, WalFormat};
use super::event_ref::EventRef;
use super::identity::CollectionKind;
use crate::error::{ClassifyError, ErrorCategory};
use bytes::Bytes;
use std::iter;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use thiserror::Error;

/// Non-empty ordered operation list.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NonEmptyOps<T> {
    first: T,
    rest: Vec<T>,
}

impl<T> NonEmptyOps<T> {
    /// Creates a non-empty operation list.
    #[must_use]
    pub fn new(first: T, rest: Vec<T>) -> Self {
        Self { first, rest }
    }

    /// Creates a non-empty operation list from a vector.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_from_vec(ops: Vec<T>) -> Result<Self, EmptyOperationsError> {
        let mut items = ops.into_iter();
        let first = items.next().ok_or(EmptyOperationsError)?;
        Ok(Self::new(first, items.collect()))
    }

    /// Iterates over the operations in order.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        iter::once(&self.first).chain(self.rest.iter())
    }

    /// Decomposes the list into a vector.
    #[must_use]
    pub fn into_vec(self) -> Vec<T> {
        let mut ops = Vec::with_capacity(1 + self.rest.len());
        ops.push(self.first);
        ops.extend(self.rest);
        ops
    }

    /// Returns the number of operations.
    ///
    /// Always `1 + rest.len()` — the list holds `first` plus `rest`.
    #[must_use]
    pub fn len(&self) -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(self.rest.len() as u64)
    }
}

/// Materialized typed WAL payload.
///
/// The on-wire WAL header (`version`, `kind`, `op_count`) is an encoding
/// detail owned by [`encoding::encode_wal`] and [`encoding::decode_wal`].
/// Persisting it here would create two operation-count sources that could
/// disagree; instead the header is constructed at encode time from
/// [`Self::operation_count`] and validated and discarded at decode time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEnvelope<K>
where
    K: CollectionKind,
{
    ops: NonEmptyOps<K::Op>,
    _kind: PhantomData<K>,
}

impl<K> WalEnvelope<K>
where
    K: CollectionKind,
{
    /// Creates a typed WAL payload from a non-empty operation list.
    #[must_use]
    pub fn new(ops: NonEmptyOps<K::Op>) -> Self {
        Self {
            ops,
            _kind: PhantomData,
        }
    }

    /// Creates a typed WAL payload from ordered operations.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyOperationsError`] when `ops` is empty.
    pub fn try_from_ops(ops: Vec<K::Op>) -> Result<Self, EmptyOperationsError> {
        Ok(Self::new(NonEmptyOps::try_from_vec(ops)?))
    }

    /// Returns ordered WAL operations.
    pub fn ops(&self) -> impl Iterator<Item = &K::Op> {
        self.ops.iter()
    }

    /// Decomposes this WAL into ordered operations.
    #[must_use]
    pub fn into_ops(self) -> Vec<K::Op> {
        self.ops.into_vec()
    }

    /// Returns the number of WAL operations.
    #[must_use]
    pub fn operation_count(&self) -> NonZeroU64 {
        self.ops.len()
    }
}

/// Encoded WAL bytes tagged with their collection kind.
///
/// The encoded body owns the operation count via its [`WalFormat`]-specific
/// header frame, so this type carries only the bytes and the format
/// discriminator. Callers needing the materialized op stream call
/// [`encoding::decode_wal`] to recover a [`WalEnvelope`].
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct WalBlob<K>
where
    K: CollectionKind,
{
    bytes: Bytes,
    format: WalFormat,
    _kind: PhantomData<K>,
}

impl<K> WalBlob<K>
where
    K: CollectionKind,
{
    /// Creates an encoded typed WAL.
    #[must_use]
    pub fn new(bytes: Bytes, format: WalFormat) -> Self {
        Self {
            bytes,
            format,
            _kind: PhantomData,
        }
    }

    /// Returns the encoded WAL bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the encoded WAL format discriminator.
    #[must_use]
    pub fn format(&self) -> WalFormat {
        self.format
    }
}

/// Durable sealed state for one event and non-empty WAL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedWal<K>
where
    K: CollectionKind,
{
    event: EventRef,
    wal: WalBlob<K>,
}

impl<K> SealedWal<K>
where
    K: CollectionKind,
{
    /// Creates durable sealed state from an encoded WAL blob.
    #[must_use]
    pub fn new(event: EventRef, wal: WalBlob<K>) -> Self {
        Self { event, wal }
    }

    /// Returns the event that owns the sealed operations.
    #[must_use]
    pub fn event(&self) -> EventRef {
        self.event
    }

    /// Returns the sealed WAL blob.
    #[must_use]
    pub fn wal(&self) -> &WalBlob<K> {
        &self.wal
    }
}

impl<K> SealedWal<K>
where
    K: CollectionKind,
    K::Op: encoding::EncodableOp,
{
    /// Creates durable sealed state by encoding a non-empty ordered operation
    /// list.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError`] when `ops` is empty or the WAL encoder fails.
    pub fn try_new(
        event: EventRef,
        ops: Vec<K::Op>,
        format: WalFormat,
    ) -> Result<Self, EncodingError> {
        let envelope = WalEnvelope::<K>::try_from_ops(ops)?;
        let wal = encoding::encode_wal::<K>(&envelope, format)?;
        Ok(Self::new(event, wal))
    }
}

/// Error returned when a non-empty operation list is required.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("state operation list must not be empty")]
pub struct EmptyOperationsError;

impl ClassifyError for EmptyOperationsError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}
