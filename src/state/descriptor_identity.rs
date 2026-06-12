//! Durable per-segment descriptor-identity validation.
//!
//! Each `(segment, collection name)` pair has one frozen
//! [`StructuralIdentity`] row, written on first use by the partition's
//! single owner (Kafka partition ownership ⇒ no LWT needed).
//! `acquire_descriptor_identities` validates
//! every registered descriptor against the segment's durable rows; the
//! state manager runs it eagerly at partition acquisition, and acquisition
//! failures retry until shutdown, so transient store failures never gate a
//! dispatch.
//!
//! Invariant: **no state operation executes under an unvalidated
//! identity** — acquisition fails before any session is minted for the
//! partition.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateName;
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::CollectionDefRegistry;
use crate::timers::store::SegmentId;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use thiserror::Error;

/// The only identity version this build writes or accepts.
///
/// Identity rows are versioned (append-only clustering column) and every
/// authoritative value cell stamps the version its bytes were written
/// under, but bumping is impossible today: acquisition writes version 1,
/// any other stored version fails Permanent, and a value row stamped with
/// a different version is rejected at decode. Version resolution,
/// bump-append, and per-key migration are future work — the schema is the
/// hook they build on.
pub const INITIAL_IDENTITY_VERSION: i32 = 1;

/// One durable identity row in wire form.
///
/// Comparison happens on the wire encoding (the discriminator integers,
/// the codec token, and the raw label) rather than on decoded enums, so a
/// row written by a *future* build with discriminants this build does not
/// know simply compares unequal — the acquisition fails Permanent instead
/// of being silently coerced. The scylla row serde is derived here so the
/// Cassandra store reads this type directly instead of a primitive tuple.
#[derive(Clone, Debug, PartialEq, Eq, scylla::DeserializeRow)]
pub struct DurableDescriptorIdentity {
    /// Collection name the row freezes.
    pub name: String,

    /// Identity version (clustering column). Only
    /// [`INITIAL_IDENTITY_VERSION`] exists until migration ships.
    pub version: i32,

    /// [`CollectionKindId`](crate::state::CollectionKindId) discriminator.
    pub kind: i8,

    /// [`CellKind`](crate::state::descriptor::CellKind) discriminator.
    pub cell_kind: i16,

    /// Codec token ([`Codec::CODEC_ID`](crate::codec::Codec::CODEC_ID);
    /// `None` for framework-defined cells).
    pub codec_id: Option<String>,
}

impl DurableDescriptorIdentity {
    /// Wire form of a registered descriptor identity.
    pub(crate) fn from_identity(name: &StateName, identity: &StructuralIdentity) -> Self {
        Self {
            name: name.as_str().to_owned(),
            version: INITIAL_IDENTITY_VERSION,
            kind: identity.kind.into(),
            cell_kind: identity.cell_kind.into(),
            codec_id: identity.codec_id.map(str::to_owned),
        }
    }
}

/// Durable storage for per-segment descriptor identity rows.
///
/// Implemented by every durable Value bundle (memory, Cassandra, and the
/// layered/recovering combinators by delegation), so the middleware needs
/// no extra wiring parameter — the handler validates through the same `D`
/// it stores state in.
pub trait DescriptorIdentityStore: Send + Sync + 'static {
    /// Error type for identity reads and writes.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads every identity row for `segment_id`.
    fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> impl Future<Output = Result<Vec<DurableDescriptorIdentity>, Self::Error>> + Send;

    /// Inserts identity rows for `segment_id`.
    ///
    /// Single-owner per segment ⇒ a plain write, never an LWT.
    fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Validates every registered descriptor against the segment's durable
/// identity rows, writing rows for first-seen names. An empty registry
/// skips the I/O entirely.
///
/// # Errors
///
/// Returns [`DescriptorIdentityError::Mismatch`] (Permanent; the row is
/// **not** overwritten) when a durable row disagrees with the registered
/// identity, or [`DescriptorIdentityError::Store`] when the store fails.
pub(crate) async fn acquire_descriptor_identities<St>(
    store: &St,
    registry: &CollectionDefRegistry,
    segment_id: SegmentId,
) -> Result<(), DescriptorIdentityError<St::Error>>
where
    St: DescriptorIdentityStore,
{
    let asserted: Vec<DurableDescriptorIdentity> = registry
        .identities()
        .map(|(name, identity)| DurableDescriptorIdentity::from_identity(name, identity))
        .collect();
    if asserted.is_empty() {
        return Ok(());
    }

    let stored = store
        .read_descriptor_identities(segment_id)
        .await
        .map_err(DescriptorIdentityError::Store)?;
    let by_name: HashMap<&str, &DurableDescriptorIdentity> =
        stored.iter().map(|row| (row.name.as_str(), row)).collect();

    let mut missing = Vec::new();
    for row in asserted {
        match by_name.get(row.name.as_str()) {
            Some(&existing) if *existing == row => {}
            Some(&existing) => {
                return Err(DescriptorIdentityError::Mismatch {
                    stored: Box::new(existing.clone()),
                    asserted: Box::new(row),
                });
            }
            None => missing.push(row),
        }
    }
    if !missing.is_empty() {
        store
            .write_descriptor_identities(segment_id, missing)
            .await
            .map_err(DescriptorIdentityError::Store)?;
    }
    Ok(())
}

/// Error raised by durable descriptor-identity validation.
#[derive(Debug, Error)]
pub enum DescriptorIdentityError<StoreErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// The durable identity row disagrees with the registered descriptor.
    /// The row is left untouched; dispatch fails Permanent until the
    /// deployed descriptors match the segment's frozen identity.
    #[error(
        "descriptor identity mismatch for {name:?}: durable {stored:?}, registered {asserted:?}",
        name = asserted.name
    )]
    Mismatch {
        /// Identity currently frozen in durable storage.
        stored: Box<DurableDescriptorIdentity>,

        /// Identity the registered descriptor asserts.
        asserted: Box<DurableDescriptorIdentity>,
    },

    /// The identity store failed.
    #[error("descriptor identity store failed")]
    Store(#[source] StoreErr),
}

impl<StoreErr> ClassifyError for DescriptorIdentityError<StoreErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Mismatch { .. } => ErrorCategory::Permanent,
            Self::Store(e) => e.classify_error(),
        }
    }
}
