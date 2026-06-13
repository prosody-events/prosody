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
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::CollectionDefRegistry;
use crate::state::{StateName, StateNameError};
use crate::timers::store::SegmentId;
use std::collections::{HashMap, HashSet};
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
/// Implemented by every durable cell store (memory and Cassandra), so the
/// framework needs no extra wiring parameter — the handler validates through
/// the same backend it stores state in.
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
/// identity rows, writing rows for first-seen names, and returns **every
/// name durable on the segment** — the union of the stored rows and the
/// registered descriptors. An empty registry skips the I/O entirely and
/// returns no names.
///
/// The returned set is what the recovery sweep enumerates (invariant 5,
/// sweep-covers-everything): it includes names whose descriptor was since
/// removed from the application, so crash residue on a deregistered
/// collection is still swept rather than stranded until its TTL. Because no
/// state op runs before its identity row is written, a provisional cell
/// always implies a durable identity row, so this set covers every cell that
/// could exist on the segment.
///
/// # Errors
///
/// Returns [`DescriptorIdentityError::Mismatch`] (Permanent; the row is
/// **not** overwritten) when a durable row disagrees with the registered
/// identity, [`DescriptorIdentityError::Store`] when the store fails, or
/// [`DescriptorIdentityError::Name`] when a stored row carries an empty name.
pub(crate) async fn acquire_descriptor_identities<St>(
    store: &St,
    registry: &CollectionDefRegistry,
    segment_id: SegmentId,
) -> Result<Vec<StateName>, DescriptorIdentityError<St::Error>>
where
    St: DescriptorIdentityStore,
{
    let asserted: Vec<DurableDescriptorIdentity> = registry
        .identities()
        .map(|(name, identity)| DurableDescriptorIdentity::from_identity(name, identity))
        .collect();
    if asserted.is_empty() {
        return Ok(Vec::new());
    }

    let stored = store
        .read_descriptor_identities(segment_id)
        .await
        .map_err(DescriptorIdentityError::Store)?;
    let by_name: HashMap<&str, &DurableDescriptorIdentity> =
        stored.iter().map(|row| (row.name.as_str(), row)).collect();

    let mut missing = Vec::new();
    for row in &asserted {
        match by_name.get(row.name.as_str()) {
            Some(&existing) if existing == row => {}
            Some(&existing) => {
                return Err(DescriptorIdentityError::Mismatch {
                    stored: Box::new(existing.clone()),
                    asserted: Box::new(row.clone()),
                });
            }
            None => missing.push(row.clone()),
        }
    }
    if !missing.is_empty() {
        store
            .write_descriptor_identities(segment_id, missing)
            .await
            .map_err(DescriptorIdentityError::Store)?;
    }

    // The durable name set: every stored row (including deregistered
    // collections this build no longer asserts) plus every asserted name.
    let mut names: Vec<StateName> = Vec::with_capacity(stored.len() + asserted.len());
    let mut seen: HashSet<&str> = HashSet::new();
    for row_name in stored
        .iter()
        .map(|row| row.name.as_str())
        .chain(asserted.iter().map(|row| row.name.as_str()))
    {
        if seen.insert(row_name) {
            names.push(StateName::try_new(row_name)?);
        }
    }
    Ok(names)
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

    /// A durable identity row carried an empty collection name — only
    /// reachable from a corrupt row, since registration rejects empty names.
    #[error(transparent)]
    Name(#[from] StateNameError),
}

impl<StoreErr> ClassifyError for DescriptorIdentityError<StoreErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Mismatch { .. } | Self::Name(_) => ErrorCategory::Permanent,
            Self::Store(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableDescriptorIdentity, acquire_descriptor_identities};
    use crate::state::descriptor::{DescriptorIdentity, ValueDescriptor, value_state};
    use crate::state::memory::MemoryCellStore;
    use crate::state::registry::{CollectionDef, CollectionDefRegistry};
    use crate::state::{StateName, descriptor_identity::DescriptorIdentityStore};
    use color_eyre::eyre::Result;
    use std::collections::HashSet;
    use uuid::Uuid;

    /// The durable name set returned by acquisition is the union of the
    /// stored rows and the registered descriptors — so a collection whose
    /// descriptor was since removed from the application (a stored row with
    /// no registry entry) is still enumerated for the recovery sweep
    /// (invariant 5). Regression: this fails if acquisition returns only the
    /// asserted (registered) names.
    #[tokio::test]
    async fn returns_stored_names_the_registry_no_longer_holds() -> Result<()> {
        let store = MemoryCellStore::new();
        let segment_id = Uuid::from_u128(0xABC);

        // A prior deployment registered "wishlist" and wrote its identity row.
        let wishlist: ValueDescriptor = value_state("wishlist");
        let wishlist_name = StateName::try_new("wishlist")?;
        store
            .write_descriptor_identities(
                segment_id,
                vec![DurableDescriptorIdentity::from_identity(
                    &wishlist_name,
                    &wishlist.structural_identity(),
                )],
            )
            .await?;

        // The current deployment registers only "cart".
        let mut registry = CollectionDefRegistry::default();
        let cart: ValueDescriptor = value_state("cart");
        registry.register(&cart, CollectionDef::new(None))?;

        let names = acquire_descriptor_identities(&store, &registry, segment_id).await?;
        let set: HashSet<&str> = names.iter().map(StateName::as_str).collect();

        assert!(set.contains("cart"), "the registered name must be present");
        assert!(
            set.contains("wishlist"),
            "the deregistered durable name must still be swept"
        );
        Ok(())
    }

    /// An empty registry does no identity I/O and returns no names — the
    /// inert state layer.
    #[tokio::test]
    async fn empty_registry_returns_no_names() -> Result<()> {
        let store = MemoryCellStore::new();
        let registry = CollectionDefRegistry::default();
        let names = acquire_descriptor_identities(&store, &registry, Uuid::from_u128(1)).await?;
        assert!(names.is_empty());
        Ok(())
    }
}
