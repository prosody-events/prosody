//! Group-global descriptor-identity validation.
//!
//! A collection's durable identity — its kind plus its key-format and
//! payload-format tokens — is a complete description of the collection's
//! bytes, compiled into the binary as an application-global fact. The
//! `keyed_state_identity` table exists for exactly one purpose: to make that
//! identity **immutable**. Once a collection has been used, a user can never
//! silently change any of those axes; a process carrying an incompatible
//! descriptor fails loudly (`Permanent`) instead of misreading cells. It is
//! **not** a sweep index, a migration ledger, or an enumeration source — the
//! recovery sweep sources names from the in-process registry, the
//! authoritative declared set.
//!
//! The resolver is deliberately absent: it is application *behavior* over
//! decoded payloads, not a property of the data, and
//! [`Codec::FORMAT_ID`](crate::codec::Codec::FORMAT_ID)'s completeness law
//! forbids a resolver from changing what stored bytes mean. Resolver
//! consistency is checked in-process at bind time
//! (`verify_state_registration` compares the full
//! [`StructuralIdentity`], resolver token included).
//!
//! Each `(group_id, state_type, name)` has one frozen
//! [`DurableDescriptorIdentity`] row. `acquire_descriptor_identities`
//! validates every registered descriptor against it:
//!
//! 1. [`read_identity`](DescriptorIdentityStore::read_identity) — a present row
//!    is validated against the asserted identity (mismatch ⇒
//!    [`DescriptorIdentityError::Mismatch`]).
//! 2. If absent,
//!    [`register_identity`](DescriptorIdentityStore::register_identity)
//!    attempts first-use registration via an LWT. A concurrent registrant may
//!    win the race ([`RegisterOutcome::Conflict`]); the loser validates the
//!    returned existing row in the same round-trip, no re-read.
//!
//! Two registrants with identical identities both succeed (one applies, one
//! validates the conflict); differing identities fail `Permanent` whichever
//! wins. Because the table is group-global, validation is a **process-level**
//! concern — the state manager runs it once at the first partition acquisition
//! and retries until shutdown, so transient store failures never gate a
//! dispatch.
//!
//! Invariant: **no state operation executes under an unvalidated identity** —
//! acquisition fails before any session is minted for the partition.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateType;
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::CollectionDefRegistry;
use std::error::Error;
use std::future::Future;
use thiserror::Error;

/// One durable identity row in wire form.
///
/// Comparison happens on the wire encoding (the discriminator integers and the
/// format/resolver tokens) rather than on decoded enums, so a row written by a
/// *future* build with discriminants this build does not know simply compares
/// unequal — acquisition fails `Permanent` instead of silently coercing it.
/// The raw-`i8` discriminator fields are also what keeps this DTO
/// backend-agnostic (Cassandra and memory persist it as-is), so no scylla
/// serde bridge should be re-added for them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableDescriptorIdentity {
    /// [`StateType`] discriminator (part of the row key).
    pub state_type: i8,

    /// Collection name (part of the row key).
    pub name: String,

    /// [`CollectionKindId`](crate::state::CollectionKindId) discriminator.
    pub kind: i8,

    /// Payload-format token
    /// ([`Codec::FORMAT_ID`](crate::codec::Codec::FORMAT_ID))
    /// — always present, every cell is codec-produced.
    pub format_id: String,

    /// Key-format token — the
    /// [`Codec::FORMAT_ID`](crate::codec::Codec::FORMAT_ID) of the cell's key
    /// axis. Always present: single-cell kinds carry the unit codec's token.
    pub key_format_id: String,
}

impl DurableDescriptorIdentity {
    /// Wire form of a registered descriptor identity for `(state_type, name)`.
    /// The resolver token is deliberately not projected — behavior, not data
    /// (see the module doc).
    pub(crate) fn from_identity(
        state_type: StateType,
        name: &str,
        identity: &StructuralIdentity,
    ) -> Self {
        Self {
            state_type: state_type.into(),
            name: name.to_owned(),
            kind: identity.kind.into(),
            format_id: identity.format_id.to_owned(),
            key_format_id: identity.key_format_id.to_owned(),
        }
    }
}

/// Outcome of a first-use
/// [`register_identity`](DescriptorIdentityStore::register_identity).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
    /// The row did not exist and this caller registered it.
    Applied,

    /// A concurrent registrant won the race; carries the existing row so the
    /// loser validates it without a re-read.
    Conflict(DurableDescriptorIdentity),
}

/// Group-global control-plane store for frozen descriptor-identity rows.
///
/// Implemented by the dedicated memory and Cassandra identity stores —
/// distinct from any kind's cell store. Both methods are keyed by `group_id`
/// plus the `(state_type, name)` collection key, so any partition's store
/// handle is equivalent — identity is decoupled from any partition's cell data.
pub trait DescriptorIdentityStore: Send + Sync + 'static {
    /// Error type for identity reads and writes.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Point-reads the identity row for `(group_id, state_type, name)`.
    fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> impl Future<Output = Result<Option<DurableDescriptorIdentity>, Self::Error>> + Send;

    /// Registers `row` if its `(group_id, state_type, name)` key is unused,
    /// atomically. Returns [`RegisterOutcome::Applied`] when this caller
    /// registered it, or [`RegisterOutcome::Conflict`] carrying the existing
    /// row when a concurrent registrant won.
    fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> impl Future<Output = Result<RegisterOutcome, Self::Error>> + Send;
}

/// Validates every registered descriptor against the group's durable identity
/// rows, registering first-seen collections on the way.
///
/// Runs the read-then-register flow for each registered `(state_type, name)`.
/// An empty registry does no I/O. This is a process-level concern (the table
/// is group-global), so the state manager runs it once and coalesces
/// concurrent first-acquires.
///
/// Fails with [`DescriptorIdentityError::Mismatch`] when a durable row
/// disagrees with the registered identity, or
/// [`DescriptorIdentityError::Store`] when the store fails.
pub(crate) async fn acquire_descriptor_identities<St>(
    store: &St,
    registry: &CollectionDefRegistry,
    group_id: &str,
) -> Result<(), DescriptorIdentityError<St::Error>>
where
    St: DescriptorIdentityStore,
{
    for (state_type, name, identity) in registry.identities() {
        let asserted =
            DurableDescriptorIdentity::from_identity(state_type, name.as_str(), identity);
        match store
            .read_identity(group_id, state_type, name.as_str())
            .await
            .map_err(DescriptorIdentityError::Store)?
        {
            Some(stored) => validate(stored, &asserted)?,
            None => match store
                .register_identity(group_id, &asserted)
                .await
                .map_err(DescriptorIdentityError::Store)?
            {
                RegisterOutcome::Applied => {}
                RegisterOutcome::Conflict(existing) => validate(existing, &asserted)?,
            },
        }
    }
    Ok(())
}

/// Accepts a durable row that equals the asserted identity; otherwise fails
/// `Permanent` without overwriting the row.
///
/// Equality follows the wire-form comparison owned by
/// [`DurableDescriptorIdentity`].
///
/// `pub(crate)` because the reader validates each source's identity through
/// this same function.
pub(crate) fn validate<StoreErr>(
    stored: DurableDescriptorIdentity,
    asserted: &DurableDescriptorIdentity,
) -> Result<(), DescriptorIdentityError<StoreErr>>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    if stored == *asserted {
        Ok(())
    } else {
        Err(DescriptorIdentityError::Mismatch {
            stored: Box::new(stored),
            asserted: Box::new(asserted.clone()),
        })
    }
}

/// Error raised by durable descriptor-identity validation.
#[derive(Debug, Error)]
pub enum DescriptorIdentityError<StoreErr>
where
    StoreErr: ClassifyError + Error + Send + Sync + 'static,
{
    /// The durable identity row disagrees with the registered descriptor. The
    /// row is left untouched; dispatch fails Permanent until the deployed
    /// descriptors match the group's frozen identity.
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

#[cfg(test)]
mod tests;
