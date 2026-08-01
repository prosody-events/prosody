//! In-memory descriptor identity storage.

use crate::state::StateType;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use ahash::RandomState;
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;

/// Group-global identity key: `(group_id, state_type discriminator, name)`.
type IdentityKey = (String, i8, String);

/// In-memory group-global [`DescriptorIdentityStore`].
///
/// One instance is shared across partition reassignments. Registered
/// identities therefore survive an in-process rebalance.
#[derive(Clone, Debug, Default)]
pub struct MemoryDescriptorIdentityStore {
    inner: Arc<scc::HashMap<IdentityKey, DurableDescriptorIdentity, RandomState>>,
}

impl MemoryDescriptorIdentityStore {
    /// Creates an empty identity store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DescriptorIdentityStore for MemoryDescriptorIdentityStore {
    type Error = Infallible;

    async fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, Self::Error> {
        let key = (group_id.to_owned(), state_type.into(), name.to_owned());
        Ok(self.inner.read_async(&key, |_, row| row.clone()).await)
    }

    async fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> Result<RegisterOutcome, Self::Error> {
        let key = (group_id.to_owned(), row.state_type, row.name.clone());
        match self.inner.entry_async(key).await {
            Entry::Vacant(slot) => {
                slot.insert_entry(row.clone());
                Ok(RegisterOutcome::Applied)
            }
            Entry::Occupied(existing) => Ok(RegisterOutcome::Conflict(existing.get().clone())),
        }
    }
}
