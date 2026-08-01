//! Bounded sharing of publication-source snapshots across collection readers.

use crate::state::descriptor::StructuralIdentity;
use crate::state::{CollectionKindId, StateName, StateType};
use crate::state_reader::reader::acquisition::PublicationSnapshot;
use crate::subsystem::SubsystemName;
use quick_cache::sync::Cache;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// Publication metadata is keyed by declared collections, not application
/// keys. The fixed capacity prevents an unbounded set of runtime descriptors
/// from retaining snapshots.
const PUBLICATION_CACHE_CAPACITY: usize = 1_024;

type PublicationCacheKey = (
    SubsystemName,
    StateType,
    StateName,
    CollectionKindId,
    &'static str,
    Option<&'static str>,
    &'static str,
    Duration,
);

/// One bounded publication snapshot cache shared by a reader dependency
/// bundle.
#[derive(Clone)]
pub(crate) struct PublicationCache {
    inner: Arc<Cache<PublicationCacheKey, Arc<PublicationSnapshot>>>,
}

impl PublicationCache {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Cache::new(PUBLICATION_CACHE_CAPACITY)),
        }
    }

    pub(crate) fn snapshot(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        identity: &StructuralIdentity,
        refresh_interval: Duration,
    ) -> Arc<PublicationSnapshot> {
        let key = (
            subsystem.clone(),
            state_type,
            name.clone(),
            identity.kind,
            identity.format_id,
            identity.resolver_id,
            identity.key_format_id,
            refresh_interval,
        );
        match self
            .inner
            .get_or_insert_with(&key, || Ok::<_, Infallible>(Arc::default()))
        {
            Ok(snapshot) => snapshot,
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use crate::state::descriptor::{DescriptorIdentity, value_state};
    use color_eyre::eyre::Result;

    #[test]
    fn readers_share_one_collection_snapshot() -> Result<()> {
        let cache = PublicationCache::new();
        let subsystem = SubsystemName::try_new("carts")?;
        let name = StateName::try_new("cart")?;
        let descriptor = value_state::<JsonCodec>("cart");
        let identity = descriptor.structural_identity();
        let first = cache.snapshot(
            &subsystem,
            descriptor.state_type(),
            &name,
            &identity,
            Duration::from_mins(1),
        );
        let second = cache.snapshot(
            &subsystem,
            descriptor.state_type(),
            &name,
            &identity,
            Duration::from_mins(1),
        );

        assert!(
            Arc::ptr_eq(&first, &second),
            "one typed collection must share one publication snapshot"
        );
        Ok(())
    }
}
