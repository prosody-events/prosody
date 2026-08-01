//! Collection metadata and shared handles cloned into read sessions.

use crate::codec::Codec;
use crate::state::registry::CollectionDef;
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderBackend;
use crate::state_reader::cache::ReaderCache;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

/// A collection definition whose inherited reader-cache policy is resolved.
#[derive(Clone, Copy)]
pub(crate) struct ReaderCollectionDef {
    pub(super) collection: CollectionDef,
    pub(super) read_cache_ttl: Option<Duration>,
}

impl ReaderCollectionDef {
    pub(crate) fn new(collection: CollectionDef, read_cache_ttl: Option<Duration>) -> Self {
        Self {
            collection,
            read_cache_ttl,
        }
    }
}

/// Everything a reader shares with every session it builds.
pub(crate) struct ReaderContext<C: Codec, B> {
    pub(crate) backend: Arc<B>,
    codec: PhantomData<fn() -> C>,
    pub(crate) cache: ReaderCache,
    pub(crate) def: ReaderCollectionDef,
    pub(crate) state_type: StateType,
    pub(crate) name: StateName,
}

impl<C: Codec, B: ReaderBackend<C>> ReaderContext<C, B> {
    pub(crate) fn new(
        backend: Arc<B>,
        cache: ReaderCache,
        def: ReaderCollectionDef,
        state_type: StateType,
        name: StateName,
    ) -> Self {
        Self {
            backend,
            codec: PhantomData,
            cache,
            def,
            state_type,
            name,
        }
    }
}

/// Cloning shares handles without requiring `C: Clone`.
impl<C: Codec, B> Clone for ReaderContext<C, B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            codec: PhantomData,
            cache: self.cache.clone(),
            def: self.def,
            state_type: self.state_type,
            name: self.name.clone(),
        }
    }
}
