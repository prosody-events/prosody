//! Standalone ownership of reader infrastructure.

use crate::codec::Codec;
use crate::state::descriptor::StateDescriptor;
use crate::state_reader::backend::{CassandraReaderBackend, ReaderBackend};
use crate::state_reader::{StateReader, StateReaderDependencies, StateReaderError};
use crate::subsystem::SubsystemName;

/// A standalone client for published keyed-state reads.
///
/// It retains the Cassandra session, Kafka message loader, caches, and source
/// snapshots supplied as dependencies. Those components can be shared with a
/// producer or consumer, while this client's lifecycle remains independent.
pub struct StateReaderClient<C: Codec, B> {
    deps: StateReaderDependencies<C, B>,
}

impl<C, B> Clone for StateReaderClient<C, B>
where
    C: Codec,
{
    fn clone(&self) -> Self {
        Self {
            deps: self.deps.clone(),
        }
    }
}

impl<C, B> StateReaderClient<C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
{
    /// Creates a standalone reader from its shared dependencies.
    #[must_use]
    pub fn new(deps: StateReaderDependencies<C, B>) -> Self {
        Self { deps }
    }

    /// Creates a collection reader managed by this client.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor or effective cache policy is
    /// invalid.
    pub fn state<D>(
        &self,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<StateReader<D, C, B>, StateReaderError>
    where
        D: StateDescriptor,
    {
        StateReader::new(&self.deps, subsystem, descriptor)
    }

    pub(crate) fn deps(&self) -> StateReaderDependencies<C, B> {
        self.deps.clone()
    }
}

/// Standalone reader client backed by Cassandra and Kafka.
pub type CassandraStateReaderClient<C = crate::JsonCodec> =
    StateReaderClient<C, CassandraReaderBackend<C>>;
