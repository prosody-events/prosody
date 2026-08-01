//! Shared scaffolding for the reader suites: the vocabulary every suite uses,
//! and the re-exports that keep one `support::` import path across the split.
//!
//! [`owner`] seeds committed — and for the window arm provisional — state
//! through the real
//! [`KeyedStateSession`](crate::state::session::KeyedStateSession). [`backend`]
//! composes the memory harness and the backend-generic reader seam.
//! [`scripted`] holds the fault-injectable stores and the environment the probe
//! and refresh suites drive.
//!
//! Committed state is never hand-written at a cell address. It always flows
//! through the owner session, so the reader reads exactly what the owner wrote,
//! under the segment that
//! [`partition_segment_id`](crate::segment::partition_segment_id) computes.

mod backend;
mod owner;
mod scripted;

pub(crate) use backend::publish_source;
pub(super) use backend::{MemoryHarness, MemoryReaderBackend, ReaderBackend};
pub(super) use owner::{OwnerSession, owner_commit_cell, owner_stage};
pub(crate) use owner::{owner_commit, registry_of, source_state_key};
pub(crate) use scripted::{CountingIdentityStore, ScriptedCellSource};
pub(super) use scripted::{FaultPoint, ScriptedEnv};

use crate::Topic;
use crate::state::identity::StateName;
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::{PartitionCount, StateReaderError};
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, TryStreamExt};
use internment::Intern;
use quanta::{Clock, Mock};
use std::sync::Arc;

/// The subsystem every suite routes under.
pub(super) const SUBSYSTEM: &str = "orders";

/// A distinct-source key space: two groups, lexicographically ordered so
/// `GROUP_A` is the deterministic lowest source and `GROUP_B` the decoy.
pub(super) const GROUP_A: &str = "group-aaa";
pub(super) const GROUP_B: &str = "group-zzz";

/// The mock topology's fixed partition count.
pub(crate) fn mock_count() -> PartitionCount {
    PartitionCount::MOCK
}

/// The subsystem name.
pub(super) fn subsystem() -> Result<SubsystemName> {
    SubsystemName::try_new(SUBSYSTEM).map_err(|e| eyre!("subsystem: {e}"))
}

/// A collection state name.
pub(crate) fn state_name(name: &str) -> Result<StateName> {
    StateName::try_new(name).map_err(|e| eyre!("name: {e}"))
}

/// An interned topic.
pub(crate) fn topic(name: &str) -> Topic {
    Intern::<str>::from(name)
}

/// A cache with a mocked clock over `budget` declared bytes, returning the
/// [`Mock`] handle the test advances (never a sleep). The mock starts at zero
/// and only moves forward, mirroring the monotonic clock production uses.
pub(super) fn mock_clock_cache(budget: u64) -> (ReaderCache, Arc<Mock>) {
    let (clock, mock) = Clock::mock();
    (ReaderCache::with_clock(budget, clock), mock)
}

/// Collects a fallible reader stream into a `Vec`, surfacing the first error.
pub(super) async fn collect_stream<T>(
    stream: impl Stream<Item = Result<T, StateReaderError>>,
) -> Result<Vec<T>> {
    Ok(stream.try_collect().await?)
}
