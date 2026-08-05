//! Tests for both node directories and the shared address cache.
//!
//! The `memory` suite runs in this process alone. The `cassandra` suite and the
//! cache's own suite run against the live local cluster in the shared
//! `prosody_test` keyspace. A down cluster fails those tests rather than
//! skipping them.

pub(crate) mod suite;
pub(crate) mod support;

mod cache;
mod cassandra;
mod memory;
