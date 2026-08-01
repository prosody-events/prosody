//! Tests for the node directory and its address cache.
//!
//! Everything here runs against the live local cluster in the shared
//! `prosody_test` keyspace. A down cluster fails these tests rather than
//! skipping them.

pub(crate) mod support;

mod cache;
mod registration;
