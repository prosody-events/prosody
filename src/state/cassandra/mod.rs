//! Cassandra-backed durable cell store.
//!
//! [`CassandraCellStore`] implements
//! [`CellStore<ValueKind>`](crate::state::store::CellStore) and
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore)
//! over the `keyed_state_value` and `keyed_state_descriptor` tables
//! provisioned by migration `20260522_create_keyed_state.cql`. The store and
//! its prepared statements, the decoder shape table, and the errors live in
//! the submodules re-exported below.

mod cell;
mod error;
mod serialize;
mod udt;

pub use cell::{CassandraCellStore, CellCorruptReason, CellQueries};
pub use error::{CassandraValueStoreError, CorruptUdtError};
