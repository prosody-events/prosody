//! Cassandra-backed durable keyed-state stores.
//!
//! [`CassandraStore`] implements
//! [`CellStore`](crate::state::store::CellStore) over the
//! `keyed_state_cell` table, and [`CassandraDescriptorIdentityStore`]
//! implements
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore)
//! over the group-global `keyed_state_identity` table — both provisioned by
//! migration `20260522_create_keyed_state.cql`. The data plane (cells) and the
//! control plane (identity) are distinct types: a cell store does not implement
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore),
//! so "which kind owns identity?" is un-askable.
//! The stores and their prepared statements, the decoder shape table, and the
//! errors live in the submodules re-exported below.

mod cell;
mod error;
mod identity;
mod serialize;
#[cfg(test)]
mod tests;
mod udt;

pub use cell::{
    CassandraCellResources, CassandraStore, CellCorruptReason, CellQueries, CellStoreError,
    EncodingError,
};
pub use error::{CassandraCellStoreError, CorruptUdtError};
pub use identity::{
    CassandraDescriptorIdentityError, CassandraDescriptorIdentityStore, IdentityQueries,
};
