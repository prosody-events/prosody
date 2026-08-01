//! Cassandra-backed durable keyed-state stores.
//!
//! [`CassandraStore`] implements
//! `CellStore` over the
//! `keyed_state_cell` table, and [`CassandraDescriptorIdentityStore`]
//! implements
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore)
//! over the group-global `keyed_state_identity` table — both provisioned by
//! migration `20260522_create_keyed_state.cql`. The data plane (cells) and the
//! control plane (identity) are distinct types: a cell store does not implement
//! [`DescriptorIdentityStore`](crate::state::descriptor_identity::DescriptorIdentityStore),
//! so "which kind owns identity?" is un-askable. A third distinct type,
//! [`CassandraPublicationStore`], serves the routing-only
//! `keyed_state_publication` table, provisioned by migration
//! `20260722_create_keyed_state_publication.cql`. It holds no identity, so the
//! data, control, and discovery planes stay separable.
//! The stores and their prepared statements, the decoder shape table, and the
//! errors live in the submodules re-exported below.

mod cell;
mod error;
mod identity;
mod publication;
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
pub use publication::{CassandraPublicationError, CassandraPublicationStore, PublicationQueries};
