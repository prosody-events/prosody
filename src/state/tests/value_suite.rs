//! Shared fixtures for keyed-state test modules.
//!
//! The WAL-era trace runners that lived here were removed with the
//! write-ahead-log durability model; the provisional-cell property suites
//! that replace them are built in the test-restructure step. What remains is
//! the small set of fixtures every keyed-state test module shares — random
//! collection identities, the canonical payload, and the bounded-vector
//! generator the dirty-store suite reuses.

use super::super::{CollectionId, StateKey, StateName, StateType, ValueKind};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen};
use std::sync::Arc;
use uuid::Uuid;

/// Upper bound on generated trace lengths, keeping property runs bounded.
pub(crate) const MAX_TRACE_OPS: usize = 40;

/// Random-keyed fixture identity for the named Value collection.
///
/// Shared by every keyed-state test module; the segment is a fresh random
/// UUID so independent fixtures never collide on the durable store.
pub(crate) fn collection_id(name: &str) -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// Generates an [`Arbitrary`] vector capped at `max` elements, keeping trace
/// lengths bounded without each fixture repeating the take-and-collect dance.
pub(crate) fn capped_vec<T: Arbitrary>(g: &mut Gen, max: usize) -> Vec<T> {
    Vec::<T>::arbitrary(g).into_iter().take(max).collect()
}

/// Canonical single-byte payload cell, shared by every keyed-state test
/// module (the cell content is opaque to the LWW state machine).
pub(crate) fn bytes(value: u8) -> Bytes {
    Bytes::from(vec![value])
}
