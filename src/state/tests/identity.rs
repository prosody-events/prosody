//! Property tests for the persisted identity discriminators.
//!
//! [`CollectionKindId`] and [`StateType`] are stored beside durable keyed
//! state as `i8` discriminators and validated back through `TryFrom` on the
//! Cassandra decode skip-path, mirroring the `Encoding` peer in
//! [`crate::state::encoding`]: every valid discriminator must round-trip
//! through `i8`, and every other value must be rejected (the `TryFrom` error
//! type is the only possible `Err`, so `is_err` is enough to prove the value
//! was not silently coerced to a variant).

use super::super::identity::{CollectionKindId, StateType};
use quickcheck::QuickCheck;

#[test]
fn prop_collection_kind_id_discriminator_round_trip() {
    fn property(value: i8) -> bool {
        match value {
            // `Value` (1) is the only collection kind; it round-trips through
            // `i8`, and every other discriminant is rejected.
            1 => CollectionKindId::try_from(value).is_ok_and(|kind| i8::from(kind) == value),
            _ => CollectionKindId::try_from(value).is_err(),
        }
    }

    QuickCheck::new().quickcheck(property as fn(i8) -> bool);
}

#[test]
fn prop_state_type_discriminator_round_trip() {
    fn property(value: i8) -> bool {
        match value {
            // `Application` (0) is the only production namespace; the `Framework`
            // (1) fixture exists under `cfg(test)`. Both round-trip through `i8`.
            0 | 1 => {
                StateType::try_from(value).is_ok_and(|state_type| i8::from(state_type) == value)
            }
            _ => StateType::try_from(value).is_err(),
        }
    }

    QuickCheck::new().quickcheck(property as fn(i8) -> bool);
}
