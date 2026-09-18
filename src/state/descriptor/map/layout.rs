//! Map cell families and their durable layout contracts.

use super::{Keyed, KeysetLayout, MapKeysetCodec, MapKeysetKey};
use crate::codec::JsonCodec;
use crate::state::StateName;
use crate::state::cell_key::Direction;
use crate::state::collection::{
    CellFamily, CollectionLayout, collection_layout, same_token, spec_matches,
};
use crate::state::descriptor::CellType;
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec, UnitKey};
use tracing::{Span, info_span};

collection_layout! {
    /// The Map collection kind: one keyset cell plus one cell per key. The key
    /// codec `KC` is frozen into the collection's durable identity.
    pub struct MapKind<KC, V> {
        /// The keyset cell — current membership (see the module's
        /// current-membership invariant).
        #[id(0)]
        KEYSET: Keyed<MapKeysetKey, MapKeysetCodec>,
        /// One cell per key.
        #[id(1)]
        ENTRIES: Keyed<KC, V>,
    }
}

impl<KC, V> KeysetLayout for MapKind<KC, V>
where
    KC: OrderedKeyCodec,
    V: CellType<Key = UnitKey>,
{
    const KEYSET: CellFamily<Self, Keyed<MapKeysetKey, MapKeysetCodec>> = Self::KEYSET;
    const MEMBERS: CellFamily<Self, Self::Cell> = Self::ENTRIES;

    fn stream_span(collection: &StateName, dir: Direction, projection: &'static str) -> Span {
        info_span!("map.stream", collection = collection.as_str(), direction = ?dir, projection)
    }
}

/// The instantiation the frozen-layout pin and the test-only cell-address
/// helpers read their sections and format tokens from. A family's durable
/// section and its declared codecs come from the layout, never from the type
/// parameters, so every instantiation answers identically.
pub(super) type FrozenLayout = MapKind<I64KeyCodec, JsonCodec>;

/// Map's durable layout, frozen. The ids and the keyset family's format tokens
/// below address every Map cell ever written; changing one silently re-points
/// existing rows, and no type can compare this crate against yesterday's
/// schema. The entries family's key and payload tokens are the *user's* choice
/// and ride the collection's structural identity instead. The pin is a
/// compile-time assertion rather than a test so it cannot be filtered out of a
/// run.
const _: () = {
    let families = <FrozenLayout as CollectionLayout>::DESCRIPTOR;
    assert!(
        families.len() == 2,
        "Map declares exactly two cell families"
    );
    assert!(
        families[0].id() == 0,
        "Map's keyset family is durably section 0"
    );
    assert!(
        same_token(families[0].key_format(), "map-keyset-key.v1"),
        "the keyset cell is durably addressed by the Map keyset key"
    );
    assert!(
        same_token(families[0].format(), "map-keyset.v1"),
        "the keyset cell is durably encoded by the Map keyset frame codec"
    );
    assert!(
        families[1].id() == 1,
        "Map's entries family is durably section 1"
    );
    assert!(
        spec_matches::<FrozenLayout>(families[1]),
        "the spec's cell type addresses and encodes the entries family"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::SECTIONS.len() == 2,
        "Map's reset domain is its two families"
    );
    assert!(
        <FrozenLayout as CollectionLayout>::RESERVED.is_empty(),
        "Map has never removed a family"
    );
};
