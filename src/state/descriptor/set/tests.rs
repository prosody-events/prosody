//! Frozen set layout tests.

use super::*;
use crate::codec::Codec;
use crate::state::cell_key::CellKey;
use crate::state::order_codec::OrderedKeyCodec;

#[test]
fn set_layout_is_frozen() {
    let sections = <FrozenLayout as CollectionLayout>::SECTIONS;
    assert_eq!(
        sections
            .iter()
            .map(|section| i8::from(*section))
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let keyset = CellKey {
        section: FrozenLayout::KEYSET.section(),
        coordinate: MapKeysetKey::encode(&()),
    };
    assert_eq!(i8::from(keyset.section), 0);
    assert_eq!(keyset.coordinate.as_bytes(), &[2]);
    assert_eq!(i8::from(FrozenLayout::MEMBERS.section()), 1);
    let families = <FrozenLayout as CollectionLayout>::DESCRIPTOR;
    assert_eq!(families[0].key_format(), "map-keyset-key.v1");
    assert_eq!(families[0].format(), "map-keyset.v1");
    assert_eq!(families[1].key_format(), I64KeyCodec::FORMAT_ID);
    assert_eq!(families[1].format(), UnitCodec::FORMAT_ID);
}
