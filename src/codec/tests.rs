use super::{Codec, JsonBinaryCodec, JsonCodec};

/// The format tokens are persisted in keyed-state identity rows; changing
/// one orphans every cell written under it. Frozen by construction. The two
/// JSON codecs are deliberately format-equal — that equality is what lets
/// differently-implemented consumers share a collection.
#[test]
fn format_ids_are_stable() {
    assert_eq!(JsonCodec::FORMAT_ID, "json");
    assert_eq!(JsonBinaryCodec::FORMAT_ID, "json");
}
