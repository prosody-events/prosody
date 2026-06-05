use super::{Codec, JsonBinaryCodec, JsonCodec};

/// The codec tokens are persisted in keyed-state identity rows; changing
/// one orphans every cell written under it. Frozen by construction.
#[test]
fn codec_ids_are_stable() {
    assert_eq!(JsonCodec::CODEC_ID, "json");
    assert_eq!(JsonBinaryCodec::CODEC_ID, "binary");
}
