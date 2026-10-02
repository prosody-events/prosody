//! Value descriptors preserve typed payloads and absence.

use super::*;

pub(super) fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Round-trip invariant: for every JSON-representable value, `set(v)` then
/// `get()` returns `Some(v)` — the value survives the full
/// `T → codec → cell bytes → store → cell bytes → codec → T` path through
/// the real session substrate.
pub(super) async fn roundtrip(value: Value) -> Result<bool> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    handle.set(value.clone()).await?;
    Ok(handle.get().await? == Some(value))
}

#[test]
pub(super) fn prop_descriptor_set_get_roundtrip() {
    fn prop(value: ArbJson) -> TestResult {
        let input_dbg = format!("{value:#?}");
        let result = TEST_RUNTIME.block_on(roundtrip(value.0));
        finish_trace(result, "typed roundtrip lost", &input_dbg)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

/// A never-written collection reads as `None`.
#[tokio::test]
pub(super) async fn descriptor_get_absent_returns_none() -> Result<()> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// `set` then `clear` reads as `None`.
#[tokio::test]
pub(super) async fn descriptor_clear_then_get_none() -> Result<()> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    handle.set(json!({"items": [1_i32, 2_i32]})).await?;
    handle.clear().await?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// A user-written typed cell: the codec **is** the typing, so a `Cart`
/// cell is one `Codec` impl away — no second encoding layer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct Cart {
    items: Vec<String>,
}

#[derive(Default)]
pub(super) struct CartCodec;

impl Codec for CartCodec {
    type Error = JsonCodecError;
    type Payload = Cart;

    const FORMAT_ID: &'static str = "test-cart";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Cart, JsonCodecError> {
        serde_json::from_slice(buf).map_err(JsonCodecError::Serde)
    }

    fn deserialize_owned(&mut self, buf: bytes::BytesMut) -> Result<Cart, JsonCodecError> {
        serde_json::from_slice(&buf).map_err(JsonCodecError::Serde)
    }

    fn serialize(&mut self, payload: Cart, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
        serde_json::to_writer(buf, &payload).map_err(JsonCodecError::Serde)
    }

    fn serialize_ref(&mut self, payload: &Cart, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
        serde_json::to_writer(buf, payload).map_err(JsonCodecError::Serde)
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        thread_local! {
            static CACHE: RefCell<CartCodec> = const { RefCell::new(CartCodec) };
        }
        CACHE.with_borrow_mut(f)
    }
}

/// Typed round-trip: a cell declared with a user codec round-trips its payload
/// type and records the codec's token in the structural identity.
#[tokio::test]
pub(super) async fn custom_codec_cell_roundtrips_typed_payload() -> Result<()> {
    let typed_cart: ValueDescriptor<CartCodec> = value_state("typed_cart");
    assert_eq!(typed_cart.structural_identity().format_id, "test-cart");

    let handle = bind_registered(typed_cart, MemoryLoader::new())?;
    let cart = Cart {
        items: vec!["a".into(), "b".into()],
    };
    handle.set(cart.clone()).await?;
    assert_eq!(handle.get().await?, Some(cart));
    Ok(())
}
