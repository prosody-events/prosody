//! Value descriptors preserve typed payloads and absence.

use super::*;
use color_eyre::eyre::WrapErr;
use std::iter::{empty, once};

pub(super) fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// One step of a Value trace.
#[derive(Clone, Debug)]
pub(super) enum ValueStep {
    Set(Value),
    Clear,
    Commit,
    Rollback,
}

impl Arbitrary for ValueStep {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 | 1 => Self::Set(ArbJson::arbitrary(g).0),
            2 => Self::Clear,
            3 => Self::Commit,
            _ => Self::Rollback,
        }
    }

    /// Shrinks a set payload to JSON null, the smallest payload.
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Set(value) if !value.is_null() => Box::new(once(Self::Set(Value::Null))),
            _ => Box::new(empty()),
        }
    }
}

/// Oracle invariant: before the first step and after each step, `get`
/// returns the model value and `contains` reports whether the model holds
/// one. The model is the visible value and the last committed value.
/// `rollback` restores the committed value. A JSON null is a stored payload,
/// so `contains` reports it present. The value survives the full
/// `T → codec → cell bytes → store → cell bytes → codec → T` path through
/// the real session substrate.
pub(super) async fn value_trace(steps: Vec<ValueStep>) -> Result<bool> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    let mut visible = None;
    let mut committed = None;
    if !matches_model(&handle, visible.as_ref()).await? {
        return Ok(false);
    }

    for (index, step) in steps.into_iter().enumerate() {
        apply(&handle, &step)
            .await
            .wrap_err_with(|| format!("step {index}: {step:?}"))?;
        match step {
            ValueStep::Set(value) => visible = Some(value),
            ValueStep::Clear => visible = None,
            ValueStep::Commit => committed.clone_from(&visible),
            ValueStep::Rollback => visible.clone_from(&committed),
        }
        let matches = matches_model(&handle, visible.as_ref()).await;
        if !matches.wrap_err_with(|| format!("read after step {index}"))? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Applies `step` to the real handle.
async fn apply(handle: &ValueHandle<TestSession, JsonCodec>, step: &ValueStep) -> Result<()> {
    match step {
        ValueStep::Set(value) => handle.set(value.clone()).await?,
        ValueStep::Clear => handle.clear().await?,
        ValueStep::Commit => {
            handle.commit().await?;
        }
        ValueStep::Rollback => {
            handle.rollback().await;
        }
    }
    Ok(())
}

/// Whether `get` and `contains` both agree with `model`.
async fn matches_model(
    handle: &ValueHandle<TestSession, JsonCodec>,
    model: Option<&Value>,
) -> Result<bool> {
    Ok(handle.get().await?.as_ref() == model && handle.contains().await? == model.is_some())
}

#[test]
pub(super) fn prop_value_trace_matches_model() {
    fn prop(steps: Vec<ValueStep>) -> TestResult {
        let input_dbg = format!("{steps:#?}");
        let result = TEST_RUNTIME.block_on(value_trace(steps));
        finish_trace(result, "value trace diverged from the model", &input_dbg)
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<ValueStep>) -> TestResult);
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
