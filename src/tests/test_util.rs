use quickcheck::{Arbitrary, Gen};
use serde_json::{Map, Value};
use std::sync::LazyLock;
use tokio::runtime::{Builder, Runtime};

/// Shared multi-threaded runtime for all unit tests in the crate.
#[expect(
    clippy::expect_used,
    reason = "LazyLock requires non-fallible closure; test infra cannot recover from failure"
)]
pub static TEST_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
});

/// Depth-bounded `serde_json::Value` generator shared by the state-codec
/// and descriptor round-trip properties.
///
/// Floats are deliberately excluded: JSON has no NaN and float identity
/// is not the invariant under test — structural round-tripping is.
#[derive(Clone, Debug)]
pub struct ArbJson(pub Value);

impl Arbitrary for ArbJson {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(arbitrary_json(g, 3))
    }
}

fn arbitrary_json(g: &mut Gen, depth: u8) -> Value {
    let variants = if depth == 0 { 4 } else { 6 };
    match u8::arbitrary(g) % variants {
        0 => Value::Null,
        1 => Value::Bool(bool::arbitrary(g)),
        2 => Value::from(i64::arbitrary(g)),
        3 => Value::String(String::arbitrary(g)),
        4 => Value::Array(
            (0..u8::arbitrary(g) % 4)
                .map(|_| arbitrary_json(g, depth - 1))
                .collect(),
        ),
        _ => Value::Object(
            (0..u8::arbitrary(g) % 4)
                .map(|_| (String::arbitrary(g), arbitrary_json(g, depth - 1)))
                .collect::<Map<_, _>>(),
        ),
    }
}
