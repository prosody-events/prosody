//! Fixed-width codec composition.
//!
//! A [`FixedCodec`] is a [`Codec`] whose wire form is exactly `WIDTH` bytes for
//! every payload. That single guarantee is what lets two of them compose into a
//! tuple codec with no framing: the pair's bytes are the two halves
//! back-to-back, split at the known first width. [`I64Codec`] is the primitive;
//! the blanket `impl Codec for (A, B)` is the composer, and its
//! [`FORMAT_ID`](Codec::FORMAT_ID) is derived at compile time from the
//! components' ids so a composed codec names its own durable identity.

use crate::codec::Codec;
use crate::codec::const_id::ConstId;
use std::convert::Infallible;
use std::error::Error;
use thiserror::Error;

/// A [`Codec`] whose wire form is exactly [`WIDTH`](FixedCodec::WIDTH) bytes
/// for every payload — the property that lets codecs compose without delimiters
/// or length prefixes.
///
/// Composition is offered *only* here, which makes a tuple codec that would
/// need framing unrepresentable. Of the three ways to frame a concatenation —
/// fixed-width, delimited, length-prefixed — fixed-width is the only one that
/// is zero-overhead, order-preserving under concatenation, and byte-compatible
/// with a frozen format. Delimited and length-prefixed framing would each be a
/// distinct trait; neither is built until a real caller needs it.
pub trait FixedCodec: Codec {
    /// The exact byte width of this codec's wire form.
    const WIDTH: usize;
}

/// Codec for an empty success value.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnitCodec;

impl Codec for UnitCodec {
    type Error = UnitCodecError;
    type Payload = ();

    const FORMAT_ID: &'static str = "unit";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<(), UnitCodecError> {
        if buf.is_empty() {
            Ok(())
        } else {
            Err(UnitCodecError { actual: buf.len() })
        }
    }

    fn serialize(&mut self, (): (), _buf: &mut Vec<u8>) -> Result<(), UnitCodecError> {
        Ok(())
    }
}

impl FixedCodec for UnitCodec {
    const WIDTH: usize = 0;
}

/// Codec for an error type that has no values.
#[derive(Clone, Copy, Debug, Default)]
pub struct InfallibleCodec;

impl Codec for InfallibleCodec {
    type Error = InfallibleCodecError;
    type Payload = Infallible;

    const FORMAT_ID: &'static str = "infallible";

    fn deserialize(&mut self, _buf: &mut [u8]) -> Result<Infallible, InfallibleCodecError> {
        Err(InfallibleCodecError)
    }

    fn serialize(
        &mut self,
        value: Infallible,
        _buf: &mut Vec<u8>,
    ) -> Result<(), InfallibleCodecError> {
        match value {}
    }
}

/// Plain big-endian `i64` payload codec (8 bytes).
///
/// This is the *payload* encoding — the raw two's-complement big-endian bytes.
/// It is deliberately distinct from the sign-flipped, order-preserving *key*
/// encoding in [`order_codec`](crate::state::order_codec): a key codec must
/// sort by memcmp, a payload codec need not, so the two never share bytes or a
/// token.
#[derive(Clone, Copy, Debug, Default)]
pub struct I64Codec;

impl Codec for I64Codec {
    type Error = I64CodecError;
    type Payload = i64;

    const FORMAT_ID: &'static str = "i64-be";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<i64, I64CodecError> {
        let bytes = <[u8; 8]>::try_from(&*buf).map_err(|_| I64CodecError { actual: buf.len() })?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn serialize(&mut self, payload: i64, buf: &mut Vec<u8>) -> Result<(), I64CodecError> {
        buf.extend_from_slice(&payload.to_be_bytes());
        Ok(())
    }
}

impl FixedCodec for I64Codec {
    const WIDTH: usize = 8;
}

/// A fixed-width pair codec: `(A, B)` writes `A`'s bytes followed by `B`'s, and
/// reads them back by splitting at [`A::WIDTH`](FixedCodec::WIDTH). Its
/// [`FORMAT_ID`](Codec::FORMAT_ID) is `"(a,b)"` from the components' ids,
/// derived at compile time (see `ConstId`) so a composed codec asserts a
/// durable identity distinct from either component's. Arity 2 only; a wider
/// tuple is a macro away when a caller needs one.
impl<A, B> Codec for (A, B)
where
    A: FixedCodec,
    B: FixedCodec,
{
    type Error = PairCodecError<A::Error, B::Error>;
    type Payload = (A::Payload, B::Payload);

    const FORMAT_ID: &'static str = {
        const fn build(a: &str, b: &str) -> ConstId {
            ConstId::new().raw("(").push(a).raw(",").push(b).raw(")")
        }
        // Reference-to-const promotion inside a const initializer promotes the
        // temporary to `'static` — stable Rust, no macros.
        build(A::FORMAT_ID, B::FORMAT_ID).as_static_str()
    };

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        // The wire-FORM check — a codec's job. `head <= tail` and other
        // *meaning* checks belong to the collection that owns the payload.
        let expected = A::WIDTH + B::WIDTH;
        if buf.len() != expected {
            return Err(PairCodecError::Length {
                expected,
                actual: buf.len(),
            });
        }
        let (first, second) = buf.split_at_mut(A::WIDTH);
        let a = self.0.deserialize(first).map_err(PairCodecError::First)?;
        let b = self.1.deserialize(second).map_err(PairCodecError::Second)?;
        Ok((a, b))
    }

    fn serialize(&mut self, payload: Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        self.0
            .serialize(payload.0, buf)
            .map_err(PairCodecError::First)?;
        self.1
            .serialize(payload.1, buf)
            .map_err(PairCodecError::Second)?;
        Ok(())
    }
}

impl<A, B> FixedCodec for (A, B)
where
    A: FixedCodec,
    B: FixedCodec,
{
    // Const arithmetic over generic consts is legal (not an array length).
    const WIDTH: usize = A::WIDTH + B::WIDTH;
}

/// Error from a fixed-width pair codec: a wrong combined length, or a failure
/// in one of the two component codecs.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PairCodecError<A: Error, B: Error> {
    /// The buffer was not the pair's combined fixed width.
    #[error("bad fixed-pair length: expected {expected}, got {actual}")]
    Length {
        /// The combined width the pair requires.
        expected: usize,
        /// The width the buffer actually had.
        actual: usize,
    },

    /// The first component codec failed.
    #[error(transparent)]
    First(A),

    /// The second component codec failed.
    #[error(transparent)]
    Second(B),
}

/// Error decoding an [`I64Codec`] cell whose width was not the fixed 8 bytes.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("bad i64 codec width: expected 8, got {actual}")]
pub struct I64CodecError {
    /// The width the cell actually had.
    pub actual: usize,
}

/// Error from a [`UnitCodec`] buffer that contains bytes.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("bad unit codec width: expected 0, got {actual}")]
pub struct UnitCodecError {
    /// The unexpected byte count.
    pub actual: usize,
}

/// Error from an attempt to decode an [`Infallible`] value.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("an infallible value has no wire representation")]
pub struct InfallibleCodecError;

#[cfg(test)]
mod tests;
