//! Composing a `Result` codec from an output codec and an error codec.

use super::Codec;
use super::const_id::ConstId;
use std::error::Error as StdError;
use thiserror::Error;

/// Discriminant for the success arm.
const OK_TAG: u8 = 0;
/// Discriminant for the failure arm.
const ERR_TAG: u8 = 1;

/// Composes an output codec and an error codec into one [`Codec`] over
/// `Result<Output, Error>`.
///
/// A response carries a whole result, so *something* has to say how the two
/// arms are told apart. This is the one place the framework picks that
/// representation: a one-byte discriminant — `0x00` for `Ok`, `0x01` for `Err`
/// — followed by that arm's codec bytes. An application that wants a different
/// framing supplies its own codec over the whole `Result` instead.
///
/// The discriminant leads rather than trails. A trailing byte would leave the
/// buffer empty for the arm's codec and so preserve [`BinaryCodec`]'s
/// documented move-instead-of-copy, but that move would *replace* a reused
/// scratch buffer's allocation with the payload's own — losing the reuse the
/// send path depends on. A leading tag reads the way every other tagged format
/// does and costs one bounded copy on a path that ends in a network round trip.
///
/// [`BinaryCodec`]: super::BinaryCodec
#[derive(Debug, Default)]
pub struct ResultCodec<O, E> {
    output: O,
    error: E,
}

impl<O: Codec, E: Codec> Codec for ResultCodec<O, E> {
    type Error = ResultCodecError<O::Error, E::Error>;
    type Payload = Result<O::Payload, E::Payload>;

    const FORMAT_ID: &'static str = {
        const fn build(output: &str, error: &str) -> ConstId {
            ConstId::new()
                .raw("result(")
                .push(output)
                .raw(",")
                .push(error)
                .raw(")")
        }
        // Reference-to-const promotion inside a const initializer promotes the
        // temporary to `'static` — stable Rust, no macros.
        build(O::FORMAT_ID, E::FORMAT_ID).as_static_str()
    };

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Self::Payload, Self::Error> {
        let (tag, rest) = buf.split_first_mut().ok_or(ResultCodecError::Empty)?;
        match *tag {
            OK_TAG => self
                .output
                .deserialize(rest)
                .map(Ok)
                .map_err(ResultCodecError::Output),
            ERR_TAG => self
                .error
                .deserialize(rest)
                .map(Err)
                .map_err(ResultCodecError::Error),
            other => Err(ResultCodecError::UnknownDiscriminant(other)),
        }
    }

    fn serialize(&mut self, payload: Self::Payload, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        match payload {
            Ok(value) => {
                buf.push(OK_TAG);
                self.output
                    .serialize(value, buf)
                    .map_err(ResultCodecError::Output)
            }
            Err(error) => {
                buf.push(ERR_TAG);
                self.error
                    .serialize(error, buf)
                    .map_err(ResultCodecError::Error)
            }
        }
    }
}

/// Why a [`ResultCodec`] could not read or write a result.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ResultCodecError<O: StdError, E: StdError> {
    /// The buffer carries no discriminant byte at all.
    #[error("result codec buffer is empty")]
    Empty,

    /// The discriminant names neither arm.
    #[error("unknown result discriminant: {0}")]
    UnknownDiscriminant(u8),

    /// The output codec failed on the success arm.
    #[error(transparent)]
    Output(O),

    /// The error codec failed on the failure arm.
    #[error(transparent)]
    Error(E),
}

#[cfg(test)]
mod tests;
