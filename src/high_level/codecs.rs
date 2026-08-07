//! Wire codecs selected by one high-level handler.

use crate::codec::{Codec, JsonCodec, ResultCodec};
use crate::consumer::middleware::FallibleHandler;
use std::marker::PhantomData;

/// The message, output, and error codecs for one handler contract.
pub trait CodecSet<P, O, E>: Send + Sync + 'static {
    /// Encodes produced messages and decodes consumed messages.
    type Message: Codec<Payload = P>;
    /// Encodes successful handler responses.
    type Output: Codec<Payload = O>;
    /// Encodes failed handler responses.
    type Error: Codec<Payload = E>;
}

/// A high-level handler with one compile-time wire contract.
pub trait ClientHandler: FallibleHandler {
    /// The only wire codec choice for this handler.
    type Codecs: CodecSet<Self::Payload, Self::Output, Self::Error>;
}

/// A codec set composed from three codecs.
pub struct Codecs<M, O, E>(PhantomData<(M, O, E)>);

impl<M, O, E> CodecSet<M::Payload, O::Payload, E::Payload> for Codecs<M, O, E>
where
    M: Codec,
    O: Codec,
    E: Codec,
{
    type Error = E;
    type Message = M;
    type Output = O;
}

/// JSON message and output codecs with a caller-selected error codec.
pub type JsonCodecs<E> = Codecs<JsonCodec, JsonCodec, E>;

/// The message codec selected by `H`.
pub type MessageCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
    <H as FallibleHandler>::Error,
>>::Message;

/// The response codec derived from `H`'s output and error codecs.
pub type ResponseCodec<H> = ResultCodec<
    <<H as ClientHandler>::Codecs as CodecSet<
        <H as FallibleHandler>::Payload,
        <H as FallibleHandler>::Output,
        <H as FallibleHandler>::Error,
    >>::Output,
    <<H as ClientHandler>::Codecs as CodecSet<
        <H as FallibleHandler>::Payload,
        <H as FallibleHandler>::Output,
        <H as FallibleHandler>::Error,
    >>::Error,
>;
