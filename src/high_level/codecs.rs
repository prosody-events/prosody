//! Wire codecs selected by one high-level handler.

use crate::codec::{Codec, JsonBinaryCodec, JsonBinaryMessageCodec, JsonCodec};
use crate::consumer::middleware::FallibleHandler;
use std::marker::PhantomData;

/// The message and successful response codecs for one handler contract.
pub trait CodecSet<P, O>: Send + Sync + 'static {
    /// Encodes produced messages and decodes consumed messages.
    type Message: Codec<Payload = P>;
    /// Encodes successful handler responses.
    type Response: Codec<Payload = O>;
}

/// A high-level handler with one compile-time wire contract.
pub trait ClientHandler: FallibleHandler {
    /// The only wire codec choice for this handler.
    type Codecs: CodecSet<Self::Payload, Self::Output>;
}

/// A codec set composed from a message codec and a response codec.
pub struct Codecs<M, O>(PhantomData<(M, O)>);

impl<M, O> CodecSet<M::Payload, O::Payload> for Codecs<M, O>
where
    M: Codec,
    O: Codec,
{
    type Message = M;
    type Response = O;
}

/// JSON message and response codecs.
pub type JsonCodecs = Codecs<JsonCodec, JsonCodec>;

/// JSON codecs for callers that supply encoded bytes.
///
/// Messages extract event metadata. Responses pass through unchanged.
pub type JsonBinaryCodecs = Codecs<JsonBinaryMessageCodec, JsonBinaryCodec>;

/// The message codec selected by `H`.
pub type MessageCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::Message;

/// The successful response codec selected by `H`.
pub type ResponseCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::Response;
