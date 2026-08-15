//! Codecs selected by one high-level handler.

use crate::codec::{Codec, ErasedStateCodec, JsonBinaryCodec, JsonBinaryMessageCodec, JsonCodec};
use crate::consumer::middleware::FallibleHandler;
use std::marker::PhantomData;

/// The message and response codecs for one high-level handler contract.
///
/// Erased state readers derive their codec from [`ErasedStateCodec`].
pub trait CodecSet<P, O>: Send + Sync + 'static {
    /// Encodes produced messages and decodes consumed messages.
    type Message: Codec<Payload = P>;
    /// Encodes successful handler responses.
    type Response: Codec<Payload = O>;
}

/// A high-level handler with compile-time message and response codecs.
pub trait ClientHandler: FallibleHandler {
    /// The codecs for messages and successful responses.
    type Codecs: CodecSet<Self::Payload, Self::Output>;
}

/// A codec set composed from message and response codecs.
pub struct Codecs<M, R>(PhantomData<(M, R)>);

impl<M, R> CodecSet<M::Payload, R::Payload> for Codecs<M, R>
where
    M: Codec,
    R: Codec,
{
    type Message = M;
    type Response = R;
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

/// The state codec selected by the handler payload type.
pub type StateCodec<H> = <<H as FallibleHandler>::Payload as ErasedStateCodec>::Codec;

/// The successful response codec selected by `H`.
pub type ResponseCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::Response;
