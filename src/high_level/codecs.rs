//! Codecs selected by one high-level handler.

use crate::codec::{Codec, JsonBinaryCodec, JsonBinaryMessageCodec, JsonCodec};
use crate::consumer::middleware::FallibleHandler;
use std::marker::PhantomData;

/// The codecs for one high-level handler contract.
pub trait CodecSet<P, O>: Send + Sync + 'static {
    /// Encodes produced messages and decodes consumed messages.
    type Message: Codec<Payload = P>;
    /// Decodes values for erased state readers.
    type State: Codec<Payload = P>;
    /// Encodes successful handler responses.
    type Response: Codec<Payload = O>;
}

/// A high-level handler with one compile-time wire contract.
pub trait ClientHandler: FallibleHandler {
    /// The only wire codec choice for this handler.
    type Codecs: CodecSet<Self::Payload, Self::Output>;
}

/// A codec set composed from message, state, and response codecs.
pub struct Codecs<M, S, R>(PhantomData<(M, S, R)>);

impl<M, S, R> CodecSet<M::Payload, R::Payload> for Codecs<M, S, R>
where
    M: Codec,
    S: Codec<Payload = M::Payload>,
    R: Codec,
{
    type Message = M;
    type Response = R;
    type State = S;
}

/// JSON message, state, and response codecs.
pub type JsonCodecs = Codecs<JsonCodec, JsonCodec, JsonCodec>;

/// JSON codecs for callers that supply encoded bytes.
///
/// Messages extract event metadata. State and response values pass through
/// unchanged.
pub type JsonBinaryCodecs = Codecs<JsonBinaryMessageCodec, JsonBinaryCodec, JsonBinaryCodec>;

/// The message codec selected by `H`.
pub type MessageCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::Message;

/// The state codec selected by `H`.
pub type StateCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::State;

/// The successful response codec selected by `H`.
pub type ResponseCodec<H> = <<H as ClientHandler>::Codecs as CodecSet<
    <H as FallibleHandler>::Payload,
    <H as FallibleHandler>::Output,
>>::Response;
