//! `EventContext::state` accepts only a `Registered<DESC>` capability handle,
//! never a raw descriptor — so binding a collection you never registered is a
//! type error, not a runtime one.

use prosody::consumer::event_context::EventContext;
use prosody::JsonCodec;
use prosody::state::descriptor::value_state;

fn bind_unregistered<C: EventContext>(ctx: &C) {
    // Passing a raw `ValueDescriptor` where a `Registered<_>` is required.
    let _ = ctx.state(value_state::<JsonCodec>("cart"));
}

fn main() {}
