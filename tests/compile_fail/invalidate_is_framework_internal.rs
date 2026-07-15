//! `invalidate` is framework-internal teardown, not a public `EventContext`
//! method. A handler receives its context by value, so if `invalidate` were
//! callable it could invalidate its own context mid-dispatch and then return
//! `Ok`, and settle would commit the offset without draining the keyed-state
//! dirty overlay — a silent lost write. Sealing it off the trait makes the
//! misuse uncompilable.

use prosody::consumer::event_context::EventContext;

fn misuse<C: EventContext>(c: C) {
    c.invalidate();
}

fn main() {}
