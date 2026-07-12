//! A middleware-chain component cannot reach the `EventHandler` durability
//! boundary without the crate-internal settlement classification: the blanket
//! `FallibleEventHandler → EventHandler` impl also requires the private
//! `SettlementHandler` trait, which downstream crates can neither name nor
//! implement. A downstream handler that implements only the public traits
//! therefore never gets `EventHandler` for free — "a chain component without
//! `settlement()`" does not compile. (Chains built through the consumer
//! constructors are unaffected: `into_provider` mints the classifying leaf
//! adapter around the user's handler.)

use prosody::consumer::DemandType;
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::{FallibleEventHandler, FallibleHandler};
use prosody::timers::Trigger;
use std::convert::Infallible;

#[derive(Clone)]
struct MyHandler;

impl FallibleHandler for MyHandler {
    type Error = Infallible;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        _context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        Ok(())
    }

    async fn shutdown(self) {}
}

impl FallibleEventHandler for MyHandler {}

fn requires_event_handler<T: prosody::consumer::EventHandler>(_handler: T) {}

fn main() {
    requires_event_handler(MyHandler);
}
