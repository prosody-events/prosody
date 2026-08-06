//! What a flood of frames for other processes costs this one's own responses.

use super::{ALPHA, BUDGET, CAP_BYTES, PAYLOAD, THIS, frame};
use crate::error::ErrorCategory;
use crate::response::frame::encode::Forwarded;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameCap, FrameHeader};
use crate::response::sender::TypedSender;
use crate::response::{RequestId, ResponseStatus};
use crate::router::RelayHop;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Delivery, Script, TestRouter, node, port};
use crate::router::relay::{Relay, RelayFailure};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use opentelemetry::Context;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

/// The process's own destination. Its response is held in flight for the whole
/// flood.
const OWN: u8 = 1;

/// The one node a forward has room for once the process's own response holds a
/// cell.
const FIRST: u8 = 2;

/// Nodes the flood names once both cells are busy.
const FLOODED: [u8; 3] = [3, 4, 5];

/// Cells and slots the shared fleet holds. Two cells with one slot each, so one
/// own response and one forward fill it exactly.
const CELLS: usize = 2;
const SLOTS: usize = 1;

/// How long a queued response may take. Far longer than this case runs, so the
/// held response is ended by its release and never by its own deadline.
const PATIENCE: Duration = Duration::from_mins(1);

/// A forward draws on the same bounded fleet as a process's own responses, so a
/// flood of frames for other processes takes no cell from a response already in
/// flight and grows the table by nothing.
///
/// Every forward here is held rather than failed. A failing forward would give
/// its slot back before the next reservation, so the table would never fill and
/// the flood would risk nothing.
#[test]
fn a_flood_of_forwards_cannot_take_a_busy_cell() -> Result<()> {
    let runtime = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?;
    runtime.block_on(async {
        let barrier = Arc::new(Semaphore::new(0));
        let (router, mut deliveries) = TestRouter::new(fleet_config())?;
        let fleet = Arc::clone(router.fleet());
        for index in [OWN, FIRST].into_iter().chain(FLOODED) {
            router.script(index, Script::Hold(Arc::clone(&barrier)));
        }

        // This process's own response takes the first cell and holds it.
        let (sender, workers) =
            TypedSender::<CountingCodec>::new_without_local(&router, FrameCap::new(CAP_BYTES)?)?;
        sender
            .send(header(OWN)?, Context::current(), PAYLOAD.to_vec())
            .map_err(|_| eyre!("the fleet refused this process's own response"))?;
        expect_port(&mut deliveries, port(OWN)).await?;
        let held = fleet
            .live(node(OWN))
            .ok_or_else(|| eyre!("the process's own destination must be live"))?;

        // One forward takes the other cell and holds it too.
        let relay = Arc::new(Relay::new(router.clone()));
        let first = tokio::spawn({
            let relay = Arc::clone(&relay);
            let sent = forwardable()?;
            async move {
                relay
                    .forward(node(FIRST), Instant::now() + BUDGET, &sent)
                    .await
            }
        });
        expect_port(&mut deliveries, port(FIRST)).await?;

        // Every cell now holds a destination with a send in flight.
        let refused = fleet.refused();
        let sent = forwardable()?;
        for index in FLOODED {
            let outcome = relay
                .forward(node(index), Instant::now() + BUDGET, &sent)
                .await;
            assert_eq!(
                outcome,
                Err(RelayFailure::NoCapacity),
                "a forward for node {index} must be refused while every cell is busy"
            );
        }
        assert_eq!(
            fleet.refused(),
            refused + FLOODED.len() as u64,
            "every refused forward must be counted by the fleet"
        );
        assert_eq!(
            fleet.live(node(OWN)),
            Some(held),
            "the process's own destination must keep the cell it was admitted into"
        );
        assert!(
            fleet.live_count() <= fleet.capacity(),
            "{} destinations are live, more than the {} cells the table has",
            fleet.live_count(),
            fleet.capacity()
        );

        barrier.add_permits(2);
        if first.await?.is_err() {
            bail!("the held forward must deliver once it is released");
        }
        drop(sender);
        workers.join().await;
        Ok(())
    })
}

/// The fleet this case shares between its own responses and its forwards.
fn fleet_config() -> FleetConfiguration {
    FleetConfiguration {
        max_destinations: CELLS,
        slots_each: SLOTS,
        send_deadline: PATIENCE,
        ..FleetConfiguration::default()
    }
}

/// The header this process's own response carries.
fn header(index: u8) -> Result<FrameHeader> {
    Ok(FrameHeader {
        target: node(index),
        request: RequestId::from_bytes([9; 16]),
        subsystem: SubsystemName::try_new(ALPHA)?,
        status: ResponseStatus::Error(ErrorCategory::Permanent),
        relay: None,
    })
}

/// One frame ready to be sent on. Its bytes are not the subject here; the slot
/// it needs is.
fn forwardable() -> Result<Forwarded> {
    let cap = FrameCap::new(CAP_BYTES)?;
    let request = RequestId::from_bytes([4; 16]);
    Forwarded::new(frame(node(FIRST), request, None)?, node(THIS), cap)
        .ok_or_else(|| eyre!("a short frame must fit the cap once it is forwarded"))
}

/// Waits for the next recorded attempt and holds it to `expected`.
async fn expect_port(deliveries: &mut UnboundedReceiver<Delivery>, expected: u16) -> Result<()> {
    match deliveries.recv().await {
        Some(delivery) if delivery.port == expected => Ok(()),
        Some(delivery) => bail!("an attempt reached port {}, not {expected}", delivery.port),
        None => bail!("the transport stopped recording before an attempt arrived"),
    }
}
