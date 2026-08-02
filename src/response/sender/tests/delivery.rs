//! What happens to a queued response between its hook and the wire.

use super::{CAP_BYTES, Harness, PAYLOAD, UNPUBLISHED_NODE, attempts, config, node, paused, port};
use crate::response::frame::FrameCap;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::tests::{CountingCodec, serialized_on_this_thread};
use crate::router::SendFailure;
use crate::router::loopback::Script;
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::time::Duration;
use tonic::Code;

/// The destination these suites address.
const TARGET: u8 = 1;

/// Cells and slots the delivery fleets are built with.
const CELLS: usize = 4;
const SLOTS: usize = 4;

/// The gRPC status codes, in wire order. Frozen here so a reordering or an
/// omission fails the round trip below rather than silently narrowing the
/// table.
const CODES: [Code; 17] = [
    Code::Ok,
    Code::Cancelled,
    Code::Unknown,
    Code::InvalidArgument,
    Code::DeadlineExceeded,
    Code::NotFound,
    Code::AlreadyExists,
    Code::PermissionDenied,
    Code::ResourceExhausted,
    Code::FailedPrecondition,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Internal,
    Code::Unavailable,
    Code::DataLoss,
    Code::Unauthenticated,
];

/// The statuses whose outcome a retry could still resolve, named here
/// independently of the rule under test.
const AMBIGUOUS: [Code; 2] = [Code::Unavailable, Code::DeadlineExceeded];

/// A response queued before the layer that queued it goes away is still
/// delivered, and what reaches the wire is the response that was queued.
///
/// The event committed before its hook ran, so the response stays valid after
/// ownership of the partition moves.
#[test]
fn a_queued_response_survives_the_sender_that_queued_it() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::new(config(CELLS, SLOTS))?;
        harness.send(TARGET)?;

        let mut drained = harness.drain().await?;
        assert_eq!(
            drained.sent, 1,
            "the queued response must be delivered after its sender is dropped"
        );
        let Some(mut delivery) = drained.deliveries.pop() else {
            bail!("the queued response reached no destination at all");
        };
        assert_eq!(
            delivery.port,
            port(TARGET),
            "the response must reach the node it was queued for"
        );

        let mut frame = decode_frame(&mut delivery.bytes, FrameCap::new(CAP_BYTES)?)?;
        assert_eq!(
            frame.header.target,
            node(TARGET),
            "the frame must name the node it was queued for"
        );
        assert_eq!(
            frame.decode_with(&mut CountingCodec::default())?,
            PAYLOAD,
            "the frame must carry the response that was queued"
        );
        Ok(())
    })
}

/// Only a failure whose outcome is unknown is tried again. Every status the
/// destination chose is final, however transient it sounds.
#[test]
fn only_ambiguous_statuses_are_retried() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        for (index, code) in CODES.into_iter().enumerate() {
            if Code::from_i32(index as i32) != code {
                bail!(
                    "code {index} is {:?}, not {code:?}",
                    Code::from_i32(index as i32)
                );
            }
            let expected = if AMBIGUOUS.contains(&code) {
                config(CELLS, SLOTS).max_send_attempts as usize
            } else {
                1
            };
            let made = attempts_against(SendFailure::Status(code)).await?;
            if made != expected {
                bail!("{code:?} was attempted {made} times, not {expected}");
            }
        }

        let attempts = config(CELLS, SLOTS).max_send_attempts as usize;
        let made = attempts_against(SendFailure::Unreachable).await?;
        if made != attempts {
            bail!("an unreachable destination was attempted {made} times, not {attempts}");
        }
        Ok(())
    })
}

/// A response that spends its whole deadline waiting is dropped before it is
/// encoded, and its slot goes back.
///
/// The destination is paced at one send per second and the deadline is half of
/// that, so the second response's turn is provably past its expiry. Counting
/// what the worker's codec serialized is what makes "before it is encoded" a
/// claim the test risks: reaching no transport alone would still hold if the
/// pacing wait moved after the encode.
#[test]
fn an_expired_response_is_dropped_before_it_is_encoded() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut settings = config(CELLS, SLOTS);
        settings.sends_per_second = 1;
        settings.send_deadline = Duration::from_millis(500);
        let harness = Harness::new(settings)?;
        let fleet = harness.fleet();
        let serialized = serialized_on_this_thread();
        harness.send(TARGET)?;
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        assert_eq!(
            serialized_on_this_thread() - serialized,
            1,
            "the expired response must never be encoded"
        );
        assert_eq!(
            attempts(&drained.deliveries, TARGET),
            1,
            "the expired response must never reach the transport"
        );
        assert_eq!(drained.dropped, 1, "the expired response must be counted");
        assert_eq!(
            fleet.available(node(TARGET)),
            Some(SLOTS),
            "every slot must go back once the workers finish"
        );
        Ok(())
    })
}

/// A node the directory publishes nothing for is not dialed at all: no address
/// is invented, nothing is sent, and the slot goes back.
#[test]
fn an_unpublished_node_is_never_dialed() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::new(config(CELLS, SLOTS))?;
        let fleet = harness.fleet();
        harness.send(UNPUBLISHED_NODE)?;

        let drained = harness.drain().await?;
        assert!(
            drained.deliveries.is_empty(),
            "an unpublished node must reach no address at all"
        );
        assert_eq!(drained.dropped, 1, "the undeliverable response is counted");
        assert_eq!(
            fleet.available(node(UNPUBLISHED_NODE)),
            Some(SLOTS),
            "the slot must go back when no address is found"
        );
        Ok(())
    })
}

/// How many attempts one response makes against a destination that always
/// answers `failure`.
async fn attempts_against(failure: SendFailure) -> Result<usize> {
    let harness = Harness::new(config(CELLS, SLOTS))?;
    harness.script(
        TARGET,
        Script::Fail {
            failure,
            times: usize::MAX,
        },
    );
    harness.send(TARGET)?;
    let drained = harness.drain().await?;
    Ok(drained
        .deliveries
        .iter()
        .filter(|delivery| delivery.port == port(TARGET))
        .count())
}
