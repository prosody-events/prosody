//! Which process a frame is accepted by, forwarded by, or refused by.

use super::{BUDGET, Process, THIS, frame};
use crate::response::frame::decode::decode_frame;
use crate::router::loopback::{direct_uri, paused, peer};
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tonic::Code;

/// How many peer ids a case draws from. Small, so `target == this` and a frame
/// that already names a relay both occur often.
const POOL: u8 = 4;

/// One frame's routing fields, drawn from a pool small enough that every arm of
/// the decision is reached.
#[derive(Clone, Copy, Debug)]
struct Routed {
    target: u8,
    relay: Option<u8>,
}

impl Arbitrary for Routed {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            target: u8::arbitrary(g) % POOL,
            relay: Option::<u8>::arbitrary(g).map(|relay| relay % POOL),
        }
    }
}

/// A process accepts only frames that name it, sends on only frames that name
/// another process and no relay, and refuses every frame that already named a
/// relay.
///
/// The three claims are read from three independent places — what the registry
/// holds, what the transport recorded, and what the call answered — against the
/// table below written out as data. Asking the decision function again would
/// prove only that it agrees with itself.
#[quickcheck]
fn prop_a_frame_is_accepted_only_by_the_process_it_names(routed: Routed) -> TestResult {
    let Ok(runtime) = paused() else {
        return TestResult::error("a paused runtime must be buildable");
    };
    match runtime.block_on(play(&routed)) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{routed:?}: {error:#}")),
    }
}

/// Drives one case and holds it to the table.
async fn play(routed: &Routed) -> Result<()> {
    let mut process = Process::new()?;
    let mut request = process.expects()?;
    let target = peer(routed.target);
    let relay = routed.relay.map(peer);

    // The table. `this` is the peer the process answers for.
    let mine = routed.target == THIS;
    let accepted = mine;
    let forwarded = !mine && relay.is_none();
    let refused = !mine && relay.is_some();

    let answered = process
        .deliver(frame(target, request.id(), relay)?, BUDGET)
        .await?;

    if request.received() != accepted {
        bail!(
            "the registry {} the response, and a frame is stored exactly when it names this \
             process",
            if accepted { "did not store" } else { "stored" }
        );
    }

    let recorded = process.recorded();
    if recorded.is_some() != forwarded {
        bail!(
            "the transport recorded {} attempt, and a frame is sent on exactly when it names \
             another process and no relay",
            if forwarded { "no" } else { "an" }
        );
    }
    if let Some(mut sent) = recorded {
        let expected = direct_uri(routed.target)?;
        if sent.uri != expected {
            bail!(
                "the frame went to {}, not to the URI peer {} published",
                sent.uri,
                routed.target
            );
        }
        let sent_frame = decode_frame(&mut sent.bytes)?;
        if sent_frame.header.relay != Some(process.peer) {
            bail!(
                "the sent frame names relay {:?}, not the process that sent it on",
                sent_frame.header.relay
            );
        }
        if sent_frame.header.target != target {
            bail!("the sent frame no longer names the process it was addressed to");
        }
    }

    if (answered == Code::FailedPrecondition) != refused {
        bail!(
            "the call answered {answered:?}, and FAILED_PRECONDITION is the answer exactly when \
             the frame already named a relay"
        );
    }
    if accepted && answered != Code::Ok {
        bail!("a stored response must answer OK, not {answered:?}");
    }
    if forwarded && answered != Code::Ok {
        bail!("a forwarded frame that reached its target must answer OK, not {answered:?}");
    }
    Ok(())
}
