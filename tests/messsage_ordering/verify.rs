//! The delivery check for one key: at least once, in produce order.

use color_eyre::eyre::{Result, ensure, eyre};
use quickcheck::{QuickCheck, TestResult};
use std::collections::BTreeSet;

/// A step at or above this value rewinds the walk in [`redeliver`].
const REWIND: u8 = 224;

/// Checks that `deliveries` delivers every message of `produced` at least
/// once, in produce order.
///
/// The direct mode has no deduplication. A revoke discards the progress that
/// the old owner did not store, so the next owner delivers part of the key
/// again. Number each produced message by its position. A delivery can go
/// back to any earlier position. It can go forward by one position only.
///
/// `produced` must be in produce order, and that order must be ascending.
pub(super) fn verify_key(key: u64, produced: &[u64], deliveries: &[u64]) -> Result<()> {
    // The position after the previous delivery.
    let mut next = 0;
    // The number of messages delivered at least once.
    let mut reached = 0;

    for (index, message) in deliveries.iter().enumerate() {
        let position = produced.binary_search(message).map_err(|_| {
            eyre!("key {key}: delivery {index} is message {message}, which was not produced")
        })?;
        ensure!(
            position <= next,
            "key {key}: delivery {index} skips from position {next} to position {position}; \
             deliveries: {deliveries:?}"
        );
        next = position + 1;
        reached = reached.max(next);
    }

    ensure!(
        reached == produced.len(),
        "key {key}: {reached} of {} messages arrived",
        produced.len()
    );
    Ok(())
}

/// Delivers `produced` as a consumer group can after revokes. A step below
/// [`REWIND`] delivers the next message. A larger step goes back by its excess
/// over [`REWIND`]. After the last step, the remaining messages arrive in
/// order.
fn redeliver(produced: &[u64], steps: Vec<u8>) -> Vec<u64> {
    let mut deliveries = Vec::with_capacity(produced.len() + steps.len());
    let mut position = 0_usize;

    for step in steps {
        match step.checked_sub(REWIND) {
            Some(back) => position = position.saturating_sub(usize::from(back)),
            None => {
                if let Some(&message) = produced.get(position) {
                    deliveries.push(message);
                    position += 1;
                }
            }
        }
    }

    deliveries.extend(produced.iter().skip(position));
    deliveries
}

/// [`verify_key`] accepts every redelivery that [`redeliver`] makes. It
/// rejects a reorder of two adjacent messages and the loss of the last one.
#[test]
fn verify_key_accepts_redelivery_and_rejects_reorder_and_loss() {
    fn property(produced: BTreeSet<u64>, steps: Vec<u8>) -> TestResult {
        if produced.is_empty() {
            return TestResult::discard();
        }
        let produced: Vec<u64> = produced.into_iter().collect();
        let deliveries = redeliver(&produced, steps);
        if let Err(error) = verify_key(0, &produced, &deliveries) {
            return TestResult::error(format!("rejected a redelivery: {error:#}"));
        }

        // The index of the first delivery of each message.
        let Some(firsts) = produced
            .iter()
            .map(|message| deliveries.iter().position(|delivered| delivered == message))
            .collect::<Option<Vec<usize>>>()
        else {
            return TestResult::error("the redelivery lost a message");
        };

        // Put each message before its predecessor's first delivery.
        for pair in firsts.windows(2) {
            if let &[earlier, later] = pair {
                let mut reordered = deliveries.clone();
                reordered.swap(earlier, later);
                if verify_key(0, &produced, &reordered).is_ok() {
                    return TestResult::error(format!("accepted a reorder: {reordered:?}"));
                }
            }
        }

        // Stop before the first delivery of the last message.
        let lost = firsts.last().and_then(|&last| deliveries.get(..last));
        TestResult::from_bool(lost.is_some_and(|lost| verify_key(0, &produced, lost).is_err()))
    }

    QuickCheck::new().quickcheck(property as fn(BTreeSet<u64>, Vec<u8>) -> TestResult);
}
