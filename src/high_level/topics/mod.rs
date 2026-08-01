//! Reconciling the topics a consumer subscribed to against the ones the Kafka
//! cluster actually has.

use crate::producer::{ProducerError, ProsodyProducer};
use crate::{Codec, Topic};
use rdkafka::metadata::MetadataTopic;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// How long to wait for the cluster's metadata.
const METADATA_TIMEOUT: Duration = Duration::from_mins(1);

/// Which of `topics` the cluster does not have.
///
/// # Errors
///
/// Returns [`ProducerError`] if the metadata fetch fails.
pub(super) fn missing_topics<C: Codec>(
    producer: &ProsodyProducer<C>,
    topics: Vec<Topic>,
) -> Result<Vec<Topic>, ProducerError<C::Error>>
where
    C::Payload: crate::EventIdentity,
{
    let metadata = producer
        .kafka_client()
        .fetch_metadata(None, METADATA_TIMEOUT)?;
    Ok(missing_from(
        topics,
        metadata.topics().iter().map(MetadataTopic::name),
    ))
}

/// Which of `requested` are absent from `existing`.
///
/// Duplicates collapse, and a name starting with `^` is dropped: that is a
/// pattern subscription the broker expands, not a topic to look up. The
/// returned order is unspecified — callers report the set, never its sequence.
fn missing_from<'a>(
    mut requested: Vec<Topic>,
    existing: impl IntoIterator<Item = &'a str>,
) -> Vec<Topic> {
    requested.sort_unstable();
    requested.dedup();
    requested.retain(|topic| !topic.starts_with('^'));

    for name in existing {
        let Some(position) = requested.iter().position(|&topic| topic.as_ref() == name) else {
            continue;
        };

        requested.swap_remove(position);
        if requested.is_empty() {
            return requested;
        }
    }

    requested
}
