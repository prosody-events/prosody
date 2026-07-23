//! The validated publication snapshot and its source identity.

use crate::Topic;
use crate::state_reader::PartitionCount;
use crate::state_reader::error::StateReaderError;
use smallvec::SmallVec;
use std::sync::Arc;

/// The most publication sources one collection may advertise. A collection
/// beyond this fails `Permanent` ([`StateReaderError::TooManySources`]); the
/// bound is liftable in a future release but never at runtime.
pub(crate) const MAX_PUBLICATION_SOURCES: usize = 16;

/// A source's **stable** identity: the publishing consumer group and the topic
/// whose messages wrote the state.
///
/// Deliberately **not** an ordinal index into the snapshot: a refresh can
/// reorder or remove sources, and an index-keyed cache entry or pin would then
/// alias a *different* source's cells — a committed-only violation, not mere
/// staleness. Every cache key and pin carries this instead. `Ord` is
/// lexicographic `(group_id, topic)`, giving the snapshot its deterministic
/// source-preference order.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceId {
    /// The publishing consumer group.
    pub(crate) group_id: Arc<str>,
    /// The topic whose messages wrote the state.
    pub(crate) topic: Topic,
}

/// One admitted source: its stable [`SourceId`] and the topic's Kafka partition
/// count (for the reader's key→partition step). The count rides on the
/// publication row, so the read path needs no live partition fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Source {
    /// Stable identity.
    pub(crate) id: SourceId,
    /// The topic's partition count.
    pub(crate) partition_count: PartitionCount,
}

/// A validated publication snapshot: a **non-empty**, `SourceId`-ordered,
/// count-bounded list of sources whose per-group frozen identities matched the
/// reader's descriptor at validation time.
///
/// The restricted constructor ([`Self::new`], `pub(super)`) is the *only* way
/// to mint one, so an unvalidated, empty, or oversized snapshot is
/// unrepresentable in a [`ReadSession`](super::session::ReadSession): the
/// reader stores the *absence* of a snapshot (an `Option`) rather than an empty
/// one.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedPublications {
    sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
}

impl ValidatedPublications {
    /// Mints a validated snapshot from admitted sources, sorted by
    /// [`SourceId`].
    ///
    /// # Errors
    ///
    /// Returns [`StateReaderError::TooManySources`] beyond
    /// [`MAX_PUBLICATION_SOURCES`]. An **empty** `sources` also errors here
    /// ([`StateReaderError::UnknownPublication`] is the caller's concern) — the
    /// non-empty invariant is structural, so the caller stores `None` for an
    /// empty admission rather than an empty snapshot.
    pub(super) fn new(
        mut sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
        subsystem: &str,
        name: &str,
    ) -> Result<Self, StateReaderError> {
        if sources.is_empty() {
            return Err(StateReaderError::UnknownPublication {
                subsystem: subsystem.to_owned(),
                name: name.to_owned(),
            });
        }
        if sources.len() > MAX_PUBLICATION_SOURCES {
            return Err(StateReaderError::TooManySources {
                found: sources.len(),
                max: MAX_PUBLICATION_SOURCES,
            });
        }
        sources.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Self { sources })
    }

    /// The admitted sources in deterministic `SourceId` order (non-empty).
    pub(crate) fn sources(&self) -> &[Source] {
        &self.sources
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_reader::PartitionCount;
    use internment::Intern;

    /// Advertising more than [`MAX_PUBLICATION_SOURCES`] fails `TooManySources`
    /// (Permanent) at mint — never a silent truncation.
    ///
    /// Falsify: drop the length check in [`ValidatedPublications::new`] — the
    /// oversized snapshot builds and the match arm is never reached.
    #[test]
    fn oversized_snapshot_is_too_many_sources() -> color_eyre::Result<()> {
        let sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]> = (0..=MAX_PUBLICATION_SOURCES)
            .map(|i| Source {
                id: SourceId {
                    group_id: Arc::from(format!("group-{i}")),
                    topic: Intern::<str>::from("topic"),
                },
                partition_count: PartitionCount::MIN,
            })
            .collect();
        assert_eq!(sources.len(), MAX_PUBLICATION_SOURCES + 1);
        match ValidatedPublications::new(sources, "orders", "coll") {
            Err(StateReaderError::TooManySources { found, max }) => {
                assert_eq!(found, MAX_PUBLICATION_SOURCES + 1);
                assert_eq!(max, MAX_PUBLICATION_SOURCES);
            }
            other => color_eyre::eyre::bail!("expected TooManySources, got {other:?}"),
        }
        Ok(())
    }
}
