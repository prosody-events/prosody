//! The validated publication snapshot and its source identity.

use crate::Topic;
use crate::state_reader::PartitionCount;
use crate::state_reader::error::StateReaderError;
use smallvec::SmallVec;
use std::sync::Arc;

/// The maximum number of publication sources one collection may advertise.
/// A collection that advertises more fails `Permanent` with
/// [`StateReaderError::TooManySources`]. A future release may raise the bound,
/// but it never changes at runtime.
pub(crate) const MAX_PUBLICATION_SOURCES: usize = 16;

/// A source's **stable** identity: the publishing consumer group and the topic
/// whose messages wrote the state.
///
/// This identity is not an ordinal index into the snapshot. A refresh can
/// reorder or remove sources. A cache key or pin based on the index would then
/// point at a *different* source's cells after a refresh. The reader would
/// serve the wrong source's committed data, which is a correctness violation
/// rather than mere staleness. Every cache key and pin carries this identity
/// instead. `Ord` is lexicographic over `(group_id, topic)`, which gives the
/// snapshot a deterministic order for preferring one source over another.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceId {
    /// The publishing consumer group.
    pub(crate) group_id: Arc<str>,
    /// The topic whose messages wrote the state.
    pub(crate) topic: Topic,
}

/// One admitted source: its stable [`SourceId`] and the topic's Kafka partition
/// count. The reader uses the count to map a key to its partition. The count is
/// stored on the publication row, so the read path needs no live partition
/// fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Source {
    /// Stable identity.
    pub(crate) id: SourceId,
    /// The topic's partition count.
    pub(crate) partition_count: PartitionCount,
}

/// A validated publication snapshot. It is a **non-empty** list of at most
/// [`MAX_PUBLICATION_SOURCES`] sources, ordered by [`SourceId`]. Every source's
/// frozen identity matched the reader's descriptor at validation time.
///
/// [`Self::new`] is `pub(super)` and the only way to build one. An unvalidated,
/// empty, or oversized snapshot is therefore unrepresentable in a
/// [`ReadSession`](super::session::ReadSession). The reader stores `None` for
/// the absence of a snapshot rather than an empty one.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedPublications {
    sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
}

impl ValidatedPublications {
    /// Builds a validated snapshot from admitted sources, sorted by
    /// [`SourceId`].
    ///
    /// # Errors
    ///
    /// Returns [`StateReaderError::TooManySources`] when `sources` holds more
    /// than [`MAX_PUBLICATION_SOURCES`] entries. Returns
    /// [`StateReaderError::UnknownPublication`] when `sources` is empty. The
    /// non-empty invariant is structural, so the caller stores `None` rather
    /// than building an empty snapshot.
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

    /// Advertising more than [`MAX_PUBLICATION_SOURCES`] fails with
    /// `TooManySources` (a `Permanent` error) at construction, never a silent
    /// truncation.
    ///
    /// Falsify: drop the length check in [`ValidatedPublications::new`]. The
    /// oversized snapshot then builds and the match arm is never reached.
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
