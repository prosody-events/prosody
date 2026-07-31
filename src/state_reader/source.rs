//! The validated publication snapshot and its source identity.

use crate::Topic;
use crate::state_reader::PartitionCount;
use smallvec::SmallVec;
use std::sync::Arc;
use thiserror::Error;

/// The maximum number of publication sources one collection may advertise. A
/// collection that advertises more is rejected with
/// [`NoSnapshot::TooManySources`], which the reader surfaces as a `Permanent`
/// error. A future release may raise the bound, but it never changes at
/// runtime.
pub(crate) const MAX_PUBLICATION_SOURCES: usize = 16;
/// Rows requested from the publication store. The extra row proves overflow
/// without reading the rest of an oversized routing partition.
pub(crate) const PUBLICATION_READ_LIMIT: usize = MAX_PUBLICATION_SOURCES + 1;

/// A source's **stable** identity: the publishing consumer group and the topic
/// whose messages wrote the state.
///
/// This identity is not an ordinal index into the snapshot. It includes every
/// input that determines the backing segment. A refresh can reorder sources or
/// observe a changed partition count without letting cached cells alias the
/// newly routed source. `Ord` gives the snapshot a deterministic source order.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceId {
    /// The publishing consumer group.
    pub(crate) group_id: Arc<str>,
    /// The topic whose messages wrote the state.
    pub(crate) topic: Topic,
    /// The topic's partition count.
    pub(crate) partition_count: PartitionCount,
}

/// One admitted publication source.
pub(crate) type Source = SourceId;

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
    /// Returns [`NoSnapshot`] when `sources` is empty or oversized. Both keep
    /// the snapshot's invariants structural: the caller stores `None` rather
    /// than building an empty or oversized snapshot.
    pub(super) fn new(
        mut sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
    ) -> Result<Self, NoSnapshot> {
        if sources.is_empty() {
            return Err(NoSnapshot::NoSource);
        }
        if sources.len() > MAX_PUBLICATION_SOURCES {
            return Err(NoSnapshot::TooManySources {
                found: sources.len(),
            });
        }
        sources.sort();
        Ok(Self { sources })
    }

    /// The admitted sources in deterministic `SourceId` order (non-empty).
    pub(crate) fn sources(&self) -> &[Source] {
        &self.sources
    }
}

/// Why a set of admitted sources yields no validated snapshot. The two arms
/// classify differently for the reader: no source at all is a Transient absence
/// that a later read re-admits, while an oversized routing table is a Permanent
/// misconfiguration.
#[derive(Debug, Error)]
pub(super) enum NoSnapshot {
    /// No source was admitted, so there is nothing to validate.
    #[error("no admitted publication source")]
    NoSource,
    /// More sources are advertised than [`MAX_PUBLICATION_SOURCES`] admits.
    #[error("too many publication sources (at least {found}; maximum {MAX_PUBLICATION_SOURCES})")]
    TooManySources {
        /// The number of sources advertised.
        found: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_reader::PartitionCount;
    use internment::Intern;

    /// Advertising more than [`MAX_PUBLICATION_SOURCES`] is rejected at
    /// construction, never silently truncated. `StateReader` records the
    /// rejection as a sticky Permanent fault; see
    /// `oversized_routing_table_is_sticky`.
    ///
    /// Falsify: drop the length check in [`ValidatedPublications::new`]. The
    /// oversized snapshot then builds and the match arm is never reached.
    #[test]
    fn oversized_snapshot_is_too_many_sources() -> color_eyre::Result<()> {
        let sources: SmallVec<[Source; MAX_PUBLICATION_SOURCES]> = (0..=MAX_PUBLICATION_SOURCES)
            .map(|i| Source {
                group_id: Arc::from(format!("group-{i}")),
                topic: Intern::<str>::from("topic"),
                partition_count: PartitionCount::MIN,
            })
            .collect();
        assert_eq!(sources.len(), MAX_PUBLICATION_SOURCES + 1);
        match ValidatedPublications::new(sources) {
            Err(NoSnapshot::TooManySources { found }) => {
                assert_eq!(found, MAX_PUBLICATION_SOURCES + 1);
            }
            other => color_eyre::eyre::bail!("expected TooManySources, got {other:?}"),
        }
        Ok(())
    }
}
