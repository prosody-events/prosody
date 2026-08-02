//! Publication-source identity admission.

use crate::state::access::StateAccessError;
use crate::state::descriptor::StateDescriptor;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::descriptor_identity::{self, DescriptorIdentityError, DurableDescriptorIdentity};
use crate::state::publication::StatePublication;
use crate::state_reader::ReaderBackend;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::source::{
    MAX_PUBLICATION_SOURCES, Source, SourceId, ValidatedPublications,
};
use crate::{Codec, state_reader::reader::StateReader};
use futures::stream::{self, StreamExt};
use smallvec::SmallVec;
use std::sync::Arc;
use tokio::task::coop::cooperative;
use tracing::warn;

/// Publishing group ids gathered during one admission, bounded by the source
/// cap so the common case stays on the stack.
type GroupIds = SmallVec<[Arc<str>; MAX_PUBLICATION_SOURCES]>;

/// How a frozen identity row can disagree with the reader's descriptor.
type IdentityError = DescriptorIdentityError<StateAccessError>;

/// What one admission pass observed about the sources it skipped.
///
/// Only the caller whose refresh wins publication emits these, so a burst of
/// concurrent refreshes reports each observation once. A healthy collection
/// records none, so the common case stays on the stack and logs nothing.
#[derive(Default)]
pub(super) struct Diagnostics {
    /// Advertised groups that have frozen no identity yet.
    missing: SmallVec<[Arc<str>; 2]>,
    /// Groups whose frozen identity disagrees with the reader's descriptor,
    /// each with why. The error is boxed because a misconfiguration is never
    /// the steady state.
    disagreed: SmallVec<[(Arc<str>, Box<IdentityError>); 1]>,
}

/// The running result of one identity validation pass.
#[derive(Default)]
pub(super) struct Admission {
    pub(super) admitted: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
    pub(super) diagnostics: Diagnostics,
}

impl Diagnostics {
    /// The first group whose frozen identity disagreed with the descriptor.
    pub(super) fn mismatch(&self) -> Option<Arc<str>> {
        self.disagreed.first().map(|(group, _)| Arc::clone(group))
    }

    /// Whether an advertised group has frozen no identity yet.
    pub(super) fn any_missing(&self) -> bool {
        !self.missing.is_empty()
    }

    /// Logs every observation this pass recorded for collection `name`.
    pub(super) fn emit(&self, name: &str) {
        for group in &self.missing {
            warn!(group = %group, name = %name, "publication source has no frozen identity yet");
        }
        for (group, error) in &self.disagreed {
            warn!(
                group = %group,
                name = %name,
                error = %error,
                "publication source descriptor identity disagrees"
            );
        }
    }

    fn record_missing(&mut self, group: &Arc<str>) {
        self.missing.push(Arc::clone(group));
    }

    fn record_mismatch(&mut self, group: &Arc<str>, error: IdentityError) {
        self.disagreed.push((Arc::clone(group), Box::new(error)));
    }
}

impl<D, C, B> StateReader<D, C, B>
where
    D: StateDescriptor,
    C: Codec,
    B: ReaderBackend<C>,
{
    /// Validates each advertised source's frozen identity, admitting the ones
    /// whose group is already in `prior` without a fresh read.
    pub(super) async fn admit(
        &self,
        rows: &[StatePublication],
        prior: Option<&ValidatedPublications>,
    ) -> Result<Admission, StateReaderError> {
        let prior_groups: GroupIds = prior
            .map(|snapshot| {
                snapshot
                    .sources()
                    .iter()
                    .map(|source| source.group_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let asserted = DurableDescriptorIdentity::from_identity(
            self.context.state_type,
            self.context.name.as_str(),
            &self.descriptor.structural_identity(),
        );

        let mut groups = GroupIds::new();
        for row in rows {
            if !groups.iter().any(|group| **group == *row.group_id) {
                groups.push(row.group_id.clone());
            }
        }

        let mut new_groups = GroupIds::new();
        for group in &groups {
            if !prior_groups.iter().any(|prior| prior == group) {
                new_groups.push(group.clone());
            }
        }
        let mut reads = stream::iter(new_groups)
            .map(|group| {
                cooperative(async move {
                    self.context
                        .backend
                        .identities()
                        .read_identity(&group, self.context.state_type, self.context.name.as_str())
                        .await
                        .map_err(|error| StateReaderError::store(&error))
                })
            })
            .buffered(MAX_PUBLICATION_SOURCES)
            .collect::<SmallVec<[_; MAX_PUBLICATION_SOURCES]>>()
            .await
            .into_iter();

        let mut admission = Admission::default();
        for group in &groups {
            let admitted = if prior_groups.iter().any(|prior| prior == group) {
                true
            } else {
                match reads.next() {
                    Some(stored) => {
                        classify_identity(stored?, group, &asserted, &mut admission.diagnostics)
                    }
                    None => false,
                }
            };
            if admitted {
                admission
                    .admitted
                    .extend(
                        rows.iter()
                            .filter(|row| *row.group_id == **group)
                            .map(|row| SourceId {
                                group_id: row.group_id.clone(),
                                topic: row.topic,
                                partition_count: row.partition_count,
                            }),
                    );
            }
        }
        Ok(admission)
    }
}

/// Classifies one group's frozen identity, recording in `diagnostics` why a
/// source is skipped.
fn classify_identity(
    stored: Option<DurableDescriptorIdentity>,
    group: &Arc<str>,
    asserted: &DurableDescriptorIdentity,
    diagnostics: &mut Diagnostics,
) -> bool {
    let Some(stored) = stored else {
        diagnostics.record_missing(group);
        return false;
    };
    if let Err(error) = descriptor_identity::validate::<StateAccessError>(stored, asserted) {
        diagnostics.record_mismatch(group, error);
        return false;
    }
    true
}
