//! Publication-source identity admission.

use crate::state::access::StateAccessError;
use crate::state::descriptor::StateDescriptor;
use crate::state::descriptor_identity::DescriptorIdentityStore;
use crate::state::descriptor_identity::{self, DurableDescriptorIdentity};
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

/// The running result of one identity validation pass.
#[derive(Default)]
pub(super) struct Admission {
    pub(super) admitted: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
    pub(super) mismatch: Option<Arc<str>>,
    pub(super) any_missing: bool,
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
                        self.classify_identity(stored?, group, &asserted, &mut admission)
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

    /// Classifies one group's frozen identity.
    fn classify_identity(
        &self,
        stored: Option<DurableDescriptorIdentity>,
        group: &Arc<str>,
        asserted: &DurableDescriptorIdentity,
        admission: &mut Admission,
    ) -> bool {
        let Some(stored) = stored else {
            warn!(group = %group, name = %self.context.name.as_str(), "publication source has no frozen identity yet");
            admission.any_missing = true;
            return false;
        };
        if let Err(error) = descriptor_identity::validate::<StateAccessError>(stored, asserted) {
            warn!(
                group = %group,
                name = %self.context.name.as_str(),
                error = %error,
                "publication source descriptor identity disagrees"
            );
            if admission.mismatch.is_none() {
                admission.mismatch = Some(Arc::clone(group));
            }
            return false;
        }
        true
    }
}
