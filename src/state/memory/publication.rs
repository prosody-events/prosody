//! In-memory publication routing rows.

use crate::Topic;
use crate::state::dirty::{Edge, remove_span};
use crate::state::publication::{PublicationRows, PublicationStore, StatePublication};
use crate::state::{StateName, StateType};
use crate::state_reader::PUBLICATION_READ_LIMIT;
use crate::subsystem::SubsystemName;
use scc::{Guard, TreeIndex};
use std::cmp::Ordering;
use std::convert::Infallible;
use std::ops::RangeInclusive;
use std::sync::Arc;

/// An in-memory routing store shared for the lifetime of its backend.
#[derive(Clone, Debug, Default)]
pub struct MemoryPublicationStore {
    rows: Arc<TreeIndex<PublicationKey, StatePublication>>,
}

impl MemoryPublicationStore {
    /// Creates an empty publication store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PublicationStore for MemoryPublicationStore {
    type Error = Infallible;

    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), Self::Error> {
        self.rows.upsert_sync(
            publication_key(subsystem, state_type, name, row),
            row.clone(),
        );
        Ok(())
    }

    async fn remove_group(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> Result<(), Self::Error> {
        remove_span(
            &self.rows,
            PublicationScope::group_range(subsystem, state_type, name, group_id),
        );
        Ok(())
    }

    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<PublicationRows, Self::Error> {
        let guard = Guard::new();
        let rows = self
            .rows
            .range(PublicationScope::range(subsystem, state_type, name), &guard)
            .take(PUBLICATION_READ_LIMIT)
            .map(|(_key, row)| row.clone())
            .collect();
        drop(guard);
        Ok(rows)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PublicationKey {
    subsystem: SubsystemName,
    state_type: StateType,
    name: StateName,
    group_id: Arc<str>,
    topic: Topic,
}

#[derive(Clone, Eq, PartialEq)]
struct PublicationScope {
    subsystem: SubsystemName,
    state_type: StateType,
    name: StateName,
    group_id: Option<Arc<str>>,
    edge: Edge,
}

impl PublicationScope {
    fn range(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> RangeInclusive<Self> {
        Self::spanning(subsystem, state_type, name, None)
    }

    fn group_range(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> RangeInclusive<Self> {
        Self::spanning(subsystem, state_type, name, Some(Arc::from(group_id)))
    }

    fn spanning(
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: Option<Arc<str>>,
    ) -> RangeInclusive<Self> {
        let at = |edge, group_id| Self {
            subsystem: subsystem.clone(),
            state_type,
            name: name.clone(),
            group_id,
            edge,
        };
        at(Edge::Low, group_id.clone())..=at(Edge::High, group_id)
    }

    fn cmp_key(&self, key: &PublicationKey) -> Ordering {
        self.subsystem
            .cmp(&key.subsystem)
            .then(self.state_type.cmp(&key.state_type))
            .then(self.name.cmp(&key.name))
            .then_with(|| {
                self.group_id.as_ref().map_or(Ordering::Equal, |group| {
                    group.as_ref().cmp(key.group_id.as_ref())
                })
            })
    }
}

impl scc::Equivalent<PublicationKey> for PublicationScope {
    fn equivalent(&self, key: &PublicationKey) -> bool {
        scc::Comparable::compare(self, key) == Ordering::Equal
    }
}

impl scc::Comparable<PublicationKey> for PublicationScope {
    fn compare(&self, key: &PublicationKey) -> Ordering {
        self.cmp_key(key).then(self.edge.beyond())
    }
}

fn publication_key(
    subsystem: &SubsystemName,
    state_type: StateType,
    name: &StateName,
    row: &StatePublication,
) -> PublicationKey {
    PublicationKey {
        subsystem: subsystem.clone(),
        state_type,
        name: name.clone(),
        group_id: row.group_id.clone(),
        topic: row.topic,
    }
}
