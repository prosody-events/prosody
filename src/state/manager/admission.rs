//! Admission resolves durable residue and retires committed sources before a
//! key dispatches.

use super::{Admission, StateManager};
use crate::Key;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::CommitDecision;
use crate::state::marker::{EventMarker, MarkerState, MarkerVersion};
use crate::state::resolve::resolve_event_marker;
use crate::state::retry::{StepOutcome, retry_step};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend, StateKey,
    StateName, StateType,
};
use crate::timers::TimerManager;
use crate::timers::store::TriggerStore;
use futures::stream::{self, StreamExt, TryStreamExt};
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use smallvec::SmallVec;
use std::error::Error;
use std::future::Future;
use std::sync::LazyLock;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tracing::error;
use uuid::Uuid;

/// Counts each marker read that admission skips after a permanent rejection.
static CORRUPT_MARKER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.admission.corrupt_marker")
        .with_description("Marker reads that admission skips after a permanent rejection")
        .with_unit("{marker}")
        .build()
});

/// Counts each committed-marker resolution that admission rolls back after a
/// permanent rejection.
static ADMISSION_TORN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.admission.torn")
        .with_description("Permanent rejections of committed-marker resolutions during admission")
        .with_unit("{resolve}")
        .build()
});

impl<B, L> StateManager<B, L>
where
    B: StateBackend,
{
    async fn load_markers(
        &self,
        key: &Key,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<SmallVec<[(CollectionRef, MarkerState); 8]>, Admission> {
        let cancelled = || *shutdown.borrow() >= ShutdownPhase::Cancelling;
        let registry = &self.inner.registry;
        let state_key = StateKey::new(self.inner.segment_id, key.clone());
        let mut pending: SmallVec<[(StateType, StateName); 8]> = registry
            .collections()
            .map(|(kind, name)| (kind, name.clone()))
            .collect();
        let mut states: SmallVec<[(CollectionRef, MarkerState); 8]> =
            SmallVec::with_capacity(pending.len());

        while !pending.is_empty() {
            let loaded = stream::iter(pending.drain(..))
                .map(|(kind, name)| {
                    let ttl = registry.ttl_for(kind, &name);
                    let id = CollectionId::new(state_key.clone(), kind, name);
                    cooperative(async move {
                        let state = admission_step(cancelled, key, id.name().as_str(), || {
                            self.inner.cell.marker_state(&id)
                        })
                        .await?;
                        // A corrupt marker cannot name cells for repair. Keep those cells
                        // unchanged.
                        let state = state.unwrap_or_else(|| {
                            CORRUPT_MARKER.add(1, &[]);
                            MarkerState::default()
                        });
                        let collection = CollectionRef::new(id, ttl);
                        Ok::<_, Admission>((collection, state))
                    })
                })
                .buffer_unordered(STATE_FANOUT_CONCURRENCY)
                .try_collect::<SmallVec<[_; 8]>>()
                .await?;
            states.extend(loaded);

            for (_, state) in &states {
                let touched = state.staged.iter().flat_map(EventMarker::touched).chain(
                    state
                        .committed
                        .iter()
                        .flat_map(|marker| marker.touched.iter()),
                );
                for (kind, name) in touched {
                    if !states.iter().any(|(collection, _)| {
                        collection.id().state_type() == *kind && collection.id().name() == name
                    }) && !pending.contains(&(*kind, name.clone()))
                    {
                        pending.push((*kind, name.clone()));
                    }
                }
            }
        }

        Ok(states)
    }

    /// Discovers collection markers and certifies Staged rows through commit
    /// evidence. Resolves the discovered residue.
    /// Retires committed sources before dispatch.
    pub(super) async fn admit_unchecked<T: TriggerStore>(
        &self,
        key: &Key,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<(), Admission> {
        let states = self.load_markers(key, shutdown).await?;
        let mut legacy_events: SmallVec<[EventRef; 8]> = SmallVec::with_capacity(states.len());
        for marker in states.iter().filter_map(|(_, state)| state.staged.as_ref()) {
            if marker.version() == MarkerVersion::V1 && !legacy_events.contains(&marker.event()) {
                legacy_events.push(marker.event());
            }
        }
        let decisions = SmallVec::<[_; 8]>::with_capacity(legacy_events.len());
        let legacy = stream::iter(legacy_events)
            .map(|event| {
                cooperative(async move {
                    Ok::<_, Admission>((
                        event,
                        self.legacy_committed(key, event, timers, shutdown).await?,
                    ))
                })
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_fold(decisions, |mut decisions, entry| async move {
                decisions.push(entry);
                Ok(decisions)
            })
            .await?;

        // Freeze decisions before any resolve changes the durable evidence.
        // Each collection has two markers, each with at most two sources.
        let mut sources: SmallVec<[EventRef; 32]> = SmallVec::with_capacity(states.len() * 4);
        let mut resolutions: SmallVec<[_; 8]> = SmallVec::with_capacity(states.len());
        for (collection, state) in &states {
            if let Some(marker) = &state.committed {
                retain_sources(&mut sources, marker.event, marker.dedup);
            }
            let Some(marker) = &state.staged else {
                continue;
            };
            let committed = if marker.version() == MarkerVersion::V1 {
                legacy
                    .iter()
                    .any(|(event, decision)| *event == marker.event() && *decision == Some(true))
            } else {
                states
                    .iter()
                    .filter_map(|(_, state)| state.committed.as_ref())
                    .any(|entry| entry.certifies(marker))
            };
            let decision = if committed {
                retain_sources(&mut sources, marker.event(), marker.dedup());
                CommitDecision::Committed
            } else {
                CommitDecision::NotCommitted
            };
            resolutions.push((collection.clone(), marker.clone(), decision));
        }

        // Owned shared handles avoid higher-ranked closures and large future buffers.
        stream::iter(resolutions)
            .map(|(collection, marker, decision)| {
                cooperative(async move {
                    self.resolve_admission(key, &collection, &marker, decision, shutdown)
                        .await
                })
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_collect::<()>()
            .await?;
        let independent = (0..sources.len()).filter(|&index| {
            !sources[..index]
                .iter()
                .any(|&other| same_timer_coordinate(other, sources[index]))
        });
        stream::iter(independent)
            .map(|index| {
                cooperative(self.retire_source(
                    key,
                    sources[index],
                    &sources,
                    &legacy,
                    timers,
                    shutdown,
                ))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    async fn resolve_admission(
        &self,
        key: &Key,
        collection: &CollectionRef,
        marker: &EventMarker,
        decision: CommitDecision,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<(), Admission> {
        let cancelled = || *shutdown.borrow() >= ShutdownPhase::Cancelling;
        if decision == CommitDecision::NotCommitted
            && !self.inner.registry.collections().any(|(kind, name)| {
                kind == collection.id().state_type() && name == collection.id().name()
            })
        {
            if marker.version() == MarkerVersion::V1 {
                admission_step(cancelled, key, collection.id().name().as_str(), || {
                    self.inner.cell.abort_provisional(collection, &[])
                })
                .await?;
            }
            return Ok(());
        }

        let marker = marker.for_admission(self.inner.dedup_ttl);
        let resolved = admission_step(cancelled, key, collection.id().name().as_str(), || {
            resolve_event_marker(&self.inner.cell, collection, &marker, decision)
        })
        .await?;
        if resolved.is_none() && decision == CommitDecision::Committed {
            ADMISSION_TORN.add(1, &[]);
            admission_step(cancelled, key, collection.id().name().as_str(), || {
                resolve_event_marker(
                    &self.inner.cell,
                    collection,
                    &marker,
                    CommitDecision::NotCommitted,
                )
            })
            .await?;
        }
        Ok(())
    }

    async fn retire_source<T: TriggerStore>(
        &self,
        key: &Key,
        source: EventRef,
        sources: &[EventRef],
        legacy: &[(EventRef, Option<bool>)],
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<(), Admission> {
        let cancelled = || *shutdown.borrow() >= ShutdownPhase::Cancelling;
        match source {
            EventRef::Message { dedup_id } => {
                // Reuse legacy reads, including Permanent rejections.
                let exists = match legacy.iter().find(|(event, _)| *event == source) {
                    Some((_, exists)) => *exists,
                    None => {
                        admission_step(
                            cancelled,
                            key,
                            "dedup read rejected; redelivery can reach the handler",
                            || self.inner.dedup.exists(dedup_id),
                        )
                        .await?
                    }
                };
                if exists == Some(false) {
                    admission_step(
                        cancelled,
                        key,
                        "dedup write rejected; redelivery can reach the handler",
                        || self.inner.dedup.insert(dedup_id),
                    )
                    .await?;
                }
            }
            EventRef::Timer(_) => {
                // Attempts at one coordinate share durable rows. Retire them in order.
                for &other in sources {
                    if let EventRef::Timer(timer) = other
                        && same_timer_coordinate(source, other)
                    {
                        admission_step(cancelled, key, "timer retirement", || {
                            timers.retire_committed(key, timer)
                        })
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Reads the old commit point for residue staged before the V4 layout.
    /// This rule remains necessary while an idle key can retain a version 1
    /// payload without expiry.
    async fn legacy_committed<T: TriggerStore>(
        &self,
        key: &Key,
        event: EventRef,
        timers: &TimerManager<T>,
        shutdown: &watch::Receiver<ShutdownPhase>,
    ) -> Result<Option<bool>, Admission> {
        let cancelled = || *shutdown.borrow() >= ShutdownPhase::Cancelling;
        match event {
            EventRef::Message { dedup_id } => {
                admission_step(
                    cancelled,
                    key,
                    "legacy dedup read; redelivery can reach the handler",
                    || self.inner.dedup.exists(dedup_id),
                )
                .await
            }
            EventRef::Timer(timer) => {
                let tag = admission_step(cancelled, key, "legacy timer read", || {
                    timers.current_timer_tag(key, timer.time, timer.timer_type)
                })
                .await?;
                Ok(tag.map(|tag| tag != Some(timer.tag)))
            }
        }
    }
}

/// Compares durable timer coordinates within one admitted key.
fn same_timer_coordinate(left: EventRef, right: EventRef) -> bool {
    match (left, right) {
        (EventRef::Timer(left), EventRef::Timer(right)) => {
            left.time == right.time && left.timer_type == right.timer_type
        }
        _ => false,
    }
}

/// Retains each dedup id and timer attempt once for this admission.
fn retain_sources(sources: &mut SmallVec<[EventRef; 32]>, event: EventRef, dedup: Option<Uuid>) {
    let timer = match event {
        EventRef::Timer(_) => Some(event),
        EventRef::Message { .. } => None,
    };
    for source in [dedup.map(|dedup_id| EventRef::Message { dedup_id }), timer]
        .into_iter()
        .flatten()
    {
        if !sources.contains(&source) {
            sources.push(source);
        }
    }
}

pub(super) async fn admission_step<R, E, Fut>(
    cancelled: impl Fn() -> bool,
    key: &Key,
    collection: &str,
    mut step: impl FnMut() -> Fut,
) -> Result<Option<R>, Admission>
where
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + Error,
{
    match retry_step(cancelled, "keyed-state admission", || {
        let result = step();
        async move {
            let result = result.await;
            if let Err(error) = &result
                && error.classify_error() == ErrorCategory::Permanent
            {
                error!(%error, %key, collection, "admission rejected store operation; continue with local repair");
            }
            result
        }
    }).await {
        StepOutcome::Done(value) => Ok(Some(value)),
        StepOutcome::Skip => Ok(None),
        StepOutcome::Abandon => Err(Admission::Abandoned),
    }
}
