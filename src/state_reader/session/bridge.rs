//! Bridge: the raw [`CellRead`] surface over a read session.
//!
//! The collection kinds that do not run through the reader engine yet reach
//! cells through this impl instead of through a scoped operation, so it repeats
//! probe-and-pin over the session-shared selection: `get` delegates to the
//! session's point read, while `get_many` and `scan` carry their own fan-out
//! and sequential probe. The whole file dies once Map and Deque run through the
//! engine.

use super::{PinnedSource, ReadSession};
use crate::codec::Codec;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::MAX_KEYSET_LIMIT;
use crate::state::session::CellRead;
use crate::state::session::sealed::ReadAdmission;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::{CommittedCellSource, ReaderBackend};
use bytes::Bytes;
use futures::stream::{FuturesOrdered, Stream, StreamExt};
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

impl<C: Codec, B: ReaderBackend<C>> ReadAdmission for ReadSession<C, B> {
    type Permit<'s> = ();

    async fn permit(&self) -> Self::Permit<'_> {}

    fn attempt_current(&self) -> bool {
        true
    }
}

impl<C: Codec, B: ReaderBackend<C>> CellRead for ReadSession<C, B>
where
    C::Payload: Clone,
{
    fn is_terminated(&self) -> bool {
        false
    }

    fn collection_has_ttl(&self, _state_type: StateType, _name: &StateName) -> bool {
        self.context.def.collection.ttl.is_some()
    }

    fn collection_keyset_limit(&self, _state_type: StateType, _name: &StateName) -> usize {
        // The persisted keyset records its own overflow. The global validated
        // ceiling safely admits every tracked keyset regardless of the
        // reader's local operational setting.
        MAX_KEYSET_LIMIT
    }

    fn collection_capacity(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Option<NonZeroUsize> {
        self.context.def.collection.capacity
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        // Identity was validated at acquisition against every source. The
        // descriptor is the source of both this session's collection and the
        // handle's asserted identity, so a self-check is redundant.
        Ok(self.context.name.clone())
    }

    async fn get(
        &self,
        _state_type: StateType,
        _name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        // Bridge: reads the session-shared selection rather than carrying an
        // invocation-local one. Dies with the `CellRead` surface, once Map and
        // Deque run through the engine.
        if let Some(pin) = self.pin.get() {
            return self
                .cached_point(Some(&pin.collection), &pin.source, cell)
                .await;
        }
        let mut selection = None;
        let result = self.point_read(&mut selection, cell).await;
        if let Some(pin) = selection {
            // Discarding the `set` result is safe because unpinned reads in
            // one operation are issued sequentially (the
            // `SingleSourceCoherence` invariant), so this is the first pin.
            let _ = self.pin.set(pin);
        }
        result
    }

    async fn get_many(
        &self,
        _state_type: StateType,
        _name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        // Bridge: the session-shared selection, as in `get` above.
        if let Some(pin) = self.pin.get() {
            return self
                .cached_batch(Some(&pin.collection), &pin.source, section, batch)
                .await;
        }
        let sources = self.snapshot.sources();
        // Bounded per-operation fan-out; see the ruling on the point-read
        // `FuturesOrdered` above.
        let mut ordered = FuturesOrdered::new();
        for source in sources {
            ordered.push_back(cooperative(async move {
                (
                    source,
                    self.cached_batch(None, source, section, batch).await,
                )
            }));
        }
        let mut first_err = None;
        let mut all_none: Option<CellBuffer<Option<Bytes>>> = None;
        while let Some((source, result)) = cooperative(ordered.next()).await {
            match result {
                Ok(buffer) => {
                    if buffer.iter().any(Option::is_some) {
                        // First pin, so the set succeeds and discarding its
                        // result is safe; see the `get` pin above.
                        let collection = self.collection_id_for(source)?;
                        let _ = self.pin.set(PinnedSource {
                            source: source.clone(),
                            collection,
                        });
                        return Ok(buffer);
                    }
                    if all_none.is_none() {
                        all_none = Some(buffer);
                    }
                }
                Err(error) => {
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
        }
        match (all_none, first_err) {
            // A data-bearing buffer already returned early above. Among the
            // remaining outcomes an error outranks an all-`None` buffer, because
            // absence cannot be proven through a source that failed. This
            // mirrors the point read, where a remembered error beats the `None`
            // answers.
            (_, Some(error)) => Err(error),
            (Some(buffer), None) => Ok(buffer),
            // Unreachable: the snapshot is non-empty, so at least one source
            // answered with data, all-`None`, or an error.
            (None, None) => Ok((0..batch.len()).map(|_| None).collect()),
        }
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        async_stream::try_stream! {
            // Bridge: the session-shared selection, as in `get` above.
            if let Some(pin) = self.pin.get() {
                let id = pin.collection.clone();
                let inner = self.context.backend.cells().scan(&id, scan);
                futures::pin_mut!(inner);
                while let Some(item) = cooperative(inner.next()).await {
                    yield item.map_err(|error| StateAccessError::store(&error))?;
                }
                return;
            }
            // Unpinned sequential probe: the general `CellRead::scan` contract
            // for a session that no read has pinned yet. Sources are tried in
            // preference order and the first to yield a cell pins. The public
            // Map and Deque reader streams reach it only if their keyset or
            // bounds read found nothing, since that read pins first.
            let sources = self.snapshot.sources();
            let mut first_err = None;
            let mut pinned = false;
            for source in sources {
                let id = self.collection_id_for(source)?;
                let inner = self.context.backend.cells().scan(&id, scan);
                futures::pin_mut!(inner);
                let mut yielded_any = false;
                loop {
                    match cooperative(inner.next()).await {
                        Some(Ok(item)) => {
                            if !yielded_any {
                                let _ = self.pin.set(PinnedSource {
                                    source: source.clone(),
                                    collection: id.clone(),
                                });
                                yielded_any = true;
                            }
                            yield item;
                        }
                        Some(Err(error)) => {
                            let error = StateAccessError::store(&error);
                            if yielded_any {
                                // Mid-stream error after the pin. Terminate,
                                // never restart, because a restart would
                                // double-yield or skip cells.
                                Err(error)?;
                            } else if first_err.is_none() {
                                first_err = Some(error);
                            }
                            break;
                        }
                        None => break,
                    }
                }
                if yielded_any {
                    pinned = true;
                    break;
                }
            }
            // Nothing pinned: propagate a remembered error, else the stream is
            // empty (every source completed empty).
            if !pinned && let Some(error) = first_err {
                Err(error)?;
            }
        }
    }
}
