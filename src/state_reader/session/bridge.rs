//! Bridge: the raw [`CellRead`] surface over a read session.
//!
//! Every collection reaches cells through a scoped operation, so nothing here
//! answers a collection command. The impl survives because binding needs it:
//! [`Descriptor::bind`](crate::state::descriptor::StateDescriptor::bind)
//! validates through [`CellRead`], so a reader session must offer that surface
//! to be bindable at all.
//!
//! Its read paths therefore stay honest. Each one reads and publishes the
//! session-shared selection, not an invocation-local one. The whole file dies
//! with the old cell-command architecture.

use super::ReadSession;
use crate::codec::Codec;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::{CollectionDef, MAX_KEYSET_LIMIT};
use crate::state::session::CellRead;
use crate::state::session::sealed::ReadAdmission;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use crate::state_reader::backend::ReaderBackend;
use bytes::Bytes;
use futures::stream::Stream;

impl<C: Codec, B: ReaderBackend<C>> ReadAdmission for ReadSession<C, B> {
    type Permit<'s> = ();

    async fn permit(&self) -> Self::Permit<'_> {}

    fn attempt_current(&self) -> bool {
        true
    }

    fn collection_def(&self, _state_type: StateType, _name: &StateName) -> CollectionDef {
        // The reader's own descriptor settings, except the keyset bound: the
        // persisted keyset records its own overflow, and the global validated
        // ceiling safely admits every tracked keyset regardless of the reader's
        // local operational setting — so binding can never change a reader
        // stream's tracked-versus-scan arm.
        CollectionDef {
            keyset_limit: MAX_KEYSET_LIMIT,
            ..self.context.def.collection
        }
    }
}

impl<C: Codec, B: ReaderBackend<C>> CellRead for ReadSession<C, B>
where
    C::Payload: Clone,
{
    fn is_terminated(&self) -> bool {
        false
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
        // Publish only what this read selected: unpinned reads in one operation
        // are issued sequentially (`SingleSourceCoherence`), so a selection that
        // was absent when the read began is this session's first, and the
        // write-once cell accepts it.
        let mut selection = self.pin.get().cloned();
        let unselected = selection.is_none();
        let result = self.point_read(&mut selection, cell).await;
        if unselected && let Some(pin) = selection {
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
        let mut selection = self.pin.get().cloned();
        let unselected = selection.is_none();
        let result = self.batch_read(&mut selection, section, batch).await;
        if unselected && let Some(pin) = selection {
            let _ = self.pin.set(pin);
        }
        result
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        // No captured selection: `scan_from` samples the session pin itself, at
        // first poll rather than here, so a stream built before a sibling read
        // pins still addresses that selection.
        self.scan_from(None, scan)
    }
}
