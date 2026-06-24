//! Cassandra `event_ref` UDT serde for [`EventRef`].
//!
//! Cassandra UDTs cannot directly represent a Rust `enum`, so the on-wire
//! shape is a flat struct with optional fields and a `kind` discriminator:
//!
//! ```text
//! CREATE TYPE event_ref (
//!     kind         tinyint, -- 0 = Message, 1 = Timer
//!     msg_dedup_id uuid,
//!     timer_type   tinyint,
//!     time         int,
//!     tag          int
//! );
//! ```
//!
//! [`RawEventRef`] is the bridge that derives scylla's `SerializeValue` and
//! `DeserializeValue`. The [`SerializeValue`] impl on [`EventRef`] routes
//! through it so the Rust-side enum representation stays decoupled from the
//! Cassandra-side flat representation.
//!
//! Reads deserialize into [`RawEventRef`] (structural only) and validate it
//! into an [`EventRef`] with [`RawEventRef::try_into_event`] in a fallible
//! post-step run by the row decoder, **not** inside scylla's
//! `DeserializeValue`. A semantically-corrupt UDT (e.g. `kind == 7`, or a
//! Message-kind row with timer fields set) deserializes fine but fails
//! validation with a typed [`CorruptUdtError`]; routing that through the
//! decoder keeps the typed error classifiable as `Permanent` (skip the
//! message) instead of being laundered into scylla's opaque
//! `DeserializationError`, which classifies `Terminal` (shut the partition
//! down). That keeps "kind == 0 but `msg_dedup_id` is NULL" out of the type
//! system without tearing down a consumer over one bad row.

use super::error::CorruptUdtError;
use crate::state::{EventRef, TimerEventRef};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use uuid::Uuid;

/// On-wire representation of the `event_ref` UDT.
///
/// Bridge between [`EventRef`] and the scylla derive macros. Crate-internal
/// to the Cassandra cell store so the row decoder can validate it into an
/// [`EventRef`] after scylla has done the structural deserialization.
#[derive(Clone, Debug, scylla::DeserializeValue, scylla::SerializeValue)]
pub(in crate::state::cassandra) struct RawEventRef {
    pub(in crate::state::cassandra) kind: i8,
    pub(in crate::state::cassandra) msg_dedup_id: Option<Uuid>,
    pub(in crate::state::cassandra) timer_type: Option<TimerType>,
    pub(in crate::state::cassandra) time: Option<CompactDateTime>,
    pub(in crate::state::cassandra) tag: Option<i32>,
}

impl RawEventRef {
    pub(in crate::state::cassandra) fn from_event(event: EventRef) -> Self {
        match event {
            EventRef::Message { dedup_id } => Self {
                kind: EventRef::MESSAGE_KIND,
                msg_dedup_id: Some(dedup_id),
                timer_type: None,
                time: None,
                tag: None,
            },
            EventRef::Timer(TimerEventRef {
                timer_type,
                time,
                tag,
            }) => Self {
                kind: EventRef::TIMER_KIND,
                msg_dedup_id: None,
                timer_type: Some(timer_type),
                time: Some(time),
                tag: Some(tag),
            },
        }
    }

    pub(in crate::state::cassandra) fn try_into_event(self) -> Result<EventRef, CorruptUdtError> {
        match self.kind {
            EventRef::MESSAGE_KIND => {
                if self.timer_type.is_some() || self.time.is_some() || self.tag.is_some() {
                    return Err(CorruptUdtError::MessageHasTimerFields);
                }
                let Some(dedup_id) = self.msg_dedup_id else {
                    return Err(CorruptUdtError::MessageMissingDedupId);
                };
                Ok(EventRef::Message { dedup_id })
            }
            EventRef::TIMER_KIND => {
                if self.msg_dedup_id.is_some() {
                    return Err(CorruptUdtError::TimerHasDedupId);
                }
                let (Some(timer_type), Some(time), Some(tag)) =
                    (self.timer_type, self.time, self.tag)
                else {
                    return Err(CorruptUdtError::TimerMissingField);
                };
                Ok(EventRef::Timer(TimerEventRef::new(timer_type, time, tag)))
            }
            other => Err(CorruptUdtError::UnknownKind(other)),
        }
    }
}

impl SerializeValue for EventRef {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        RawEventRef::from_event(*self).serialize(typ, writer)
    }
}

#[cfg(test)]
mod tests;
