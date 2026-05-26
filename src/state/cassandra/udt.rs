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
//! [`RawEventRef`] is a private bridge that derives scylla's
//! `SerializeValue` and `DeserializeValue`. The public [`SerializeValue`]
//! and [`DeserializeValue`] impls on [`EventRef`] route through it so the
//! Rust-side enum representation stays decoupled from the Cassandra-side
//! flat representation.
//!
//! The deserializer validates that each variant carries the expected
//! fields and rejects anything else with a [`CorruptUdtError`]. That keeps
//! "kind == 0 but `msg_dedup_id` is NULL" out of the type system.

use super::error::{CassandraValueStoreError, CorruptUdtError};
use crate::state::{EventRef, TimerEventRef};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use uuid::Uuid;

const KIND_MESSAGE: i8 = 0;
const KIND_TIMER: i8 = 1;

/// On-wire representation of the `event_ref` UDT.
///
/// Private bridge between [`EventRef`] and the scylla derive macros.
#[derive(Clone, Debug, scylla::DeserializeValue, scylla::SerializeValue)]
struct RawEventRef {
    kind: i8,
    msg_dedup_id: Option<Uuid>,
    timer_type: Option<TimerType>,
    time: Option<CompactDateTime>,
    tag: Option<i32>,
}

impl RawEventRef {
    fn from_event(event: EventRef) -> Self {
        match event {
            EventRef::Message { dedup_id } => Self {
                kind: KIND_MESSAGE,
                msg_dedup_id: Some(dedup_id),
                timer_type: None,
                time: None,
                tag: None,
            },
            EventRef::Timer(timer) => Self {
                kind: KIND_TIMER,
                msg_dedup_id: None,
                timer_type: Some(timer.timer_type),
                time: Some(timer.time),
                tag: Some(timer.tag),
            },
        }
    }

    fn try_into_event(self) -> Result<EventRef, CorruptUdtError> {
        match self.kind {
            KIND_MESSAGE => {
                if self.timer_type.is_some() || self.time.is_some() || self.tag.is_some() {
                    return Err(CorruptUdtError::MessageHasTimerFields);
                }
                let Some(dedup_id) = self.msg_dedup_id else {
                    return Err(CorruptUdtError::MessageMissingDedupId);
                };
                Ok(EventRef::Message { dedup_id })
            }
            KIND_TIMER => {
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

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for EventRef {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <RawEventRef as DeserializeValue>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let raw = RawEventRef::deserialize(typ, v)?;
        raw.try_into_event()
            .map_err(|err| DeserializationError::new(CassandraValueStoreError::CorruptUdt(err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::TimerEventRef;
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use color_eyre::eyre::{self, Result};
    #[allow(
        unused_imports,
        reason = "ergonomic in test helpers; some platforms warn"
    )]
    use uuid::Uuid;

    #[test]
    fn round_trips_message_variant() -> Result<()> {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0xdead_beef),
        };
        let raw = RawEventRef::from_event(event);
        let decoded = raw.try_into_event().map_err(|e| eyre::eyre!("{e}"))?;
        assert_eq!(decoded, event);
        Ok(())
    }

    #[test]
    fn round_trips_timer_variant() -> Result<()> {
        let event = EventRef::Timer(TimerEventRef::new(
            TimerType::Application,
            CompactDateTime::from(123_456_u32),
            42,
        ));
        let raw = RawEventRef::from_event(event);
        let decoded = raw.try_into_event().map_err(|e| eyre::eyre!("{e}"))?;
        assert_eq!(decoded, event);
        Ok(())
    }

    #[test]
    fn udt_unknown_kind_rejected() -> Result<()> {
        let raw = RawEventRef {
            kind: 7,
            msg_dedup_id: None,
            timer_type: None,
            time: None,
            tag: None,
        };
        match raw.try_into_event() {
            Err(CorruptUdtError::UnknownKind(7)) => Ok(()),
            other => Err(eyre::eyre!("expected UnknownKind(7), got {other:?}")),
        }
    }

    #[test]
    fn udt_message_missing_dedup_id_rejected() -> Result<()> {
        let raw = RawEventRef {
            kind: KIND_MESSAGE,
            msg_dedup_id: None,
            timer_type: None,
            time: None,
            tag: None,
        };
        match raw.try_into_event() {
            Err(CorruptUdtError::MessageMissingDedupId) => Ok(()),
            other => Err(eyre::eyre!("expected MessageMissingDedupId, got {other:?}")),
        }
    }

    #[test]
    fn udt_message_with_timer_fields_rejected() -> Result<()> {
        let raw = RawEventRef {
            kind: KIND_MESSAGE,
            msg_dedup_id: Some(Uuid::from_u128(1)),
            timer_type: Some(TimerType::Application),
            time: None,
            tag: None,
        };
        match raw.try_into_event() {
            Err(CorruptUdtError::MessageHasTimerFields) => Ok(()),
            other => Err(eyre::eyre!("expected MessageHasTimerFields, got {other:?}")),
        }
    }

    #[test]
    fn udt_timer_with_dedup_id_rejected() -> Result<()> {
        let raw = RawEventRef {
            kind: KIND_TIMER,
            msg_dedup_id: Some(Uuid::from_u128(1)),
            timer_type: Some(TimerType::Application),
            time: Some(CompactDateTime::from(0_u32)),
            tag: Some(0_i32),
        };
        match raw.try_into_event() {
            Err(CorruptUdtError::TimerHasDedupId) => Ok(()),
            other => Err(eyre::eyre!("expected TimerHasDedupId, got {other:?}")),
        }
    }

    #[test]
    fn udt_timer_missing_field_rejected() -> Result<()> {
        let raw = RawEventRef {
            kind: KIND_TIMER,
            msg_dedup_id: None,
            timer_type: Some(TimerType::Application),
            time: None,
            tag: Some(0_i32),
        };
        match raw.try_into_event() {
            Err(CorruptUdtError::TimerMissingField) => Ok(()),
            other => Err(eyre::eyre!("expected TimerMissingField, got {other:?}")),
        }
    }
}
