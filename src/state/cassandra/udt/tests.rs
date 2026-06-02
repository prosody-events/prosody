use super::*;
use crate::state::TimerEventRef;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use color_eyre::eyre::{self, Result};
use uuid::Uuid;

/// Invariant: `from_event` followed by `try_into_event` recovers the original.
fn assert_round_trips(event: EventRef) -> Result<()> {
    let decoded = RawEventRef::from_event(event)
        .try_into_event()
        .map_err(|e| eyre::eyre!("{e}"))?;
    assert_eq!(decoded, event);
    Ok(())
}

#[test]
fn round_trips_message_variant() -> Result<()> {
    assert_round_trips(EventRef::Message {
        dedup_id: Uuid::from_u128(0xdead_beef),
    })
}

#[test]
fn round_trips_timer_variant() -> Result<()> {
    assert_round_trips(EventRef::Timer(TimerEventRef::new(
        TimerType::Application,
        CompactDateTime::from(123_456_u32),
        42,
    )))
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
        kind: EventRef::MESSAGE_KIND,
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
        kind: EventRef::MESSAGE_KIND,
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
        kind: EventRef::TIMER_KIND,
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
        kind: EventRef::TIMER_KIND,
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
