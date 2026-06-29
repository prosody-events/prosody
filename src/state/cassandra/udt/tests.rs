use super::*;
use crate::state::TimerEventRef;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use color_eyre::eyre::{self, Result};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use uuid::Uuid;

/// An arbitrary [`EventRef`] of either variant, with random message dedup
/// ids and random timer coordinates, for the round-trip property.
#[derive(Clone, Copy, Debug)]
struct ArbEventRef(EventRef);

impl Arbitrary for ArbEventRef {
    fn arbitrary(g: &mut Gen) -> Self {
        const TIMER_TYPES: [TimerType; 4] = [
            TimerType::Application,
            TimerType::DeferredMessage,
            TimerType::DeferredTimer,
            TimerType::StateRecovery,
        ];
        let event = if bool::arbitrary(g) {
            EventRef::Message {
                dedup_id: Uuid::from_u128(u128::arbitrary(g)),
            }
        } else {
            let timer_type = g
                .choose(&TIMER_TYPES)
                .copied()
                .unwrap_or(TimerType::Application);
            EventRef::Timer(TimerEventRef::new(
                timer_type,
                CompactDateTime::from(u32::arbitrary(g)),
                i32::arbitrary(g),
            ))
        };
        Self(event)
    }
}

/// `from_event` followed by `try_into_event` recovers any `EventRef` —
/// generalizes the two per-variant round-trip examples over random message
/// dedup ids and timer coordinates. Iteration count comes from
/// `QUICKCHECK_TESTS`.
#[test]
fn prop_event_ref_round_trips() {
    fn prop(event: ArbEventRef) -> TestResult {
        let ArbEventRef(event) = event;
        match RawEventRef::from_event(event).try_into_event() {
            Ok(decoded) if decoded == event => TestResult::passed(),
            Ok(decoded) => {
                TestResult::error(format!("round-trip changed {event:?} into {decoded:?}"))
            }
            Err(e) => TestResult::error(format!("round-trip of {event:?} failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ArbEventRef) -> TestResult);
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
fn udt_unknown_timer_type_rejected() -> Result<()> {
    let raw = RawEventRef {
        kind: EventRef::TIMER_KIND,
        msg_dedup_id: None,
        timer_type: Some(99),
        time: Some(CompactDateTime::from(0_u32)),
        tag: Some(0_i32),
    };
    match raw.try_into_event() {
        Err(CorruptUdtError::UnknownTimerType(99)) => Ok(()),
        other => Err(eyre::eyre!("expected UnknownTimerType(99), got {other:?}")),
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
        timer_type: Some(0_i8),
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
        timer_type: Some(0_i8),
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
        timer_type: Some(0_i8),
        time: None,
        tag: Some(0_i32),
    };
    match raw.try_into_event() {
        Err(CorruptUdtError::TimerMissingField) => Ok(()),
        other => Err(eyre::eyre!("expected TimerMissingField, got {other:?}")),
    }
}
