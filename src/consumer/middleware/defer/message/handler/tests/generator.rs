//! Generates independent choices and shrinks them without a state model.

use super::context::TimerOp;
use super::types::{Fault, Step, Trace};
use super::{FaultKind, StoreOp};
use quickcheck::{Arbitrary, Gen};

impl Arbitrary for StoreOp {
    fn arbitrary(g: &mut Gen) -> Self {
        const ALL: [StoreOp; 7] = [
            StoreOp::IsDeferred,
            StoreOp::DeferFirst,
            StoreOp::DeferAdditional,
            StoreOp::CompleteRetrySuccess,
            StoreOp::IncrementRetryCount,
            StoreOp::GetNext,
            StoreOp::DeleteKey,
        ];
        ALL[usize::arbitrary(g) % ALL.len()]
    }
}

impl Arbitrary for TimerOp {
    fn arbitrary(g: &mut Gen) -> Self {
        const ALL: [TimerOp; 4] = [
            TimerOp::Schedule,
            TimerOp::ClearAndSchedule,
            TimerOp::ClearScheduled,
            TimerOp::Scheduled,
        ];
        ALL[usize::arbitrary(g) % ALL.len()]
    }
}

impl Arbitrary for FaultKind {
    fn arbitrary(g: &mut Gen) -> Self {
        const ALL: [FaultKind; 2] = [FaultKind::Transient, FaultKind::Permanent];
        ALL[usize::arbitrary(g) % ALL.len()]
    }
}

impl Arbitrary for Fault {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Store(StoreOp::arbitrary(g), FaultKind::arbitrary(g)),
            1 => Self::Timer(TimerOp::arbitrary(g), FaultKind::arbitrary(g)),
            2 => Self::LostTimer,
            _ => Self::LoaderThenTimer(
                FaultKind::arbitrary(g),
                TimerOp::arbitrary(g),
                FaultKind::arbitrary(g),
            ),
        }
    }
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            key_idx: u8::arbitrary(g),
            roll: u8::arbitrary(g),
            fault: (u8::arbitrary(g) % 4 == 0).then(|| Fault::arbitrary(g)),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let step = *self;
        Box::new(
            step.fault
                .into_iter()
                .map(move |_| Self {
                    fault: None,
                    ..step
                })
                .chain(step.roll.shrink().map(move |roll| Self { roll, ..step })),
        )
    }
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            key_count: usize::arbitrary(g) % 5 + 1,
            steps: Vec::<Step>::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let key_count = self.key_count;
        Box::new(
            self.steps
                .shrink()
                .map(move |steps| Self { steps, key_count }),
        )
    }
}
