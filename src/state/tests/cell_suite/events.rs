//! Generated crash-trace events and their Value mutations.

use super::*;

/// One Value mutation staged by an event.
#[derive(Clone, Copy, Debug)]
pub(super) enum Mutation {
    Set(u8),
    Clear,
}

impl Mutation {
    /// The staged outcome (`Set` → present bytes, `Clear` → known-absent).
    pub(super) fn value(self) -> Option<Bytes> {
        match self {
            Self::Set(b) => Some(bytes(b)),
            Self::Clear => None,
        }
    }
}

impl Arbitrary for Mutation {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Set(u8::arbitrary(g))
        } else {
            Self::Clear
        }
    }
}

/// How an event resolved — the six distinct outcomes.
#[derive(Clone, Copy, Debug)]
pub(super) enum Outcome {
    /// Committed and promoted inline (the hot path).
    CleanCommitted,
    /// Staged then rolled back inline before any commit-marker record
    /// (abandon).
    CleanRolledBack,
    /// All cells staged, commit marker never recorded, crash → the generated
    /// recovery (rolls back).
    CrashAfterStage,
    /// All cells staged, commit marker recorded, crash → the generated
    /// recovery (promotes).
    CrashAfterMarker,
    /// Only a prefix staged, commit marker never recorded, crash → recovery.
    CrashMidFanOut,
    /// Commit marker recorded (committed), then the settle attempt fails under
    /// a poison armed at the generated [`FaultDepth`]: the stage lingers over
    /// the **warm** in-process store — the unsettled-clear window (on a
    /// `Cached` instantiation, `Wrapper` depth leaves the settle transform
    /// unrun, and `Lower` depth leaves it run with the cleared sections
    /// deleted).
    SettleFailure,
}

impl Outcome {
    pub(super) fn marker_flushed(self) -> bool {
        matches!(
            self,
            Self::CleanCommitted | Self::CrashAfterMarker | Self::SettleFailure
        )
    }

    pub(super) fn mid_fan_out(self) -> bool {
        matches!(self, Self::CrashMidFanOut)
    }

    pub(super) fn is_crash(self) -> bool {
        matches!(
            self,
            Self::CrashAfterStage | Self::CrashAfterMarker | Self::CrashMidFanOut
        )
    }
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => Self::CleanCommitted,
            1 => Self::CleanRolledBack,
            2 => Self::CrashAfterStage,
            3 => Self::CrashAfterMarker,
            4 => Self::CrashMidFanOut,
            _ => Self::SettleFailure,
        }
    }
}

/// One event: a flat list of `(collection, section idx, coord, mutation)`
/// writes, the sections it durably clears, an outcome, and the recovery path
/// to use when the outcome is a crash. The flat list is grouped by collection
/// ([`crash::event_plan`](super::crash::event_plan)) so each touched collection
/// stages **all** its cells (and clears) in one `write_provisional` call —
/// unless `split` is set, which stages a ≥2-cell collection in two sequential
/// same-event calls carrying the same union marker, exercising the same-event
/// marker overwrite at the stage boundary (the second stage's unsettled marker
/// is the event's OWN and must not be resolved).
#[derive(Clone, Debug)]
pub(super) struct TraceEvent {
    pub(super) writes: Vec<(u8, u8, u8, Mutation)>,
    /// Blind resolved writes issued at the TOP of the event, before any stage —
    /// modelling the mid-handler `commit()` / `ReadUncommitted` direct apply
    /// (`write_resolved`). Deliberately **cells-only** (no clears): a
    /// blind-clears dimension would need gap-trim modelling, and
    /// `write_resolved`'s clears leg keeps its coverage through the stage path.
    /// Their whole purpose is to land into a section whose PRIOR event's
    /// marker with clears remains unsettled, so the write-side boundary
    /// (`resolve_unsettled_clear_before_write`) is exercised over generated
    /// traces.
    pub(super) blind: Vec<(u8, u8, u8, Mutation)>,
    /// Durable section clears as `(collection, section idx)`. Deduped; the
    /// clear's collection is drawn independently of the writes', so
    /// clears-only stages (a clear on a collection the event never writes)
    /// arise organically.
    pub(super) clears: Vec<(u8, u8)>,
    pub(super) outcome: Outcome,
    pub(super) split: bool,
    /// When set, the event's first planned stage is rejected at the lower
    /// store (a transient `write_provisional` fault) and the event is never
    /// dispatched: the model, marker model, and deferrals stay untouched —
    /// the rejected stage's boundary resolve never reached the bottom store,
    /// so a lingering prior event stage still lingers. Weighted low; shrinks to
    /// `false`.
    pub(super) stage_fault: bool,
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let writes = capped_vec::<(u8, u8, u8, Mutation)>(g, (POOL * CRASH_CELLS) as usize)
            .into_iter()
            .map(|(coll, s, c, m)| (coll % POOL, section_idx(s), c % CRASH_CELLS, m))
            .collect();
        let blind = capped_vec::<(u8, u8, u8, Mutation)>(g, 2)
            .into_iter()
            .map(|(coll, s, c, m)| (coll % POOL, section_idx(s), c % CRASH_CELLS, m))
            .collect();
        let mut clears: Vec<(u8, u8)> = capped_vec::<(u8, u8)>(g, 2)
            .into_iter()
            .map(|(coll, s)| (coll % POOL, section_idx(s)))
            .collect();
        clears.sort_unstable();
        clears.dedup();
        Self {
            writes,
            blind,
            clears,
            outcome: Outcome::arbitrary(g),
            split: bool::arbitrary(g),
            stage_fault: u8::arbitrary(g) % 8 == 0,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = self.clone();
        let unfaulted = self.stage_fault.then(|| Self {
            stage_fault: false,
            ..base.clone()
        });
        let writes = self.writes.shrink().map({
            let base = base.clone();
            move |writes| Self {
                writes,
                ..base.clone()
            }
        });
        let blind = self.blind.shrink().map({
            let base = base.clone();
            move |blind| Self {
                blind,
                ..base.clone()
            }
        });
        let clears = self.clears.shrink().map(move |clears| Self {
            clears,
            ..base.clone()
        });
        Box::new(
            unfaulted
                .into_iter()
                .chain(writes)
                .chain(blind)
                .chain(clears),
        )
    }
}

/// A shrinkable trace of events over the key pool.
#[derive(Clone, Debug)]
pub(crate) struct Trace {
    pub(super) events: Vec<TraceEvent>,
    pub(super) ttl: Option<u16>,
    pub(super) clock: u8,
    pub(super) cut: u8,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: capped_vec(g, MAX_TRACE_OPS),
            ttl: Option::arbitrary(g),
            clock: u8::arbitrary(g),
            cut: u8::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shortening the trace is the highest-value reduction.
        let base = self.clone();
        let events = self.events.shrink().map(move |events| Self {
            events,
            ..base.clone()
        });
        let base = self.clone();
        let clock = (self.ttl, self.clock, self.cut)
            .shrink()
            .map(move |(ttl, clock, cut)| Self {
                ttl,
                clock,
                cut,
                ..base.clone()
            });
        Box::new(events.chain(clock))
    }
}
