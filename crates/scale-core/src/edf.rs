use std::time::Duration;

use fearless_simd::{Simd, prelude::*};

use crate::types::WorkCohorts;

pub(crate) struct EdfScratch {
    release_order: Vec<u32>,
    deadline_order: Vec<u32>,
    prepared_len: usize,
    common_cohort: Option<CommonCohort>,
}

impl EdfScratch {
    pub(crate) fn new(cohort_count_max: u32) -> Result<Self, crate::ConfigurationError> {
        let capacity = usize::try_from(cohort_count_max)
            .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
        Ok(Self {
            release_order: Vec::with_capacity(capacity),
            deadline_order: Vec::with_capacity(capacity),
            prepared_len: 0,
            common_cohort: None,
        })
    }
}

#[derive(Clone, Copy)]
struct CommonCohort {
    release_micros: u64,
    deadline_micros: u64,
    work: f64,
}

struct CommonState {
    released: bool,
    expired: bool,
}

pub(crate) struct SupplyTrajectory<'a> {
    pub(crate) initial: f64,
    pub(crate) pause_micros: &'a [u64],
    pub(crate) ready_micros: &'a [u64],
    pub(crate) ready_boundaries: &'a [u64],
    pub(crate) during: &'a [f64],
    pub(crate) after: &'a [f64],
}

#[derive(Clone, Copy)]
pub(crate) struct SupplyStep {
    pub(crate) before: f64,
    pub(crate) during: f64,
    pub(crate) after: f64,
    pub(crate) pause_micros: u64,
    pub(crate) ready_micros: u64,
}

impl SupplyStep {
    /// Returns the supply rate in effect at the given instant.
    fn capacity_at(self, micros: u64) -> f64 {
        if micros < self.pause_micros {
            self.before
        } else if micros < self.ready_micros {
            self.during
        } else {
            self.after
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct EvaluationWindow {
    pub(crate) start_micros: u64,
    pub(crate) horizon_micros: u64,
    pub(crate) initial_debt_work: f64,
    pub(crate) deadline_budget_micros: u64,
}

/// Tracks cumulative EDF work and its late-area integral.
///
/// Expected late area integrates lateness to predicted completion within the
/// scenario horizon. A deadline never truncates this integral.
/// At every breakpoint, `queue == on_time + overdue`.
struct DeadlineState {
    queue: f64,
    on_time: f64,
    overdue: f64,
    completion_credit: f64,
    due_work: f64,
    missed: f64,
}

#[derive(Clone, Copy)]
struct DeadlineAdvance {
    queue_area: f64,
    late_area: f64,
}

struct DeadlineStateLanes<S: Simd> {
    queue: S::f64s,
    on_time: S::f64s,
    overdue: S::f64s,
    completion_credit: S::f64s,
    due_work: S::f64s,
    missed: S::f64s,
}

pub(crate) struct EdfOutcomeLanes<S: Simd> {
    pub(crate) delay_area: S::f64s,
    pub(crate) missed_work: S::f64s,
    pub(crate) late_area: S::f64s,
    pub(crate) terminal_late_area: S::f64s,
    pub(crate) drain_seconds: S::f64s,
}

impl DeadlineState {
    fn new(initial_debt: f64) -> Self {
        Self {
            queue: initial_debt,
            on_time: 0.0_f64,
            overdue: initial_debt,
            completion_credit: 0.0_f64,
            due_work: 0.0_f64,
            missed: 0.0_f64,
        }
    }

    fn release(&mut self, work: f64) {
        self.queue += work;
        self.on_time += work;
    }

    fn make_due(&mut self, work: f64) {
        self.due_work += work;
        if self.completion_credit >= work {
            self.completion_credit -= work;
            return;
        }
        let remaining_due = work - self.completion_credit;
        self.completion_credit = 0.0_f64;
        let missed = remaining_due.min(self.on_time);
        self.on_time -= missed;
        self.overdue += missed;
        self.missed += missed;
    }

    fn advance(
        &mut self,
        duration: f64,
        capacity: f64,
        arrival_rate: f64,
        due_rate: f64,
    ) -> DeadlineAdvance {
        let mut remaining = duration;
        let mut queue_area = 0.0_f64;
        let mut late_area = 0.0_f64;
        // Queue, on-time work, overdue work, and completion credit each reach
        // zero at most once in this constant-rate segment. A zero queue can
        // grow only when arrivals exceed capacity. It cannot then drain in the
        // same segment. On-time or overdue work can become positive after a
        // regime change. Its new rate then prevents a second zero crossing.
        // Completion credit cannot grow after it reaches zero. Each nonfinal
        // pass consumes one of these four crossings, so five passes suffice.
        let iteration_bound = 5_usize;
        let mut iterations = 0_usize;
        while remaining > 0.0_f64 {
            iterations += 1;
            debug_assert!(
                iterations <= iteration_bound,
                "the constant-rate breakpoint proof bounds each advance"
            );
            let service_rate = self.service_rate(capacity, arrival_rate);
            let due_from_credit = if self.completion_credit > 0.0_f64 {
                due_rate
            } else {
                0.0_f64
            };
            let due_to_queue = due_rate - due_from_credit;
            let had_overdue = self.overdue > 0.0_f64;
            let (overdue_service, on_time_service) = if had_overdue {
                (service_rate, 0.0_f64)
            } else {
                (
                    service_rate.min(due_to_queue),
                    (service_rate - due_to_queue).max(0.0_f64),
                )
            };
            let due_from_on_time = if self.on_time > 0.0_f64 {
                due_to_queue
            } else {
                due_to_queue.min((arrival_rate - on_time_service).max(0.0_f64))
            };
            let queue_rate = arrival_rate - service_rate;
            let overdue_rate = due_from_on_time - overdue_service;
            let on_time_rate = arrival_rate - due_from_on_time - on_time_service;
            let credit_rate = on_time_service - due_from_credit;
            let queue_crossing = positive_crossing(self.queue, queue_rate);
            let overdue_crossing = positive_crossing(self.overdue, overdue_rate);
            let on_time_crossing = positive_crossing(self.on_time, on_time_rate);
            let credit_crossing = positive_crossing(self.completion_credit, credit_rate);
            let crossing = queue_crossing
                .min(overdue_crossing)
                .min(on_time_crossing)
                .min(credit_crossing);
            let span = crossing.min(remaining);
            let queue_after = self.queue + queue_rate * span;
            let overdue_after = self.overdue + overdue_rate * span;
            queue_area += 0.5_f64 * (self.queue + queue_after) * span;
            late_area += 0.5_f64 * (self.overdue + overdue_after) * span;
            self.queue = queue_after;
            self.overdue = overdue_after;
            self.on_time += on_time_rate * span;
            self.completion_credit += credit_rate * span;
            self.due_work += due_rate * span;
            let missed_rate = if had_overdue {
                due_from_on_time
            } else {
                overdue_rate.max(0.0_f64)
            };
            self.missed += missed_rate * span;
            if queue_crossing.total_cmp(&span).is_eq() {
                self.queue = 0.0_f64;
            }
            if overdue_crossing.total_cmp(&span).is_eq() {
                self.overdue = 0.0_f64;
            }
            if on_time_crossing.total_cmp(&span).is_eq() {
                self.on_time = 0.0_f64;
            }
            if credit_crossing.total_cmp(&span).is_eq() {
                self.completion_credit = 0.0_f64;
            }
            remaining -= span;
        }
        DeadlineAdvance {
            queue_area,
            late_area,
        }
    }

    fn service_rate(&self, capacity: f64, arrival_rate: f64) -> f64 {
        if self.queue > 0.0_f64 {
            capacity
        } else {
            capacity.min(arrival_rate)
        }
    }
}

impl<S: Simd> DeadlineStateLanes<S> {
    fn new(simd: S) -> Self {
        let zero = S::f64s::splat(simd, 0.0_f64);
        Self {
            queue: zero,
            on_time: zero,
            overdue: zero,
            completion_credit: zero,
            due_work: zero,
            missed: zero,
        }
    }

    fn release(&mut self, work: f64) {
        self.queue += work;
        self.on_time += work;
    }

    fn make_due(&mut self, simd: S, work: f64) {
        self.due_work += work;
        let zero = S::f64s::splat(simd, 0.0_f64);
        let work = S::f64s::splat(simd, work);
        let covered = self.completion_credit.simd_ge(work);
        let remaining_due = work - self.completion_credit;
        let missed = remaining_due.min(self.on_time);
        self.completion_credit = covered.select(self.completion_credit - work, zero);
        self.on_time = covered.select(self.on_time, self.on_time - missed);
        self.overdue = covered.select(self.overdue, self.overdue + missed);
        self.missed = covered.select(self.missed, self.missed + missed);
    }

    fn advance(&mut self, simd: S, duration: f64, capacity: S::f64s) -> DeadlineAdvanceLanes<S> {
        let zero = S::f64s::splat(simd, 0.0_f64);
        let half = S::f64s::splat(simd, 0.5_f64);
        let infinity = S::f64s::splat(simd, f64::INFINITY);
        let mut remaining = S::f64s::splat(simd, duration);
        let mut queue_area = zero;
        let mut late_area = zero;
        // Each scalar lane has at most four zero crossings. The vector loop
        // uses the largest lane count, so five passes still suffice.
        let iteration_bound = 5_usize;
        let mut iterations = 0_usize;
        while remaining.simd_gt(zero).any_true() {
            iterations += 1;
            debug_assert!(
                iterations <= iteration_bound,
                "the constant-rate breakpoint proof bounds each advance"
            );
            let queue_positive = self.queue.simd_gt(zero);
            let service_rate = queue_positive.select(capacity, zero);
            let had_overdue = self.overdue.simd_gt(zero);
            let overdue_service = had_overdue.select(service_rate, zero);
            let on_time_service = had_overdue.select(zero, service_rate);
            let queue_rate = -service_rate;
            let overdue_rate = -overdue_service;
            let on_time_rate = -on_time_service;
            let credit_rate = on_time_service;
            let queue_crossing =
                positive_crossing_lanes::<S>(self.queue, queue_rate, zero, infinity);
            let overdue_crossing =
                positive_crossing_lanes::<S>(self.overdue, overdue_rate, zero, infinity);
            let on_time_crossing =
                positive_crossing_lanes::<S>(self.on_time, on_time_rate, zero, infinity);
            let credit_crossing =
                positive_crossing_lanes::<S>(self.completion_credit, credit_rate, zero, infinity);
            let crossing = queue_crossing
                .min(overdue_crossing)
                .min(on_time_crossing)
                .min(credit_crossing);
            let span = crossing.min(remaining);
            let queue_after = self.queue + queue_rate * span;
            let overdue_after = self.overdue + overdue_rate * span;
            queue_area += half * (self.queue + queue_after) * span;
            late_area += half * (self.overdue + overdue_after) * span;
            self.queue = queue_after;
            self.overdue = overdue_after;
            self.on_time += on_time_rate * span;
            self.completion_credit += credit_rate * span;
            let missed_rate = had_overdue.select(zero, overdue_rate.max(zero));
            self.missed += missed_rate * span;
            self.queue = queue_crossing.simd_eq(span).select(zero, self.queue);
            self.overdue = overdue_crossing.simd_eq(span).select(zero, self.overdue);
            self.on_time = on_time_crossing.simd_eq(span).select(zero, self.on_time);
            self.completion_credit = credit_crossing
                .simd_eq(span)
                .select(zero, self.completion_credit);
            remaining -= span;
        }
        DeadlineAdvanceLanes {
            queue_area,
            late_area,
        }
    }
}

struct DeadlineAdvanceLanes<S: Simd> {
    queue_area: S::f64s,
    late_area: S::f64s,
}

fn positive_crossing_lanes<S: Simd>(
    value: S::f64s,
    rate: S::f64s,
    zero: S::f64s,
    infinity: S::f64s,
) -> S::f64s {
    (value.simd_gt(zero) & rate.simd_lt(zero)).select(value / -rate, infinity)
}

fn positive_crossing(value: f64, rate: f64) -> f64 {
    if value > 0.0_f64 && rate < 0.0_f64 {
        value / -rate
    } else {
        f64::INFINITY
    }
}

fn edf_outcome(
    deadline: &DeadlineState,
    delay_area: f64,
    late_area: f64,
    terminal_capacity: f64,
    continuation_seconds: f64,
) -> EdfOutcome {
    let (terminal_late_area, drain_seconds) =
        terminal_closure(deadline.queue, terminal_capacity, continuation_seconds);
    EdfOutcome {
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_late_area,
        drain_seconds,
    }
}

/// Prices a constant-rate continuation over one finite planning horizon.
fn terminal_closure(queue: f64, capacity: f64, horizon_seconds: f64) -> (f64, f64) {
    if queue == 0.0_f64 {
        return (0.0_f64, 0.0_f64);
    }
    let drain_seconds = if capacity > 0.0_f64 {
        (queue / capacity).min(horizon_seconds)
    } else {
        horizon_seconds
    };
    let residual = (queue - capacity * drain_seconds).max(0.0_f64);
    let late_area = queue * drain_seconds - 0.5_f64 * capacity * drain_seconds * drain_seconds
        + residual * horizon_seconds;
    (late_area, drain_seconds)
}

impl SupplyTrajectory<'_> {
    #[cfg(test)]
    fn capacity_at_micros(&self, at_micros: u64) -> f64 {
        let mut capacity = self.initial;
        for event in 0..self.pause_micros.len() {
            let pause_micros = self.pause_micros[event];
            if at_micros < pause_micros {
                break;
            }
            capacity = if at_micros < self.ready_micros[event] {
                self.during[event]
            } else {
                self.after[event]
            };
        }
        capacity
    }

    #[cfg(test)]
    fn next_boundary_micros(&self, after_micros: u64) -> Option<u64> {
        self.pause_micros
            .iter()
            .chain(self.ready_micros)
            .copied()
            .filter(|boundary| *boundary > after_micros)
            .min()
    }
}

/// `SupplyTrajectory` owns sorted pause times, so equal pauses keep their input
/// order and the last tie wins. Its ready-boundary column is a sorted copy of
/// all ready times.
struct TrajectoryCursor<'a> {
    trajectory: &'a SupplyTrajectory<'a>,
    pause_count: usize,
    ready_boundary_count: usize,
}

impl<'a> TrajectoryCursor<'a> {
    const fn new(trajectory: &'a SupplyTrajectory<'a>) -> Self {
        Self {
            trajectory,
            pause_count: 0,
            ready_boundary_count: 0,
        }
    }

    fn advance_to(&mut self, at_micros: u64) {
        while self
            .trajectory
            .pause_micros
            .get(self.pause_count)
            .is_some_and(|pause| *pause <= at_micros)
        {
            self.pause_count += 1;
        }
        while self
            .trajectory
            .ready_boundaries
            .get(self.ready_boundary_count)
            .is_some_and(|ready| *ready <= at_micros)
        {
            self.ready_boundary_count += 1;
        }
    }

    fn capacity(&self, at_micros: u64) -> f64 {
        if self.pause_count == 0 {
            return self.trajectory.initial;
        }
        let event = self.pause_count - 1;
        if at_micros < self.trajectory.ready_micros[event] {
            self.trajectory.during[event]
        } else {
            self.trajectory.after[event]
        }
    }

    fn next_boundary_micros(&self) -> Option<u64> {
        self.trajectory
            .pause_micros
            .get(self.pause_count)
            .into_iter()
            .chain(
                self.trajectory
                    .ready_boundaries
                    .get(self.ready_boundary_count),
            )
            .copied()
            .min()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ArrivalPath<'a> {
    pub(crate) start_seconds: f64,
    pub(crate) end_seconds: &'a [f64],
    pub(crate) rates: &'a [f64],
}

impl ArrivalPath<'_> {
    pub(crate) fn integrated_count(&self, start: f64, end: f64) -> f64 {
        if end <= start {
            return 0.0_f64;
        }
        let relative_start = (start - self.start_seconds).max(0.0_f64);
        let relative_end = (end - self.start_seconds).max(0.0_f64);
        let mut segment_start = 0.0_f64;
        let mut count = 0.0_f64;
        for (&segment_end, &rate) in self.end_seconds.iter().zip(self.rates) {
            let overlap_start = relative_start.max(segment_start);
            let overlap_end = relative_end.min(segment_end);
            let overlap = (overlap_end - overlap_start).max(0.0_f64);
            count += rate * overlap;
            if segment_end >= relative_end {
                break;
            }
            segment_start = segment_end;
        }
        count
    }

    #[cfg(test)]
    fn rate_at(&self, at: f64) -> f64 {
        let relative = (at - self.start_seconds).max(0.0_f64);
        if self.end_seconds.last().is_none_or(|end| relative >= *end) {
            return 0.0_f64;
        }
        let index = self
            .end_seconds
            .partition_point(|end| relative >= *end)
            .min(self.rates.len().saturating_sub(1));
        self.rates.get(index).copied().map_or(0.0_f64, |rate| rate)
    }

    #[cfg(test)]
    fn next_boundary(&self, after: f64) -> Option<f64> {
        let relative = (after - self.start_seconds).max(0.0_f64);
        self.end_seconds
            .get(self.end_seconds.partition_point(|end| *end <= relative))
            .copied()
            .map(|end| self.start_seconds + end)
    }

    #[cfg(test)]
    fn deadline_rate_at(&self, at: f64, budget_seconds: f64) -> f64 {
        if at < self.start_seconds + budget_seconds {
            0.0_f64
        } else {
            self.rate_at(at - budget_seconds)
        }
    }

    #[cfg(test)]
    fn next_deadline_boundary(&self, after: f64, budget_seconds: f64) -> Option<f64> {
        let first = self.start_seconds + budget_seconds;
        if first > after {
            return Some(first);
        }
        let relative = after - self.start_seconds - budget_seconds;
        self.end_seconds
            .get(self.end_seconds.partition_point(|end| *end <= relative))
            .copied()
            .map(|end| self.start_seconds + end + budget_seconds)
    }
}

struct ArrivalCursor<'a> {
    path: &'a ArrivalPath<'a>,
    segment: usize,
    deadline_rate_segment: usize,
    deadline_boundary_segment: usize,
}

impl<'a> ArrivalCursor<'a> {
    const fn new(path: &'a ArrivalPath<'a>) -> Self {
        Self {
            path,
            segment: 0,
            deadline_rate_segment: 0,
            deadline_boundary_segment: 0,
        }
    }

    fn advance_to(&mut self, at: f64, budget_seconds: f64) {
        let relative = (at - self.path.start_seconds).max(0.0_f64);
        while self
            .path
            .end_seconds
            .get(self.segment)
            .is_some_and(|end| relative >= *end)
        {
            self.segment += 1;
        }
        if at < self.path.start_seconds + budget_seconds {
            return;
        }
        let deadline_rate_relative = ((at - budget_seconds) - self.path.start_seconds).max(0.0_f64);
        while self
            .path
            .end_seconds
            .get(self.deadline_rate_segment)
            .is_some_and(|end| deadline_rate_relative >= *end)
        {
            self.deadline_rate_segment += 1;
        }
        let deadline_boundary_relative = at - self.path.start_seconds - budget_seconds;
        while self
            .path
            .end_seconds
            .get(self.deadline_boundary_segment)
            .is_some_and(|end| deadline_boundary_relative >= *end)
        {
            self.deadline_boundary_segment += 1;
        }
    }

    fn rate(&self, at: f64) -> f64 {
        let relative = (at - self.path.start_seconds).max(0.0_f64);
        if self
            .path
            .end_seconds
            .last()
            .is_none_or(|end| relative >= *end)
        {
            return 0.0_f64;
        }
        self.path
            .rates
            .get(self.segment.min(self.path.rates.len().saturating_sub(1)))
            .copied()
            .map_or(0.0_f64, |rate| rate)
    }

    fn next_boundary(&self) -> Option<f64> {
        self.path
            .end_seconds
            .get(self.segment)
            .copied()
            .map(|end| self.path.start_seconds + end)
    }

    fn deadline_rate(&self, at: f64, budget_seconds: f64) -> f64 {
        if at < self.path.start_seconds + budget_seconds {
            0.0_f64
        } else {
            let relative = ((at - budget_seconds) - self.path.start_seconds).max(0.0_f64);
            if self
                .path
                .end_seconds
                .last()
                .is_none_or(|end| relative >= *end)
            {
                return 0.0_f64;
            }
            self.path
                .rates
                .get(
                    self.deadline_rate_segment
                        .min(self.path.rates.len().saturating_sub(1)),
                )
                .copied()
                .map_or(0.0_f64, |rate| rate)
        }
    }

    fn next_deadline_boundary(&self, at: f64, budget_seconds: f64) -> Option<f64> {
        let first = self.path.start_seconds + budget_seconds;
        if first > at {
            return Some(first);
        }
        self.path
            .end_seconds
            .get(self.deadline_boundary_segment)
            .copied()
            .map(|end| self.path.start_seconds + end + budget_seconds)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EdfOutcome {
    pub(crate) delay_area: f64,
    pub(crate) missed_work: f64,
    pub(crate) late_area: f64,
    pub(crate) terminal_late_area: f64,
    pub(crate) drain_seconds: f64,
}

pub(crate) fn prepare<Unit>(cohorts: &WorkCohorts<Unit>, scratch: &mut EdfScratch) {
    assert!(
        cohorts.len() <= scratch.release_order.capacity(),
        "cohorts must fit the release-order scratch"
    );
    scratch.release_order.clear();
    for index in 0..cohorts.len() {
        scratch.release_order.push(index as u32);
    }
    scratch.release_order.sort_unstable_by_key(|&index| {
        let index = index as usize;
        (
            cohorts.release_micros(index),
            cohorts.deadline_micros(index),
        )
    });
    scratch.deadline_order.clear();
    scratch
        .deadline_order
        .extend_from_slice(&scratch.release_order);
    scratch.deadline_order.sort_unstable_by_key(|&index| {
        let index = index as usize;
        (
            cohorts.deadline_micros(index),
            cohorts.release_micros(index),
        )
    });
    scratch.prepared_len = cohorts.len();
    scratch.common_cohort = common_cohort(cohorts);
}

fn common_cohort<Unit>(cohorts: &WorkCohorts<Unit>) -> Option<CommonCohort> {
    if cohorts.is_empty() {
        return None;
    }
    let release_micros = cohorts.release_micros(0);
    let deadline_micros = cohorts.deadline_micros(0);
    let mut work = 0.0_f64;
    for index in 0..cohorts.len() {
        if cohorts.release_micros(index) != release_micros
            || cohorts.deadline_micros(index) != deadline_micros
        {
            return None;
        }
        let cohort_work = cohorts.work(index);
        work += cohort_work;
    }
    Some(CommonCohort {
        release_micros,
        deadline_micros,
        work,
    })
}

fn deadline_work<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    cursor: &mut usize,
    now_micros: u64,
) -> f64 {
    let mut work = 0.0_f64;
    while *cursor < scratch.deadline_order.len() {
        let cohort = scratch.deadline_order[*cursor] as usize;
        if cohorts.deadline_micros(cohort) > now_micros {
            break;
        }
        work += cohorts.work(cohort);
        *cursor += 1;
    }
    work
}

fn release_work<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    cursor: &mut usize,
    now_micros: u64,
) -> f64 {
    let mut work = 0.0_f64;
    while *cursor < scratch.release_order.len() {
        let cohort = scratch.release_order[*cursor] as usize;
        if cohorts.release_micros(cohort) > now_micros {
            break;
        }
        work += cohorts.work(cohort);
        *cursor += 1;
    }
    work
}

fn next_release_micros<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    cursor: usize,
) -> u64 {
    scratch
        .release_order
        .get(cursor)
        .map_or(u64::MAX, |&cohort| cohorts.release_micros(cohort as usize))
}

fn next_deadline_micros<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    cursor: usize,
) -> u64 {
    scratch
        .deadline_order
        .get(cursor)
        .map_or(u64::MAX, |&cohort| cohorts.deadline_micros(cohort as usize))
}

pub(crate) fn evaluate_prepared_step<Unit>(
    cohorts: &WorkCohorts<Unit>,
    supply: SupplyStep,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    assert!(
        cohorts.is_valid_at(window.start_micros),
        "invalid cohort order"
    );
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = window.start_micros;
    let mut deadline = DeadlineState::new(window.initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
    let budget_seconds = Duration::from_micros(window.deadline_budget_micros).as_secs_f64();
    let mut arrival_cursor = ArrivalCursor::new(future_arrivals);
    while now_micros < window.horizon_micros {
        deadline.release(release_work(
            cohorts,
            scratch,
            &mut release_cursor,
            now_micros,
        ));
        deadline.make_due(deadline_work(
            cohorts,
            scratch,
            &mut deadline_cursor,
            now_micros,
        ));
        let mut next_micros = window.horizon_micros;
        next_micros = next_micros.min(next_release_micros(cohorts, scratch, release_cursor));
        next_micros = next_micros.min(next_deadline_micros(cohorts, scratch, deadline_cursor));
        if supply.pause_micros > now_micros {
            next_micros = next_micros.min(supply.pause_micros);
        }
        if supply.ready_micros > now_micros {
            next_micros = next_micros.min(supply.ready_micros);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        arrival_cursor.advance_to(now_seconds, budget_seconds);
        if let Some(boundary) = arrival_cursor.next_boundary() {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if let Some(boundary) = arrival_cursor.next_deadline_boundary(now_seconds, budget_seconds) {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = supply.capacity_at(now_micros);
        let advance = deadline.advance(
            duration_seconds,
            capacity,
            arrival_cursor.rate(now_seconds),
            arrival_cursor.deadline_rate(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        now_micros = next_micros;
    }
    deadline.release(release_work(
        cohorts,
        scratch,
        &mut release_cursor,
        now_micros,
    ));
    deadline.make_due(deadline_work(
        cohorts,
        scratch,
        &mut deadline_cursor,
        now_micros,
    ));
    let terminal_capacity = supply.capacity_at(window.horizon_micros);
    edf_outcome(
        &deadline,
        delay_area,
        late_area,
        terminal_capacity,
        Duration::from_micros(window.horizon_micros.saturating_sub(window.start_micros))
            .as_secs_f64(),
    )
}

/// Evaluates one constant-capacity scenario in each SIMD lane.
///
/// All lanes share cohorts, times, zero arrivals, and zero initial debt. Each
/// lane supplies one constant capacity for the complete window.
pub(crate) fn evaluate_prepared_step_capacities<S: Simd, Unit>(
    simd: S,
    cohorts: &WorkCohorts<Unit>,
    capacities: S::f64s,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &EdfScratch,
) -> EdfOutcomeLanes<S> {
    assert!(
        cohorts.is_valid_at(window.start_micros),
        "invalid cohort order"
    );
    assert!(
        window.initial_debt_work.to_bits().trailing_zeros() >= 63,
        "batched EDF requires zero initial debt"
    );
    assert!(
        future_arrivals
            .rates
            .iter()
            .all(|rate| rate.to_bits().trailing_zeros() >= 63),
        "batched EDF requires zero arrivals"
    );
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = window.start_micros;
    let mut deadline = DeadlineStateLanes::new(simd);
    let zero = S::f64s::splat(simd, 0.0_f64);
    let mut delay_area = zero;
    let mut late_area = zero;
    let budget_seconds = Duration::from_micros(window.deadline_budget_micros).as_secs_f64();
    let mut arrival_cursor = ArrivalCursor::new(future_arrivals);
    while now_micros < window.horizon_micros {
        deadline.release(release_work(
            cohorts,
            scratch,
            &mut release_cursor,
            now_micros,
        ));
        deadline.make_due(
            simd,
            deadline_work(cohorts, scratch, &mut deadline_cursor, now_micros),
        );
        let mut next_micros = window.horizon_micros;
        next_micros = next_micros.min(next_release_micros(cohorts, scratch, release_cursor));
        next_micros = next_micros.min(next_deadline_micros(cohorts, scratch, deadline_cursor));
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        arrival_cursor.advance_to(now_seconds, budget_seconds);
        if let Some(boundary) = arrival_cursor.next_boundary() {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if let Some(boundary) = arrival_cursor.next_deadline_boundary(now_seconds, budget_seconds) {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let advance = deadline.advance(simd, duration_seconds, capacities);
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        now_micros = next_micros;
    }
    deadline.release(release_work(
        cohorts,
        scratch,
        &mut release_cursor,
        now_micros,
    ));
    deadline.make_due(
        simd,
        deadline_work(cohorts, scratch, &mut deadline_cursor, now_micros),
    );
    edf_outcome_lanes(
        simd,
        &deadline,
        delay_area,
        late_area,
        capacities,
        Duration::from_micros(window.horizon_micros.saturating_sub(window.start_micros))
            .as_secs_f64(),
    )
}

fn edf_outcome_lanes<S: Simd>(
    simd: S,
    deadline: &DeadlineStateLanes<S>,
    delay_area: S::f64s,
    late_area: S::f64s,
    terminal_capacity: S::f64s,
    continuation_seconds: f64,
) -> EdfOutcomeLanes<S> {
    let zero = S::f64s::splat(simd, 0.0_f64);
    let half = S::f64s::splat(simd, 0.5_f64);
    let one = S::f64s::splat(simd, 1.0_f64);
    let horizon = S::f64s::splat(simd, continuation_seconds);
    let queue_zero = deadline.queue.simd_eq(zero);
    let capacity_positive = terminal_capacity.simd_gt(zero);
    let safe_capacity = capacity_positive.select(terminal_capacity, one);
    let positive_drain = (deadline.queue / safe_capacity).min(horizon);
    let drain_seconds = queue_zero.select(zero, capacity_positive.select(positive_drain, horizon));
    let residual = (deadline.queue - terminal_capacity * drain_seconds).max(zero);
    let terminal_late_area = queue_zero.select(
        zero,
        deadline.queue * drain_seconds - half * terminal_capacity * drain_seconds * drain_seconds
            + residual * horizon,
    );
    EdfOutcomeLanes {
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_late_area,
        drain_seconds,
    }
}

/// Clamps one event step to the next supply or arrival boundary.
fn shared_boundary_micros(
    trajectory: &TrajectoryCursor<'_>,
    future_arrivals: &ArrivalCursor<'_>,
    now_micros: u64,
    budget_seconds: f64,
    mut next_micros: u64,
) -> u64 {
    if let Some(boundary) = trajectory.next_boundary_micros() {
        next_micros = next_micros.min(boundary);
    }
    let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
    if let Some(boundary) = future_arrivals.next_boundary() {
        next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
    }
    if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds) {
        next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
    }
    next_micros
}

#[cfg_attr(
    feature = "hotpath",
    hotpath::measure(label = "evaluate_prepared_trajectory")
)]
pub(crate) fn evaluate_prepared_trajectory<Unit>(
    cohorts: &WorkCohorts<Unit>,
    trajectory: &SupplyTrajectory<'_>,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    assert!(
        cohorts.is_valid_at(window.start_micros),
        "invalid cohort order"
    );
    if let Some(common) = scratch.common_cohort {
        return evaluate_common_trajectory(common, trajectory, window, future_arrivals);
    }
    evaluate_general_trajectory(cohorts, trajectory, window, future_arrivals, scratch)
}

fn evaluate_general_trajectory<Unit>(
    cohorts: &WorkCohorts<Unit>,
    trajectory: &SupplyTrajectory<'_>,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    let EvaluationWindow {
        start_micros,
        horizon_micros,
        initial_debt_work,
        deadline_budget_micros,
    } = window;
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    let mut trajectory_cursor = TrajectoryCursor::new(trajectory);
    let mut arrival_cursor = ArrivalCursor::new(future_arrivals);
    while now_micros < horizon_micros {
        deadline.release(release_work(
            cohorts,
            scratch,
            &mut release_cursor,
            now_micros,
        ));
        deadline.make_due(deadline_work(
            cohorts,
            scratch,
            &mut deadline_cursor,
            now_micros,
        ));
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(next_release_micros(cohorts, scratch, release_cursor));
        next_micros = next_micros.min(next_deadline_micros(cohorts, scratch, deadline_cursor));
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        trajectory_cursor.advance_to(now_micros);
        arrival_cursor.advance_to(now_seconds, budget_seconds);
        let next_micros = shared_boundary_micros(
            &trajectory_cursor,
            &arrival_cursor,
            now_micros,
            budget_seconds,
            next_micros,
        );
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory_cursor.capacity(now_micros);
        let advance = deadline.advance(
            duration_seconds,
            capacity,
            arrival_cursor.rate(now_seconds),
            arrival_cursor.deadline_rate(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        now_micros = next_micros;
    }
    deadline.release(release_work(
        cohorts,
        scratch,
        &mut release_cursor,
        now_micros,
    ));
    deadline.make_due(deadline_work(
        cohorts,
        scratch,
        &mut deadline_cursor,
        now_micros,
    ));
    trajectory_cursor.advance_to(horizon_micros);
    edf_outcome(
        &deadline,
        delay_area,
        late_area,
        trajectory_cursor.capacity(horizon_micros),
        Duration::from_micros(horizon_micros.saturating_sub(start_micros)).as_secs_f64(),
    )
}

#[cfg(test)]
fn evaluate_general_trajectory_reference<Unit>(
    cohorts: &WorkCohorts<Unit>,
    trajectory: &SupplyTrajectory<'_>,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    let EvaluationWindow {
        start_micros,
        horizon_micros,
        initial_debt_work,
        deadline_budget_micros,
    } = window;
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    while now_micros < horizon_micros {
        deadline.release(release_work(
            cohorts,
            scratch,
            &mut release_cursor,
            now_micros,
        ));
        deadline.make_due(deadline_work(
            cohorts,
            scratch,
            &mut deadline_cursor,
            now_micros,
        ));
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(next_release_micros(cohorts, scratch, release_cursor));
        next_micros = next_micros.min(next_deadline_micros(cohorts, scratch, deadline_cursor));
        if let Some(boundary) = trajectory.next_boundary_micros(now_micros) {
            next_micros = next_micros.min(boundary);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds)
        {
            next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory.capacity_at_micros(now_micros);
        let advance = deadline.advance(
            duration_seconds,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        now_micros = next_micros;
    }
    deadline.release(release_work(
        cohorts,
        scratch,
        &mut release_cursor,
        now_micros,
    ));
    deadline.make_due(deadline_work(
        cohorts,
        scratch,
        &mut deadline_cursor,
        now_micros,
    ));
    edf_outcome(
        &deadline,
        delay_area,
        late_area,
        trajectory.capacity_at_micros(horizon_micros),
        Duration::from_micros(horizon_micros.saturating_sub(start_micros)).as_secs_f64(),
    )
}

fn evaluate_common_trajectory(
    cohort: CommonCohort,
    trajectory: &SupplyTrajectory<'_>,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
) -> EdfOutcome {
    let EvaluationWindow {
        start_micros,
        horizon_micros,
        initial_debt_work,
        deadline_budget_micros,
    } = window;
    let mut now_micros = start_micros;
    let mut state = CommonState {
        released: false,
        expired: false,
    };
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    let mut trajectory_cursor = TrajectoryCursor::new(trajectory);
    let mut arrival_cursor = ArrivalCursor::new(future_arrivals);
    while now_micros < horizon_micros {
        let (released, due) = update_common_boundaries(cohort, now_micros, &mut state);
        deadline.release(released);
        deadline.make_due(due);
        let mut next_micros = horizon_micros;
        if !state.released {
            next_micros = next_micros.min(cohort.release_micros);
        }
        if state.released && !state.expired {
            next_micros = next_micros.min(cohort.deadline_micros);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        trajectory_cursor.advance_to(now_micros);
        arrival_cursor.advance_to(now_seconds, budget_seconds);
        let next_micros = shared_boundary_micros(
            &trajectory_cursor,
            &arrival_cursor,
            now_micros,
            budget_seconds,
            next_micros,
        );
        if next_micros <= now_micros {
            break;
        }
        let duration = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory_cursor.capacity(now_micros);
        let advance = deadline.advance(
            duration,
            capacity,
            arrival_cursor.rate(now_seconds),
            arrival_cursor.deadline_rate(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        now_micros = next_micros;
    }
    let (released, due) = update_common_boundaries(cohort, now_micros, &mut state);
    deadline.release(released);
    deadline.make_due(due);
    trajectory_cursor.advance_to(horizon_micros);
    edf_outcome(
        &deadline,
        delay_area,
        late_area,
        trajectory_cursor.capacity(horizon_micros),
        Duration::from_micros(horizon_micros.saturating_sub(start_micros)).as_secs_f64(),
    )
}

fn update_common_boundaries(
    cohort: CommonCohort,
    now_micros: u64,
    state: &mut CommonState,
) -> (f64, f64) {
    let mut released = 0.0_f64;
    let mut due = 0.0_f64;
    if !state.released && cohort.release_micros <= now_micros {
        released = cohort.work;
        state.released = true;
    }
    if state.released && !state.expired && cohort.deadline_micros <= now_micros {
        due = cohort.work;
        state.expired = true;
    }
    (released, due)
}

#[cfg(test)]
pub(crate) fn required_capacity_prepared<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &mut EdfScratch,
) -> f64 {
    if cohorts.is_empty() {
        return 0.0_f64;
    }
    assert_eq!(
        cohorts.len(),
        scratch.prepared_len,
        "prepare the EDF order for these cohorts"
    );
    if let Some(common) = scratch.common_cohort {
        let seconds =
            Duration::from_micros(common.deadline_micros - common.release_micros).as_secs_f64();
        return common.work / seconds;
    }

    // Preemptive EDF is feasible when each release-to-deadline interval has enough
    // capacity.
    let mut required = 0.0_f64;
    for release_position in 0..cohorts.len() {
        let release = cohorts.release_micros(scratch.release_order[release_position] as usize);
        if release_position > 0
            && release
                == cohorts.release_micros(scratch.release_order[release_position - 1] as usize)
        {
            continue;
        }
        let mut work = 0.0_f64;
        for &cohort_index in &scratch.deadline_order {
            let cohort = cohort_index as usize;
            if cohorts.release_micros(cohort) < release {
                continue;
            }
            work += cohorts.work(cohort);
            let interval =
                Duration::from_micros(cohorts.deadline_micros(cohort) - release).as_secs_f64();
            required = required.max(work / interval);
        }
    }
    required
}

fn seconds_to_micros_ceil(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64).ceil() as u64
}

#[cfg(test)]
mod tests {
    use std::iter::repeat;

    use fearless_simd::{Level, Simd, dispatch, prelude::*};
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use super::{
        ArrivalPath, DeadlineState, EdfScratch, EvaluationWindow, SupplyStep, SupplyTrajectory,
        TrajectoryCursor, evaluate_general_trajectory, evaluate_general_trajectory_reference,
        evaluate_prepared_step, evaluate_prepared_step_capacities, evaluate_prepared_trajectory,
        prepare, terminal_closure,
    };
    use crate::types::SlotSecondCohorts;

    const NO_FUTURE_ARRIVALS: ArrivalPath<'static> = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };

    #[quickcheck]
    fn batched_step_matches_each_scalar_lane(
        release_seeds: Vec<u16>,
        deadline_seeds: Vec<u16>,
        work_seeds: Vec<u16>,
        capacity_seed: u64,
        budget_seed: u16,
    ) -> TestResult {
        let count = release_seeds.len().clamp(1, 16);
        let releases = release_seeds.into_iter().chain(repeat(0));
        let deadlines = deadline_seeds.into_iter().chain(repeat(0));
        let works = work_seeds.into_iter().chain(repeat(1));
        let mut cohorts = SlotSecondCohorts::new(count);
        let mut horizon_micros = 1_u64;
        for (index, ((release_seed, deadline_seed), work_seed)) in
            releases.zip(deadlines).zip(works).take(count).enumerate()
        {
            let release_micros = u64::from(release_seed % 2_000) * 1_000;
            let deadline_micros = release_micros + u64::from(deadline_seed % 2_000 + 1) * 1_000;
            let work = f64::from(work_seed % 1_000 + 1) / 10.0_f64;
            let partition = u32::try_from(index).map_or(u32::MAX, |value| value);
            cohorts.push_values(release_micros, deadline_micros, work, partition);
            horizon_micros = horizon_micros.max(deadline_micros + 1_000_000);
        }
        let window = EvaluationWindow {
            start_micros: 0,
            horizon_micros,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: u64::from(budget_seed % 2_000 + 1) * 1_000,
        };
        let Ok(count_u32) = u32::try_from(count) else {
            return TestResult::error("the cohort count exceeded u32");
        };
        let Ok(mut scratch) = EdfScratch::new(count_u32) else {
            return TestResult::error("EDF scratch rejected a bounded cohort count");
        };
        prepare(&cohorts, &mut scratch);
        TestResult::from_bool(dispatch!(Level::new(), simd => batch_step_matches_scalar(
            simd,
            &cohorts,
            &mut scratch,
            window,
            capacity_seed,
        )))
    }

    fn batch_step_matches_scalar<S: Simd>(
        simd: S,
        cohorts: &SlotSecondCohorts,
        scratch: &mut EdfScratch,
        window: EvaluationWindow,
        capacity_seed: u64,
    ) -> bool {
        let capacities = S::f64s::from_fn(simd, |lane| {
            let shift = u32::try_from((lane % 8) * 8).map_or(0, |value| value);
            let rotated = capacity_seed.rotate_left(shift) & u64::from(u16::MAX);
            let seed = u16::try_from(rotated).map_or(u16::MAX, |value| value);
            f64::from(seed % 2_000 + 1) / 100.0_f64
        });
        let batch = evaluate_prepared_step_capacities(
            simd,
            cohorts,
            capacities,
            window,
            &NO_FUTURE_ARRIVALS,
            scratch,
        );
        for (lane, &capacity) in capacities.as_slice().iter().enumerate() {
            let scalar = evaluate_prepared_step(
                cohorts,
                SupplyStep {
                    before: capacity,
                    during: capacity,
                    after: capacity,
                    pause_micros: window.start_micros,
                    ready_micros: window.start_micros,
                },
                window,
                &NO_FUTURE_ARRIVALS,
                scratch,
            );
            if !edf_float_matches(batch.delay_area.as_slice()[lane], scalar.delay_area)
                || !edf_float_matches(batch.missed_work.as_slice()[lane], scalar.missed_work)
                || !edf_float_matches(batch.late_area.as_slice()[lane], scalar.late_area)
                || !edf_float_matches(
                    batch.terminal_late_area.as_slice()[lane],
                    scalar.terminal_late_area,
                )
                || !edf_float_matches(batch.drain_seconds.as_slice()[lane], scalar.drain_seconds)
            {
                return false;
            }
        }
        true
    }

    fn edf_float_matches(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
    }

    #[test]
    fn supply_trajectory_keeps_exact_microsecond_boundaries() {
        let trajectory = SupplyTrajectory {
            initial: 3.0_f64,
            pause_micros: &[500_001],
            ready_micros: &[500_003],
            ready_boundaries: &[500_003],
            during: &[2.0_f64],
            after: &[4.0_f64],
        };

        assert_eq!(
            trajectory.capacity_at_micros(500_000).to_bits(),
            3.0_f64.to_bits()
        );
        assert_eq!(
            trajectory.capacity_at_micros(500_001).to_bits(),
            2.0_f64.to_bits()
        );
        assert_eq!(
            trajectory.capacity_at_micros(500_003).to_bits(),
            4.0_f64.to_bits()
        );
    }

    #[test]
    fn trajectory_cursor_keeps_last_equal_pause() {
        let trajectory = SupplyTrajectory {
            initial: 1.0_f64,
            pause_micros: &[500_000, 500_000],
            ready_micros: &[900_000, 900_000],
            ready_boundaries: &[900_000, 900_000],
            during: &[2.0_f64, 3.0_f64],
            after: &[4.0_f64, 5.0_f64],
        };
        let mut cursor = TrajectoryCursor::new(&trajectory);
        cursor.advance_to(500_000);

        assert_eq!(cursor.capacity(500_000).to_bits(), 3.0_f64.to_bits());
        assert!(cursor_solver_matches_reference(
            &trajectory,
            &NO_FUTURE_ARRIVALS
        ));
    }

    #[quickcheck]
    fn common_cohort_trajectory_matches_general_edf(
        count_seed: u8,
        work_seed: u16,
        debt_seed: u8,
        supply_seed: u8,
    ) -> bool {
        let count = usize::from(count_seed % 8 + 1);
        let work = f64::from(work_seed % 1_000) / 10.0_f64;
        let debt = f64::from(debt_seed);
        let supply = f64::from(supply_seed) + 1.0_f64;
        let mut cohorts = SlotSecondCohorts::new(count);
        for partition in 0..count {
            cohorts.push_values(250_000, 1_500_000, work, partition as u32);
        }
        let trajectory = SupplyTrajectory {
            initial: supply,
            pause_micros: &[500_000_u64],
            ready_micros: &[1_000_000_u64],
            ready_boundaries: &[1_000_000_u64],
            during: &[supply * 0.5_f64],
            after: &[supply * 1.5_f64],
        };
        let Ok(mut scratch) = EdfScratch::new(count as u32) else {
            return false;
        };
        prepare(&cohorts, &mut scratch);
        let window = EvaluationWindow {
            start_micros: 0,
            horizon_micros: 2_000_000,
            initial_debt_work: debt,
            deadline_budget_micros: 1_500_000,
        };
        let fast = evaluate_prepared_trajectory(
            &cohorts,
            &trajectory,
            window,
            &NO_FUTURE_ARRIVALS,
            &mut scratch,
        );
        let general = evaluate_general_trajectory(
            &cohorts,
            &trajectory,
            window,
            &NO_FUTURE_ARRIVALS,
            &mut scratch,
        );

        let matches = close_relative(fast.delay_area, general.delay_area)
            && close_relative(fast.drain_seconds, general.drain_seconds);
        assert!(matches, "fast={fast:?}, general={general:?}");
        true
    }

    #[quickcheck]
    fn trajectory_cursor_matches_search_solver(
        pause_seeds: Vec<u8>,
        ready_seeds: Vec<u8>,
        supply_seed: u8,
    ) -> bool {
        let count = pause_seeds.len().clamp(1, 8);
        let mut pause_micros = Vec::with_capacity(count);
        let mut ready_micros = Vec::with_capacity(count);
        let mut during = Vec::with_capacity(count);
        let mut after = Vec::with_capacity(count);
        let mut pause = 100_000_u64;
        let pause_seeds = pause_seeds.into_iter().chain(repeat(1));
        let ready_seeds = ready_seeds.into_iter().chain(repeat(1));
        for (pause_seed, ready_seed) in pause_seeds.zip(ready_seeds).take(count) {
            pause = pause.saturating_add(u64::from(pause_seed % 20 + 1) * 10_000);
            pause_micros.push(pause);
            ready_micros.push(pause.saturating_add(u64::from(ready_seed) * 5_000));
            during.push(f64::from(supply_seed % 20 + 1));
            after.push(f64::from(ready_seed % 20 + 1));
        }
        let mut ready_boundaries = ready_micros.clone();
        ready_boundaries.sort_unstable();
        let trajectory = SupplyTrajectory {
            initial: f64::from(supply_seed % 20 + 1),
            pause_micros: &pause_micros,
            ready_micros: &ready_micros,
            ready_boundaries: &ready_boundaries,
            during: &during,
            after: &after,
        };
        cursor_solver_matches_reference(&trajectory, &NO_FUTURE_ARRIVALS)
    }

    #[quickcheck]
    fn arrival_cursor_matches_search_solver(end_seeds: Vec<u8>, rate_seeds: Vec<u8>) -> bool {
        let count = end_seeds.len().clamp(1, 8);
        let mut end_seconds = Vec::with_capacity(count);
        let mut rates = Vec::with_capacity(count);
        let mut end = 0.0_f64;
        let end_seeds = end_seeds.into_iter().chain(repeat(1));
        let rate_seeds = rate_seeds.into_iter().chain(repeat(1));
        for (end_seed, rate_seed) in end_seeds.zip(rate_seeds).take(count) {
            end += f64::from(end_seed % 20 + 1) * 0.05_f64;
            end_seconds.push(end);
            rates.push(f64::from(rate_seed % 20));
        }
        let future_arrivals = ArrivalPath {
            start_seconds: 0.125_f64,
            end_seconds: &end_seconds,
            rates: &rates,
        };
        let trajectory = SupplyTrajectory {
            initial: 7.0_f64,
            pause_micros: &[],
            ready_micros: &[],
            ready_boundaries: &[],
            during: &[],
            after: &[],
        };
        cursor_solver_matches_reference(&trajectory, &future_arrivals)
    }

    fn cursor_solver_matches_reference(
        trajectory: &SupplyTrajectory<'_>,
        future_arrivals: &ArrivalPath<'_>,
    ) -> bool {
        let mut cohorts = SlotSecondCohorts::new(2);
        cohorts.push_values(50_000, 1_500_000, 3.0_f64, 0);
        cohorts.push_values(200_000, 2_500_000, 5.0_f64, 1);
        let Ok(mut scratch) = EdfScratch::new(2) else {
            return false;
        };
        prepare(&cohorts, &mut scratch);
        let window = EvaluationWindow {
            start_micros: 0,
            horizon_micros: 3_000_000,
            initial_debt_work: 2.0_f64,
            deadline_budget_micros: 400_000,
        };
        let cursor = evaluate_general_trajectory(
            &cohorts,
            trajectory,
            window,
            future_arrivals,
            &mut scratch,
        );
        let reference = evaluate_general_trajectory_reference(
            &cohorts,
            trajectory,
            window,
            future_arrivals,
            &mut scratch,
        );
        cursor == reference
    }

    fn close_relative(left: f64, right: f64) -> bool {
        let scale = left.abs().max(right.abs()).max(1.0_f64);
        (left - right).abs() <= 1.0e-12_f64 * scale
    }

    #[test]
    fn terminal_triangle_matches_closed_form_cases() {
        // q=12 and c=3 give T=q/c=4 and V=q*T/2=24.
        assert_eq!(
            terminal_closure(12.0_f64, 3.0_f64, 10.0_f64),
            (24.0_f64, 4.0_f64)
        );
        // An empty queue has zero continuation for every terminal capacity.
        assert_eq!(
            terminal_closure(0.0_f64, 0.0_f64, 10.0_f64),
            (0.0_f64, 0.0_f64)
        );
        assert_eq!(
            terminal_closure(1.0_f64, 0.0_f64, 10.0_f64),
            (20.0_f64, 10.0_f64)
        );
    }

    /// The debt remainder sits between the ledger error bound and the span
    /// resolution of a large capacity. A span-epsilon loop stalls here; the
    /// breakpoint passes resolve it exactly.
    #[test]
    fn advance_terminates_on_a_debt_sliver() {
        let mut state = DeadlineState::new(0.5_f64);
        state.overdue = 1.0e-12_f64;
        state.queue = state.overdue;
        let advanced = state.advance(1.0_f64, 1.0e6_f64, 0.0_f64, 0.0_f64);

        assert!(state.overdue.total_cmp(&0.0_f64).is_eq());
        assert!(advanced.queue_area >= 0.0_f64);
    }

    #[test]
    fn excess_due_vanishes_while_debt_drains() {
        let mut state = DeadlineState::new(5.0_f64);

        let advanced = state.advance(10.0_f64, 1.0_f64, 0.0_f64, 3.0_f64);

        assert!(state.on_time.total_cmp(&0.0_f64).is_eq());
        assert!(state.queue.total_cmp(&0.0_f64).is_eq());
        assert!(state.overdue.total_cmp(&0.0_f64).is_eq());
        assert!(state.missed.total_cmp(&0.0_f64).is_eq());
        assert!(advanced.late_area.total_cmp(&12.5_f64).is_eq());
    }

    #[test]
    fn due_spike_cannot_create_phantom_overdue_work() {
        let mut state = DeadlineState::new(0.0_f64);
        state.release(5.0_f64);

        let advanced = state.advance(4.0_f64, 0.0_f64, 1.0_f64, 3.0_f64);

        assert!(state.on_time.total_cmp(&0.0_f64).is_eq());
        assert!(state.queue.total_cmp(&9.0_f64).is_eq());
        assert!(state.overdue.total_cmp(&9.0_f64).is_eq());
        assert!(state.missed.total_cmp(&9.0_f64).is_eq());
        assert!(advanced.late_area.total_cmp(&21.75_f64).is_eq());
    }

    #[test]
    fn late_area_accrues_after_one_budget() {
        let mut state = DeadlineState::new(0.0_f64);
        state.release(10.0_f64);
        state.make_due(10.0_f64);

        let within_budget = state.advance(2.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
        let after_budget = state.advance(5.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);

        assert!(within_budget.late_area.total_cmp(&20.0_f64).is_eq());
        assert!(after_budget.late_area.total_cmp(&50.0_f64).is_eq());
    }
}
