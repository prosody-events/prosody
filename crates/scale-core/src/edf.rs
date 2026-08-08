use std::time::Duration;

#[cfg(test)]
use fearless_simd::{Level, dispatch};
use fearless_simd::{Simd, prelude::*};

use crate::types::WorkCohorts;

pub(crate) struct EdfScratch {
    release_order: Vec<u32>,
    deadline_order: Vec<u32>,
    heap: Vec<u32>,
    remaining: Vec<f64>,
    prepared_len: usize,
    common_cohort: Option<CommonCohort>,
    ordered_deadlines: bool,
}

impl EdfScratch {
    pub(crate) fn has_common_interval(&self) -> bool {
        self.common_cohort.is_some()
    }
}

#[derive(Clone, Copy)]
struct CommonCohort {
    release_micros: u64,
    deadline_micros: u64,
    work: f64,
    last_positive_work: f64,
}

struct CommonState {
    late_work: f64,
    on_time_work: f64,
    shortfall: f64,
    released: bool,
    expired: bool,
}

pub(crate) struct SupplyTrajectory<'a> {
    pub(crate) initial: f64,
    pub(crate) pause_seconds: &'a [f64],
    pub(crate) ready_seconds: &'a [f64],
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

#[derive(Clone, Copy)]
pub(crate) struct EvaluationWindow {
    pub(crate) start_micros: u64,
    pub(crate) horizon_micros: u64,
    pub(crate) initial_debt_work: f64,
    pub(crate) deadline_budget_micros: u64,
}

struct DeadlineState {
    queue: f64,
    initial_debt: f64,
    completed: f64,
    released: f64,
    due: f64,
    shortfall: f64,
}

impl DeadlineState {
    fn new(initial_debt: f64) -> Self {
        Self {
            queue: initial_debt,
            initial_debt,
            completed: 0.0_f64,
            released: 0.0_f64,
            due: 0.0_f64,
            shortfall: 0.0_f64,
        }
    }

    fn release(&mut self, work: f64) {
        self.queue += work;
        self.released += work;
    }

    fn make_due(&mut self, work: f64) {
        self.due += work;
        self.update_shortfall();
    }

    fn advance(&mut self, duration: f64, capacity: f64, arrival_rate: f64, due_rate: f64) -> f64 {
        let mut remaining = duration;
        let mut area = 0.0_f64;
        while remaining > f64::EPSILON {
            let service_rate = if self.queue > f64::EPSILON {
                capacity
            } else {
                capacity.min(arrival_rate)
            };
            let net_rate = arrival_rate - service_rate;
            let mut span = remaining;
            if net_rate < -f64::EPSILON {
                span = span.min(self.queue / -net_rate);
            }
            if self.completed < self.initial_debt && service_rate > f64::EPSILON {
                span = span.min((self.initial_debt - self.completed) / service_rate);
            }
            if span <= f64::EPSILON {
                if self.queue <= f64::EPSILON * capacity.max(1.0_f64) {
                    self.queue = 0.0_f64;
                }
                if self.initial_debt - self.completed
                    <= f64::EPSILON * self.initial_debt.max(1.0_f64)
                {
                    self.completed = self.initial_debt.max(self.completed);
                }
                continue;
            }
            area += self.queue * span + 0.5_f64 * net_rate * span * span;
            self.queue = (self.queue + net_rate * span).max(0.0_f64);
            self.completed += service_rate * span;
            self.released += arrival_rate * span;
            self.due += due_rate * span;
            self.update_shortfall();
            remaining -= span;
        }
        area
    }

    fn update_shortfall(&mut self) {
        let actionable_completed = (self.completed - self.initial_debt)
            .max(0.0_f64)
            .min(self.released);
        let deficit = (self.due - actionable_completed).max(0.0_f64);
        if deficit > f64::EPSILON * self.due.max(1.0_f64) {
            self.shortfall = self.shortfall.max(deficit / self.due);
        }
    }
}

impl SupplyTrajectory<'_> {
    fn capacity_at_micros(&self, at_micros: u64) -> f64 {
        let mut capacity = self.initial;
        for event in 0..self.pause_seconds.len() {
            let pause_micros = seconds_to_micros(self.pause_seconds[event]);
            if at_micros < pause_micros {
                break;
            }
            capacity = if at_micros < seconds_to_micros(self.ready_seconds[event]) {
                self.during[event]
            } else {
                self.after[event]
            };
        }
        capacity
    }

    fn next_boundary_micros(&self, after_micros: u64) -> Option<u64> {
        self.pause_seconds
            .iter()
            .chain(self.ready_seconds)
            .copied()
            .map(seconds_to_micros)
            .filter(|boundary| *boundary > after_micros)
            .min()
    }
}

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

    /// Returns the largest mean rate in one fixed window.
    pub(crate) fn maximum_window_rate(&self, start: f64, end: f64, window: f64) -> f64 {
        if end <= start || window <= f64::EPSILON {
            return 0.0_f64;
        }
        let last_start = (end - window).max(start);
        let mut maximum = self.integrated_count(start, (start + window).min(end)) / window;
        for &relative_boundary in self.end_seconds {
            let boundary = self.start_seconds + relative_boundary;
            if boundary >= start && boundary <= last_start {
                maximum = maximum.max(self.integrated_count(boundary, boundary + window) / window);
            }
            let aligned_start = boundary - window;
            if aligned_start >= start && aligned_start <= last_start {
                maximum = maximum
                    .max(self.integrated_count(aligned_start, aligned_start + window) / window);
            }
        }
        maximum.max(self.integrated_count(last_start, end) / window)
    }

    fn rate_at(&self, at: f64) -> f64 {
        let relative = (at - self.start_seconds).max(0.0_f64);
        let index = self
            .end_seconds
            .partition_point(|end| relative >= *end)
            .min(self.rates.len().saturating_sub(1));
        self.rates.get(index).copied().map_or(0.0_f64, |rate| rate)
    }

    fn next_boundary(&self, after: f64) -> Option<f64> {
        let relative = (after - self.start_seconds).max(0.0_f64);
        self.end_seconds
            .get(self.end_seconds.partition_point(|end| *end <= relative))
            .copied()
            .map(|end| self.start_seconds + end)
    }

    fn deadline_rate_at(&self, at: f64, budget_seconds: f64) -> f64 {
        if at < self.start_seconds + budget_seconds {
            0.0_f64
        } else {
            self.rate_at(at - budget_seconds)
        }
    }

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

pub(crate) struct StepCandidates<'a> {
    pub(crate) before: f64,
    pub(crate) during: &'a [f64],
    pub(crate) after: &'a [f64],
    pub(crate) pause_seconds: &'a [f64],
    pub(crate) ready_seconds: &'a [f64],
}

#[derive(Clone, Copy)]
struct ScalarStep {
    pause: f64,
    ready: f64,
    before: f64,
    during: f64,
    after: f64,
}

#[cfg(test)]
pub(crate) fn evaluate_empty_steps(
    level: Level,
    supply: &StepCandidates<'_>,
    start_seconds: f64,
    horizon_seconds: f64,
    initial_work: f64,
    arrivals: &ArrivalPath<'_>,
    deadline_budget_micros: u64,
    delay_area: &mut [f64],
    terminal_work: &mut [f64],
    shortfall: &mut [f64],
) {
    let count = supply.after.len();
    assert_eq!(supply.during.len(), count);
    assert_eq!(supply.pause_seconds.len(), count);
    assert_eq!(supply.ready_seconds.len(), count);
    assert_eq!(delay_area.len(), count);
    assert_eq!(terminal_work.len(), count);
    assert_eq!(shortfall.len(), count);
    dispatch!(level, simd => evaluate_empty_steps_simd(
        simd,
        supply,
        start_seconds,
        horizon_seconds,
        initial_work,
        arrivals,
        deadline_budget_micros,
        delay_area,
        terminal_work,
        shortfall,
    ));
}

pub(crate) fn evaluate_empty_steps_simd<S: Simd>(
    simd: S,
    supply: &StepCandidates<'_>,
    start_seconds: f64,
    horizon_seconds: f64,
    initial_work: f64,
    arrivals: &ArrivalPath<'_>,
    deadline_budget_micros: u64,
    delay_area: &mut [f64],
    terminal_work: &mut [f64],
    shortfall: &mut [f64],
) {
    let lane_count = S::f64s::N;
    let vector_count = supply.after.len() / lane_count;
    for vector in 0..vector_count {
        let first = vector * lane_count;
        let last = first + lane_count;
        let pause = S::f64s::from_slice(simd, &supply.pause_seconds[first..last]);
        let ready = S::f64s::from_slice(simd, &supply.ready_seconds[first..last]);
        let during = S::f64s::from_slice(simd, &supply.during[first..last]);
        let after = S::f64s::from_slice(simd, &supply.after[first..last]);
        let before = S::f64s::splat(simd, supply.before);
        let mut queue = S::f64s::splat(simd, initial_work);
        let mut area = S::f64s::splat(simd, 0.0_f64);
        let mut released = S::f64s::splat(simd, 0.0_f64);
        let mut due = S::f64s::splat(simd, 0.0_f64);
        let mut maximum_shortfall = S::f64s::splat(simd, 0.0_f64);
        let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
        for_forecast_intervals(
            arrivals,
            start_seconds,
            horizon_seconds,
            budget_seconds,
            |start, end, rate, due_rate| {
                let start = S::f64s::splat(simd, start);
                let end = S::f64s::splat(simd, end);
                let zero = S::f64s::splat(simd, 0.0_f64);
                let before_end = pause.simd_lt(end).select(pause, end);
                let before_duration = (before_end - start)
                    .simd_gt(zero)
                    .select(before_end - start, zero);
                accumulate_deadline_segment(
                    simd,
                    &mut queue,
                    &mut area,
                    &mut released,
                    &mut due,
                    &mut maximum_shortfall,
                    before_duration,
                    before,
                    rate,
                    due_rate,
                );

                let during_start = pause.simd_gt(start).select(pause, start);
                let during_end = ready.simd_lt(end).select(ready, end);
                let during_duration = (during_end - during_start)
                    .simd_gt(zero)
                    .select(during_end - during_start, zero);
                accumulate_deadline_segment(
                    simd,
                    &mut queue,
                    &mut area,
                    &mut released,
                    &mut due,
                    &mut maximum_shortfall,
                    during_duration,
                    during,
                    rate,
                    due_rate,
                );

                let after_start = ready.simd_gt(start).select(ready, start);
                let after_duration = (end - after_start)
                    .simd_gt(zero)
                    .select(end - after_start, zero);
                accumulate_deadline_segment(
                    simd,
                    &mut queue,
                    &mut area,
                    &mut released,
                    &mut due,
                    &mut maximum_shortfall,
                    after_duration,
                    after,
                    rate,
                    due_rate,
                );
            },
        );
        area.store_slice(&mut delay_area[first..last]);
        queue.store_slice(&mut terminal_work[first..last]);
        maximum_shortfall.store_slice(&mut shortfall[first..last]);
    }
    for candidate in vector_count * lane_count..supply.after.len() {
        let mut queue = initial_work;
        let mut area = 0.0_f64;
        for_arrival_intervals(
            arrivals,
            start_seconds,
            horizon_seconds,
            |start, end, rate| {
                area += phased_queue_segment(
                    &mut queue,
                    start,
                    end,
                    ScalarStep {
                        pause: supply.pause_seconds[candidate],
                        ready: supply.ready_seconds[candidate],
                        before: supply.before,
                        during: supply.during[candidate],
                        after: supply.after[candidate],
                    },
                    rate,
                );
            },
        );
        delay_area[candidate] = area;
        terminal_work[candidate] = queue;
        shortfall[candidate] = forecast_step_shortfall(
            ScalarStep {
                pause: supply.pause_seconds[candidate],
                ready: supply.ready_seconds[candidate],
                before: supply.before,
                during: supply.during[candidate],
                after: supply.after[candidate],
            },
            start_seconds,
            horizon_seconds,
            initial_work,
            Duration::from_micros(deadline_budget_micros).as_secs_f64(),
            arrivals,
        );
    }
}

fn accumulate_deadline_segment<S: Simd>(
    simd: S,
    queue: &mut S::f64s,
    area: &mut S::f64s,
    released: &mut S::f64s,
    due: &mut S::f64s,
    maximum_shortfall: &mut S::f64s,
    duration: S::f64s,
    capacity: S::f64s,
    arrival_rate: f64,
    due_rate: f64,
) {
    let zero = S::f64s::splat(simd, 0.0_f64);
    let minimum = S::f64s::splat(simd, f64::MIN_POSITIVE);
    let arrivals = S::f64s::splat(simd, arrival_rate);
    let due_arrivals = S::f64s::splat(simd, due_rate);
    let safe_capacity = capacity.simd_gt(minimum).select(capacity, minimum);
    let debt_time = (*queue - *released) / safe_capacity;
    let debt_valid =
        capacity.simd_gt(minimum) & debt_time.simd_gt(zero) & debt_time.simd_lt(duration);
    update_deadline_shortfall(
        simd,
        *queue,
        *released,
        *due,
        capacity,
        arrivals,
        due_arrivals,
        debt_time,
        debt_valid,
        maximum_shortfall,
    );

    let drain_rate = capacity - arrivals;
    let safe_drain_rate = drain_rate.simd_gt(minimum).select(drain_rate, minimum);
    let drain_time = *queue / safe_drain_rate;
    let drain_valid =
        drain_rate.simd_gt(minimum) & drain_time.simd_gt(zero) & drain_time.simd_lt(duration);
    update_deadline_shortfall(
        simd,
        *queue,
        *released,
        *due,
        capacity,
        arrivals,
        due_arrivals,
        drain_time,
        drain_valid,
        maximum_shortfall,
    );
    update_deadline_shortfall(
        simd,
        *queue,
        *released,
        *due,
        capacity,
        arrivals,
        due_arrivals,
        duration,
        duration.simd_gt(zero),
        maximum_shortfall,
    );
    accumulate_queue_segment(simd, queue, area, duration, capacity, arrival_rate);
    *released += arrivals * duration;
    *due += due_arrivals * duration;
}

fn update_deadline_shortfall<S: Simd>(
    simd: S,
    queue: S::f64s,
    released: S::f64s,
    due: S::f64s,
    capacity: S::f64s,
    arrival_rate: S::f64s,
    due_rate: S::f64s,
    elapsed: S::f64s,
    valid: <S::f64s as SimdBase<S>>::Mask,
    maximum: &mut S::f64s,
) {
    let zero = S::f64s::splat(simd, 0.0_f64);
    let minimum = S::f64s::splat(simd, f64::MIN_POSITIVE);
    let terminal_queue = queue + (arrival_rate - capacity) * elapsed;
    let terminal_queue = terminal_queue.simd_gt(zero).select(terminal_queue, zero);
    let completed = released + arrival_rate * elapsed - terminal_queue;
    let completed = completed.simd_gt(zero).select(completed, zero);
    let terminal_due = due + due_rate * elapsed;
    let deficit = terminal_due - completed;
    let deficit = deficit.simd_gt(zero).select(deficit, zero);
    let safe_due = terminal_due.simd_gt(minimum).select(terminal_due, minimum);
    let fraction = deficit / safe_due;
    let fraction = valid.select(fraction, zero);
    *maximum = fraction.simd_gt(*maximum).select(fraction, *maximum);
}

fn forecast_step_shortfall(
    step: ScalarStep,
    start_seconds: f64,
    horizon_seconds: f64,
    initial_debt: f64,
    budget_seconds: f64,
    arrivals: &ArrivalPath<'_>,
) -> f64 {
    let mut deadline = DeadlineState::new(initial_debt);
    let mut now = start_seconds;
    while now < horizon_seconds {
        let mut next = horizon_seconds;
        if step.pause > now {
            next = next.min(step.pause);
        }
        if step.ready > now {
            next = next.min(step.ready);
        }
        if let Some(boundary) = arrivals.next_boundary(now) {
            next = next.min(boundary);
        }
        if let Some(boundary) = arrivals.next_deadline_boundary(now, budget_seconds) {
            next = next.min(boundary);
        }
        if next <= now {
            break;
        }
        let capacity = if now < step.pause {
            step.before
        } else if now < step.ready {
            step.during
        } else {
            step.after
        };
        let _ = deadline.advance(
            next - now,
            capacity,
            arrivals.rate_at(now),
            arrivals.deadline_rate_at(now, budget_seconds),
        );
        now = next;
    }
    deadline.shortfall
}

fn accumulate_queue_segment<S: Simd>(
    simd: S,
    queue: &mut S::f64s,
    area: &mut S::f64s,
    duration: S::f64s,
    capacity: S::f64s,
    arrival_rate: f64,
) {
    let zero = S::f64s::splat(simd, 0.0_f64);
    let half = S::f64s::splat(simd, 0.5_f64);
    let rate = S::f64s::splat(simd, arrival_rate);
    let net = rate - capacity;
    let growing = net.simd_ge(zero);
    let growing_area = *queue * duration + half * net * duration * duration;
    let growing_queue = *queue + net * duration;
    let net_drain_rate = zero - net;
    let minimum_rate = S::f64s::splat(simd, f64::MIN_POSITIVE);
    let drain_rate = net_drain_rate
        .simd_gt(minimum_rate)
        .select(net_drain_rate, minimum_rate);
    let complete_drain_duration = *queue / drain_rate;
    let drain_duration = complete_drain_duration
        .simd_lt(duration)
        .select(complete_drain_duration, duration);
    let draining_area = *queue * drain_duration + half * net * drain_duration * drain_duration;
    let terminal_queue = *queue + net * duration;
    let draining_queue = terminal_queue.simd_gt(zero).select(terminal_queue, zero);
    *area += growing.select(growing_area, draining_area);
    *queue = growing.select(growing_queue, draining_queue);
}

fn for_arrival_intervals(
    arrivals: &ArrivalPath<'_>,
    start: f64,
    end: f64,
    mut evaluate: impl FnMut(f64, f64, f64),
) {
    let mut cursor = start;
    for (&relative_end, &rate) in arrivals.end_seconds.iter().zip(arrivals.rates) {
        let interval_end = (arrivals.start_seconds + relative_end).min(end);
        if interval_end > cursor {
            evaluate(cursor, interval_end, rate);
            cursor = interval_end;
        }
        if cursor >= end {
            return;
        }
    }
    if cursor < end {
        evaluate(cursor, end, arrivals.rate_at(cursor));
    }
}

fn for_forecast_intervals(
    arrivals: &ArrivalPath<'_>,
    start: f64,
    end: f64,
    budget_seconds: f64,
    mut evaluate: impl FnMut(f64, f64, f64, f64),
) {
    let mut cursor = start;
    while cursor < end {
        let mut next = end;
        if let Some(boundary) = arrivals.next_boundary(cursor) {
            next = next.min(boundary);
        }
        if let Some(boundary) = arrivals.next_deadline_boundary(cursor, budget_seconds) {
            next = next.min(boundary);
        }
        if next <= cursor {
            break;
        }
        evaluate(
            cursor,
            next,
            arrivals.rate_at(cursor),
            arrivals.deadline_rate_at(cursor, budget_seconds),
        );
        cursor = next;
    }
}

fn phased_queue_segment(
    queue: &mut f64,
    start: f64,
    end: f64,
    step: ScalarStep,
    arrival_rate: f64,
) -> f64 {
    let before_end = step.pause.min(end);
    let during_start = step.pause.max(start);
    let during_end = step.ready.min(end);
    let after_start = step.ready.max(start);
    queue_area_segment(
        queue,
        (before_end - start).max(0.0_f64),
        step.before,
        arrival_rate,
    ) + queue_area_segment(
        queue,
        (during_end - during_start).max(0.0_f64),
        step.during,
        arrival_rate,
    ) + queue_area_segment(
        queue,
        (end - after_start).max(0.0_f64),
        step.after,
        arrival_rate,
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EdfOutcome {
    pub(crate) shortfall: f64,
    pub(crate) delay_area: f64,
    pub(crate) terminal_work: f64,
}

impl EdfScratch {
    pub(crate) fn new(cohort_count_max: u32) -> Result<Self, crate::ConfigurationError> {
        let capacity = usize::try_from(cohort_count_max)
            .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
        Ok(Self {
            release_order: Vec::with_capacity(capacity),
            deadline_order: Vec::with_capacity(capacity),
            heap: Vec::with_capacity(capacity),
            remaining: vec![0.0; capacity],
            prepared_len: 0,
            common_cohort: None,
            ordered_deadlines: true,
        })
    }
}

pub(crate) fn prepare(cohorts: &WorkCohorts, scratch: &mut EdfScratch) {
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
    scratch.ordered_deadlines = scratch.release_order.windows(2).all(|pair| {
        cohorts.deadline_micros(pair[0] as usize) <= cohorts.deadline_micros(pair[1] as usize)
    });
}

fn common_cohort(cohorts: &WorkCohorts) -> Option<CommonCohort> {
    if cohorts.is_empty() {
        return None;
    }
    let release_micros = cohorts.release_micros(0);
    let deadline_micros = cohorts.deadline_micros(0);
    let mut work = 0.0_f64;
    let mut last_positive_work = 0.0_f64;
    for index in 0..cohorts.len() {
        if cohorts.release_micros(index) != release_micros
            || cohorts.deadline_micros(index) != deadline_micros
        {
            return None;
        }
        let cohort_work = cohorts.work_slot_seconds(index);
        work += cohort_work;
        if cohort_work > 0.0_f64 {
            last_positive_work = cohort_work;
        }
    }
    Some(CommonCohort {
        release_micros,
        deadline_micros,
        work,
        last_positive_work,
    })
}

fn deadline_work(
    cohorts: &WorkCohorts,
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
        work += cohorts.work_slot_seconds(cohort);
        *cursor += 1;
    }
    work
}

/// Computes all candidate shortfalls through one deadline traversal.
fn queue_area_segment(queue: &mut f64, duration: f64, capacity: f64, arrival_rate: f64) -> f64 {
    if duration <= f64::EPSILON {
        return 0.0_f64;
    }
    let net_rate = arrival_rate - capacity;
    if net_rate >= 0.0_f64 {
        let area = *queue * duration + 0.5_f64 * net_rate * duration * duration;
        *queue += net_rate * duration;
        return area;
    }
    let drain_duration = (*queue / -net_rate).min(duration);
    let area = *queue * drain_duration + 0.5_f64 * net_rate * drain_duration * drain_duration;
    *queue = (*queue + net_rate * duration).max(0.0_f64);
    area
}

pub(crate) fn evaluate_prepared_step(
    cohorts: &WorkCohorts,
    supply: SupplyStep,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = window.start_micros;
    let mut late_work = window.initial_debt_work;
    let mut deadline = DeadlineState::new(window.initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut shortfall = 0.0_f64;
    let budget_seconds = Duration::from_micros(window.deadline_budget_micros).as_secs_f64();
    while now_micros < window.horizon_micros {
        deadline.release(shortfall_release(
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
        let expired = expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
        late_work += expired;
        let mut next_micros = window.horizon_micros;
        next_micros = next_micros.min(shortfall_next_release(cohorts, scratch, release_cursor));
        if let Some(&cohort_index) = scratch.heap.first() {
            next_micros = next_micros.min(cohorts.deadline_micros(cohort_index as usize));
        }
        if supply.pause_micros > now_micros {
            next_micros = next_micros.min(supply.pause_micros);
        }
        if supply.ready_micros > now_micros {
            next_micros = next_micros.min(supply.ready_micros);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds)
        {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = if now_micros < supply.pause_micros {
            supply.before
        } else if now_micros < supply.ready_micros {
            supply.during
        } else {
            supply.after
        };
        delay_area += deadline.advance(
            duration_seconds,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        let supply = capacity * duration_seconds;
        let late_supply = supply.min(late_work);
        late_work -= late_supply;
        shortfall_serve(cohorts, scratch, supply - late_supply);
        now_micros = next_micros;
    }
    deadline.release(shortfall_release(
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
    expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        terminal_work: deadline.queue,
    }
}

pub(crate) fn evaluate_prepared_trajectory(
    cohorts: &WorkCohorts,
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    deadline_budget_micros: u64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    if let Some(common) = scratch.common_cohort {
        return evaluate_common_trajectory(
            common,
            trajectory,
            start_micros,
            horizon_micros,
            initial_debt_work,
            deadline_budget_micros,
            future_arrivals,
        );
    }
    if scratch.ordered_deadlines {
        return evaluate_ordered_trajectory(
            cohorts,
            trajectory,
            start_micros,
            horizon_micros,
            initial_debt_work,
            deadline_budget_micros,
            future_arrivals,
            scratch,
        );
    }
    evaluate_general_trajectory(
        cohorts,
        trajectory,
        start_micros,
        horizon_micros,
        initial_debt_work,
        deadline_budget_micros,
        future_arrivals,
        scratch,
    )
}

fn evaluate_ordered_trajectory(
    cohorts: &WorkCohorts,
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    deadline_budget_micros: u64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut service_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut shortfall = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    while now_micros < horizon_micros {
        deadline.release(ordered_release(
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
        late_work += ordered_expire(
            cohorts,
            scratch,
            release_cursor,
            &mut service_cursor,
            now_micros,
            &mut shortfall,
        );
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(shortfall_next_release(cohorts, scratch, release_cursor));
        if service_cursor < release_cursor {
            let cohort = scratch.release_order[service_cursor] as usize;
            next_micros = next_micros.min(cohorts.deadline_micros(cohort));
        }
        if let Some(boundary) = trajectory.next_boundary_micros(now_micros) {
            next_micros = next_micros.min(boundary);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds)
        {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory.capacity_at_micros(now_micros);
        delay_area += deadline.advance(
            duration_seconds,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        let supply = capacity * duration_seconds;
        let debt_supply = supply.min(late_work);
        late_work -= debt_supply;
        ordered_serve(
            scratch,
            release_cursor,
            &mut service_cursor,
            supply - debt_supply,
        );
        now_micros = next_micros;
    }
    deadline.release(ordered_release(
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
    ordered_expire(
        cohorts,
        scratch,
        release_cursor,
        &mut service_cursor,
        now_micros,
        &mut shortfall,
    );
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        terminal_work: deadline.queue,
    }
}

fn ordered_release(
    cohorts: &WorkCohorts,
    scratch: &EdfScratch,
    release_cursor: &mut usize,
    now_micros: u64,
) -> f64 {
    let mut released = 0.0_f64;
    while *release_cursor < cohorts.len() {
        let cohort = scratch.release_order[*release_cursor] as usize;
        if cohorts.release_micros(cohort) > now_micros {
            break;
        }
        released += cohorts.work_slot_seconds(cohort);
        *release_cursor += 1;
    }
    released
}

fn ordered_expire(
    cohorts: &WorkCohorts,
    scratch: &mut EdfScratch,
    release_cursor: usize,
    service_cursor: &mut usize,
    now_micros: u64,
    shortfall: &mut f64,
) -> f64 {
    let mut debt = 0.0_f64;
    while *service_cursor < release_cursor {
        let cohort = scratch.release_order[*service_cursor] as usize;
        let remaining = scratch.remaining[cohort];
        if remaining <= f64::EPSILON {
            *service_cursor += 1;
            continue;
        }
        if cohorts.deadline_micros(cohort) > now_micros {
            break;
        }
        let work = cohorts.work_slot_seconds(cohort);
        if work > 0.0_f64 {
            *shortfall = shortfall.max(remaining / work);
        }
        debt += remaining;
        scratch.remaining[cohort] = 0.0_f64;
        *service_cursor += 1;
    }
    debt
}

fn ordered_serve(
    scratch: &mut EdfScratch,
    release_cursor: usize,
    service_cursor: &mut usize,
    mut supply: f64,
) {
    while supply > 0.0_f64 && *service_cursor < release_cursor {
        let cohort = scratch.release_order[*service_cursor] as usize;
        let served = supply.min(scratch.remaining[cohort]);
        scratch.remaining[cohort] -= served;
        supply -= served;
        if scratch.remaining[cohort] <= f64::EPSILON {
            *service_cursor += 1;
        }
    }
}

pub(crate) fn evaluate_general_trajectory(
    cohorts: &WorkCohorts,
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    deadline_budget_micros: u64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut shortfall = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    while now_micros < horizon_micros {
        deadline.release(shortfall_release(
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
        late_work += expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(shortfall_next_release(cohorts, scratch, release_cursor));
        if let Some(&cohort_index) = scratch.heap.first() {
            next_micros = next_micros.min(cohorts.deadline_micros(cohort_index as usize));
        }
        if let Some(boundary) = trajectory.next_boundary_micros(now_micros) {
            next_micros = next_micros.min(boundary);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds)
        {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory.capacity_at_micros(now_micros);
        delay_area += deadline.advance(
            duration_seconds,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        let supply = capacity * duration_seconds;
        let late_supply = supply.min(late_work);
        late_work -= late_supply;
        shortfall_serve(cohorts, scratch, supply - late_supply);
        now_micros = next_micros;
    }
    deadline.release(shortfall_release(
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
    expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        terminal_work: deadline.queue,
    }
}

fn evaluate_common_trajectory(
    cohort: CommonCohort,
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    deadline_budget_micros: u64,
    future_arrivals: &ArrivalPath<'_>,
) -> EdfOutcome {
    let mut now_micros = start_micros;
    let mut state = CommonState {
        late_work: initial_debt_work,
        on_time_work: 0.0_f64,
        shortfall: 0.0_f64,
        released: false,
        expired: false,
    };
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
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
        if let Some(boundary) = trajectory.next_boundary_micros(now_micros) {
            next_micros = next_micros.min(boundary);
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds)
        {
            next_micros = next_micros.min(seconds_to_micros(boundary));
        }
        if next_micros <= now_micros {
            break;
        }
        let duration = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory.capacity_at_micros(now_micros);
        delay_area += deadline.advance(
            duration,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        let mut supply = capacity * duration;
        let debt_supply = supply.min(state.late_work);
        state.late_work -= debt_supply;
        supply -= debt_supply;
        state.on_time_work = (state.on_time_work - supply).max(0.0_f64);
        now_micros = next_micros;
    }
    let (released, due) = update_common_boundaries(cohort, now_micros, &mut state);
    deadline.release(released);
    deadline.make_due(due);
    EdfOutcome {
        shortfall: state.shortfall.max(deadline.shortfall),
        delay_area,
        terminal_work: deadline.queue,
    }
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
        state.on_time_work = cohort.work;
        state.released = true;
    }
    if state.released && !state.expired && cohort.deadline_micros <= now_micros {
        due = cohort.work;
        if state.on_time_work > 0.0_f64 {
            state.shortfall = if state.on_time_work > cohort.last_positive_work {
                1.0_f64
            } else {
                state.on_time_work / cohort.last_positive_work
            };
        }
        state.late_work += state.on_time_work;
        state.on_time_work = 0.0_f64;
        state.expired = true;
    }
    (released, due)
}

pub(crate) fn required_capacity_prepared(cohorts: &WorkCohorts, scratch: &mut EdfScratch) -> f64 {
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
            work += cohorts.work_slot_seconds(cohort);
            let interval =
                Duration::from_micros(cohorts.deadline_micros(cohort) - release).as_secs_f64();
            required = required.max(work / interval);
        }
    }
    required
}

fn shortfall_reset(cohorts: &WorkCohorts, scratch: &mut EdfScratch) {
    assert!(
        cohorts.len() <= scratch.remaining.len(),
        "cohorts must fit the remaining-work scratch"
    );
    scratch.heap.clear();
    for index in 0..cohorts.len() {
        scratch.remaining[index] = cohorts.work_slot_seconds(index);
    }
}

fn shortfall_release(
    cohorts: &WorkCohorts,
    scratch: &mut EdfScratch,
    release_cursor: &mut usize,
    now_micros: u64,
) -> f64 {
    let mut released = 0.0_f64;
    while *release_cursor < cohorts.len() {
        let cohort_index = scratch.release_order[*release_cursor];
        if cohorts.release_micros(cohort_index as usize) > now_micros {
            break;
        }
        heap_push(cohorts, &mut scratch.heap, cohort_index);
        released += cohorts.work_slot_seconds(cohort_index as usize);
        *release_cursor += 1;
    }
    released
}

fn shortfall_next_release(
    cohorts: &WorkCohorts,
    scratch: &EdfScratch,
    release_cursor: usize,
) -> u64 {
    if release_cursor < cohorts.len() {
        cohorts.release_micros(scratch.release_order[release_cursor] as usize)
    } else {
        u64::MAX
    }
}

fn shortfall_serve(cohorts: &WorkCohorts, scratch: &mut EdfScratch, mut supply_slot_micros: f64) {
    while supply_slot_micros > 0.0_f64 && !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        let served = supply_slot_micros.min(scratch.remaining[cohort_index]);
        scratch.remaining[cohort_index] -= served;
        supply_slot_micros -= served;
        if scratch.remaining[cohort_index] <= f64::EPSILON {
            heap_pop(cohorts, &mut scratch.heap);
        }
    }
}

fn expire_to_debt(
    cohorts: &WorkCohorts,
    scratch: &mut EdfScratch,
    now_micros: u64,
    shortfall: &mut f64,
) -> f64 {
    let mut debt = 0.0_f64;
    while !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        if cohorts.deadline_micros(cohort_index) > now_micros {
            break;
        }
        let work = cohorts.work_slot_seconds(cohort_index);
        let remaining = scratch.remaining[cohort_index];
        if work > 0.0_f64 {
            *shortfall = shortfall.max(remaining / work);
        }
        debt += remaining;
        heap_pop(cohorts, &mut scratch.heap);
    }
    debt
}

fn heap_push(cohorts: &WorkCohorts, heap: &mut Vec<u32>, cohort_index: u32) {
    assert!(
        heap.len() < heap.capacity(),
        "the EDF heap must fit every configured cohort"
    );
    heap.push(cohort_index);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        if deadline_key(cohorts, heap[parent]) <= deadline_key(cohorts, heap[child]) {
            break;
        }
        heap.swap(parent, child);
        child = parent;
    }
}

fn heap_pop(cohorts: &WorkCohorts, heap: &mut Vec<u32>) -> u32 {
    let root = heap[0];
    let last_index = heap.len() - 1;
    let last = heap[last_index];
    heap.truncate(last_index);
    if !heap.is_empty() {
        heap[0] = last;
        heap_sift_down(cohorts, heap);
    }
    root
}

fn heap_sift_down(cohorts: &WorkCohorts, heap: &mut [u32]) {
    let mut parent = 0_usize;
    loop {
        let left = parent * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child = if right < heap.len()
            && deadline_key(cohorts, heap[right]) < deadline_key(cohorts, heap[left])
        {
            right
        } else {
            left
        };
        if deadline_key(cohorts, heap[parent]) <= deadline_key(cohorts, heap[child]) {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
}

fn deadline_key(cohorts: &WorkCohorts, cohort_index: u32) -> (u64, u64, u32) {
    let cohort = cohort_index as usize;
    (
        cohorts.deadline_micros(cohort),
        cohorts.release_micros(cohort),
        cohort_index,
    )
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}
