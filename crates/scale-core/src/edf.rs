use std::time::Duration;

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
    pub(crate) pause_micros: &'a [u64],
    pub(crate) ready_micros: &'a [u64],
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
    shortfall: f64,
}

#[derive(Clone, Copy)]
struct DeadlineAdvance {
    queue_area: f64,
    late_area: f64,
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
            shortfall: 0.0_f64,
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
        if work > 0.0_f64 {
            self.shortfall = self.shortfall.max(missed / work);
        }
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
            if self.due_work > 0.0_f64 {
                self.shortfall = self.shortfall.max(self.overdue / self.due_work);
            }
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

fn positive_crossing(value: f64, rate: f64) -> f64 {
    if value > 0.0_f64 && rate < 0.0_f64 {
        value / -rate
    } else {
        f64::INFINITY
    }
}

fn edf_outcome(
    deadline: &DeadlineState,
    shortfall: f64,
    delay_area: f64,
    late_area: f64,
    terminal_capacity: f64,
    continuation_seconds: f64,
) -> EdfOutcome {
    let (terminal_late_area, drain_seconds) =
        terminal_closure(deadline.queue, terminal_capacity, continuation_seconds);
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
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

    fn next_boundary_micros(&self, after_micros: u64) -> Option<u64> {
        self.pause_micros
            .iter()
            .chain(self.ready_micros)
            .copied()
            .filter(|boundary| *boundary > after_micros)
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EdfOutcome {
    pub(crate) shortfall: f64,
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
    scratch.ordered_deadlines = scratch.release_order.windows(2).all(|pair| {
        cohorts.deadline_micros(pair[0] as usize) <= cohorts.deadline_micros(pair[1] as usize)
    });
}

fn common_cohort<Unit>(cohorts: &WorkCohorts<Unit>) -> Option<CommonCohort> {
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
        let cohort_work = cohorts.work(index);
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
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = window.start_micros;
    let mut late_work = window.initial_debt_work;
    let mut deadline = DeadlineState::new(window.initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
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
        let capacity = supply.capacity_at(now_micros);
        let advance = deadline.advance(
            duration_seconds,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
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
    let terminal_capacity = supply.capacity_at(window.horizon_micros);
    edf_outcome(
        &deadline,
        shortfall,
        delay_area,
        late_area,
        terminal_capacity,
        Duration::from_micros(window.horizon_micros.saturating_sub(window.start_micros))
            .as_secs_f64(),
    )
}

/// Clamps one event step to the next supply or arrival boundary.
fn shared_boundary_micros(
    trajectory: &SupplyTrajectory<'_>,
    future_arrivals: &ArrivalPath<'_>,
    now_micros: u64,
    budget_seconds: f64,
    mut next_micros: u64,
) -> u64 {
    if let Some(boundary) = trajectory.next_boundary_micros(now_micros) {
        next_micros = next_micros.min(boundary);
    }
    let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
    if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
        next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
    }
    if let Some(boundary) = future_arrivals.next_deadline_boundary(now_seconds, budget_seconds) {
        next_micros = next_micros.min(seconds_to_micros_ceil(boundary));
    }
    next_micros
}

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
    if scratch.ordered_deadlines {
        return evaluate_ordered_trajectory(cohorts, trajectory, window, future_arrivals, scratch);
    }
    evaluate_general_trajectory(cohorts, trajectory, window, future_arrivals, scratch)
}

fn evaluate_ordered_trajectory<Unit>(
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
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut service_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
    let mut shortfall = 0.0_f64;
    let budget_seconds = Duration::from_micros(deadline_budget_micros).as_secs_f64();
    while now_micros < horizon_micros {
        ordered_boundaries(
            cohorts,
            scratch,
            &mut release_cursor,
            &mut deadline_cursor,
            &mut deadline,
            now_micros,
        );
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
        let next_micros = shared_boundary_micros(
            trajectory,
            future_arrivals,
            now_micros,
            budget_seconds,
            next_micros,
        );
        if next_micros <= now_micros {
            break;
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
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
    ordered_boundaries(
        cohorts,
        scratch,
        &mut release_cursor,
        &mut deadline_cursor,
        &mut deadline,
        now_micros,
    );
    ordered_expire(
        cohorts,
        scratch,
        release_cursor,
        &mut service_cursor,
        now_micros,
        &mut shortfall,
    );
    edf_outcome(
        &deadline,
        shortfall,
        delay_area,
        late_area,
        trajectory.capacity_at_micros(horizon_micros),
        Duration::from_micros(horizon_micros.saturating_sub(start_micros)).as_secs_f64(),
    )
}

/// Applies release and due boundaries up to one instant.
fn ordered_boundaries<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    release_cursor: &mut usize,
    deadline_cursor: &mut usize,
    deadline: &mut DeadlineState,
    now_micros: u64,
) {
    deadline.release(ordered_release(
        cohorts,
        scratch,
        release_cursor,
        now_micros,
    ));
    deadline.make_due(deadline_work(cohorts, scratch, deadline_cursor, now_micros));
}

fn ordered_release<Unit>(
    cohorts: &WorkCohorts<Unit>,
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
        released += cohorts.work(cohort);
        *release_cursor += 1;
    }
    released
}

fn ordered_expire<Unit>(
    cohorts: &WorkCohorts<Unit>,
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
        if remaining == 0.0_f64 {
            *service_cursor += 1;
            continue;
        }
        if cohorts.deadline_micros(cohort) > now_micros {
            break;
        }
        let work = cohorts.work(cohort);
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
        let remaining = scratch.remaining[cohort];
        if supply >= remaining {
            supply -= remaining;
            scratch.remaining[cohort] = 0.0_f64;
            *service_cursor += 1;
        } else {
            scratch.remaining[cohort] = remaining - supply;
            supply = 0.0_f64;
        }
    }
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
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut deadline_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
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
        let next_micros = shared_boundary_micros(
            trajectory,
            future_arrivals,
            now_micros,
            budget_seconds,
            next_micros,
        );
        if next_micros <= now_micros {
            break;
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
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
    edf_outcome(
        &deadline,
        shortfall,
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
        late_work: initial_debt_work,
        on_time_work: 0.0_f64,
        shortfall: 0.0_f64,
        released: false,
        expired: false,
    };
    let mut deadline = DeadlineState::new(initial_debt_work);
    let mut delay_area = 0.0_f64;
    let mut late_area = 0.0_f64;
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
        let next_micros = shared_boundary_micros(
            trajectory,
            future_arrivals,
            now_micros,
            budget_seconds,
            next_micros,
        );
        if next_micros <= now_micros {
            break;
        }
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        let duration = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = trajectory.capacity_at_micros(now_micros);
        let advance = deadline.advance(
            duration,
            capacity,
            future_arrivals.rate_at(now_seconds),
            future_arrivals.deadline_rate_at(now_seconds, budget_seconds),
        );
        delay_area += advance.queue_area;
        late_area += advance.late_area;
        let mut supply = capacity * duration;
        let debt_supply = supply.min(state.late_work);
        state.late_work -= debt_supply;
        supply -= debt_supply;
        if supply >= state.on_time_work {
            state.on_time_work = 0.0_f64;
        } else {
            state.on_time_work -= supply;
        }
        now_micros = next_micros;
    }
    let (released, due) = update_common_boundaries(cohort, now_micros, &mut state);
    deadline.release(released);
    deadline.make_due(due);
    edf_outcome(
        &deadline,
        state.shortfall,
        delay_area,
        late_area,
        trajectory.capacity_at_micros(horizon_micros),
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

fn shortfall_reset<Unit>(cohorts: &WorkCohorts<Unit>, scratch: &mut EdfScratch) {
    assert!(
        cohorts.len() <= scratch.remaining.len(),
        "cohorts must fit the remaining-work scratch"
    );
    scratch.heap.clear();
    for index in 0..cohorts.len() {
        scratch.remaining[index] = cohorts.work(index);
    }
}

fn shortfall_release<Unit>(
    cohorts: &WorkCohorts<Unit>,
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
        released += cohorts.work(cohort_index as usize);
        *release_cursor += 1;
    }
    released
}

fn shortfall_next_release<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &EdfScratch,
    release_cursor: usize,
) -> u64 {
    if release_cursor < cohorts.len() {
        cohorts.release_micros(scratch.release_order[release_cursor] as usize)
    } else {
        u64::MAX
    }
}

fn shortfall_serve<Unit>(
    cohorts: &WorkCohorts<Unit>,
    scratch: &mut EdfScratch,
    mut supply_slot_micros: f64,
) {
    while supply_slot_micros > 0.0_f64 && !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        let remaining = scratch.remaining[cohort_index];
        if supply_slot_micros >= remaining {
            supply_slot_micros -= remaining;
            scratch.remaining[cohort_index] = 0.0_f64;
            heap_pop(cohorts, &mut scratch.heap);
        } else {
            scratch.remaining[cohort_index] = remaining - supply_slot_micros;
            supply_slot_micros = 0.0_f64;
        }
    }
}

fn expire_to_debt<Unit>(
    cohorts: &WorkCohorts<Unit>,
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
        let work = cohorts.work(cohort_index);
        let remaining = scratch.remaining[cohort_index];
        if work > 0.0_f64 {
            *shortfall = shortfall.max(remaining / work);
        }
        debt += remaining;
        heap_pop(cohorts, &mut scratch.heap);
    }
    debt
}

fn heap_push<Unit>(cohorts: &WorkCohorts<Unit>, heap: &mut Vec<u32>, cohort_index: u32) {
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

fn heap_pop<Unit>(cohorts: &WorkCohorts<Unit>, heap: &mut Vec<u32>) -> u32 {
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

fn heap_sift_down<Unit>(cohorts: &WorkCohorts<Unit>, heap: &mut [u32]) {
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

fn deadline_key<Unit>(cohorts: &WorkCohorts<Unit>, cohort_index: u32) -> (u64, u64, u32) {
    let cohort = cohort_index as usize;
    (
        cohorts.deadline_micros(cohort),
        cohorts.release_micros(cohort),
        cohort_index,
    )
}

fn seconds_to_micros_ceil(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64).ceil() as u64
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use super::{
        ArrivalPath, DeadlineState, EdfScratch, EvaluationWindow, SupplyTrajectory,
        evaluate_general_trajectory, evaluate_prepared_trajectory, prepare, terminal_closure,
    };
    use crate::types::SlotSecondCohorts;

    const NO_FUTURE_ARRIVALS: ArrivalPath<'static> = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };

    #[test]
    fn supply_trajectory_keeps_exact_microsecond_boundaries() {
        let trajectory = SupplyTrajectory {
            initial: 3.0_f64,
            pause_micros: &[500_001],
            ready_micros: &[500_003],
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

        let matches = close_relative(fast.shortfall, general.shortfall)
            && close_relative(fast.delay_area, general.delay_area)
            && close_relative(fast.drain_seconds, general.drain_seconds);
        assert!(matches, "fast={fast:?}, general={general:?}");
        true
    }

    #[quickcheck]
    fn ordered_deadline_trajectory_matches_general_edf(
        count_seed: u8,
        gap_seed: u8,
        work_seed: u16,
        supply_seed: u8,
    ) -> bool {
        let count = usize::from(count_seed % 16 + 1);
        let gap_micros = u64::from(gap_seed) * 10_000 + 1;
        let work = f64::from(work_seed % 1_000) / 10.0_f64;
        let supply = f64::from(supply_seed) + 1.0_f64;
        let mut cohorts = SlotSecondCohorts::new(count);
        for cohort in 0..count {
            let release_micros = cohort as u64 * gap_micros;
            cohorts.push_values(
                release_micros,
                release_micros + 1_500_000,
                work + f64::from(cohort as u32),
                cohort as u32,
            );
        }
        let trajectory = SupplyTrajectory {
            initial: supply,
            pause_micros: &[500_000_u64],
            ready_micros: &[1_000_000_u64],
            during: &[supply * 0.5_f64],
            after: &[supply * 1.5_f64],
        };
        let Ok(mut scratch) = EdfScratch::new(count as u32) else {
            return false;
        };
        prepare(&cohorts, &mut scratch);
        let window = EvaluationWindow {
            start_micros: 0,
            horizon_micros: 3_000_000,
            initial_debt_work: 7.0_f64,
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

        close_relative(fast.shortfall, general.shortfall)
            && close_relative(fast.delay_area, general.delay_area)
            && close_relative(fast.drain_seconds, general.drain_seconds)
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
