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
            initial_debt,
            completed: 0.0_f64,
            released: 0.0_f64,
            due: 0.0_f64,
            missed: 0.0_f64,
            shortfall: 0.0_f64,
        }
    }

    fn release(&mut self, work: f64) {
        self.queue += work;
        self.released += work;
    }

    fn make_due(&mut self, work: f64) {
        let completion_headroom = (self.actionable_completed() - self.due).max(0.0_f64);
        let missed = (work - completion_headroom).clamp(0.0_f64, work);
        let error_bound = 8.0_f64
            * f64::EPSILON
            * self
                .released
                .max(self.completed)
                .max(self.due)
                .max(work)
                .max(1.0_f64);
        if missed > error_bound {
            self.missed += missed;
        }
        self.due += work;
        self.update_shortfall();
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
        // Rates stay constant across the call, so the state has few linear
        // breakpoints: the initial debt completes once, the queue empties
        // once, and the completion-due lead crosses zero at most once for
        // each service regime. Each pass ends at the next breakpoint and
        // resolves it exactly, so the pass budget below covers every case.
        // The tail then integrates any numerical remainder without
        // breakpoints, which keeps this loop free of stall states.
        for _ in 0_u8..6 {
            if remaining <= 0.0_f64 {
                break;
            }
            let service_rate = self.service_rate(capacity, arrival_rate);
            let net_rate = arrival_rate - service_rate;
            let mut span = remaining;
            let queue_crossing = if self.queue > 0.0_f64 && net_rate < 0.0_f64 {
                self.queue / -net_rate
            } else {
                f64::INFINITY
            };
            span = span.min(queue_crossing);
            let debt_crossing = if self.completed < self.initial_debt && service_rate > 0.0_f64 {
                (self.initial_debt - self.completed) / service_rate
            } else {
                f64::INFINITY
            };
            span = span.min(debt_crossing);
            let actionable_rate = if self.completed >= self.initial_debt {
                service_rate
            } else {
                0.0_f64
            };
            let completion_lead = self.actionable_completed() - self.due;
            let lead_rate = actionable_rate - due_rate;
            if completion_lead * lead_rate < 0.0_f64 {
                span = span.min(-completion_lead / lead_rate);
            }
            let span = span.max(0.0_f64);
            self.integrate(
                span,
                service_rate,
                arrival_rate,
                due_rate,
                &mut queue_area,
                &mut late_area,
            );
            if queue_crossing <= span {
                self.queue = 0.0_f64;
            }
            if debt_crossing <= span {
                self.completed = self.completed.max(self.initial_debt);
            }
            remaining -= span;
        }
        if remaining > 0.0_f64 {
            let service_rate = self.service_rate(capacity, arrival_rate);
            self.integrate(
                remaining,
                service_rate,
                arrival_rate,
                due_rate,
                &mut queue_area,
                &mut late_area,
            );
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

    fn integrate(
        &mut self,
        span: f64,
        service_rate: f64,
        arrival_rate: f64,
        due_rate: f64,
        queue_area: &mut f64,
        late_area: &mut f64,
    ) {
        let net_rate = arrival_rate - service_rate;
        let actionable_rate = if self.completed >= self.initial_debt {
            service_rate
        } else {
            0.0_f64
        };
        let completion_lead = self.actionable_completed() - self.due;
        let lead_rate = actionable_rate - due_rate;
        if due_rate > 0.0_f64 {
            let bound = self.ledger_error_bound();
            let lead = if completion_lead.abs() <= bound {
                0.0_f64
            } else {
                completion_lead
            };
            self.missed += due_rate * behind_duration(lead, lead_rate, span);
        }
        let late_before = self.late_work();
        *queue_area += self.queue * span + 0.5_f64 * net_rate * span * span;
        self.queue = (self.queue + net_rate * span).max(0.0_f64);
        self.completed += service_rate * span;
        self.released += arrival_rate * span;
        self.due += due_rate * span;
        self.update_shortfall();
        *late_area += 0.5_f64 * (late_before + self.late_work()) * span;
    }

    fn ledger_error_bound(&self) -> f64 {
        8.0_f64
            * f64::EPSILON
            * self
                .released
                .max(self.completed)
                .max(self.due)
                .max(self.initial_debt)
                .max(1.0_f64)
    }

    fn actionable_completed(&self) -> f64 {
        (self.completed - self.initial_debt)
            .max(0.0_f64)
            .min(self.released)
    }

    fn late_work(&self) -> f64 {
        let error_bound = 8.0_f64
            * f64::EPSILON
            * self
                .initial_debt
                .max(self.completed)
                .max(self.released)
                .max(self.due)
                .max(1.0_f64);
        let initial = self.initial_debt - self.completed;
        let initial = if initial > error_bound {
            initial
        } else {
            0.0_f64
        };
        let actionable = self.due - self.actionable_completed();
        let actionable = if actionable > error_bound {
            actionable
        } else {
            0.0_f64
        };
        initial + actionable
    }

    fn update_shortfall(&mut self) {
        let deficit = (self.due - self.actionable_completed()).max(0.0_f64);
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
    pub(crate) terminal_work: f64,
    pub(crate) terminal_late_work: f64,
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
        let capacity = if now_micros < supply.pause_micros {
            supply.before
        } else if now_micros < supply.ready_micros {
            supply.during
        } else {
            supply.after
        };
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
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_work: deadline.queue,
        terminal_late_work: deadline.late_work(),
    }
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

pub(crate) fn evaluate_prepared_trajectory(
    cohorts: &WorkCohorts,
    trajectory: &SupplyTrajectory<'_>,
    window: EvaluationWindow,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    if let Some(common) = scratch.common_cohort {
        return evaluate_common_trajectory(common, trajectory, window, future_arrivals);
    }
    if scratch.ordered_deadlines {
        return evaluate_ordered_trajectory(cohorts, trajectory, window, future_arrivals, scratch);
    }
    evaluate_general_trajectory(cohorts, trajectory, window, future_arrivals, scratch)
}

fn evaluate_ordered_trajectory(
    cohorts: &WorkCohorts,
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
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_work: deadline.queue,
        terminal_late_work: deadline.late_work(),
    }
}

/// Applies release and due boundaries up to one instant.
fn ordered_boundaries(
    cohorts: &WorkCohorts,
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
    EdfOutcome {
        shortfall: shortfall.max(deadline.shortfall),
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_work: deadline.queue,
        terminal_late_work: deadline.late_work(),
    }
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
        state.on_time_work = (state.on_time_work - supply).max(0.0_f64);
        now_micros = next_micros;
    }
    let (released, due) = update_common_boundaries(cohort, now_micros, &mut state);
    deadline.release(released);
    deadline.make_due(due);
    EdfOutcome {
        shortfall: state.shortfall.max(deadline.shortfall),
        delay_area,
        missed_work: deadline.missed,
        late_area,
        terminal_work: deadline.queue,
        terminal_late_work: deadline.late_work(),
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

/// Returns the time inside one span with completion behind due work.
///
/// The lead is completion minus due work and moves linearly at `lead_rate`.
/// Work becomes missed while the lead is negative, or while it sits at zero
/// and falls.
fn behind_duration(lead: f64, lead_rate: f64, span: f64) -> f64 {
    if lead < 0.0_f64 {
        if lead_rate > 0.0_f64 {
            (-lead / lead_rate).min(span)
        } else {
            span
        }
    } else if lead == 0.0_f64 && lead_rate < 0.0_f64 {
        span
    } else if lead > 0.0_f64 && lead_rate < 0.0_f64 {
        (span + lead / lead_rate).max(0.0_f64)
    } else {
        0.0_f64
    }
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}

fn seconds_to_micros_ceil(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64).ceil() as u64
}

#[cfg(test)]
mod tests {
    use super::{DeadlineState, behind_duration};

    /// The debt remainder sits between the ledger error bound and the span
    /// resolution of a large capacity. A span-epsilon loop stalls here; the
    /// breakpoint passes resolve it exactly.
    #[test]
    fn advance_terminates_on_a_debt_sliver() {
        let mut state = DeadlineState::new(0.5_f64);
        state.completed = 0.5_f64 - 1.0e-12_f64;
        let advanced = state.advance(1.0_f64, 1.0e6_f64, 0.0_f64, 0.0_f64);

        assert!(state.completed >= state.initial_debt);
        assert!(advanced.queue_area >= 0.0_f64);
    }

    #[test]
    fn behind_duration_integrates_each_lead_regime() {
        let cases = [
            (-1.0_f64, 0.5_f64, 2.0_f64),
            (-1.0_f64, -1.0_f64, 4.0_f64),
            (0.0_f64, -1.0_f64, 4.0_f64),
            (1.0_f64, -0.5_f64, 2.0_f64),
            (1.0_f64, 1.0_f64, 0.0_f64),
        ];
        for (lead, lead_rate, expected) in cases {
            let behind = behind_duration(lead, lead_rate, 4.0_f64);
            assert!(
                behind.total_cmp(&expected).is_eq(),
                "lead={lead} lead_rate={lead_rate} behind={behind}"
            );
        }
    }
}
