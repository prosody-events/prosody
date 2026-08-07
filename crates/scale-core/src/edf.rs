use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};

use crate::types::WorkCohort;

pub(crate) struct EdfScratch {
    release_order: Vec<u32>,
    deadline_order: Vec<u32>,
    heap: Vec<u32>,
    remaining: Vec<f64>,
    prepared_len: usize,
}

pub(crate) struct CandidateSupply<'a> {
    pub(crate) before: f64,
    pub(crate) during: &'a [f64],
    pub(crate) after: &'a [f64],
    pub(crate) pause_seconds: &'a [f64],
    pub(crate) ready_seconds: &'a [f64],
}

pub(crate) struct CandidateLoss<'a> {
    pub(crate) service_balance: &'a mut [f64],
    pub(crate) shortfall: &'a mut [f64],
    pub(crate) delay_area: &'a mut [f64],
}

/// One candidate's ordered capacity and rebalance trajectory.
pub(crate) struct SupplyTrajectory<'a> {
    pub(crate) initial: f64,
    pub(crate) pause_seconds: &'a [f64],
    pub(crate) ready_seconds: &'a [f64],
    pub(crate) during: &'a [f64],
    pub(crate) after: &'a [f64],
}

pub(crate) struct SupplyTrajectories<'a> {
    pub(crate) initial: f64,
    pub(crate) offsets: &'a [u32],
    pub(crate) pause_seconds: &'a [f64],
    pub(crate) ready_seconds: &'a [f64],
    pub(crate) during: &'a [f64],
    pub(crate) after: &'a [f64],
}

impl SupplyTrajectories<'_> {
    fn candidate(&self, index: usize) -> SupplyTrajectory<'_> {
        let first = self.offsets[index] as usize;
        let last = self.offsets[index + 1] as usize;
        SupplyTrajectory {
            initial: self.initial,
            pause_seconds: &self.pause_seconds[first..last],
            ready_seconds: &self.ready_seconds[first..last],
            during: &self.during[first..last],
            after: &self.after[first..last],
        }
    }

    fn candidate_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
}

impl SupplyTrajectory<'_> {
    fn capacity_at(&self, at_seconds: f64) -> f64 {
        let mut capacity = self.initial;
        for event in 0..self.pause_seconds.len() {
            if at_seconds < self.pause_seconds[event] {
                break;
            }
            capacity = if at_seconds < self.ready_seconds[event] {
                self.during[event]
            } else {
                self.after[event]
            };
        }
        capacity
    }

    fn next_boundary(&self, after_seconds: f64) -> Option<f64> {
        self.pause_seconds
            .iter()
            .chain(self.ready_seconds)
            .copied()
            .filter(|boundary| *boundary > after_seconds)
            .min_by(f64::total_cmp)
    }

    fn integrated_supply(&self, start_seconds: f64, end_seconds: f64) -> f64 {
        let mut cursor = start_seconds;
        let mut supply = 0.0_f64;
        while cursor < end_seconds {
            let next = self
                .next_boundary(cursor)
                .map_or(end_seconds, |boundary| boundary.min(end_seconds));
            if next <= cursor {
                break;
            }
            supply += self.capacity_at(cursor) * (next - cursor);
            cursor = next;
        }
        supply
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

    fn rate_at(&self, at: f64) -> f64 {
        let relative = (at - self.start_seconds).max(0.0_f64);
        let index = self
            .end_seconds
            .iter()
            .position(|end| relative < *end)
            .map_or(self.rates.len().saturating_sub(1), |index| index);
        self.rates.get(index).copied().map_or(0.0_f64, |rate| rate)
    }

    fn next_boundary(&self, after: f64) -> Option<f64> {
        let relative = (after - self.start_seconds).max(0.0_f64);
        self.end_seconds
            .iter()
            .copied()
            .find(|end| *end > relative)
            .map(|end| self.start_seconds + end)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EdfOutcome {
    pub(crate) shortfall: f64,
    pub(crate) delay_area: f64,
}

#[derive(Clone, Copy)]
struct ServiceInterval {
    start: f64,
    end: f64,
    work: f64,
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
        })
    }
}

#[cfg(test)]
pub(crate) fn shortfall(
    cohorts: &[WorkCohort],
    capacity_slots: f64,
    scratch: &mut EdfScratch,
) -> f64 {
    prepare(cohorts, scratch);
    shortfall_prepared(cohorts, capacity_slots, scratch)
}

pub(crate) fn shortfall_prepared_common_release_trajectories(
    simd_level: Level,
    cohorts: &[WorkCohort],
    trajectories: &SupplyTrajectories<'_>,
    initial_debt_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    start_seconds: f64,
    horizon_seconds: f64,
    results: &mut CandidateLoss<'_>,
    interval_supply: &mut [f64],
    scratch: &EdfScratch,
) {
    let candidate_count = trajectories.candidate_count();
    assert_eq!(candidate_count, results.service_balance.len());
    assert_eq!(candidate_count, results.shortfall.len());
    assert_eq!(candidate_count, results.delay_area.len());
    assert_eq!(candidate_count, interval_supply.len());
    results.service_balance.fill(-initial_debt_work);
    results.shortfall.fill(0.0_f64);
    let release = cohorts.first().map_or(start_seconds, |cohort| {
        Duration::from_micros(cohort.release_micros)
            .as_secs_f64()
            .max(start_seconds)
    });
    let released_work = cohorts
        .iter()
        .map(|cohort| cohort.work_slot_seconds)
        .sum::<f64>();
    for candidate in 0..candidate_count {
        let trajectory = trajectories.candidate(candidate);
        let mut queue = initial_debt_work;
        let mut area = trajectory_queue_area(
            &mut queue,
            start_seconds,
            release.min(horizon_seconds),
            &trajectory,
            future_arrivals,
        );
        if release < horizon_seconds {
            queue += released_work;
            area += trajectory_queue_area(
                &mut queue,
                release,
                horizon_seconds,
                &trajectory,
                future_arrivals,
            );
        }
        results.delay_area[candidate] = area;
    }
    let mut previous_seconds = start_seconds;
    if let Some(&first) = scratch.deadline_order.first() {
        let release_micros = cohorts[first as usize].release_micros;
        assert!(
            cohorts
                .iter()
                .all(|cohort| cohort.release_micros == release_micros),
            "all decision cohorts must have one release time"
        );
        let release_seconds = Duration::from_micros(release_micros).as_secs_f64();
        update_trajectory_candidates(
            simd_level,
            ServiceInterval {
                start: start_seconds,
                end: release_seconds,
                work: 0.0_f64,
            },
            trajectories,
            results.service_balance,
            results.shortfall,
            interval_supply,
        );
        previous_seconds = release_seconds;
    }
    let mut cursor = 0_usize;
    while cursor < scratch.deadline_order.len() {
        let cohort = cohorts[scratch.deadline_order[cursor] as usize];
        let deadline_micros = cohort.deadline_micros;
        let mut work_slot_seconds = 0.0_f64;
        while cursor < scratch.deadline_order.len() {
            let next = cohorts[scratch.deadline_order[cursor] as usize];
            if next.deadline_micros != deadline_micros {
                break;
            }
            work_slot_seconds += next.work_slot_seconds;
            cursor += 1;
        }
        let end_seconds = Duration::from_micros(deadline_micros).as_secs_f64();
        update_trajectory_candidates(
            simd_level,
            ServiceInterval {
                start: previous_seconds,
                end: end_seconds,
                work: work_slot_seconds,
            },
            trajectories,
            results.service_balance,
            results.shortfall,
            interval_supply,
        );
        previous_seconds = end_seconds;
    }
}

fn trajectory_queue_area(
    queue: &mut f64,
    start: f64,
    end: f64,
    trajectory: &SupplyTrajectory<'_>,
    arrivals: &ArrivalPath<'_>,
) -> f64 {
    if trajectory.pause_seconds.is_empty() {
        return arrival_queue_area(queue, start, end, trajectory.initial, arrivals);
    }
    if trajectory.pause_seconds.len() == 1 {
        return phased_queue_area(
            queue,
            start,
            end,
            trajectory.pause_seconds[0],
            trajectory.ready_seconds[0],
            trajectory.initial,
            trajectory.during[0],
            trajectory.after[0],
            arrivals,
        );
    }
    let mut area = 0.0_f64;
    let mut cursor = start;
    while cursor < end {
        let mut next = end;
        if let Some(boundary) = trajectory.next_boundary(cursor) {
            next = next.min(boundary);
        }
        if let Some(boundary) = arrivals.next_boundary(cursor) {
            next = next.min(boundary);
        }
        if next <= cursor {
            break;
        }
        area += queue_area_segment(
            queue,
            next - cursor,
            trajectory.capacity_at(cursor),
            arrivals.rate_at(cursor),
        );
        cursor = next;
    }
    area
}

fn arrival_queue_area(
    queue: &mut f64,
    start: f64,
    end: f64,
    capacity: f64,
    arrivals: &ArrivalPath<'_>,
) -> f64 {
    let mut area = 0.0_f64;
    let mut cursor = start;
    while cursor < end {
        let next = arrivals
            .next_boundary(cursor)
            .map_or(end, |boundary| boundary.min(end));
        if next <= cursor {
            break;
        }
        area += queue_area_segment(queue, next - cursor, capacity, arrivals.rate_at(cursor));
        cursor = next;
    }
    area
}

fn update_trajectory_candidates(
    simd_level: Level,
    interval: ServiceInterval,
    trajectories: &SupplyTrajectories<'_>,
    service_balance: &mut [f64],
    shortfall: &mut [f64],
    interval_supply: &mut [f64],
) {
    for (candidate, supply) in interval_supply.iter_mut().enumerate() {
        *supply = trajectories
            .candidate(candidate)
            .integrated_supply(interval.start, interval.end);
    }
    dispatch!(simd_level, simd => accumulate_trajectory_supply(
        simd,
        interval.work,
        interval_supply,
        service_balance,
        shortfall,
    ));
}

fn accumulate_trajectory_supply<S: Simd>(
    simd: S,
    work: f64,
    interval_supply: &[f64],
    service_balance: &mut [f64],
    shortfall: &mut [f64],
) {
    let lane_count = S::f64s::N;
    let vector_count = interval_supply.len() / lane_count;
    let zero = S::f64s::splat(simd, 0.0_f64);
    let work_vector = S::f64s::splat(simd, work);
    for vector in 0..vector_count {
        let first = vector * lane_count;
        let last = first + lane_count;
        let balance = S::f64s::from_slice(simd, &service_balance[first..last])
            + S::f64s::from_slice(simd, &interval_supply[first..last]);
        if work <= f64::EPSILON {
            balance.store_slice(&mut service_balance[first..last]);
            continue;
        }
        let remaining = work_vector - balance;
        let one = S::f64s::splat(simd, 1.0_f64);
        let fractional = remaining / work_vector;
        let bounded = fractional.simd_lt(one).select(fractional, one);
        let loss = bounded.simd_gt(zero).select(bounded, zero);
        let prior = S::f64s::from_slice(simd, &shortfall[first..last]);
        prior
            .simd_gt(loss)
            .select(prior, loss)
            .store_slice(&mut shortfall[first..last]);
        (balance - work_vector).store_slice(&mut service_balance[first..last]);
    }
    for candidate in vector_count * lane_count..interval_supply.len() {
        service_balance[candidate] += interval_supply[candidate];
        if work <= f64::EPSILON {
            continue;
        }
        if work > service_balance[candidate] {
            shortfall[candidate] =
                shortfall[candidate].max(((work - service_balance[candidate]) / work).min(1.0_f64));
        }
        service_balance[candidate] -= work;
    }
}

pub(crate) fn prepare(cohorts: &[WorkCohort], scratch: &mut EdfScratch) {
    assert!(
        cohorts.len() <= scratch.release_order.capacity(),
        "cohorts must fit the release-order scratch"
    );
    scratch.release_order.clear();
    for (index, _cohort) in cohorts.iter().enumerate() {
        scratch.release_order.push(index as u32);
    }
    scratch.release_order.sort_unstable_by_key(|&index| {
        let cohort = cohorts[index as usize];
        (cohort.release_micros, cohort.deadline_micros)
    });
    scratch.deadline_order.clear();
    scratch
        .deadline_order
        .extend_from_slice(&scratch.release_order);
    scratch.deadline_order.sort_unstable_by_key(|&index| {
        let cohort = cohorts[index as usize];
        (cohort.deadline_micros, cohort.release_micros)
    });
    scratch.prepared_len = cohorts.len();
}

/// Computes all candidate shortfalls through one deadline traversal.
pub(crate) fn shortfall_prepared_common_release_candidates(
    simd_level: Level,
    cohorts: &[WorkCohort],
    supply: &CandidateSupply<'_>,
    initial_debt_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    start_seconds: f64,
    horizon_seconds: f64,
    results: &mut CandidateLoss<'_>,
    scratch: &EdfScratch,
) {
    assert_eq!(
        supply.after.len(),
        supply.during.len(),
        "candidate supply columns must have equal lengths"
    );
    assert_eq!(
        supply.after.len(),
        supply.pause_seconds.len(),
        "candidate phase columns must have equal lengths"
    );
    assert_eq!(
        supply.after.len(),
        supply.ready_seconds.len(),
        "candidate phase columns must have equal lengths"
    );
    assert_eq!(
        supply.after.len(),
        results.service_balance.len(),
        "each candidate must have one service balance"
    );
    assert_eq!(
        supply.after.len(),
        results.shortfall.len(),
        "each candidate must have one shortfall"
    );
    assert_eq!(
        supply.after.len(),
        results.delay_area.len(),
        "each candidate must have one delay area"
    );
    results.service_balance.fill(-initial_debt_work);
    results.shortfall.fill(0.0_f64);
    fill_common_queue_area(
        cohorts,
        supply,
        initial_debt_work,
        future_arrivals,
        start_seconds,
        horizon_seconds,
        results.delay_area,
    );
    let mut previous_seconds = start_seconds;
    if let Some(&first) = scratch.deadline_order.first() {
        let release_micros = cohorts[first as usize].release_micros;
        assert!(
            cohorts
                .iter()
                .all(|cohort| cohort.release_micros == release_micros),
            "all decision cohorts must have one release time"
        );
        let release_seconds = Duration::from_micros(release_micros).as_secs_f64();
        let interval = ServiceInterval {
            start: start_seconds,
            end: release_seconds,
            work: 0.0_f64,
        };
        dispatch!(simd_level, simd => update_candidates(
            simd,
            interval,
            supply,
            results.service_balance,
            results.shortfall,
        ));
        previous_seconds = release_seconds;
    }
    let mut cursor = 0_usize;
    while cursor < scratch.deadline_order.len() {
        let cohort = cohorts[scratch.deadline_order[cursor] as usize];
        let deadline_micros = cohort.deadline_micros;
        let mut work_slot_seconds = 0.0_f64;
        while cursor < scratch.deadline_order.len() {
            let next = cohorts[scratch.deadline_order[cursor] as usize];
            if next.deadline_micros != deadline_micros {
                break;
            }
            work_slot_seconds += next.work_slot_seconds;
            cursor += 1;
        }
        let end_seconds = Duration::from_micros(deadline_micros).as_secs_f64();
        let interval = ServiceInterval {
            start: previous_seconds,
            end: end_seconds,
            work: work_slot_seconds,
        };
        dispatch!(simd_level, simd => update_candidates(
            simd,
            interval,
            supply,
            results.service_balance,
            results.shortfall,
        ));
        previous_seconds = end_seconds;
    }
}

fn fill_common_queue_area(
    cohorts: &[WorkCohort],
    supply: &CandidateSupply<'_>,
    initial_work: f64,
    arrivals: &ArrivalPath<'_>,
    start: f64,
    horizon: f64,
    delay_area: &mut [f64],
) {
    let release = cohorts.first().map_or(start, |cohort| {
        Duration::from_micros(cohort.release_micros)
            .as_secs_f64()
            .max(start)
    });
    let released_work = cohorts
        .iter()
        .map(|cohort| cohort.work_slot_seconds)
        .sum::<f64>();
    for candidate in 0..supply.after.len() {
        let mut queue = initial_work;
        let mut area = phased_queue_area(
            &mut queue,
            start,
            release.min(horizon),
            supply.pause_seconds[candidate],
            supply.ready_seconds[candidate],
            supply.before,
            supply.during[candidate],
            supply.after[candidate],
            arrivals,
        );
        if release < horizon {
            queue += released_work;
            area += phased_queue_area(
                &mut queue,
                release,
                horizon,
                supply.pause_seconds[candidate],
                supply.ready_seconds[candidate],
                supply.before,
                supply.during[candidate],
                supply.after[candidate],
                arrivals,
            );
        }
        delay_area[candidate] = area;
    }
}

fn phased_queue_area(
    queue: &mut f64,
    start: f64,
    end: f64,
    pause: f64,
    ready: f64,
    before_capacity: f64,
    during_capacity: f64,
    after_capacity: f64,
    arrivals: &ArrivalPath<'_>,
) -> f64 {
    let mut area = 0.0_f64;
    let mut cursor = start;
    while cursor < end {
        let mut next = end;
        if pause > cursor {
            next = next.min(pause);
        }
        if ready > cursor {
            next = next.min(ready);
        }
        if let Some(boundary) = arrivals.next_boundary(cursor) {
            next = next.min(boundary);
        }
        if next <= cursor {
            break;
        }
        let capacity = if cursor < pause {
            before_capacity
        } else if cursor < ready {
            during_capacity
        } else {
            after_capacity
        };
        area += queue_area_segment(queue, next - cursor, capacity, arrivals.rate_at(cursor));
        cursor = next;
    }
    area
}

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

fn update_candidates<S: Simd>(
    simd: S,
    interval: ServiceInterval,
    supply: &CandidateSupply<'_>,
    service_balance: &mut [f64],
    shortfall: &mut [f64],
) {
    let ServiceInterval {
        start: start_seconds,
        end: end_seconds,
        work: work_slot_seconds,
    } = interval;
    let CandidateSupply {
        before: capacity_before,
        during: capacity_during,
        after: capacity_after,
        pause_seconds,
        ready_seconds,
    } = *supply;
    let lane_count = S::f64s::N;
    let vector_count = capacity_after.len() / lane_count;
    let zero = S::f64s::splat(simd, 0.0_f64);
    let start = S::f64s::splat(simd, start_seconds);
    let end = S::f64s::splat(simd, end_seconds);
    let before_capacity = S::f64s::splat(simd, capacity_before);
    let work = S::f64s::splat(simd, work_slot_seconds);
    let records_shortfall = work_slot_seconds > f64::EPSILON;
    for vector in 0..vector_count {
        let first = vector * lane_count;
        let last = first + lane_count;
        let pause = S::f64s::from_slice(simd, &pause_seconds[first..last]);
        let ready = S::f64s::from_slice(simd, &ready_seconds[first..last]);
        let during_capacity = S::f64s::from_slice(simd, &capacity_during[first..last]);
        let after_capacity = S::f64s::from_slice(simd, &capacity_after[first..last]);
        let before_end = pause.simd_lt(end).select(pause, end);
        let before = before_end - start;
        let before = before.simd_gt(zero).select(before, zero);
        let during_start = pause.simd_gt(start).select(pause, start);
        let during_end = ready.simd_lt(end).select(ready, end);
        let during = during_end - during_start;
        let during = during.simd_gt(zero).select(during, zero);
        let after_start = ready.simd_gt(start).select(ready, start);
        let after = end - after_start;
        let after = after.simd_gt(zero).select(after, zero);
        let supply = before_capacity * before + during_capacity * during + after_capacity * after;
        let unchanged =
            during_capacity.simd_eq(before_capacity) & after_capacity.simd_eq(before_capacity);
        let unchanged_supply = before_capacity * (end - start);
        let supply = unchanged.select(unchanged_supply, supply);
        let balance = S::f64s::from_slice(simd, &service_balance[first..last]) + supply;
        if !records_shortfall {
            balance.store_slice(&mut service_balance[first..last]);
            continue;
        }
        let remaining = work - balance;
        let one = S::f64s::splat(simd, 1.0_f64);
        let fractional = remaining / work;
        let bounded = fractional.simd_lt(one).select(fractional, one);
        let loss = bounded.simd_gt(zero).select(bounded, zero);
        let prior_loss = S::f64s::from_slice(simd, &shortfall[first..last]);
        prior_loss
            .simd_gt(loss)
            .select(prior_loss, loss)
            .store_slice(&mut shortfall[first..last]);
        (balance - work).store_slice(&mut service_balance[first..last]);
    }
    for candidate in vector_count * lane_count..capacity_after.len() {
        let supply = phased_supply_seconds(
            start_seconds,
            end_seconds,
            pause_seconds[candidate],
            ready_seconds[candidate],
            capacity_before,
            capacity_during[candidate],
            capacity_after[candidate],
        );
        service_balance[candidate] += supply;
        if !records_shortfall {
            continue;
        }
        if work_slot_seconds > service_balance[candidate] {
            shortfall[candidate] = shortfall[candidate].max(
                ((work_slot_seconds - service_balance[candidate]) / work_slot_seconds).min(1.0_f64),
            );
        }
        service_balance[candidate] -= work_slot_seconds;
    }
}

fn phased_supply_seconds(
    start: f64,
    end: f64,
    pause: f64,
    ready: f64,
    before_capacity: f64,
    during_capacity: f64,
    after_capacity: f64,
) -> f64 {
    if before_capacity.to_bits() == during_capacity.to_bits()
        && before_capacity.to_bits() == after_capacity.to_bits()
    {
        return before_capacity * (end - start);
    }
    before_capacity * (end.min(pause) - start).max(0.0_f64)
        + during_capacity * (end.min(ready) - start.max(pause)).max(0.0_f64)
        + after_capacity * (end - start.max(ready)).max(0.0_f64)
}

pub(crate) fn evaluate_prepared_step(
    cohorts: &[WorkCohort],
    capacity_before: f64,
    capacity_during: f64,
    capacity_after: f64,
    pause_micros: u64,
    ready_micros: u64,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut shortfall = 0.0_f64;
    while now_micros < horizon_micros {
        shortfall_release(cohorts, scratch, &mut release_cursor, now_micros);
        let expired = expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
        late_work += expired;
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(shortfall_next_release(cohorts, scratch, release_cursor));
        if let Some(&cohort_index) = scratch.heap.first() {
            next_micros = next_micros.min(cohorts[cohort_index as usize].deadline_micros);
        }
        if pause_micros > now_micros {
            next_micros = next_micros.min(pause_micros);
        }
        if ready_micros > now_micros {
            next_micros = next_micros.min(ready_micros);
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = if now_micros < pause_micros {
            capacity_before
        } else if now_micros < ready_micros {
            capacity_during
        } else {
            capacity_after
        };
        let supply = capacity * duration_seconds;
        let late_supply = supply.min(late_work);
        late_work -= late_supply;
        shortfall_serve(cohorts, scratch, supply - late_supply);
        now_micros = next_micros;
    }
    shortfall_release(cohorts, scratch, &mut release_cursor, now_micros);
    let _new_debt = expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
    let delay_area = queue_area_prepared_step(
        cohorts,
        capacity_before,
        capacity_during,
        capacity_after,
        pause_micros,
        ready_micros,
        start_micros,
        horizon_micros,
        initial_debt_work,
        future_arrivals,
        scratch,
    );
    EdfOutcome {
        shortfall,
        delay_area,
    }
}

pub(crate) fn evaluate_prepared_trajectory(
    cohorts: &[WorkCohort],
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_debt_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    shortfall_reset(cohorts, scratch);
    let mut release_cursor = 0_usize;
    let mut now_micros = start_micros;
    let mut late_work = initial_debt_work;
    let mut shortfall = 0.0_f64;
    while now_micros < horizon_micros {
        shortfall_release(cohorts, scratch, &mut release_cursor, now_micros);
        late_work += expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        let mut next_micros = horizon_micros;
        next_micros = next_micros.min(shortfall_next_release(cohorts, scratch, release_cursor));
        if let Some(&cohort_index) = scratch.heap.first() {
            next_micros = next_micros.min(cohorts[cohort_index as usize].deadline_micros);
        }
        if let Some(boundary) = trajectory.next_boundary(now_seconds) {
            next_micros = next_micros.min((boundary * 1_000_000.0_f64) as u64);
        }
        if next_micros <= now_micros {
            break;
        }
        let duration_seconds = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let supply = trajectory.capacity_at(now_seconds) * duration_seconds;
        let late_supply = supply.min(late_work);
        late_work -= late_supply;
        shortfall_serve(cohorts, scratch, supply - late_supply);
        now_micros = next_micros;
    }
    shortfall_release(cohorts, scratch, &mut release_cursor, now_micros);
    let _new_debt = expire_to_debt(cohorts, scratch, now_micros, &mut shortfall);
    let delay_area = queue_area_prepared_trajectory(
        cohorts,
        trajectory,
        start_micros,
        horizon_micros,
        initial_debt_work,
        future_arrivals,
        scratch,
    );
    EdfOutcome {
        shortfall,
        delay_area,
    }
}

fn queue_area_prepared_trajectory(
    cohorts: &[WorkCohort],
    trajectory: &SupplyTrajectory<'_>,
    start_micros: u64,
    horizon_micros: u64,
    initial_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &EdfScratch,
) -> f64 {
    let mut queue = initial_work;
    let mut area = 0.0_f64;
    let mut release_cursor = 0_usize;
    while release_cursor < scratch.release_order.len() {
        let cohort = cohorts[scratch.release_order[release_cursor] as usize];
        if cohort.release_micros > start_micros {
            break;
        }
        queue += cohort.work_slot_seconds;
        release_cursor += 1;
    }
    let mut now_micros = start_micros;
    while now_micros < horizon_micros {
        let now_seconds = Duration::from_micros(now_micros).as_secs_f64();
        let mut next_micros = horizon_micros;
        if release_cursor < scratch.release_order.len() {
            next_micros = next_micros
                .min(cohorts[scratch.release_order[release_cursor] as usize].release_micros);
        }
        if let Some(boundary) = trajectory.next_boundary(now_seconds) {
            next_micros = next_micros.min((boundary * 1_000_000.0_f64) as u64);
        }
        if let Some(boundary) = future_arrivals.next_boundary(now_seconds) {
            next_micros = next_micros.min((boundary * 1_000_000.0_f64) as u64);
        }
        if next_micros <= now_micros {
            break;
        }
        let duration = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        area += queue_area_segment(
            &mut queue,
            duration,
            trajectory.capacity_at(now_seconds),
            future_arrivals.rate_at(now_seconds),
        );
        now_micros = next_micros;
        while release_cursor < scratch.release_order.len() {
            let cohort = cohorts[scratch.release_order[release_cursor] as usize];
            if cohort.release_micros > now_micros {
                break;
            }
            queue += cohort.work_slot_seconds;
            release_cursor += 1;
        }
    }
    area
}

fn queue_area_prepared_step(
    cohorts: &[WorkCohort],
    capacity_before: f64,
    capacity_during: f64,
    capacity_after: f64,
    pause_micros: u64,
    ready_micros: u64,
    start_micros: u64,
    horizon_micros: u64,
    initial_work: f64,
    future_arrivals: &ArrivalPath<'_>,
    scratch: &EdfScratch,
) -> f64 {
    let mut queue = initial_work;
    let mut area = 0.0_f64;
    let mut cursor = 0_usize;
    while cursor < scratch.release_order.len() {
        let cohort = cohorts[scratch.release_order[cursor] as usize];
        if cohort.release_micros > start_micros {
            break;
        }
        queue += cohort.work_slot_seconds;
        cursor += 1;
    }
    let mut now_micros = start_micros;
    while now_micros < horizon_micros {
        let mut next_micros = horizon_micros;
        if cursor < scratch.release_order.len() {
            next_micros =
                next_micros.min(cohorts[scratch.release_order[cursor] as usize].release_micros);
        }
        if pause_micros > now_micros {
            next_micros = next_micros.min(pause_micros);
        }
        if ready_micros > now_micros {
            next_micros = next_micros.min(ready_micros);
        }
        if let Some(boundary) =
            future_arrivals.next_boundary(Duration::from_micros(now_micros).as_secs_f64())
        {
            next_micros = next_micros.min((boundary * 1_000_000.0_f64) as u64);
        }
        if next_micros <= now_micros {
            break;
        }
        let duration = Duration::from_micros(next_micros - now_micros).as_secs_f64();
        let capacity = if now_micros < pause_micros {
            capacity_before
        } else if now_micros < ready_micros {
            capacity_during
        } else {
            capacity_after
        };
        area += queue_area_segment(
            &mut queue,
            duration,
            capacity,
            future_arrivals.rate_at(Duration::from_micros(now_micros).as_secs_f64()),
        );
        now_micros = next_micros;
        while cursor < scratch.release_order.len() {
            let cohort = cohorts[scratch.release_order[cursor] as usize];
            if cohort.release_micros > now_micros {
                break;
            }
            queue += cohort.work_slot_seconds;
            cursor += 1;
        }
    }
    area
}

pub(crate) fn has_common_release(cohorts: &[WorkCohort]) -> bool {
    cohorts.first().is_none_or(|first| {
        cohorts
            .iter()
            .all(|cohort| cohort.release_micros == first.release_micros)
    })
}

pub(crate) fn shortfall_prepared(
    cohorts: &[WorkCohort],
    capacity_slots: f64,
    scratch: &mut EdfScratch,
) -> f64 {
    if cohorts.is_empty() {
        return 0.0;
    }
    assert_eq!(
        cohorts.len(),
        scratch.prepared_len,
        "prepare the EDF order for these cohorts"
    );
    shortfall_reset(cohorts, scratch);

    let mut release_cursor = 0_usize;
    let mut now_micros = cohorts[scratch.release_order[0] as usize].release_micros;
    let mut shortfall_max = 0.0_f64;
    while release_cursor < cohorts.len() || !scratch.heap.is_empty() {
        shortfall_release(cohorts, scratch, &mut release_cursor, now_micros);
        if scratch.heap.is_empty() {
            now_micros = cohorts[scratch.release_order[release_cursor] as usize].release_micros;
            continue;
        }

        let deadline_micros = cohorts[scratch.heap[0] as usize].deadline_micros;
        let release_micros = shortfall_next_release(cohorts, scratch, release_cursor);
        let next_micros = deadline_micros.min(release_micros);
        let elapsed_micros = next_micros - now_micros;
        let elapsed_seconds = Duration::from_micros(elapsed_micros).as_secs_f64();
        shortfall_serve(cohorts, scratch, capacity_slots * elapsed_seconds);
        now_micros = next_micros;

        shortfall_max = shortfall_expire(cohorts, scratch, now_micros, shortfall_max);
    }
    shortfall_max
}

pub(crate) fn required_capacity_prepared(cohorts: &[WorkCohort], scratch: &mut EdfScratch) -> f64 {
    if cohorts.is_empty() {
        return 0.0_f64;
    }
    let mut high = cohorts
        .iter()
        .map(|cohort| {
            let seconds =
                Duration::from_micros(cohort.deadline_micros - cohort.release_micros).as_secs_f64();
            cohort.work_slot_seconds / seconds
        })
        .sum::<f64>();
    let mut low = 0.0_f64;
    for _iteration in 0_u32..48 {
        let middle = (low + high) * 0.5_f64;
        if shortfall_prepared(cohorts, middle, scratch) <= f64::EPSILON {
            high = middle;
        } else {
            low = middle;
        }
    }
    high
}

fn shortfall_reset(cohorts: &[WorkCohort], scratch: &mut EdfScratch) {
    assert!(
        cohorts.len() <= scratch.remaining.len(),
        "cohorts must fit the remaining-work scratch"
    );
    scratch.heap.clear();
    for (index, cohort) in cohorts.iter().enumerate() {
        scratch.remaining[index] = cohort.work_slot_seconds;
    }
}

fn shortfall_release(
    cohorts: &[WorkCohort],
    scratch: &mut EdfScratch,
    release_cursor: &mut usize,
    now_micros: u64,
) {
    while *release_cursor < cohorts.len() {
        let cohort_index = scratch.release_order[*release_cursor];
        if cohorts[cohort_index as usize].release_micros > now_micros {
            break;
        }
        heap_push(cohorts, &mut scratch.heap, cohort_index);
        *release_cursor += 1;
    }
}

fn shortfall_next_release(
    cohorts: &[WorkCohort],
    scratch: &EdfScratch,
    release_cursor: usize,
) -> u64 {
    if release_cursor < cohorts.len() {
        cohorts[scratch.release_order[release_cursor] as usize].release_micros
    } else {
        u64::MAX
    }
}

fn shortfall_serve(cohorts: &[WorkCohort], scratch: &mut EdfScratch, mut supply_slot_micros: f64) {
    while supply_slot_micros > 0.0_f64 && !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        let served = supply_slot_micros.min(scratch.remaining[cohort_index]);
        scratch.remaining[cohort_index] -= served;
        supply_slot_micros -= served;
        if scratch.remaining[cohort_index] <= f64::EPSILON {
            let _removed = heap_pop(cohorts, &mut scratch.heap);
        }
    }
}

fn shortfall_expire(
    cohorts: &[WorkCohort],
    scratch: &mut EdfScratch,
    now_micros: u64,
    mut shortfall_max: f64,
) -> f64 {
    while !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        if cohorts[cohort_index].deadline_micros > now_micros {
            break;
        }
        let work = cohorts[cohort_index].work_slot_seconds;
        if work > 0.0_f64 {
            shortfall_max = shortfall_max.max(scratch.remaining[cohort_index] / work);
        }
        let _removed = heap_pop(cohorts, &mut scratch.heap);
    }
    shortfall_max
}

fn expire_to_debt(
    cohorts: &[WorkCohort],
    scratch: &mut EdfScratch,
    now_micros: u64,
    shortfall: &mut f64,
) -> f64 {
    let mut debt = 0.0_f64;
    while !scratch.heap.is_empty() {
        let cohort_index = scratch.heap[0] as usize;
        if cohorts[cohort_index].deadline_micros > now_micros {
            break;
        }
        let work = cohorts[cohort_index].work_slot_seconds;
        let remaining = scratch.remaining[cohort_index];
        if work > 0.0_f64 {
            *shortfall = shortfall.max(remaining / work);
        }
        debt += remaining;
        let _removed = heap_pop(cohorts, &mut scratch.heap);
    }
    debt
}

fn heap_push(cohorts: &[WorkCohort], heap: &mut Vec<u32>, cohort_index: u32) {
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

fn heap_pop(cohorts: &[WorkCohort], heap: &mut Vec<u32>) -> u32 {
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

fn heap_sift_down(cohorts: &[WorkCohort], heap: &mut [u32]) {
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

fn deadline_key(cohorts: &[WorkCohort], cohort_index: u32) -> (u64, u64, u32) {
    let cohort = cohorts[cohort_index as usize];
    (cohort.deadline_micros, cohort.release_micros, cohort_index)
}
