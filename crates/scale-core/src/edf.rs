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
    pub(crate) service_credit: &'a mut [f64],
    pub(crate) shortfall: &'a mut [f64],
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
        results.service_credit.len(),
        "each candidate must have service credit"
    );
    assert_eq!(
        supply.after.len(),
        results.shortfall.len(),
        "each candidate must have one shortfall"
    );
    results.service_credit.fill(0.0_f64);
    results.shortfall.fill(0.0_f64);
    let Some(&first) = scratch.deadline_order.first() else {
        return;
    };
    let release_micros = cohorts[first as usize].release_micros;
    assert!(
        cohorts
            .iter()
            .all(|cohort| cohort.release_micros == release_micros),
        "all decision cohorts must have one release time"
    );

    let mut previous_micros = release_micros;
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
        let start_seconds = Duration::from_micros(previous_micros).as_secs_f64();
        let end_seconds = Duration::from_micros(deadline_micros).as_secs_f64();
        let interval = ServiceInterval {
            start: start_seconds,
            end: end_seconds,
            work: work_slot_seconds,
        };
        dispatch!(simd_level, simd => update_candidates(
            simd,
            interval,
            supply,
            results.service_credit,
            results.shortfall,
        ));
        previous_micros = deadline_micros;
    }
}

fn update_candidates<S: Simd>(
    simd: S,
    interval: ServiceInterval,
    supply: &CandidateSupply<'_>,
    service_credit: &mut [f64],
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
        let credit = S::f64s::from_slice(simd, &service_credit[first..last]) + supply;
        let remaining = work - credit;
        let loss = remaining.simd_gt(zero).select(remaining / work, zero);
        let prior_loss = S::f64s::from_slice(simd, &shortfall[first..last]);
        prior_loss
            .simd_gt(loss)
            .select(prior_loss, loss)
            .store_slice(&mut shortfall[first..last]);
        let remaining_credit = credit - work;
        remaining_credit
            .simd_gt(zero)
            .select(remaining_credit, zero)
            .store_slice(&mut service_credit[first..last]);
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
        service_credit[candidate] += supply;
        if work_slot_seconds > service_credit[candidate] {
            shortfall[candidate] = shortfall[candidate]
                .max((work_slot_seconds - service_credit[candidate]) / work_slot_seconds);
            service_credit[candidate] = 0.0_f64;
        } else {
            service_credit[candidate] -= work_slot_seconds;
        }
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

pub(crate) fn shortfall_prepared_step(
    cohorts: &[WorkCohort],
    capacity_before: f64,
    capacity_during: f64,
    capacity_after: f64,
    pause_micros: u64,
    ready_micros: u64,
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
        let supply = phased_supply(
            now_micros,
            next_micros,
            pause_micros,
            ready_micros,
            capacity_before,
            capacity_during,
            capacity_after,
        );
        shortfall_serve(cohorts, scratch, supply);
        now_micros = next_micros;
        shortfall_max = shortfall_expire(cohorts, scratch, now_micros, shortfall_max);
    }
    shortfall_max
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

fn phased_supply(
    start_micros: u64,
    end_micros: u64,
    pause_micros: u64,
    ready_micros: u64,
    capacity_before: f64,
    capacity_during: f64,
    capacity_after: f64,
) -> f64 {
    if capacity_before.to_bits() == capacity_during.to_bits()
        && capacity_before.to_bits() == capacity_after.to_bits()
    {
        return capacity_before
            * Duration::from_micros(end_micros.saturating_sub(start_micros)).as_secs_f64();
    }
    let before_end = end_micros.min(pause_micros);
    let before_micros = before_end.saturating_sub(start_micros);
    let during_start = start_micros.max(pause_micros);
    let during_end = end_micros.min(ready_micros);
    let during_micros = during_end.saturating_sub(during_start);
    let after_start = start_micros.max(ready_micros);
    let after_micros = end_micros.saturating_sub(after_start);
    capacity_before * Duration::from_micros(before_micros).as_secs_f64()
        + capacity_during * Duration::from_micros(during_micros).as_secs_f64()
        + capacity_after * Duration::from_micros(after_micros).as_secs_f64()
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
