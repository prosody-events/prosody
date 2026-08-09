use std::cmp::Ordering;

/// Returns one common horizon for all actions in a posterior scenario.
///
/// The horizon includes every known deadline and action completion. One SLO
/// budget values the terminal state after the last boundary.
pub(crate) fn complete_horizon_micros(
    report_micros: u64,
    ready_micros: u64,
    deadline_micros: u64,
    budget_micros: u64,
) -> u64 {
    report_micros
        .max(ready_micros)
        .max(deadline_micros)
        .saturating_add(budget_micros)
}

/// Selects one action from columnar posterior values.
///
/// The columns have one cell for each ordered replica target. An action is
/// feasible when its posterior event miss fraction does not exceed `epsilon`.
/// Replica-seconds order feasible actions. Expected excess delay and then
/// replica-seconds order infeasible actions. Target order resolves ties.
pub(crate) fn select_action(
    missed_work_sums: &[f64],
    excess_delay_sums: &[f64],
    replica_seconds_sums: &[f64],
    event_count_sum: f64,
    epsilon: f64,
) -> usize {
    (0..missed_work_sums.len())
        .min_by(|left, right| {
            compare_actions(
                *left,
                *right,
                missed_work_sums,
                excess_delay_sums,
                replica_seconds_sums,
                event_count_sum,
                epsilon,
            )
        })
        .map_or(0, |index| index)
}

pub(crate) fn compare_actions(
    left: usize,
    right: usize,
    missed_work_sums: &[f64],
    excess_delay_sums: &[f64],
    replica_seconds_sums: &[f64],
    event_count_sum: f64,
    epsilon: f64,
) -> Ordering {
    let denominator = event_count_sum.max(f64::MIN_POSITIVE);
    let left_feasible = missed_work_sums[left] / denominator <= epsilon;
    let right_feasible = missed_work_sums[right] / denominator <= epsilon;
    match (left_feasible, right_feasible) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => replica_seconds_sums[left]
            .total_cmp(&replica_seconds_sums[right])
            .then_with(|| left.cmp(&right)),
        (false, false) => excess_delay_sums[left]
            .total_cmp(&excess_delay_sums[right])
            .then_with(|| replica_seconds_sums[left].total_cmp(&replica_seconds_sums[right]))
            .then_with(|| left.cmp(&right)),
    }
}

/// Integrates physical replica count over one virtual-time interval.
///
/// The target and membership columns have equal lengths. Membership times are
/// monotonic. A target becomes a physical resource at its paired time.
pub(crate) fn replica_seconds(
    start_seconds: f64,
    end_seconds: f64,
    initial_replicas: u32,
    targets: &[u32],
    membership_seconds: &[f64],
) -> f64 {
    assert_eq!(targets.len(), membership_seconds.len());
    assert!(end_seconds >= start_seconds);
    let mut cursor = start_seconds;
    let mut replicas = initial_replicas;
    let mut area = 0.0_f64;
    for (&target, &membership) in targets.iter().zip(membership_seconds) {
        let boundary = membership.clamp(cursor, end_seconds);
        area += f64::from(replicas) * (boundary - cursor);
        cursor = boundary;
        if membership >= end_seconds {
            return area;
        }
        replicas = target;
    }
    area + f64::from(replicas) * (end_seconds - cursor).max(0.0_f64)
}

#[cfg(test)]
mod tests;
