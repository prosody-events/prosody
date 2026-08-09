use std::cmp::Ordering;

/// Returns one common horizon for all actions in a posterior scenario.
///
/// The horizon includes every known deadline and the response span: one
/// candidate transition plus one reactive repair. One SLO budget values
/// the terminal state after the last boundary. Every action shares this
/// horizon, so the comparison is fair.
pub(crate) fn complete_horizon_micros(
    report_micros: u64,
    response_micros: u64,
    deadline_micros: u64,
    budget_micros: u64,
) -> u64 {
    report_micros
        .max(response_micros)
        .max(deadline_micros)
        .saturating_add(budget_micros)
}

/// Columnar posterior values with one cell for each ordered replica target.
pub(crate) struct ActionColumns<'a> {
    pub(crate) missed_work_sums: &'a [f64],
    pub(crate) excess_delay_sums: &'a [f64],
    pub(crate) replica_seconds_sums: &'a [f64],
    /// Smallest action index whose supply covers the known arrival rate.
    ///
    /// The scenario evaluation grants every action the reactive repairs a
    /// successor controller makes. That successor is this controller, so an
    /// action the repair policy would override at the already-known rate is
    /// not a fixed point of the policy: it defers work the controller must
    /// do now. Actions below this index are never feasible.
    pub(crate) demand_floor: usize,
    pub(crate) event_count_sum: f64,
    pub(crate) epsilon: f64,
}

impl ActionColumns<'_> {
    /// Returns the missed-work allowance that bounds the feasible set.
    ///
    /// An action is feasible when its posterior missed events exceed the
    /// best action's by no more than epsilon of the posterior events. The
    /// best action is always feasible, so common-cause loss that no action
    /// prevents never empties the feasible set.
    pub(crate) fn missed_allowance(&self) -> f64 {
        let minimum = self
            .missed_work_sums
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        minimum + self.epsilon * self.event_count_sum.max(f64::MIN_POSITIVE)
    }

    fn feasible(&self, index: usize, allowance: f64) -> bool {
        index >= self.demand_floor && self.missed_work_sums[index] <= allowance
    }
}

/// Selects one action from columnar posterior values.
///
/// Replica-seconds order feasible actions; see
/// [`ActionColumns::missed_allowance`] and [`ActionColumns::demand_floor`]
/// for the feasibility rules. Expected excess delay and then
/// replica-seconds order infeasible actions. Target order resolves ties.
pub(crate) fn select_action(columns: &ActionColumns<'_>) -> usize {
    let allowance = columns.missed_allowance();
    (0..columns.missed_work_sums.len())
        .min_by(|left, right| compare_actions(*left, *right, columns, allowance))
        .map_or(0, |index| index)
}

pub(crate) fn compare_actions(
    left: usize,
    right: usize,
    columns: &ActionColumns<'_>,
    allowance: f64,
) -> Ordering {
    let left_feasible = columns.feasible(left, allowance);
    let right_feasible = columns.feasible(right, allowance);
    match (left_feasible, right_feasible) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => columns.replica_seconds_sums[left]
            .total_cmp(&columns.replica_seconds_sums[right])
            .then_with(|| left.cmp(&right)),
        (false, false) => columns.excess_delay_sums[left]
            .total_cmp(&columns.excess_delay_sums[right])
            .then_with(|| {
                columns.replica_seconds_sums[left].total_cmp(&columns.replica_seconds_sums[right])
            })
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
    assert_eq!(
        targets.len(),
        membership_seconds.len(),
        "each target must pair with one membership time"
    );
    assert!(
        end_seconds >= start_seconds,
        "the integration interval must not be inverted"
    );
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
