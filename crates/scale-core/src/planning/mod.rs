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
    pub(crate) late_area_sums: &'a [f64],
    pub(crate) replica_seconds_sums: &'a [f64],
    pub(crate) rate: f64,
    /// Smallest action index whose supply covers known demand.
    ///
    /// The scenario evaluation grants every action the reactive repairs a
    /// successor controller makes. That successor is this controller, so an
    /// action the repair policy would override is not a fixed point of the
    /// policy. This applies to the current rate. It also applies to each known
    /// release after its required transition must start. Posterior arrivals and
    /// hypothetical calendar work cannot raise this index. Actions below this
    /// index are never feasible.
    pub(crate) demand_floor: usize,
}

impl ActionColumns<'_> {
    pub(crate) fn cost(&self, index: usize) -> f64 {
        self.late_area_sums[index] + self.rate * self.replica_seconds_sums[index]
    }
}

/// Selects one action from columnar posterior values.
///
/// Expected cost orders actions at or above [`ActionColumns::demand_floor`].
/// Target order resolves exact ties.
pub(crate) fn select_action(columns: &ActionColumns<'_>) -> usize {
    (columns.demand_floor..columns.late_area_sums.len())
        .min_by(|left, right| compare_actions(*left, *right, columns))
        .map_or(0, |index| index)
}

pub(crate) fn compare_actions(left: usize, right: usize, columns: &ActionColumns<'_>) -> Ordering {
    columns
        .cost(left)
        .total_cmp(&columns.cost(right))
        .then_with(|| left.cmp(&right))
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
