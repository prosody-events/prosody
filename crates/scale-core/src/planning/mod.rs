use std::time::Duration;

/// Returns one common horizon for all actions in a posterior scenario.
///
/// The horizon includes every known deadline and the configured response span.
/// One SLO budget values the terminal state after the last boundary. Every
/// action shares this horizon, so the comparison is fair.
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
    pub(crate) miss_fraction_sums: &'a [f64],
    pub(crate) epsilon: f64,
    pub(crate) rate: f64,
}

impl ActionColumns<'_> {
    pub(crate) fn cost(&self, index: usize) -> f64 {
        self.late_area_sums[index] + self.rate * self.replica_seconds_sums[index]
    }

    /// Returns the posterior mean of the per-period miss fraction.
    ///
    /// A scenario's probability discounts its contribution. An empty period
    /// contributes zero. Each nonempty scenario uses its ratio of expected
    /// missed work to expected events.
    pub(crate) fn miss_fraction(&self, index: usize) -> f64 {
        self.miss_fraction_sums[index]
    }

    fn is_feasible(&self, index: usize) -> bool {
        self.miss_fraction(index) <= self.epsilon
    }
}

pub(crate) struct ActionSelection {
    pub(crate) index: usize,
    pub(crate) used_fallback: bool,
}

/// Selects the lowest-cost feasible action from posterior columns.
///
/// When no action meets epsilon, the lowest-cost action wins. The fallback
/// status remains separate from each scenario's rejection reasons.
#[cfg(test)]
pub(crate) fn select_action(columns: &ActionColumns<'_>) -> ActionSelection {
    select_action_by(columns, |index| columns.cost(index))
}

/// Selects from paired cost differences while preserving feasibility rules.
pub(crate) fn select_paired_action(
    columns: &ActionColumns<'_>,
    cost_differences: &[f64],
) -> ActionSelection {
    select_action_by(columns, |index| cost_differences[index])
}

fn select_action_by(columns: &ActionColumns<'_>, cost: impl Fn(usize) -> f64) -> ActionSelection {
    let feasible = (0..columns.late_area_sums.len())
        .filter(|index| columns.is_feasible(*index))
        .min_by(|left, right| {
            cost(*left)
                .total_cmp(&cost(*right))
                .then_with(|| left.cmp(right))
        })
        .map(|index| ActionSelection {
            index,
            used_fallback: false,
        });
    feasible.unwrap_or_else(|| ActionSelection {
        index: (0..columns.late_area_sums.len())
            .min_by(|left, right| {
                cost(*left)
                    .total_cmp(&cost(*right))
                    .then_with(|| left.cmp(right))
            })
            .unwrap_or(0),
        used_fallback: true,
    })
}

pub(crate) fn select_paired_runner_up(
    columns: &ActionColumns<'_>,
    cost_differences: &[f64],
    selected: usize,
    used_fallback: bool,
) -> Option<usize> {
    (0..columns.late_area_sums.len())
        .filter(|index| *index != selected && (used_fallback || columns.is_feasible(*index)))
        .min_by(|left, right| {
            cost_differences[*left]
                .total_cmp(&cost_differences[*right])
                .then_with(|| left.cmp(right))
        })
}

/// Integrates billed replica count over one virtual-time interval.
///
/// Joining replicas bill from their request time. Leaving replicas bill until
/// their transition is ready. Each target is the reached state. This rule
/// also applies when transition lifetimes overlap.
pub(crate) fn billing_replica_seconds(
    start_micros: u64,
    end_micros: u64,
    initial_replicas: u32,
    targets: &[u32],
    requested_micros: &[u64],
    ready_micros: &[u64],
) -> f64 {
    assert_eq!(
        targets.len(),
        requested_micros.len(),
        "each target must pair with one request time"
    );
    assert_eq!(
        targets.len(),
        ready_micros.len(),
        "each target must pair with one ready time"
    );
    assert!(
        end_micros >= start_micros,
        "the integration interval must not be inverted"
    );
    let interval = Duration::from_micros(end_micros - start_micros).as_secs_f64();
    let mut area = f64::from(initial_replicas) * interval;
    let mut origin = initial_replicas;
    for ((&target, &requested), &ready) in targets.iter().zip(requested_micros).zip(ready_micros) {
        let (boundary, delta, sign) = if target >= origin {
            (requested, target - origin, 1.0_f64)
        } else {
            (ready, origin - target, -1.0_f64)
        };
        let lifetime = end_micros.saturating_sub(boundary.max(start_micros));
        area += sign * f64::from(delta) * Duration::from_micros(lifetime).as_secs_f64();
        origin = target;
    }
    area
}

/// Returns the first report boundary at or after the specified time.
pub(crate) fn next_report_boundary_at_or_after(
    report_epoch_seconds: f64,
    report_interval_seconds: f64,
    at_seconds: f64,
) -> f64 {
    assert!(
        report_interval_seconds > 0.0_f64,
        "the report interval must be positive"
    );
    if at_seconds <= report_epoch_seconds {
        return report_epoch_seconds;
    }
    report_epoch_seconds
        + ((at_seconds - report_epoch_seconds) / report_interval_seconds).ceil()
            * report_interval_seconds
}

/// Prices the reached replica state after the common billing horizon.
///
/// The next report boundary caps the shared terminal budget. The smaller
/// action index still resolves an exact finite-cost tie.
pub(crate) fn terminal_replica_seconds(
    model_time_micros: u64,
    planning_horizon_micros: u64,
    billing_horizon_micros: u64,
    report_interval_micros: u64,
    replicas: u32,
) -> f64 {
    let planning_horizon_seconds = Duration::from_micros(planning_horizon_micros).as_secs_f64();
    let billing_horizon_seconds = Duration::from_micros(billing_horizon_micros).as_secs_f64();
    let report_seconds = Duration::from_micros(report_interval_micros).as_secs_f64();
    let report_epoch_seconds =
        Duration::from_micros(model_time_micros).as_secs_f64() + report_seconds;
    let mut boundary = next_report_boundary_at_or_after(
        report_epoch_seconds,
        report_seconds,
        billing_horizon_seconds.max(planning_horizon_seconds),
    );
    if boundary <= billing_horizon_seconds {
        boundary += report_seconds;
    }
    f64::from(replicas) * (boundary - billing_horizon_seconds).max(0.0_f64)
}

#[cfg(test)]
mod tests;
