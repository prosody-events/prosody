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
    pub(crate) missed_work_sums: &'a [f64],
    pub(crate) event_count: f64,
    pub(crate) epsilon: f64,
    pub(crate) rate: f64,
}

impl ActionColumns<'_> {
    pub(crate) fn cost(&self, index: usize) -> f64 {
        self.late_area_sums[index] + self.rate * self.replica_seconds_sums[index]
    }

    pub(crate) fn miss_fraction(&self, index: usize) -> f64 {
        if self.event_count == 0.0_f64 {
            0.0_f64
        } else {
            self.missed_work_sums[index] / self.event_count
        }
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
/// When no action meets epsilon, the smallest miss fraction wins. This
/// fallback keeps loss finite and marks every action as a deadline rejection.
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
                columns
                    .miss_fraction(*left)
                    .total_cmp(&columns.miss_fraction(*right))
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
            if used_fallback {
                columns
                    .miss_fraction(*left)
                    .total_cmp(&columns.miss_fraction(*right))
                    .then_with(|| left.cmp(right))
            } else {
                cost_differences[*left]
                    .total_cmp(&cost_differences[*right])
                    .then_with(|| left.cmp(right))
            }
        })
}

/// Integrates billed replica count over one virtual-time interval.
///
/// The target and pod-lifetime columns have equal lengths. Pod-lifetime times
/// are monotonic. A target changes the billed resource at its paired time.
pub(crate) fn billing_replica_seconds(
    start_micros: u64,
    end_micros: u64,
    initial_replicas: u32,
    targets: &[u32],
    pod_lifetime_micros: &[u64],
) -> f64 {
    assert_eq!(
        targets.len(),
        pod_lifetime_micros.len(),
        "each target must pair with one pod-lifetime time"
    );
    assert!(
        end_micros >= start_micros,
        "the integration interval must not be inverted"
    );
    let mut cursor = start_micros;
    let mut replicas = initial_replicas;
    let mut area = 0.0_f64;
    for (&target, &pod_lifetime) in targets.iter().zip(pod_lifetime_micros) {
        let boundary = pod_lifetime.clamp(cursor, end_micros);
        area += f64::from(replicas) * Duration::from_micros(boundary - cursor).as_secs_f64();
        cursor = boundary;
        if pod_lifetime >= end_micros {
            return area;
        }
        replicas = target;
    }
    area + f64::from(replicas) * Duration::from_micros(end_micros - cursor).as_secs_f64()
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

/// Prices terminal pod lifetime after the common billing horizon.
///
/// The planning horizon caps an infinite drain. The smaller action index still
/// resolves an exact finite-cost tie.
pub(crate) fn terminal_replica_seconds(
    model_time_micros: u64,
    planning_horizon_micros: u64,
    billing_horizon_micros: u64,
    drain_seconds: f64,
    report_interval_micros: u64,
    replicas: u32,
) -> f64 {
    if drain_seconds == 0.0_f64 {
        return drain_seconds;
    }
    let planning_horizon_seconds = Duration::from_micros(planning_horizon_micros).as_secs_f64();
    let billing_horizon_seconds = Duration::from_micros(billing_horizon_micros).as_secs_f64();
    let report_seconds = Duration::from_micros(report_interval_micros).as_secs_f64();
    let report_epoch_seconds =
        Duration::from_micros(model_time_micros).as_secs_f64() + report_seconds;
    let drain_at = planning_horizon_seconds + drain_seconds.min(planning_horizon_seconds);
    let boundary = next_report_boundary_at_or_after(report_epoch_seconds, report_seconds, drain_at);
    f64::from(replicas) * (boundary - billing_horizon_seconds).max(0.0_f64)
}

#[cfg(test)]
mod tests;
